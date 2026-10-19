"""Writing the per-revision datasets into the Parquet tier from one shard.

A shard (one ``.mwrev.zst`` bundle) is the Stage-1 unit of work and re-run.
Its rows for a page-partitioned dataset are collected in flat files while the
bundle streams, then re-partitioned once with DuckDB into
``<base>/<dataset>/domain=<d>/page_bucket=<n>/<stem>-<i>.parquet`` —
so a shard owns exactly the files named after it in every bucket, a re-run
replaces them, and nothing holds 512 open writers. Domain-partitioned
datasets (``revision_signals``) are streamed straight to
``<base>/<dataset>/domain=<d>/<stem>.parquet``.
"""
from __future__ import annotations

import glob
import os
from typing import Iterable

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from wcd_schema import parquet as layout
from wcd_schema.parquet import SCHEMAS


class StreamWriter:
    """Buffer dict rows and write them as row groups of one Parquet schema to
    numbered files (``path_for(index)``), rotating at ``rows_per_file``."""

    def __init__(self, path_for, schema: pa.Schema, rows_per_file: int = 2_000_000, row_group: int = 100_000):
        self._path_for = path_for
        self.schema = schema
        self._names = [f.name for f in schema]
        self.rows_per_file = rows_per_file
        self.row_group = row_group
        self._buffer: list[dict] = []
        self._writer = None
        self._index = 0
        self._rows_in_file = 0
        self.rows = 0
        self.paths: list[str] = []

    def write(self, rows: Iterable[dict]) -> None:
        for r in rows:
            self._buffer.append(r)
            if len(self._buffer) >= self.row_group:
                self._flush()

    def _flush(self) -> None:
        if not self._buffer:
            return
        if self._writer is None:
            path = self._path_for(self._index)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            self._writer = pq.ParquetWriter(path, self.schema, compression="zstd")
            self.paths.append(path)
        self._writer.write_table(pa.Table.from_pylist([{n: r.get(n) for n in self._names} for r in self._buffer],
                                                      schema=self.schema))
        self.rows += len(self._buffer)
        self._rows_in_file += len(self._buffer)
        self._buffer.clear()
        if self._rows_in_file >= self.rows_per_file:
            self._writer.close()
            self._writer = None
            self._index += 1
            self._rows_in_file = 0

    def close(self) -> None:
        self._flush()
        if self._writer is not None:
            self._writer.close()
            self._writer = None


class FlatWriter(StreamWriter):
    """Shard-local flat files of a page-partitioned dataset, to be re-partitioned."""

    def __init__(self, directory: str, dataset: str, stem: str, **kw):
        self.directory, self.dataset, self.stem = directory, dataset, stem
        super().__init__(lambda i: os.path.join(directory, f"{stem}-{dataset}-{i:04d}.flat.parquet"),
                         SCHEMAS[dataset], **kw)

    @property
    def glob(self) -> str:
        return os.path.join(glob.escape(self.directory), f"{glob.escape(self.stem)}-{self.dataset}-*.flat.parquet")

    def remove(self) -> None:
        for p in glob.glob(self.glob):
            os.remove(p)


class DomainFileWriter(StreamWriter):
    """Streams a domain-partitioned dataset straight into the tier
    (``<stem>.parquet``, ``<stem>-1.parquet``, … when very large)."""

    def __init__(self, parquet_base: str, dataset: str, domain: str, stem: str, **kw):
        assert dataset in layout.DOMAIN_PARTITIONED, dataset
        d = str(layout.partition_dir(parquet_base, dataset, domain))
        super().__init__(lambda i: os.path.join(d, f"{stem}.parquet" if i == 0 else f"{stem}-{i}.parquet"),
                         SCHEMAS[dataset], **kw)


def shard_files(parquet_base: str, dataset: str, domain: str, stem: str) -> list[str]:
    """Every file a shard wrote into a dataset (any bucket)."""
    base = glob.escape(str(layout.partition_dir(parquet_base, dataset, domain)))
    s = glob.escape(stem)
    if dataset in layout.PAGE_PARTITIONED:
        patterns = [os.path.join(base, "page_bucket=*", f"{s}-[0-9]*.parquet")]
    else:
        patterns = [os.path.join(base, f"{s}.parquet"), os.path.join(base, f"{s}-[0-9]*.parquet")]
    out = []
    for p in patterns:
        out.extend(glob.glob(p))
    return sorted(out)


def remove_shard_files(parquet_base: str, dataset: str, domain: str, stem: str) -> int:
    files = shard_files(parquet_base, dataset, domain, stem)
    for p in files:
        os.remove(p)
    return len(files)


def repartition(con: duckdb.DuckDBPyConnection, flat_glob: str, parquet_base: str, dataset: str,
                domain: str, stem: str, order_by: str = "page_id, revision_id") -> int:
    """Re-partition a shard's flat files of a page-partitioned dataset into the
    tier. Files are named ``<stem>-<i>.parquet`` inside each bucket directory;
    existing files of that name are overwritten (the caller removes stale ones
    first with ``remove_shard_files``). Returns the row count."""
    assert dataset in layout.PAGE_PARTITIONED, dataset
    if not glob.glob(flat_glob):
        return 0
    cols = ", ".join(f.name for f in SCHEMAS[dataset])
    target = str(layout.dataset_dir(parquet_base, dataset))
    os.makedirs(target, exist_ok=True)
    con.execute(
        f"""COPY (
                SELECT {cols}, '{domain}' AS domain, page_id % {layout.PAGE_BUCKETS} AS page_bucket
                FROM read_parquet('{flat_glob}')
                ORDER BY page_id % {layout.PAGE_BUCKETS}, {order_by}
            ) TO '{target}'
            (FORMAT PARQUET, PARTITION_BY (domain, page_bucket), OVERWRITE_OR_IGNORE true,
             FILENAME_PATTERN '{stem}-{{i}}', COMPRESSION ZSTD)"""
    )
    return con.execute(f"SELECT count(*) FROM read_parquet('{flat_glob}')").fetchone()[0]
