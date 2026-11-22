"""Page sets: re-running a page-local step for some pages of a domain.

A refresh re-computes the pages its new revisions touched (docs/design/
proposed_incremental_refresh.md §4). The set is read from any Parquet file, or
directory of files, with a ``page_id`` column; RevisionChest metadata qualifies,
so the refresh's new metadata file is its own page list.

A page-partitioned dataset's bucket is then rewritten as its other pages' rows
plus the new rows of the set's pages (``replace_pages``). Each rewrite is journaled
(``.replace.json`` in the bucket), so an interrupted one is completed or undone the
next time the bucket is replaced, and running the same page set again gives the
same rows.
"""
from __future__ import annotations

import json
import os
import uuid
from collections import defaultdict
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from wcd_schema import parquet as layout


def read_page_set(path: str | os.PathLike) -> list[int]:
    """The distinct page ids in ``path`` (a Parquet file or a directory of them), ascending."""
    p = Path(path)
    src = str(p / "*.parquet") if p.is_dir() else str(p)
    con = duckdb.connect()
    try:
        rows = con.execute(f"SELECT DISTINCT CAST(page_id AS BIGINT) FROM read_parquet('{src}', union_by_name = true) "
                           "WHERE page_id IS NOT NULL ORDER BY 1").fetchall()
    finally:
        con.close()
    return [r[0] for r in rows]


def by_bucket(pages: list[int]) -> dict[int, list[int]]:
    out: dict[int, list[int]] = defaultdict(list)
    for p in pages:
        out[p % layout.PAGE_BUCKETS].append(p)
    return dict(out)


JOURNAL = ".replace.json"


def _recover(d: Path) -> None:
    """Finish or undo a rewrite of bucket ``d`` that stopped part way."""
    journal = d / JOURNAL
    if not journal.exists():
        return
    j = json.loads(journal.read_text())
    if (d / j["new"]).exists():          # moved in: the old files go
        for f in j["old"]:
            (d / f).unlink(missing_ok=True)
    else:                                # never moved in: the old files stay
        (d / j["tmp"]).unlink(missing_ok=True)
    journal.unlink()


def replace_pages(parquet_base: str | os.PathLike, dataset: str, domain: str, bucket: int,
                  pages: list[int], new: pa.Table) -> int:
    """Rewrite one bucket of ``dataset`` as its rows for pages not in ``pages`` plus ``new``
    (the re-computed rows of ``pages``, possibly none); returns the rows written.

    The bucket becomes one file: written under a temporary name, then moved in and the
    old files removed, with a journal so that an interruption is repaired next time."""
    schema = layout.SCHEMAS[dataset]
    d = layout.partition_dir(parquet_base, dataset, domain, bucket)
    d.mkdir(parents=True, exist_ok=True)
    _recover(d)
    old_files = sorted(d.glob(layout.FILE_GLOB))
    parts = []
    if old_files:
        kept = ds.dataset([str(f) for f in old_files], schema=schema, format="parquet").to_table(
            filter=~pc.field("page_id").isin(pa.array(pages, pa.int64()).cast(schema.field("page_id").type)))
        parts.append(kept)
    if new.num_rows:
        parts.append(new.select(schema.names).cast(schema))
    table = pa.concat_tables(parts) if parts else schema.empty_table()
    token = uuid.uuid4().hex[:12]
    tmp, target = f".pages-{token}.tmp", f"pages-{token}.parquet"
    pq.write_table(table, d / tmp, compression="zstd")
    (d / JOURNAL).write_text(json.dumps({"old": [f.name for f in old_files], "new": target, "tmp": tmp}))
    os.replace(d / tmp, d / target)
    for f in old_files:
        f.unlink()
    (d / JOURNAL).unlink()
    return table.num_rows
