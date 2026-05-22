#!/usr/bin/env python3
"""
Phase 1.5: Consolidate & deduplicate staged JSONL.zst files across all shards.

Reads per-shard staged files from STAGING_DIR, deduplicates using a temporary
DuckDB database (disk-backed, optimised for bulk analytical operations), and
writes consolidated deduplicated .jsonl.zst files to STAGING_DIR/deduped/.

Usage:
    python dedup_staged.py [-d STAGING_DIR] [--shard-size N] [--batch-size N]

Pipeline:
    build_all.py  →  dedup_staged.py  →  load_all.py
"""

import argparse
import io
import json
import glob
import os
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

from dotenv import load_dotenv
load_dotenv()

import datetime

import duckdb
import zstandard as zstd


def _json_default(obj):
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

STAGING_DIR = os.getenv('STAGING_DIR', './staging')
DEDUPED_DIR_NAME = 'deduped'
MAX_ROWS_PER_SHARD = int(os.getenv('DEDUP_SHARD_SIZE', '2_000_000'))
DEDUP_BATCH_SIZE = int(os.getenv('DEDUP_BATCH_SIZE', '50_000'))

# Dedup key columns per table — used to build the DuckDB UNIQUE constraint.
# Order matters: these must match the fields present in the staged JSONL rows.
TABLE_KEYS = {
    'containers':             ['label'],
    'domains':                ['value'],
    'documents':              ['has_container_label', 'page_id'],
    'web_resources':          ['url'],
    'wiki_templates':         ['domain_label', 'name'],
    'normalized_citations':   ['record_sha1'],
    'citations':              ['record_sha1', 'reference_raw_sha1'],
    'revisions':              ['revision_id'],
    'citation_histories':     ['record_sha1', 'revision_id'],
    'ncwr':                   ['reference_normalized_sha1', 'url'],
    'template_data':          ['domain_label', 'template_name',
                               'reference_normalized_sha1', 'offset_start',
                               'parameter_key'],
}


def log(msg):
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"{ts} [dedup] {msg}", flush=True)


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def read_jsonl_zst(filepath):
    """Yield dicts from a .jsonl.zst file."""
    dctx = zstd.ZstdDecompressor()
    with open(filepath, 'rb') as fh:
        with dctx.stream_reader(fh) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')
            for line in text_stream:
                line = line.strip()
                if line:
                    yield json.loads(line)


def read_jsonl_zst_duckdb(filepath, conn):
    """Read a .jsonl.zst file using DuckDB's native JSONL reader.

    Returns a list of dicts.  DuckDB handles decompression and parsing
    in its vectorised engine, avoiding Python-side overhead.
    """
    result = conn.execute(
        "SELECT * FROM read_json_auto(?)", [filepath]
    ).fetchall()
    columns = [desc[0] for desc in conn.description]
    return [dict(zip(columns, row)) for row in result]


def find_all_files(staging_dir, table_name, deduped_dir):
    """Find all staged .jsonl.zst files for *table_name*, excluding deduped/."""
    pattern = os.path.join(staging_dir, '**', f'*-{table_name}.jsonl.zst')
    return sorted(
        p for p in glob.glob(pattern, recursive=True)
        if not p.startswith(deduped_dir)
    )


# ---------------------------------------------------------------------------
# Sharded compressed writer
# ---------------------------------------------------------------------------

class ShardedWriter:
    """Writes rows to sequentially numbered .jsonl.zst files."""

    def __init__(self, deduped_dir, table_name, max_rows=MAX_ROWS_PER_SHARD):
        self.deduped_dir = deduped_dir
        self.table_name = table_name
        self.max_rows = max_rows
        self.shard_idx = 0
        self.rows_in_shard = 0
        self.total_rows = 0
        self._fh = None
        self._writer = None
        os.makedirs(deduped_dir, exist_ok=True)
        self._open_new_shard()

    def _shard_path(self):
        return os.path.join(
            self.deduped_dir,
            f"{self.table_name}-{self.shard_idx:03d}.jsonl.zst",
        )

    def _open_new_shard(self):
        self._close()
        self._fh = open(self._shard_path(), 'wb')
        cctx = zstd.ZstdCompressor(level=3)
        self._writer = cctx.stream_writer(self._fh)
        self.rows_in_shard = 0

    def _close(self):
        if self._writer:
            self._writer.close()
        if self._fh:
            self._fh.close()

    def write_row(self, row: dict):
        line = json.dumps(row, ensure_ascii=False, default=_json_default) + '\n'
        self._writer.write(line.encode('utf-8'))
        self.rows_in_shard += 1
        self.total_rows += 1
        if self.rows_in_shard >= self.max_rows:
            self.shard_idx += 1
            self._open_new_shard()

    def write_batch(self, rows):
        if not rows:
            return
        # Buffer the entire batch into a single bytes blob to reduce
        # per-row write() syscall overhead.
        buf = []
        for row in rows:
            buf.append(json.dumps(row, ensure_ascii=False, default=_json_default))
            self.rows_in_shard += 1
            self.total_rows += 1
            if self.rows_in_shard >= self.max_rows:
                # Flush accumulated lines to current shard, then rotate
                if buf:
                    self._writer.write(('\n'.join(buf) + '\n').encode('utf-8'))
                    buf = []
                self.shard_idx += 1
                self._open_new_shard()
        if buf:
            self._writer.write(('\n'.join(buf) + '\n').encode('utf-8'))

    def finish(self):
        self._close()
        return self.total_rows, self.shard_idx + 1


# ---------------------------------------------------------------------------
# DuckDB-backed deduplication
# ---------------------------------------------------------------------------

class DuckDBDedup:
    """Disk-backed deduplication using a temporary DuckDB database.

    Uses a bulk anti-join pattern optimised for DuckDB's columnar engine:
    each batch is loaded into a staging table, new keys are identified via
    NOT EXISTS, and then inserted into the ``seen`` table.
    """

    def __init__(self, table_name, key_columns):
        self.key_columns = key_columns
        self.num_cols = len(key_columns)
        self.unique = 0
        self.dupes = 0

        # Create temp file for the DuckDB database
        fd, self.db_path = tempfile.mkstemp(
            suffix='.duckdb', prefix=f'dedup_{table_name}_'
        )
        os.close(fd)
        os.unlink(self.db_path)  # let DuckDB create its own file

        self.conn = duckdb.connect(self.db_path)

        cols_def = ', '.join(f'c{i} VARCHAR' for i in range(self.num_cols))
        self._cols_list = ', '.join(f'c{i}' for i in range(self.num_cols))
        self._join_cond = ' AND '.join(
            f'b.c{i} = s.c{i}' for i in range(self.num_cols)
        )

        self.conn.execute(
            f'CREATE TABLE seen ({cols_def}, UNIQUE({self._cols_list}))'
        )
        self.conn.execute(
            f'CREATE TABLE batch_staging ({cols_def})'
        )

    def filter_batch(self, rows):
        """Accept a batch of rows; return only the ones not yet seen."""
        if not rows:
            return []

        # 1. Bulk-load the batch into the staging table
        batch_data = [
            tuple(str(row.get(k, '')) for k in self.key_columns)
            for row in rows
        ]
        self.conn.executemany(
            f'INSERT INTO batch_staging VALUES '
            f'({", ".join("?" for _ in range(self.num_cols))})',
            batch_data,
        )

        # 2. Identify new *distinct* keys via anti-join.
        #    DISTINCT ensures within-batch duplicates are collapsed so that
        #    only one copy of each new key is kept (matching the old SQLite
        #    INSERT OR IGNORE behaviour where the first occurrence wins).
        new_keys_result = self.conn.execute(f"""
            SELECT DISTINCT {self._cols_list} FROM batch_staging b
            WHERE NOT EXISTS (
                SELECT 1 FROM seen s WHERE {self._join_cond}
            )
        """).fetchall()

        new_key_set = set(new_keys_result)

        # 3. Insert the new keys into seen
        if new_key_set:
            self.conn.execute(f"""
                INSERT INTO seen
                SELECT DISTINCT {self._cols_list} FROM batch_staging b
                WHERE NOT EXISTS (
                    SELECT 1 FROM seen s WHERE {self._join_cond}
                )
            """)

        # 4. Truncate staging for next batch
        self.conn.execute('DROP TABLE batch_staging')
        cols_def = ', '.join(f'c{i} VARCHAR' for i in range(self.num_cols))
        self.conn.execute(f'CREATE TABLE batch_staging ({cols_def})')

        # 5. Filter original rows — keep only the first occurrence per new key
        new_rows = []
        seen_in_batch = set()
        for row in rows:
            key = tuple(str(row.get(k, '')) for k in self.key_columns)
            if key in new_key_set and key not in seen_in_batch:
                new_rows.append(row)
                seen_in_batch.add(key)

        self.unique += len(new_rows)
        self.dupes += len(rows) - len(new_rows)
        return new_rows

    def close(self):
        self.conn.close()
        for suffix in ['', '.wal']:
            try:
                os.unlink(self.db_path + suffix)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# Per-table dedup driver
# ---------------------------------------------------------------------------

def dedup_table(staging_dir, deduped_dir, table_name, key_columns,
                shard_size, batch_size):
    files = find_all_files(staging_dir, table_name, deduped_dir)
    if not files:
        log(f"  {table_name}: no source files found, skipping")
        return

    log(f"  {table_name}: reading {len(files)} file(s) …")
    dedup = DuckDBDedup(table_name, key_columns)
    writer = ShardedWriter(deduped_dir, table_name, max_rows=shard_size)

    batch = []
    file_count = 0
    for fp in files:
        file_count += 1
        # Skip empty files (e.g. 0-byte or header-only compressed files)
        if os.path.getsize(fp) == 0:
            log(f"    {table_name}: skipping empty file {fp}")
            continue
        # Use DuckDB's native JSONL reader for vectorised decompression
        # and parsing; fall back to Python reader on failure.
        try:
            file_rows = read_jsonl_zst_duckdb(fp, dedup.conn)
        except Exception:
            try:
                file_rows = list(read_jsonl_zst(fp))
            except Exception as exc:
                log(f"    {table_name}: skipping unreadable file {fp} — {exc}")
                continue
        batch.extend(file_rows)
        while len(batch) >= batch_size:
            new_rows = dedup.filter_batch(batch[:batch_size])
            writer.write_batch(new_rows)
            batch = batch[batch_size:]
        # Log progress every 50 files
        if file_count % 50 == 0:
            if batch:
                new_rows = dedup.filter_batch(batch)
                writer.write_batch(new_rows)
                batch = []
            log(f"    {table_name}: processed {file_count}/{len(files)} files, "
                f"{dedup.unique} unique so far …")

    # Flush remaining batch
    if batch:
        new_rows = dedup.filter_batch(batch)
        writer.write_batch(new_rows)

    total_rows, num_shards = writer.finish()
    log(f"  {table_name}: {total_rows} unique rows, {dedup.dupes} duplicates "
        f"removed → {num_shards} shard(s)")
    dedup.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Consolidate & deduplicate staged JSONL.zst files across shards'
    )
    parser.add_argument(
        '-d', '--staging-dir',
        default=os.environ.get('STAGING_DIR', './staging'),
        help='Staging directory (default: STAGING_DIR env or ./staging)',
    )
    parser.add_argument(
        '--shard-size', type=int, default=MAX_ROWS_PER_SHARD,
        help=f'Max rows per output shard file (default: {MAX_ROWS_PER_SHARD})',
    )
    parser.add_argument(
        '--batch-size', type=int, default=DEDUP_BATCH_SIZE,
        help=f'Rows per DuckDB dedup batch (default: {DEDUP_BATCH_SIZE})',
    )
    parser.add_argument(
        '--tables', nargs='*', default=None,
        help='Only dedup these tables (default: all)',
    )
    args = parser.parse_args()

    staging_dir = os.path.abspath(args.staging_dir)
    if not os.path.isdir(staging_dir):
        raise SystemExit(f"Staging directory does not exist: {staging_dir}")

    deduped_dir = os.path.join(staging_dir, DEDUPED_DIR_NAME)

    tables = args.tables or list(TABLE_KEYS.keys())
    invalid = [t for t in tables if t not in TABLE_KEYS]
    if invalid:
        raise SystemExit(f"Unknown table(s): {', '.join(invalid)}")

    log(f"Deduplicating staged files in {staging_dir} → {deduped_dir}")
    t0 = time.time()

    max_workers = min(len(tables), os.cpu_count() or 1)
    log(f"Processing {len(tables)} table(s) with {max_workers} worker(s)")

    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                dedup_table, staging_dir, deduped_dir, table_name,
                TABLE_KEYS[table_name],
                shard_size=args.shard_size, batch_size=args.batch_size,
            ): table_name
            for table_name in tables
        }
        for future in as_completed(futures):
            table_name = futures[future]
            try:
                future.result()
            except Exception as exc:
                log(f"  {table_name}: FAILED — {exc}")
                raise

    elapsed = time.time() - t0
    log(f"Done. Total elapsed: {elapsed:.1f}s")


if __name__ == '__main__':
    main()
