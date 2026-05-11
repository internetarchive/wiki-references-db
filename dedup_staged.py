#!/usr/bin/env python3
"""
Phase 1.5: Consolidate & deduplicate staged JSONL.zst files across all shards.

Reads per-shard staged files from STAGING_DIR, deduplicates using a temporary
SQLite database (disk-backed, bounded memory), and writes consolidated
deduplicated .jsonl.zst files to STAGING_DIR/deduped/.

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
import sqlite3
import tempfile
import time

from dotenv import load_dotenv
load_dotenv()

import zstandard as zstd


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

STAGING_DIR = os.getenv('STAGING_DIR', './staging')
DEDUPED_DIR_NAME = 'deduped'
MAX_ROWS_PER_SHARD = int(os.getenv('DEDUP_SHARD_SIZE', '2_000_000'))
DEDUP_BATCH_SIZE = int(os.getenv('DEDUP_BATCH_SIZE', '10_000'))

# Dedup key columns per table — used to build the SQLite UNIQUE constraint.
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
        line = json.dumps(row, ensure_ascii=False) + '\n'
        self._writer.write(line.encode('utf-8'))
        self.rows_in_shard += 1
        self.total_rows += 1
        if self.rows_in_shard >= self.max_rows:
            self.shard_idx += 1
            self._open_new_shard()

    def write_batch(self, rows):
        for row in rows:
            self.write_row(row)

    def finish(self):
        self._close()
        return self.total_rows, self.shard_idx + 1


# ---------------------------------------------------------------------------
# SQLite-backed deduplication
# ---------------------------------------------------------------------------

class SQLiteDedup:
    """Disk-backed deduplication using a temporary SQLite database.

    Uses INSERT OR IGNORE into a table with a UNIQUE constraint on the key
    columns.  Rows are checked in batches for throughput.
    """

    def __init__(self, table_name, key_columns):
        self.key_columns = key_columns
        self.unique = 0
        self.dupes = 0

        # Create temp file for the SQLite DB
        fd, self.db_path = tempfile.mkstemp(
            suffix='.db', prefix=f'dedup_{table_name}_'
        )
        os.close(fd)

        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA synchronous=OFF')
        self.conn.execute('PRAGMA temp_store=MEMORY')
        self.conn.execute('PRAGMA cache_size=-64000')  # 64 MB page cache

        cols_def = ', '.join(f'c{i} TEXT' for i in range(len(key_columns)))
        cols_list = ', '.join(f'c{i}' for i in range(len(key_columns)))
        self.conn.execute(
            f'CREATE TABLE seen ({cols_def}, UNIQUE({cols_list}))'
        )
        self._insert_sql = (
            f'INSERT OR IGNORE INTO seen ({cols_list}) '
            f'VALUES ({",".join("?" for _ in key_columns)})'
        )

    def filter_batch(self, rows):
        """Accept a batch of rows; return only the ones not yet seen."""
        new_rows = []
        cursor = self.conn.cursor()
        for row in rows:
            key_vals = tuple(str(row.get(k, '')) for k in self.key_columns)
            cursor.execute(self._insert_sql, key_vals)
            if cursor.rowcount == 1:
                new_rows.append(row)
                self.unique += 1
            else:
                self.dupes += 1
        self.conn.commit()
        return new_rows

    def close(self):
        self.conn.close()
        try:
            os.unlink(self.db_path)
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
    dedup = SQLiteDedup(table_name, key_columns)
    writer = ShardedWriter(deduped_dir, table_name, max_rows=shard_size)

    batch = []
    file_count = 0
    for fp in files:
        file_count += 1
        for row in read_jsonl_zst(fp):
            batch.append(row)
            if len(batch) >= batch_size:
                new_rows = dedup.filter_batch(batch)
                writer.write_batch(new_rows)
                batch = []
        # Log progress every 50 files
        if file_count % 50 == 0:
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
        help=f'Rows per SQLite batch (default: {DEDUP_BATCH_SIZE})',
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

    for table_name in tables:
        dedup_table(
            staging_dir, deduped_dir, table_name, TABLE_KEYS[table_name],
            shard_size=args.shard_size, batch_size=args.batch_size,
        )

    elapsed = time.time() - t0
    log(f"Done. Total elapsed: {elapsed:.1f}s")


if __name__ == '__main__':
    main()
