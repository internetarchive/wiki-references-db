#!/usr/bin/env python3
"""
Phase 1.5: Consolidate & deduplicate staged JSONL.zst files across all shards.

Reads per-shard staged files from STAGING_DIR, deduplicates using hash
partitioning (bounded memory, no external DB), and writes consolidated
deduplicated .jsonl.zst files to STAGING_DIR/deduped/.

Usage:
    python dedup_staged.py [-d STAGING_DIR] [--shard-size N] [--batch-size N]

Pipeline:
    build_all.py  →  dedup_staged.py  →  load_all.py
"""

import argparse
import json
import glob
import os
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

from dotenv import load_dotenv
load_dotenv()

import datetime

import hashlib
import struct
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
DEDUP_NUM_PARTITIONS = int(os.getenv('DEDUP_NUM_PARTITIONS', '0'))  # 0 = auto
DEDUP_ROWS_PER_PARTITION = int(os.getenv('DEDUP_ROWS_PER_PARTITION', '5_000_000'))

# Dedup key columns per table.
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
        compressed = fh.read()
    if not compressed:
        return
    # read_to_iter is the most reliable decompression API: it handles
    # multi-frame streams and files of any size without needing to know
    # the uncompressed size up front (unlike decompress()).
    raw = b''.join(dctx.read_to_iter(compressed))
    for line in raw.decode('utf-8').splitlines():
        line = line.strip()
        if line:
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                log(f"warning: skipping malformed line in {filepath}: {line[:120]}")
                continue




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
            f"{self.table_name}-{self.shard_idx:08d}.jsonl.zst",
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
# Hash-partitioned deduplication
# ---------------------------------------------------------------------------

def _row_key(row, key_columns):
    """Return the dedup key tuple for *row*."""
    return tuple(str(row.get(k, '')) for k in key_columns)


def _partition_index(key_tuple, num_partitions):
    """Deterministically map a key tuple to a partition index [0, K)."""
    h = hashlib.md5('\x00'.join(key_tuple).encode('utf-8')).digest()
    return struct.unpack('<I', h[:4])[0] % num_partitions


class _PartitionWriter:
    """Manages K temporary .jsonl.zst bucket files for hash partitioning."""

    def __init__(self, num_partitions, table_name):
        self.num_partitions = num_partitions
        self._tmpdir = tempfile.mkdtemp(prefix=f'dedup_{table_name}_')
        self._files = []
        self._writers = []
        self._row_counts = [0] * num_partitions
        for i in range(num_partitions):
            path = os.path.join(self._tmpdir, f'part-{i:05d}.jsonl.zst')
            fh = open(path, 'wb')
            cctx = zstd.ZstdCompressor(level=1)
            writer = cctx.stream_writer(fh)
            self._files.append(fh)
            self._writers.append(writer)

    def write(self, partition_idx, row):
        line = json.dumps(row, ensure_ascii=False, default=_json_default) + '\n'
        self._writers[partition_idx].write(line.encode('utf-8'))
        self._row_counts[partition_idx] += 1

    def close(self):
        for w in self._writers:
            w.close()
        for f in self._files:
            f.close()

    def partition_paths(self):
        return [
            os.path.join(self._tmpdir, f'part-{i:05d}.jsonl.zst')
            for i in range(self.num_partitions)
        ]

    def cleanup(self):
        import shutil
        shutil.rmtree(self._tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Per-table dedup driver
# ---------------------------------------------------------------------------

# Tables whose staged rows are already unique — skip dedup, just consolidate.
NO_DEDUP_TABLES = {'citation_histories'}


def load_table(staging_dir, deduped_dir, table_name, shard_size):
    """Consolidate staged files into sharded output without deduplication."""
    files = find_all_files(staging_dir, table_name, deduped_dir)
    if not files:
        log(f"  {table_name}: no source files found, skipping")
        return

    log(f"  {table_name}: loading {len(files)} file(s) (no dedup) …")
    writer = ShardedWriter(deduped_dir, table_name, max_rows=shard_size)

    file_count = 0
    for fp in files:
        file_count += 1
        if os.path.getsize(fp) == 0:
            continue
        try:
            file_rows = list(read_jsonl_zst(fp))
        except Exception as exc:
            log(f"    {table_name}: skipping unreadable file {fp} — {exc}")
            continue
        writer.write_batch(file_rows)
        if file_count % 10 == 0:
            log(f"    {table_name}: loaded {file_count}/{len(files)} files …")

    total_rows, num_shards = writer.finish()
    log(f"  {table_name}: {total_rows} rows loaded → {num_shards} shard(s)")


def _choose_num_partitions(total_rows, explicit_k, rows_per_partition):
    """Return the number of hash partitions to use."""
    if explicit_k > 0:
        return explicit_k
    # Auto: aim for rows_per_partition rows per bucket, minimum 1
    k = max(1, (total_rows + rows_per_partition - 1) // rows_per_partition)
    # Round up to next power of 2 for even hash distribution
    p = 1
    while p < k:
        p <<= 1
    return p


def _count_rows_in_files(files):
    """Quickly count total lines across all staged .jsonl.zst files."""
    total = 0
    dctx = zstd.ZstdDecompressor()
    for fp in files:
        if os.path.getsize(fp) == 0:
            continue
        try:
            with open(fp, 'rb') as fh:
                raw = b''.join(dctx.read_to_iter(fh.read()))
            total += sum(1 for line in raw.split(b'\n') if line.strip())
        except Exception:
            pass
    return total


def dedup_table(staging_dir, deduped_dir, table_name, key_columns,
                shard_size, batch_size, num_partitions=0,
                rows_per_partition=DEDUP_ROWS_PER_PARTITION):
    files = find_all_files(staging_dir, table_name, deduped_dir)
    if not files:
        log(f"  {table_name}: no source files found, skipping")
        return

    # --- Pass 0: estimate row count to choose K ---
    log(f"  {table_name}: counting rows in {len(files)} file(s) …")
    total_rows = _count_rows_in_files(files)
    if total_rows == 0:
        log(f"  {table_name}: no rows found, skipping")
        return
    K = _choose_num_partitions(total_rows, num_partitions, rows_per_partition)
    log(f"  {table_name}: ~{total_rows} rows, using {K} hash partition(s)")

    # --- Pass 1: read files, intra-file dedup, hash-partition to buckets ---
    log(f"  {table_name}: partitioning {len(files)} file(s) …")
    pw = _PartitionWriter(K, table_name)
    total_input = 0
    total_after_intra = 0
    file_count = 0
    for fp in files:
        file_count += 1
        if os.path.getsize(fp) == 0:
            continue
        try:
            file_rows = list(read_jsonl_zst(fp))
        except Exception as exc:
            log(f"    {table_name}: skipping unreadable file {fp} — {exc}")
            continue
        # Intra-file dedup
        seen_in_file = set()
        for row in file_rows:
            total_input += 1
            key = _row_key(row, key_columns)
            if key not in seen_in_file:
                seen_in_file.add(key)
                pidx = _partition_index(key, K)
                pw.write(pidx, row)
                total_after_intra += 1
        if file_count % 10 == 0:
            log(f"    {table_name}: partitioned {file_count}/{len(files)} files …")
    pw.close()
    intra_dupes = total_input - total_after_intra
    log(f"  {table_name}: pass 1 done — {total_after_intra} rows after "
        f"intra-file dedup ({intra_dupes} intra-file dupes removed)")

    # --- Pass 2: dedup each partition independently, write to output shards ---
    log(f"  {table_name}: deduplicating {K} partition(s) …")
    writer = ShardedWriter(deduped_dir, table_name, max_rows=shard_size)
    cross_dupes = 0
    for pidx, ppath in enumerate(pw.partition_paths()):
        if not os.path.exists(ppath) or os.path.getsize(ppath) == 0:
            continue
        seen = set()
        for row in read_jsonl_zst(ppath):
            key = _row_key(row, key_columns)
            if key not in seen:
                seen.add(key)
                writer.write_row(row)
            else:
                cross_dupes += 1
        if (pidx + 1) % max(1, K // 10) == 0:
            log(f"    {table_name}: deduped {pidx + 1}/{K} partitions …")

    total_unique, num_shards = writer.finish()
    total_dupes = intra_dupes + cross_dupes
    log(f"  {table_name}: {total_unique} unique rows, {total_dupes} duplicates "
        f"removed → {num_shards} shard(s)")
    pw.cleanup()


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
        help=f'Rows per dedup batch (default: {DEDUP_BATCH_SIZE})',
    )
    parser.add_argument(
        '--num-partitions', type=int, default=DEDUP_NUM_PARTITIONS,
        help='Number of hash partitions (0 = auto)',
    )
    parser.add_argument(
        '--rows-per-partition', type=int, default=DEDUP_ROWS_PER_PARTITION,
        help=f'Target rows per partition for auto K (default: {DEDUP_ROWS_PER_PARTITION})',
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

    tables = args.tables or (list(TABLE_KEYS.keys()) + list(NO_DEDUP_TABLES))
    all_tables = set(TABLE_KEYS.keys()) | NO_DEDUP_TABLES
    invalid = [t for t in tables if t not in all_tables]
    if invalid:
        raise SystemExit(f"Unknown table(s): {', '.join(invalid)}")

    log(f"Deduplicating staged files in {staging_dir} → {deduped_dir}")
    t0 = time.time()

    max_workers = min(len(tables), os.cpu_count() or 1)
    log(f"Processing {len(tables)} table(s) with {max_workers} worker(s)")

    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        futures = {}
        for table_name in tables:
            if table_name in NO_DEDUP_TABLES:
                fut = pool.submit(
                    load_table, staging_dir, deduped_dir, table_name,
                    shard_size=args.shard_size,
                )
            else:
                fut = pool.submit(
                    dedup_table, staging_dir, deduped_dir, table_name,
                    TABLE_KEYS[table_name],
                    shard_size=args.shard_size, batch_size=args.batch_size,
                    num_partitions=args.num_partitions,
                    rows_per_partition=args.rows_per_partition,
                )
            futures[fut] = table_name
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
