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
import shutil
import tempfile
import threading
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

import psutil

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
DEFAULT_MAX_MEMORY_PCT = int(os.getenv('DEDUP_MAX_MEMORY_PCT', '80'))

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


def _done_marker_path(deduped_dir, table_name):
    """Return the path to the DONE marker file for a table."""
    return os.path.join(deduped_dir, f'DONE_{table_name}.txt')


def _is_table_done(deduped_dir, table_name):
    """Check whether a table has already been fully deduped."""
    return os.path.exists(_done_marker_path(deduped_dir, table_name))


def _mark_table_done(deduped_dir, table_name):
    """Write a DONE marker file with the current timestamp."""
    os.makedirs(deduped_dir, exist_ok=True)
    marker = _done_marker_path(deduped_dir, table_name)
    with open(marker, 'w') as f:
        f.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n')
    log(f"  {table_name}: marked as done → {marker}")


def _phase_marker_path(deduped_dir, table_name, phase):
    """Return the path to the phase-level DONE marker file."""
    return os.path.join(deduped_dir, f'DONE_{table_name}_phase{phase}.txt')


def _is_phase_done(deduped_dir, table_name, phase):
    """Check whether a phase has already been completed."""
    return os.path.exists(_phase_marker_path(deduped_dir, table_name, phase))


def _mark_phase_done(deduped_dir, table_name, phase, stats=None):
    """Write a phase-level DONE marker with timestamp and optional stats."""
    os.makedirs(deduped_dir, exist_ok=True)
    marker = _phase_marker_path(deduped_dir, table_name, phase)
    with open(marker, 'w') as f:
        f.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n')
        if stats:
            for k, v in stats.items():
                f.write(f'{k}: {v}\n')
    log(f"  {table_name}: phase {phase} marked as done → {marker}")


def _read_phase_stats(deduped_dir, table_name, phase):
    """Read stats from a phase marker file. Returns a dict of key-value pairs."""
    marker = _phase_marker_path(deduped_dir, table_name, phase)
    stats = {}
    if not os.path.exists(marker):
        return stats
    with open(marker, 'r') as f:
        lines = f.readlines()
    for line in lines[1:]:  # skip timestamp line
        line = line.strip()
        if ': ' in line:
            k, v = line.split(': ', 1)
            try:
                stats[k] = int(v)
            except ValueError:
                stats[k] = v
    return stats


def _clean_partial_table(staging_dir, deduped_dir, table_name):
    """Remove partial deduped output and incomplete intermediate files.

    Preserves intermediate data for phases that completed successfully
    (have a DONE marker) so that a resumed run can skip them.
    """
    # Always remove final deduped shards — phase 3 was not completed
    pattern = os.path.join(deduped_dir, f'{table_name}-*.jsonl.zst')
    for fp in glob.glob(pattern):
        os.remove(fp)
        log(f"  {table_name}: removed partial deduped file {os.path.basename(fp)}")

    intermediate_base = os.path.join(staging_dir, 'intermediate')
    phase1_done = _is_phase_done(deduped_dir, table_name, 1)
    phase2_done = _is_phase_done(deduped_dir, table_name, 2)

    if not phase1_done:
        # Phase 1 incomplete — clean its intermediate dir
        intra_dir = os.path.join(intermediate_base, f'dedup_{table_name}_intra')
        if os.path.isdir(intra_dir):
            shutil.rmtree(intra_dir, ignore_errors=True)
            log(f"  {table_name}: removed incomplete phase 1 intermediate dir")

    if not phase2_done:
        # Phase 2 incomplete — clean its merged dir and marker
        merged_dir = os.path.join(intermediate_base, f'dedup_{table_name}_merged')
        if os.path.isdir(merged_dir):
            shutil.rmtree(merged_dir, ignore_errors=True)
            log(f"  {table_name}: removed incomplete phase 2 intermediate dir")
        marker = _phase_marker_path(deduped_dir, table_name, 2)
        if os.path.exists(marker):
            os.remove(marker)

    # Also clean up any legacy random-suffix intermediate dirs
    if os.path.isdir(intermediate_base):
        for entry in os.listdir(intermediate_base):
            if entry.startswith(f'dedup_{table_name}_') and entry not in (
                f'dedup_{table_name}_intra', f'dedup_{table_name}_merged',
            ):
                path = os.path.join(intermediate_base, entry)
                shutil.rmtree(path, ignore_errors=True)
                log(f"  {table_name}: removed legacy intermediate dir {entry}")


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def read_jsonl_zst(filepath):
    """Yield dicts from a .jsonl.zst file (streaming, low memory)."""
    if os.path.getsize(filepath) == 0:
        return
    dctx = zstd.ZstdDecompressor()
    leftover = b''
    with open(filepath, 'rb') as fh:
        reader = dctx.stream_reader(fh)
        while True:
            chunk = reader.read(1024 * 1024)  # 1 MiB at a time
            if not chunk:
                break
            data = leftover + chunk
            # Split on newlines; last piece may be incomplete
            parts = data.split(b'\n')
            leftover = parts.pop()  # keep incomplete trailing piece
            for raw_line in parts:
                raw_line = raw_line.strip()
                if raw_line:
                    try:
                        yield json.loads(raw_line)
                    except json.JSONDecodeError:
                        log(f"warning: skipping malformed line in {filepath}: {raw_line[:120]}")
                        continue
    # Handle any remaining data after EOF
    if leftover and leftover.strip():
        try:
            yield json.loads(leftover)
        except json.JSONDecodeError:
            log(f"warning: skipping malformed line in {filepath}: {leftover[:120]}")
            pass




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

    def __init__(self, num_partitions, table_name, base_dir=None, prefix=''):
        self.num_partitions = num_partitions
        if base_dir:
            os.makedirs(base_dir, exist_ok=True)
            self._tmpdir = tempfile.mkdtemp(prefix=f'{prefix}_', dir=base_dir)
            self._owns_tmpdir = True
        else:
            self._tmpdir = tempfile.mkdtemp(prefix=f'dedup_{table_name}_')
            self._owns_tmpdir = True
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


def _count_rows_in_file(fp):
    """Worker: count lines in a single .jsonl.zst file (streaming)."""
    if os.path.getsize(fp) == 0:
        return 0
    try:
        dctx = zstd.ZstdDecompressor()
        count = 0
        leftover = b''
        with open(fp, 'rb') as fh:
            reader = dctx.stream_reader(fh)
            while True:
                chunk = reader.read(1024 * 1024)
                if not chunk:
                    break
                data = leftover + chunk
                parts = data.split(b'\n')
                leftover = parts.pop()
                count += sum(1 for line in parts if line.strip())
        if leftover and leftover.strip():
            count += 1
        return count
    except Exception:
        return 0


def _count_rows_in_files(files, num_workers=1):
    """Count total lines across all staged .jsonl.zst files."""
    if num_workers <= 1 or len(files) <= 1:
        return sum(_count_rows_in_file(fp) for fp in files)
    with ProcessPoolExecutor(max_workers=min(num_workers, len(files))) as pool:
        return sum(pool.map(_count_rows_in_file, files))


# ---------------------------------------------------------------------------
# Parallel worker functions (top-level for pickling)
# ---------------------------------------------------------------------------

def _dedup_single_file(fp, key_columns, K, tmp_dir, table_name):
    """Worker: read one file, intra-file dedup, write K partition temp files."""
    if os.path.getsize(fp) == 0:
        return 0, 0, []

    pw = _PartitionWriter(K, table_name, base_dir=tmp_dir,
                          prefix=os.path.basename(fp))
    total_input = 0
    total_kept = 0
    seen = set()
    try:
        for row in read_jsonl_zst(fp):
            total_input += 1
            key = _row_key(row, key_columns)
            if key not in seen:
                seen.add(key)
                pw.write(_partition_index(key, K), row)
                total_kept += 1
    except Exception as exc:
        log(f"    {table_name}: skipping unreadable file {fp} — {exc}")
        pw.close()
        return 0, 0, []
    pw.close()
    return total_input, total_kept, pw.partition_paths()


def _dedup_partition(partition_idx, file_list, key_columns, output_path):
    """Worker: merge & dedup all intermediate files for one partition."""
    seen = set()
    cctx = zstd.ZstdCompressor(level=1)
    with open(output_path, 'wb') as fh:
        writer = cctx.stream_writer(fh)
        count = 0
        for fp in file_list:
            if not os.path.exists(fp) or os.path.getsize(fp) == 0:
                continue
            for row in read_jsonl_zst(fp):
                key = _row_key(row, key_columns)
                if key not in seen:
                    seen.add(key)
                    line = json.dumps(row, ensure_ascii=False,
                                      default=_json_default) + '\n'
                    writer.write(line.encode('utf-8'))
                    count += 1
        writer.close()
    return count


def _wait_for_memory(threshold_pct, table_name=''):
    """Block until system memory usage drops below *threshold_pct*."""
    warned = False
    while psutil.virtual_memory().percent > threshold_pct:
        if not warned:
            log(f"    {table_name}: memory at "
                f"{psutil.virtual_memory().percent:.0f}% "
                f"(threshold {threshold_pct}%), waiting …")
            warned = True
        time.sleep(2)


def dedup_table(staging_dir, deduped_dir, table_name, key_columns,
                shard_size, batch_size, num_partitions=0,
                rows_per_partition=DEDUP_ROWS_PER_PARTITION,
                num_workers=1, max_memory_pct=DEFAULT_MAX_MEMORY_PCT):
    files = find_all_files(staging_dir, table_name, deduped_dir)
    if not files:
        log(f"  {table_name}: no source files found, skipping")
        return

    # --- Pass 0: estimate row count to choose K ---
    log(f"  {table_name}: counting rows in {len(files)} file(s) …")
    total_rows = _count_rows_in_files(files, num_workers=num_workers)
    if total_rows == 0:
        log(f"  {table_name}: no rows found, skipping")
        return
    K = _choose_num_partitions(total_rows, num_partitions, rows_per_partition)
    log(f"  {table_name}: ~{total_rows} rows, using {K} hash partition(s)")

    # Use deterministic intermediate directories so they survive restarts
    intermediate_base = os.path.join(staging_dir, 'intermediate')
    os.makedirs(intermediate_base, exist_ok=True)
    tmp_dir = os.path.join(intermediate_base, f'dedup_{table_name}_intra')
    os.makedirs(tmp_dir, exist_ok=True)
    merge_dir = os.path.join(intermediate_base, f'dedup_{table_name}_merged')
    os.makedirs(merge_dir, exist_ok=True)

    # --- Phase 1: parallel intra-file dedup ---
    if _is_phase_done(deduped_dir, table_name, 1):
        phase1_stats = _read_phase_stats(deduped_dir, table_name, 1)
        total_input = phase1_stats.get('total_input', 0)
        total_after_intra = phase1_stats.get('after_intra_dedup', 0)
        intra_dupes = total_input - total_after_intra
        log(f"  {table_name}: phase 1 already done, skipping "
            f"({total_after_intra} rows after intra-file dedup)")
        # Reconstruct all_partition_paths from existing intermediate files
        all_partition_paths = []
        # Discover partition files written by phase 1
        for entry in sorted(os.listdir(tmp_dir)):
            if entry.endswith('.jsonl.zst'):
                # Files are named like: <source_basename>-part<pidx>.jsonl.zst
                # We need to group them by source file
                pass
        # Simpler: just collect all partition files grouped by partition index
        # (phase 2 only needs grouped[pidx] -> list of files)
        grouped = {pidx: [] for pidx in range(K)}
        for entry in sorted(os.listdir(tmp_dir)):
            fp = os.path.join(tmp_dir, entry)
            if entry.endswith('.jsonl.zst') and os.path.getsize(fp) > 0:
                # Extract partition index from filename: *-part<NNNNN>.jsonl.zst
                try:
                    part_str = entry.rsplit('-part', 1)[1].replace('.jsonl.zst', '')
                    pidx = int(part_str)
                    grouped[pidx].append(fp)
                except (IndexError, ValueError):
                    pass
    else:
        log(f"  {table_name}: phase 1 — parallel intra-file dedup of "
            f"{len(files)} file(s) with {num_workers} worker(s) …")
        total_input = 0
        total_after_intra = 0
        all_partition_paths = []  # list of lists, one per file

        phase1_workers = min(num_workers, len(files))
        if phase1_workers <= 1:
            # Sequential fallback for single worker
            for fp in files:
                _wait_for_memory(max_memory_pct, table_name)
                inp, kept, paths = _dedup_single_file(
                    fp, key_columns, K, tmp_dir, table_name)
                total_input += inp
                total_after_intra += kept
                if paths:
                    all_partition_paths.append(paths)
        else:
            sem = threading.Semaphore(phase1_workers)
            with ProcessPoolExecutor(max_workers=phase1_workers) as pool:
                futures = {}
                for fp in files:
                    _wait_for_memory(max_memory_pct, table_name)
                    sem.acquire()
                    fut = pool.submit(_dedup_single_file, fp, key_columns, K,
                                      tmp_dir, table_name)
                    fut.add_done_callback(lambda f: sem.release())
                    futures[fut] = fp
                done_count = 0
                for fut in as_completed(futures):
                    inp, kept, paths = fut.result()
                    total_input += inp
                    total_after_intra += kept
                    if paths:
                        all_partition_paths.append(paths)
                    done_count += 1
                    if done_count % 10 == 0:
                        log(f"    {table_name}: phase 1 — "
                            f"{done_count}/{len(files)} files done …")

        intra_dupes = total_input - total_after_intra
        log(f"  {table_name}: phase 1 done — {total_after_intra} rows after "
            f"intra-file dedup ({intra_dupes} intra-file dupes removed)")

        _mark_phase_done(deduped_dir, table_name, 1, stats={
            'total_input': total_input,
            'after_intra_dedup': total_after_intra,
            'intra_dupes_removed': intra_dupes,
        })

        # Build grouped for phase 2
        grouped = {pidx: [] for pidx in range(K)}
        for paths in all_partition_paths:
            for pidx, ppath in enumerate(paths):
                if os.path.exists(ppath) and os.path.getsize(ppath) > 0:
                    grouped[pidx].append(ppath)

    # --- Phase 2: parallel cross-file dedup per partition ---
    if _is_phase_done(deduped_dir, table_name, 2):
        phase2_stats = _read_phase_stats(deduped_dir, table_name, 2)
        total_unique = phase2_stats.get('total_unique', 0)
        cross_dupes = phase2_stats.get('cross_dupes_removed', 0)
        log(f"  {table_name}: phase 2 already done, skipping "
            f"({total_unique} unique rows)")
        # Reconstruct partition_outputs from existing merged files
        partition_outputs = {}
        for entry in sorted(os.listdir(merge_dir)):
            fp = os.path.join(merge_dir, entry)
            if entry.startswith('merged-') and entry.endswith('.jsonl.zst'):
                try:
                    pidx = int(entry.replace('merged-', '').replace('.jsonl.zst', ''))
                    if os.path.getsize(fp) > 0:
                        partition_outputs[pidx] = fp
                except ValueError:
                    pass
    else:
        log(f"  {table_name}: phase 2 — parallel cross-file dedup of "
            f"{K} partition(s) with {num_workers} worker(s) …")

        phase2_workers = min(num_workers, K)
        partition_outputs = {}  # pidx -> output_path
        total_unique = 0

        # Build tasks
        tasks = []
        for pidx in range(K):
            if not grouped[pidx]:
                continue
            out_path = os.path.join(merge_dir, f'merged-{pidx:05d}.jsonl.zst')
            partition_outputs[pidx] = out_path
            tasks.append((pidx, grouped[pidx], key_columns, out_path))

        if phase2_workers <= 1 or len(tasks) <= 1:
            for pidx, flist, kcols, out_path in tasks:
                _wait_for_memory(max_memory_pct, table_name)
                count = _dedup_partition(pidx, flist, kcols, out_path)
                total_unique += count
        else:
            sem = threading.Semaphore(phase2_workers)
            with ProcessPoolExecutor(max_workers=phase2_workers) as pool:
                futures = {}
                for pidx, flist, kcols, out_path in tasks:
                    _wait_for_memory(max_memory_pct, table_name)
                    sem.acquire()
                    fut = pool.submit(_dedup_partition, pidx, flist, kcols, out_path)
                    fut.add_done_callback(lambda f: sem.release())
                    futures[fut] = pidx
                done_count = 0
                for fut in as_completed(futures):
                    count = fut.result()
                    total_unique += count
                    done_count += 1
                    if done_count % max(1, len(tasks) // 10) == 0:
                        log(f"    {table_name}: phase 2 — "
                            f"{done_count}/{len(tasks)} partitions done …")

        cross_dupes = total_after_intra - total_unique

        _mark_phase_done(deduped_dir, table_name, 2, stats={
            'total_unique': total_unique,
            'cross_dupes_removed': cross_dupes,
        })

    # --- Phase 3: write merged partitions into final sharded output ---
    log(f"  {table_name}: writing final shards …")
    writer = ShardedWriter(deduped_dir, table_name, max_rows=shard_size)
    for pidx in sorted(partition_outputs.keys()):
        out_path = partition_outputs[pidx]
        if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
            continue
        for row in read_jsonl_zst(out_path):
            writer.write_row(row)
    final_total, num_shards = writer.finish()

    total_dupes = intra_dupes + cross_dupes
    log(f"  {table_name}: {final_total} unique rows, {total_dupes} duplicates "
        f"removed → {num_shards} shard(s)")

    # Cleanup intermediate files and phase markers
    shutil.rmtree(tmp_dir, ignore_errors=True)
    shutil.rmtree(merge_dir, ignore_errors=True)
    for phase in (1, 2):
        marker = _phase_marker_path(deduped_dir, table_name, phase)
        if os.path.exists(marker):
            os.remove(marker)


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
    parser.add_argument(
        '--workers', type=int, default=0,
        help='Number of parallel workers (default: cpu_count)',
    )
    parser.add_argument(
        '--max-memory-pct', type=int, default=DEFAULT_MAX_MEMORY_PCT,
        help=f'Pause submitting new tasks when memory usage exceeds this percent (default: {DEFAULT_MAX_MEMORY_PCT})',
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

    num_workers = args.workers if args.workers > 0 else max(1, (os.cpu_count() or 1) - 2)

    max_memory_pct = args.max_memory_pct

    log(f"Deduplicating staged files in {staging_dir} → {deduped_dir}")
    log(f"Processing {len(tables)} table(s) sequentially, "
        f"up to {num_workers} worker(s) per table, "
        f"memory threshold {max_memory_pct}%")
    t0 = time.time()

    for table_name in tables:
        if _is_table_done(deduped_dir, table_name):
            log(f"  {table_name}: already done, skipping")
            continue

        # Clean up any partial results from a previous incomplete run
        _clean_partial_table(staging_dir, deduped_dir, table_name)

        try:
            if table_name in NO_DEDUP_TABLES:
                load_table(staging_dir, deduped_dir, table_name,
                           shard_size=args.shard_size)
            else:
                dedup_table(staging_dir, deduped_dir, table_name,
                            TABLE_KEYS[table_name],
                            shard_size=args.shard_size,
                            batch_size=args.batch_size,
                            num_partitions=args.num_partitions,
                            rows_per_partition=args.rows_per_partition,
                            num_workers=num_workers,
                            max_memory_pct=max_memory_pct)
            _mark_table_done(deduped_dir, table_name)
        except Exception as exc:
            log(f"  {table_name}: FAILED — {exc}")
            raise

    elapsed = time.time() - t0
    log(f"Done. Total elapsed: {elapsed:.1f}s")


if __name__ == '__main__':
    main()
