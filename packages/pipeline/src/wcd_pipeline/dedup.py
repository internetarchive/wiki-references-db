"""Stage 3 (first half): consolidate and deduplicate the staged Parquet tables.

The large tables are deduplicated in key partitions (``_partitioned_copy``):
staged rows are already nearly unique after Stage 1's per-page dedup, so one
GROUP BY over a whole table holds a group per row (~330M for enwiki's
citation_instances), and DuckDB keeps MIN(varchar) state outside its memory
limit; it was OOM-killed at 28 GB with a 20 GB limit. Each key falls in exactly
one partition, so the output is the same as one GROUP BY over everything. Only the staging tables of ``wcd_schema.staging`` pass through here;
the per-revision datasets (citation_history, named_ref_uses,
revision_signals) are written by Stage 1 in their final layout.

The **instance-id collision gate** runs with ``citation_instances``: the
deterministic ``citation_instance_id`` must be unique per distinct
``(domain, page_id, raw_sha1)`` over the whole staging set; a collision
aborts the stage (the data model would then have to move to a wider id).

Usage:
    wcd dedup -d ./staging
    wcd dedup -d ./staging --memory-limit 8GB
    wcd dedup -d ./staging --tables citation_instances normalized_citations
"""

import argparse
import os
import sys
import time

import duckdb
from .config import load_dotenv  # noqa: F401  (loads .env on import)



# Staged rows per key partition for the large tables (DEDUP_ROWS_PER_PARTITION);
# ~20M groups peaked at ~7 GB with a 4 GB DuckDB limit on enwiki.
ROWS_PER_PARTITION = int(os.environ.get('DEDUP_ROWS_PER_PARTITION', '20000000'))
# Where partition files go (main sets it to --temp-dir, else the deduped dir).
PARTITION_BASE = None


def log(msg):
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"{ts} [dedup] {msg}", flush=True)


def _glob(staging_dir, table_name):
    """Return a DuckDB glob pattern matching all Parquet files for a table."""
    return os.path.join(staging_dir, f'**/*-{table_name}-*.parquet')


def _out(deduped_dir, table_name):
    """Return the output path for a deduped table."""
    return os.path.join(deduped_dir, f'{table_name}.parquet')


def _done_marker(deduped_dir, table_name):
    return os.path.join(deduped_dir, f'.done-{table_name}')


def _running_marker(deduped_dir, table_name):
    return os.path.join(deduped_dir, f'.running-{table_name}')


def _is_done(deduped_dir, table_name):
    return os.path.exists(_done_marker(deduped_dir, table_name))


def _mark_done(deduped_dir, table_name):
    with open(_done_marker(deduped_dir, table_name), 'w') as f:
        f.write(time.strftime('%Y-%m-%d %H:%M:%S'))


def _is_running(deduped_dir, table_name):
    return os.path.exists(_running_marker(deduped_dir, table_name))


def _mark_running(deduped_dir, table_name):
    with open(_running_marker(deduped_dir, table_name), 'w') as f:
        f.write(time.strftime('%Y-%m-%d %H:%M:%S'))


def _clear_running(deduped_dir, table_name):
    marker = _running_marker(deduped_dir, table_name)
    if os.path.exists(marker):
        os.remove(marker)


def _parse_bool(value):
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {'1', 'true', 'yes', 'y', 'on'}:
        return True
    if text in {'0', 'false', 'no', 'n', 'off'}:
        return False
    raise argparse.ArgumentTypeError(
        f"Invalid boolean value '{value}'. Use true/false."
    )


def _partitioned_copy(con, glob, part_expr, query, out, row_group_size, table_name):
    """Write ``query`` (SQL with a ``{src}`` placeholder for the staged rows) to
    ``out``, one key partition at a time when the table is large.

    ``part_expr`` maps a row to an integer by the columns every row of a group
    shares (e.g. ``page_id`` or ``hash(normalized_sha1)``). With more than one
    partition the staged rows are first split into Parquet partitions in one
    pass; ``query`` then runs per partition and the results are streamed into a
    single file, written under a temporary name and renamed when complete.
    """
    import math
    import shutil

    import pyarrow.parquet as pq

    n_rows = con.execute(f"SELECT coalesce(sum(num_rows), 0) FROM parquet_file_metadata('{glob}')").fetchone()[0]
    parts = max(1, math.ceil(n_rows / ROWS_PER_PARTITION))
    if parts == 1:
        con.execute(f"""COPY ({query.format(src=f"'{glob}'")}) TO '{out}'
                        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {row_group_size})""")
        return
    base = PARTITION_BASE or os.path.dirname(out)
    split = os.path.join(base, f'.partitions-{table_name}')
    shutil.rmtree(split, ignore_errors=True)
    os.makedirs(split)
    tmp_out = out + '.partial'
    try:
        t = time.time()
        try:
            con.execute(f"SET partitioned_write_max_open_files = {parts + 64}")
        except duckdb.Error:
            pass
        con.execute(f"""COPY (SELECT *, CAST(coalesce({part_expr}, 0) % {parts} AS INTEGER) AS __part FROM '{glob}')
                        TO '{split}' (FORMAT PARQUET, PARTITION_BY (__part), COMPRESSION ZSTD)""")
        log(f"{table_name}: {n_rows} staged rows split into {parts} key partitions in {time.time() - t:.0f}s")
        writer = None
        for i in range(parts):
            part_glob = os.path.join(split, f'__part={i}', '*.parquet')
            if not os.path.isdir(os.path.dirname(part_glob)):
                continue
            src = f"read_parquet('{part_glob}', hive_partitioning=false)"
            result = con.execute(query.format(src=src))
            # to_arrow_reader() replaces fetch_record_batch() in newer DuckDB; the dependency allows >= 1.0
            reader = (getattr(result, 'to_arrow_reader', None) or result.fetch_record_batch)(row_group_size)
            for batch in reader:
                if batch.num_rows == 0:
                    continue
                if writer is None:
                    writer = pq.ParquetWriter(tmp_out, batch.schema, compression='zstd')
                writer.write_batch(batch, row_group_size=row_group_size)
        if writer is None:  # nothing passed the filters: an empty file with the right columns
            con.execute(f"""COPY (SELECT * FROM ({query.format(src=f"'{glob}'")}) LIMIT 0) TO '{tmp_out}'
                            (FORMAT PARQUET, COMPRESSION ZSTD)""")
        else:
            writer.close()
        os.replace(tmp_out, out)
    finally:
        shutil.rmtree(split, ignore_errors=True)
        if os.path.exists(tmp_out):
            os.remove(tmp_out)


def _has_files(con, glob_pattern):
    """Check if any files match the glob pattern."""
    try:
        result = con.execute(f"SELECT COUNT(*) FROM glob('{glob_pattern}')").fetchone()
        return result[0] > 0
    except Exception:
        return False


def dedup_containers(con, staging_dir, deduped_dir):
    glob = _glob(staging_dir, 'containers')
    if not _has_files(con, glob):
        return
    con.execute(f"""
        COPY (
            SELECT DISTINCT label
            FROM '{glob}'
            WHERE label IS NOT NULL
        ) TO '{_out(deduped_dir, "containers")}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
    """)


def dedup_domains(con, staging_dir, deduped_dir):
    glob = _glob(staging_dir, 'domains')
    if not _has_files(con, glob):
        return
    con.execute(f"""
        COPY (
            SELECT value, MIN(for_container_label) AS for_container_label
            FROM '{glob}'
            WHERE value IS NOT NULL
            GROUP BY value
        ) TO '{_out(deduped_dir, "domains")}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
    """)


def dedup_web_resources(con, staging_dir, deduped_dir):
    glob = _glob(staging_dir, 'web_resources')
    if not _has_files(con, glob):
        return
    con.execute(f"""
        COPY (
            SELECT url, MIN(domain_label) AS domain_label
            FROM '{glob}'
            WHERE url IS NOT NULL
            GROUP BY url
        ) TO '{_out(deduped_dir, "web_resources")}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)
    """)


class InstanceIdCollision(RuntimeError):
    """Two distinct (domain, page_id, raw_sha1) keys produced the same instance id."""


def dedup_citation_instances(con, staging_dir, deduped_dir):
    glob = _glob(staging_dir, 'citation_instances')
    if not _has_files(con, glob):
        return
    out = _out(deduped_dir, "citation_instances")
    _partitioned_copy(con, glob, "page_id", """
            SELECT citation_instance_id, domain, page_id, raw_sha1,
                   MIN(normalized_sha1) AS normalized_sha1,
                   MIN(reference_type) AS reference_type,
                   MIN(reference_name) AS reference_name
            FROM {src}
            WHERE domain IS NOT NULL AND page_id IS NOT NULL AND raw_sha1 IS NOT NULL
            GROUP BY citation_instance_id, domain, page_id, raw_sha1""", out, 500000, "citation_instances")
    n_ids, n_keys = con.execute(f"""
        SELECT count(DISTINCT citation_instance_id), count(*) FROM '{out}'
    """).fetchone()
    if n_ids != n_keys:
        collided = con.execute(f"""
            SELECT citation_instance_id, list((domain, page_id, raw_sha1))
            FROM '{out}' GROUP BY citation_instance_id HAVING count(*) > 1 LIMIT 5
        """).fetchall()
        raise InstanceIdCollision(
            f"citation_instance_id collision gate failed: {n_keys} distinct (domain, page_id, raw_sha1) "
            f"keys but {n_ids} distinct ids; e.g. {collided}")
    log(f"citation_instances: {n_keys} instances, instance-id collision gate passed")


def dedup_normalized_citations(con, staging_dir, deduped_dir):
    glob = _glob(staging_dir, 'normalized_citations')
    if not _has_files(con, glob):
        return
    _partitioned_copy(con, glob, "hash(normalized_sha1)", """
            SELECT normalized_sha1,
                   MIN(reference_normalized) AS reference_normalized,
                   MIN(appears_on_page_id) AS appears_on_page_id,
                   MIN(appears_on_domain) AS appears_on_domain,
                   MIN(citation_type) AS citation_type
            FROM {src}
            WHERE normalized_sha1 IS NOT NULL
            GROUP BY normalized_sha1""", _out(deduped_dir, "normalized_citations"), 500000, "normalized_citations")


def dedup_ncwr(con, staging_dir, deduped_dir):
    glob = _glob(staging_dir, 'ncwr')
    if not _has_files(con, glob):
        return
    _partitioned_copy(con, glob, "hash(normalized_sha1)", """
            SELECT DISTINCT normalized_sha1, url
            FROM {src}
            WHERE normalized_sha1 IS NOT NULL AND url IS NOT NULL""", _out(deduped_dir, "ncwr"), 500000, "ncwr")


def dedup_wiki_templates(con, staging_dir, deduped_dir):
    glob = _glob(staging_dir, 'wiki_templates')
    if not _has_files(con, glob):
        return
    con.execute(f"""
        COPY (
            SELECT DISTINCT domain_label, name
            FROM '{glob}'
            WHERE domain_label IS NOT NULL AND name IS NOT NULL
        ) TO '{_out(deduped_dir, "wiki_templates")}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
    """)


def dedup_template_data(con, staging_dir, deduped_dir):
    glob = _glob(staging_dir, 'template_data')
    if not _has_files(con, glob):
        return
    _partitioned_copy(con, glob, "hash(normalized_sha1)", """
            SELECT domain_label, template_name, normalized_sha1, offset_start,
                   parameter_key, MIN(parameter_value) AS parameter_value
            FROM {src}
            WHERE domain_label IS NOT NULL AND template_name IS NOT NULL
            GROUP BY domain_label, template_name, normalized_sha1, offset_start, parameter_key""",
                      _out(deduped_dir, "template_data"), 500000, "template_data")


ALL_TABLES = [
    ('containers',          dedup_containers),
    ('domains',             dedup_domains),
    ('web_resources',       dedup_web_resources),
    ('citation_instances',  dedup_citation_instances),
    ('normalized_citations', dedup_normalized_citations),
    ('ncwr',                dedup_ncwr),
    ('wiki_templates',      dedup_wiki_templates),
    ('template_data',       dedup_template_data),
]


def main(argv=None):
    global ROWS_PER_PARTITION, PARTITION_BASE
    parser = argparse.ArgumentParser(
        description='Deduplicate staged Parquet files using DuckDB')
    parser.add_argument('-d', '--staging-dir',
                        default=os.environ.get('STAGING_DIR', './staging'),
                        help='Staging directory containing raw Parquet files')
    parser.add_argument('--memory-limit',
                        default=os.environ.get('DEDUP_MEMORY_LIMIT', '8GB'),
                        help='DuckDB memory limit (default: DEDUP_MEMORY_LIMIT or 8GB)')
    parser.add_argument('--temp-dir',
                        default=os.environ.get('DEDUP_TEMP_DIR'),
                        help='DuckDB temp/spill directory (default: auto)')
    parser.add_argument('--threads', type=int,
                        default=int(os.environ['DEDUP_THREADS']) if os.environ.get('DEDUP_THREADS') else None,
                        help='DuckDB execution threads (default: DEDUP_THREADS or DuckDB default)')
    parser.add_argument('--preserve-insertion-order', type=_parse_bool,
                        default=_parse_bool(os.environ.get('DEDUP_PRESERVE_INSERTION_ORDER', 'false')),
                        help='DuckDB preserve_insertion_order setting (default: DEDUP_PRESERVE_INSERTION_ORDER or false)')
    parser.add_argument('--max-temp-dir-size',
                        default=os.environ.get('DEDUP_MAX_TEMP_DIRECTORY_SIZE'),
                        help='DuckDB max_temp_directory_size (default: DEDUP_MAX_TEMP_DIRECTORY_SIZE)')
    parser.add_argument('--rows-per-partition', type=int, default=ROWS_PER_PARTITION,
                        help='Staged rows per key partition for the large tables '
                             '(default: DEDUP_ROWS_PER_PARTITION or 20000000); lower it if a partition runs out of memory')
    parser.add_argument('--tables', nargs='+', metavar='TABLE',
                        choices=[t for t, _ in ALL_TABLES],
                        help='Dedup only the specified table(s)')
    args = parser.parse_args(argv)

    staging_dir = args.staging_dir
    if not os.path.isdir(staging_dir):
        print(f"Error: staging directory does not exist: {staging_dir}", file=sys.stderr)
        sys.exit(1)

    deduped_dir = os.path.join(staging_dir, 'deduped')
    os.makedirs(deduped_dir, exist_ok=True)
    ROWS_PER_PARTITION = args.rows_per_partition
    PARTITION_BASE = args.temp_dir or deduped_dir

    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{args.memory_limit}'")
    if args.temp_dir:
        con.execute(f"SET temp_directory = '{args.temp_dir}'")
    if args.threads is not None:
        con.execute(f"SET threads = {args.threads}")
    con.execute(f"SET preserve_insertion_order = {'true' if args.preserve_insertion_order else 'false'}")
    if args.max_temp_dir_size:
        con.execute(f"SET max_temp_directory_size = '{args.max_temp_dir_size}'")

    log(
        'DuckDB settings: '
        f"memory_limit={args.memory_limit}, "
        f"temp_directory={args.temp_dir or 'auto'}, "
        f"threads={args.threads if args.threads is not None else 'duckdb-default'}, "
        f"preserve_insertion_order={'true' if args.preserve_insertion_order else 'false'}, "
        f"max_temp_directory_size={args.max_temp_dir_size or 'duckdb-default'}"
    )

    tables_to_run = ALL_TABLES
    if args.tables:
        table_set = set(args.tables)
        tables_to_run = [(n, f) for n, f in ALL_TABLES if n in table_set]

    t0 = time.time()
    for table_name, dedup_fn in tables_to_run:
        if _is_done(deduped_dir, table_name):
            log(f"{table_name}: already done, skipping")
            continue
        if _is_running(deduped_dir, table_name):
            log(f"{table_name}: found stale running marker; resuming")
        log(f"{table_name}: deduplicating...")
        _mark_running(deduped_dir, table_name)
        t1 = time.time()
        try:
            dedup_fn(con, staging_dir, deduped_dir)
            elapsed = time.time() - t1
            _mark_done(deduped_dir, table_name)
            log(f"{table_name}: done in {elapsed:.1f}s")
        finally:
            _clear_running(deduped_dir, table_name)

    total = time.time() - t0
    log(f"All done. Total elapsed: {total:.1f}s")
    con.close()


if __name__ == '__main__':
    main()
