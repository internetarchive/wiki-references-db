"""Deduplicate staged Parquet files using DuckDB.

DuckDB handles out-of-core dedup automatically (spills to disk when RAM is tight).

Pipeline:
    build_db.py (Parquet output)  →  dedup_parquet.py  →  load_all.py

Usage:
    python3 dedup_parquet.py -d ./staging
    python3 dedup_parquet.py -d ./staging --memory-limit 8GB
    python3 dedup_parquet.py -d ./staging --tables citation_instances citation_histories
"""

import argparse
import os
import sys
import time

import duckdb
from dotenv import load_dotenv

load_dotenv()


def log(msg):
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"{ts} [dedup_parquet] {msg}", flush=True)


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
            SELECT DISTINCT ON (value)
                value, for_container_label
            FROM '{glob}'
            WHERE value IS NOT NULL
        ) TO '{_out(deduped_dir, "domains")}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
    """)


def dedup_documents(con, staging_dir, deduped_dir):
    glob = _glob(staging_dir, 'documents')
    if not _has_files(con, glob):
        return
    con.execute(f"""
        COPY (
            SELECT DISTINCT ON (has_container_label, page_id)
                language_code, has_container_label, page_id
            FROM '{glob}'
            WHERE page_id IS NOT NULL
        ) TO '{_out(deduped_dir, "documents")}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
    """)


def dedup_web_resources(con, staging_dir, deduped_dir):
    glob = _glob(staging_dir, 'web_resources')
    if not _has_files(con, glob):
        return
    con.execute(f"""
        COPY (
            SELECT DISTINCT ON (url)
                url, domain_label, numeric_page_id, numeric_namespace_id, page_id
            FROM '{glob}'
            WHERE url IS NOT NULL
        ) TO '{_out(deduped_dir, "web_resources")}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)
    """)


def dedup_citation_instances(con, staging_dir, deduped_dir):
    glob = _glob(staging_dir, 'citation_instances')
    if not _has_files(con, glob):
        return
    con.execute(f"""
        COPY (
            SELECT DISTINCT ON (page_id, raw_sha1)
                page_id, raw_sha1, normalized_sha1, reference_type, reference_name
            FROM '{glob}'
            WHERE page_id IS NOT NULL AND raw_sha1 IS NOT NULL
        ) TO '{_out(deduped_dir, "citation_instances")}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)
    """)


def dedup_normalized_citations(con, staging_dir, deduped_dir):
    glob = _glob(staging_dir, 'normalized_citations')
    if not _has_files(con, glob):
        return
    con.execute(f"""
        COPY (
            SELECT DISTINCT ON (normalized_sha1)
                normalized_sha1, reference_normalized, appears_on_page_id, appears_on_domain
            FROM '{glob}'
            WHERE normalized_sha1 IS NOT NULL
        ) TO '{_out(deduped_dir, "normalized_citations")}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)
    """)


def dedup_citation_histories(con, staging_dir, deduped_dir):
    glob = _glob(staging_dir, 'citation_histories')
    if not _has_files(con, glob):
        return
    log('citation_histories: consolidating without global dedup (load-time conflict handling applies)')
    con.execute(f"""
        COPY (
            SELECT page_id, raw_sha1, revision_id
            FROM '{glob}'
            WHERE page_id IS NOT NULL AND raw_sha1 IS NOT NULL AND revision_id IS NOT NULL
        ) TO '{_out(deduped_dir, "citation_histories")}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000)
    """)


def dedup_revisions(con, staging_dir, deduped_dir):
    glob = _glob(staging_dir, 'revisions')
    if not _has_files(con, glob):
        return
    con.execute(f"""
        COPY (
            SELECT DISTINCT ON (revision_id)
                revision_id, page_id, parent_revision_id, revision_timestamp
            FROM '{glob}'
            WHERE revision_id IS NOT NULL
        ) TO '{_out(deduped_dir, "revisions")}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)
    """)


def dedup_ncwr(con, staging_dir, deduped_dir):
    glob = _glob(staging_dir, 'ncwr')
    if not _has_files(con, glob):
        return
    con.execute(f"""
        COPY (
            SELECT DISTINCT normalized_sha1, url
            FROM '{glob}'
            WHERE normalized_sha1 IS NOT NULL AND url IS NOT NULL
        ) TO '{_out(deduped_dir, "ncwr")}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)
    """)


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
    con.execute(f"""
        COPY (
            SELECT DISTINCT ON (domain_label, template_name, normalized_sha1, offset_start, parameter_key)
                domain_label, template_name, normalized_sha1, offset_start,
                parameter_key, parameter_value
            FROM '{glob}'
            WHERE domain_label IS NOT NULL AND template_name IS NOT NULL
        ) TO '{_out(deduped_dir, "template_data")}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)
    """)


ALL_TABLES = [
    ('containers',          dedup_containers),
    ('domains',             dedup_domains),
    ('documents',           dedup_documents),
    ('web_resources',       dedup_web_resources),
    ('citation_instances',  dedup_citation_instances),
    ('normalized_citations', dedup_normalized_citations),
    ('citation_histories',  dedup_citation_histories),
    ('revisions',           dedup_revisions),
    ('ncwr',                dedup_ncwr),
    ('wiki_templates',      dedup_wiki_templates),
    ('template_data',       dedup_template_data),
]


def main():
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
    parser.add_argument('--tables', nargs='+', metavar='TABLE',
                        choices=[t for t, _ in ALL_TABLES],
                        help='Dedup only the specified table(s)')
    args = parser.parse_args()

    staging_dir = args.staging_dir
    if not os.path.isdir(staging_dir):
        print(f"Error: staging directory does not exist: {staging_dir}", file=sys.stderr)
        sys.exit(1)

    deduped_dir = os.path.join(staging_dir, 'deduped')
    os.makedirs(deduped_dir, exist_ok=True)

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
