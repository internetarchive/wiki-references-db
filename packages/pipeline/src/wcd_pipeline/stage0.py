"""Stage 0 (RevisionChest) on the Python side.

RevisionChest writes the bundles and, with ``--postgres``, the entity tables
itself. When it ran with ``--parquet`` instead, ``wcd stage0-load`` loads that
metadata file into Postgres — ``domains``, ``revision_bundles``, ``revisions``,
``page_metadata`` and the article ``web_resources`` rows, the same contract as
RevisionChest's own Postgres sink — and, either way, ``wcd stage0-load``
writes the re-partitioned ``revisions`` Parquet dataset
(``revisions/domain=…/page_bucket=…``) that Stages 7d and 8 read so they never
scan ``pg.revisions`` from DuckDB.

The metadata file has no domain column (RevisionChest's ``.mwrev`` header has
none either), so ``--domain`` is required.

Revisions are loaded by ``--jobs`` connections in parallel, each committing
one ``rev_id`` range at a time, so an interrupted load can simply be re-run
(``ON CONFLICT DO NOTHING``). Postgres' ``wal_buffers`` matters at this rate:
the 16 MB default left the connections waiting on WAL (runbook).

Usage:
    wcd stage0-load --domain en.wikipedia.org --metadata bundles/metadata.parquet --parquet-dir data/parquet
    wcd stage0-load --domain en.wikipedia.org --metadata bundles/metadata.parquet --no-postgres   # Parquet only
    wcd stage0-load --domain en.wikipedia.org --metadata bundles/sync/ …                           # a directory of files
"""
from __future__ import annotations

import argparse
import io
import os
import shutil
import time

import duckdb
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from wcd_schema import parquet as layout
from wcd_schema.postgres import Domain, RevisionBundle

from .config import database_url, load_dotenv  # noqa: F401

BATCH = 100_000
NULL = "\\N"   # COPY text-format NULL


def log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} [stage0] {msg}", flush=True)


def metadata_glob(path: str) -> str:
    """A file, or every ``*.parquet`` in a directory."""
    return os.path.join(path, "*.parquet") if os.path.isdir(path) else path


def metadata_counts(path: str) -> dict:
    con = duckdb.connect()
    rows = con.execute(f"SELECT type, count(*) FROM read_parquet('{metadata_glob(path)}') GROUP BY type").fetchall()
    return {t: n for t, n in rows}


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------

def ensure_domain(session: Session, domain: str) -> int:
    Domain.upsert(session, value=domain)
    session.commit()
    return session.execute(select(Domain.id).where(Domain.value == domain)).scalar_one()


def load_bundles(session: Session, path: str) -> dict[str, int]:
    con = duckdb.connect()
    paths = [r[0] for r in con.execute(
        f"SELECT DISTINCT file_path FROM read_parquet('{metadata_glob(path)}') WHERE type = 'revision' AND file_path IS NOT NULL ORDER BY 1"
    ).fetchall()]
    RevisionBundle.bulk_upsert(session, [{"file_path": p} for p in paths])
    session.commit()
    rows = session.execute(select(RevisionBundle.file_path, RevisionBundle.id).where(RevisionBundle.file_path.in_(paths))).all()
    return {p: i for p, i in rows}


def _copy_text(v) -> str:
    """A value in COPY text format: NULL as ``\\N``; backslash, tab, newline
    and carriage return escaped (page titles may contain backslashes)."""
    if v is None:
        return NULL
    return (str(v).replace("\\", "\\\\").replace("\t", "\\t")
            .replace("\n", "\\n").replace("\r", "\\r"))


def load_pages(session: Session, path: str, domain: str, domain_id: int) -> int:
    """``page_metadata`` and the article ``web_resources`` rows: each batch is
    ``COPY``'d into a temp table and upserted with two set-based
    ``INSERT … SELECT … ON CONFLICT`` statements (``url_hash`` is ``md5(url)``,
    as ``WebResource.compute_url_hash``). A page reported more than once in a
    batch (overlapping ``sync`` files) is written once."""
    con = duckdb.connect()
    cur = con.execute(
        f"SELECT page_id, ns, title FROM read_parquet('{metadata_glob(path)}') WHERE type = 'page' ORDER BY page_id"
    )
    url_prefix = f"https://{domain}/w/index.php?curid="
    n = 0
    while True:
        chunk = cur.fetchmany(BATCH)
        if not chunk:
            break
        buf = io.StringIO()
        for pid, ns, title in chunk:
            buf.write(f"{_copy_text(pid)}\t{_copy_text(ns)}\t{_copy_text(title)}\n")
        buf.seek(0)
        raw = session.connection().connection  # the DBAPI (psycopg2) connection
        with raw.cursor() as c:
            c.execute("""CREATE TEMP TABLE IF NOT EXISTS temp_pages (
                             page_id INTEGER, page_namespace INTEGER, page_title TEXT) ON COMMIT DROP""")
            c.copy_expert("COPY temp_pages (page_id, page_namespace, page_title) FROM STDIN", buf)
            c.execute(
                """INSERT INTO page_metadata (domain_id, page_id, page_title, page_namespace)
                   SELECT DISTINCT ON (page_id) %s, page_id, page_title, page_namespace
                   FROM temp_pages WHERE page_id IS NOT NULL ORDER BY page_id
                   ON CONFLICT (domain_id, page_id)
                   DO UPDATE SET page_title = EXCLUDED.page_title, page_namespace = EXCLUDED.page_namespace""",
                (domain_id,),
            )
            # Columns left out (instance_of_document, …) keep their values, as
            # coalesce(NULL, existing) does in WebResource.bulk_upsert.
            c.execute(
                """INSERT INTO web_resources (url, url_hash, domain_id, numeric_page_id, numeric_namespace_id)
                   SELECT url, md5(url), %s, page_id, page_namespace
                   FROM (SELECT DISTINCT ON (page_id) %s || page_id AS url, page_id, page_namespace
                         FROM temp_pages WHERE page_id IS NOT NULL ORDER BY page_id) p
                   ON CONFLICT (url_hash) DO UPDATE SET
                       url = EXCLUDED.url,
                       domain_id = coalesce(EXCLUDED.domain_id, web_resources.domain_id),
                       numeric_page_id = coalesce(EXCLUDED.numeric_page_id, web_resources.numeric_page_id),
                       numeric_namespace_id = coalesce(EXCLUDED.numeric_namespace_id, web_resources.numeric_namespace_id)""",
                (domain_id, url_prefix),
            )
        session.commit()
        n += len(chunk)
    return n


REVISION_GROUP = 1_000_000   # rows per work unit (a row group of the sorted intermediate file)


def _sorted_revisions(path: str, bundle_ids: dict[str, int], work_dir: str) -> str:
    """One DuckDB pass: the revision rows with their bundle id resolved, sorted
    by ``rev_id``, written to a Parquet file in ``work_dir`` whose row groups
    are the load's work units (their ``rev_id`` ranges are disjoint)."""
    out = os.path.join(work_dir, "revisions-by-rev_id.parquet")
    con = duckdb.connect()
    con.execute(f"SET temp_directory = '{os.path.join(work_dir, 'duckdb-tmp')}'")
    con.execute("CREATE TEMP TABLE bundle_ids (file_path VARCHAR, id INTEGER)")
    con.executemany("INSERT INTO bundle_ids VALUES (?, ?)", list(bundle_ids.items()))
    con.execute(
        f"""COPY (SELECT m.rev_id, m.page_id, m.parent_rev_id, m.timestamp, b.id AS bundle_id,
                         m.offset_begin, m.length
                  FROM read_parquet('{metadata_glob(path)}') m LEFT JOIN bundle_ids b USING (file_path)
                  WHERE m.type = 'revision' ORDER BY m.rev_id)
            TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {REVISION_GROUP})"""
    )
    return out


def _revision_ranges(sorted_file: str) -> list[tuple[int, int | None]]:
    """Half-open ``rev_id`` ranges, one per row group of the sorted file."""
    con = duckdb.connect()
    mins = [r[0] for r in con.execute(
        f"""SELECT stats_min_value::BIGINT FROM parquet_metadata('{sorted_file}')
            WHERE path_in_schema = 'rev_id' ORDER BY row_group_id"""
    ).fetchall()]
    return [(lo, mins[i + 1] if i + 1 < len(mins) else None) for i, lo in enumerate(mins)]


def _load_revision_range(engine, sorted_file: str, domain_id: int, lo: int, hi: int | None) -> int:
    """``COPY`` one ``rev_id`` range into a temp table, ``INSERT … ON CONFLICT DO
    NOTHING`` into ``revisions``, commit. Safe to repeat."""
    import pyarrow.csv as pacsv

    where = f"rev_id >= {lo}" + (f" AND rev_id < {hi}" if hi is not None else "")
    result = duckdb.connect().execute(
        f"""SELECT rev_id, page_id, parent_rev_id, timestamp, bundle_id, offset_begin, length
            FROM read_parquet('{sorted_file}') WHERE {where}"""
    )
    # to_arrow_table() replaces fetch_arrow_table() in newer DuckDB; the dependency allows >= 1.0.
    table = (getattr(result, "to_arrow_table", None) or result.fetch_arrow_table)()
    if table.num_rows == 0:
        return 0
    buf = io.BytesIO()
    pacsv.write_csv(table, buf, pacsv.WriteOptions(include_header=False))
    buf.seek(0)
    raw = engine.raw_connection()
    try:
        with raw.cursor() as c:
            c.execute(
                """CREATE TEMP TABLE temp_revisions (
                       revision_id BIGINT, page_id INTEGER, parent_revision_id BIGINT, revision_timestamp TEXT,
                       found_in_bundle INTEGER, offset_begin BIGINT, length BIGINT) ON COMMIT DROP"""
            )
            c.copy_expert(
                "COPY temp_revisions (revision_id, page_id, parent_revision_id, revision_timestamp, "
                "found_in_bundle, offset_begin, length) FROM STDIN (FORMAT csv)",
                buf,
            )
            c.execute(
                """INSERT INTO revisions (revision_id, domain_id, page_id, parent_revision_id, revision_timestamp,
                                          found_in_bundle, offset_begin, length)
                   SELECT revision_id, %s, page_id, parent_revision_id, revision_timestamp::timestamp,
                          found_in_bundle, offset_begin, length
                   FROM temp_revisions
                   ON CONFLICT (revision_id) DO NOTHING""",
                (domain_id,),
            )
        raw.commit()
    finally:
        raw.close()
    return table.num_rows


def load_revisions(session: Session, path: str, domain_id: int, bundle_ids: dict[str, int],
                   jobs: int = 8, work_dir: str | None = None) -> int:
    """All revision rows, ``jobs`` connections at a time.

    The metadata is sorted by ``rev_id`` once (``_sorted_revisions``), and each
    row group of that file is one unit: ``COPY`` into a temp table, ``INSERT …
    ON CONFLICT DO NOTHING``, commit. Units have disjoint ``rev_id`` ranges, so
    the primary-key B-tree stays append-like, and a unit that is repeated
    (a re-run after a failure) changes nothing. A single backend was the
    bottleneck: one insert at a time, each with its primary-key entry and two
    foreign-key checks.
    """
    from concurrent.futures import ThreadPoolExecutor
    import tempfile

    session.commit()  # domains / bundles are visible to the other connections
    engine = session.get_bind()
    base = work_dir or os.environ.get("STAGING_DIR") or os.getcwd()
    os.makedirs(base, exist_ok=True)
    tmp = tempfile.mkdtemp(prefix="stage0-revisions-", dir=base)
    try:
        t = time.time()
        sorted_file = _sorted_revisions(path, bundle_ids, tmp)
        ranges = _revision_ranges(sorted_file)
        log(f"revisions sorted by rev_id in {time.time() - t:.0f} s: {len(ranges)} unit(s), {jobs} job(s)")
        n = 0
        with ThreadPoolExecutor(max_workers=jobs) as pool:
            for i, k in enumerate(pool.map(lambda r: _load_revision_range(engine, sorted_file, domain_id, *r), ranges), 1):
                n += k
                if i % 50 == 0 or i == len(ranges):
                    log(f"revisions: {i}/{len(ranges)} units, {n} rows")
        return n
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def load_to_postgres(session: Session, path: str, domain: str, jobs: int = 8, work_dir: str | None = None) -> dict:
    domain_id = ensure_domain(session, domain)
    bundles = load_bundles(session, path)
    log(f"{len(bundles)} bundle(s)")
    n_pages = load_pages(session, path, domain, domain_id)
    log(f"{n_pages} page_metadata rows")
    n_revs = load_revisions(session, path, domain_id, bundles, jobs=jobs, work_dir=work_dir)
    log(f"{n_revs} revisions")
    return {"domain_id": domain_id, "bundles": len(bundles), "pages": n_pages, "revisions": n_revs}


# ---------------------------------------------------------------------------
# Parquet
# ---------------------------------------------------------------------------

def write_revisions_parquet(path: str, domain: str, parquet_base: str) -> int:
    """``revisions/domain=<domain>/page_bucket=<n>/`` from the metadata under
    ``path`` (a file or a directory of files — point it at *all* the domain's
    metadata: the domain's partitions are replaced, and a revision reported
    by several files, as overlapping ``sync`` batches do, is written once)."""
    con = duckdb.connect()
    # Rows arrive in page_id order, so every chunk touches every bucket. With
    # DuckDB's default of 100 open partition files it kept closing and
    # reopening them: 685,899 files (~19 KB each) for enwiki's 512 buckets,
    # which every per-bucket reader then pays for. Keep one file per bucket.
    try:
        con.execute(f"SET partitioned_write_max_open_files = {layout.PAGE_BUCKETS + 64}")
    except duckdb.Error:
        pass  # DuckDB without the setting (< 1.1)
    target = layout.dataset_dir(parquet_base, "revisions")
    target.mkdir(parents=True, exist_ok=True)
    domain_dir = layout.partition_dir(parquet_base, "revisions", domain)
    if domain_dir.exists():
        shutil.rmtree(domain_dir)
    dom = domain.replace("'", "''")
    con.execute(
        f"""COPY (
                SELECT '{dom}' AS domain,
                       CAST(page_id % {layout.PAGE_BUCKETS} AS INTEGER) AS page_bucket,
                       CAST(rev_id AS BIGINT) AS revision_id,
                       CAST(page_id AS INTEGER) AS page_id,
                       CAST(parent_rev_id AS BIGINT) AS parent_revision_id,
                       CAST(strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ') AS TIMESTAMP) AS revision_timestamp
                FROM read_parquet('{metadata_glob(path)}')
                WHERE type = 'revision'
                QUALIFY row_number() OVER (PARTITION BY rev_id ORDER BY timestamp, file_path) = 1
                ORDER BY page_id, rev_id
            ) TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (domain, page_bucket), OVERWRITE_OR_IGNORE)"""
    )
    return con.execute(
        f"SELECT count(*) FROM read_parquet('{layout.partition_glob(parquet_base, 'revisions', domain)}', hive_partitioning=true)"
    ).fetchone()[0]


# ---------------------------------------------------------------------------
# Gate (docs/design/proposed_data_integrity.md §4, "after Stage 0")
# ---------------------------------------------------------------------------

def gate(session: Session | None, domain: str, parquet_base: str | None) -> list[str]:
    """The Stage-0 gate (``wcd_pipeline.gates.stage0``); returns failures."""
    from .gates import stage0
    return stage0(session, domain, parquet_base)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def make_session() -> Session:
    engine = create_engine(database_url(), poolclass=NullPool, hide_parameters=True)
    return sessionmaker(bind=engine)()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Load RevisionChest --parquet metadata into Postgres and write the revisions Parquet dataset")
    ap.add_argument("--domain", required=True, help="site domain the metadata belongs to (e.g. en.wikipedia.org)")
    ap.add_argument("--metadata", required=True, help="RevisionChest metadata .parquet file, or a directory of them")
    ap.add_argument("--parquet-dir", default=os.environ.get("PARQUET_DIR"), help="Parquet base directory for the revisions dataset (default: PARQUET_DIR)")
    ap.add_argument("--no-postgres", action="store_true", help="only write the revisions Parquet dataset")
    ap.add_argument("--no-gate", action="store_true", help="skip the Stage-0 checks afterwards")
    ap.add_argument("--jobs", type=int, default=8,
                    help="Postgres connections loading revisions in parallel (default 8)")
    ap.add_argument("--work-dir", default=None,
                    help="directory for the rev_id-sorted intermediate file and DuckDB spill "
                         "(default: STAGING_DIR, else the current directory; ~19 bytes per revision for the "
                         "file plus DuckDB's sort spill, ~60 GB for enwiki)")
    args = ap.parse_args(argv)

    counts = metadata_counts(args.metadata)
    log(f"metadata: {counts.get('page', 0)} page rows, {counts.get('revision', 0)} revision rows")
    session = None
    if not args.no_postgres:
        session = make_session()
        try:
            load_to_postgres(session, args.metadata, args.domain, jobs=args.jobs, work_dir=args.work_dir)
        except Exception:
            session.rollback()
            raise
    if args.parquet_dir:
        n = write_revisions_parquet(args.metadata, args.domain, args.parquet_dir)
        log(f"revisions Parquet: {n} rows under {layout.partition_dir(args.parquet_dir, 'revisions', args.domain)}")
    elif args.no_postgres:
        ap.error("--no-postgres without --parquet-dir does nothing")

    if not args.no_gate:
        failures = gate(session, args.domain, args.parquet_dir)
        if failures:
            for f in failures:
                log(f"GATE FAIL: {f}")
            return 1
        log("Stage 0 gate: pass")
    if session is not None:
        session.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
