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

Usage:
    wcd stage0-load --domain en.wikipedia.org --metadata bundles/metadata.parquet --parquet-dir data/parquet
    wcd stage0-load --domain en.wikipedia.org --metadata bundles/metadata.parquet --no-postgres   # Parquet only
    wcd stage0-load --domain en.wikipedia.org --metadata bundles/sync/ …                           # a directory of files
"""
from __future__ import annotations

import argparse
import io
import os
import time

import duckdb
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from wcd_schema import parquet as layout
from wcd_schema.postgres import Domain, PageMetadata, RevisionBundle, WebResource

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


def load_pages(session: Session, path: str, domain: str, domain_id: int) -> int:
    con = duckdb.connect()
    cur = con.execute(
        f"SELECT page_id, ns, title FROM read_parquet('{metadata_glob(path)}') WHERE type = 'page' ORDER BY page_id"
    )
    n = 0
    while True:
        chunk = cur.fetchmany(BATCH)
        if not chunk:
            break
        PageMetadata.bulk_upsert(session, [
            {"domain_id": domain_id, "page_id": pid, "page_title": title, "page_namespace": ns} for pid, ns, title in chunk
        ])
        WebResource.bulk_upsert(session, [
            {"url": f"https://{domain}/w/index.php?curid={pid}", "domain_id": domain_id,
             "numeric_page_id": pid, "numeric_namespace_id": ns} for pid, ns, _ in chunk
        ])
        session.commit()
        n += len(chunk)
    return n


def load_revisions(session: Session, path: str, domain_id: int, bundle_ids: dict[str, int]) -> int:
    """``COPY`` into a temp table, then ``INSERT … ON CONFLICT DO NOTHING``."""
    con = duckdb.connect()
    cur = con.execute(
        f"""SELECT rev_id, page_id, parent_rev_id, timestamp, file_path, offset_begin, length
            FROM read_parquet('{metadata_glob(path)}') WHERE type = 'revision' ORDER BY rev_id"""
    )
    raw = session.connection().connection  # the DBAPI (psycopg2) connection
    n = 0
    with raw.cursor() as c:
        c.execute(
            """CREATE TEMP TABLE IF NOT EXISTS temp_revisions (
                   revision_id BIGINT, page_id INTEGER, parent_revision_id BIGINT, revision_timestamp TEXT,
                   found_in_bundle INTEGER, offset_begin BIGINT, length BIGINT) ON COMMIT DROP"""
        )
        while True:
            chunk = cur.fetchmany(BATCH)
            if not chunk:
                break
            buf = io.StringIO()
            for rev_id, page_id, parent, ts, file_path, off, length in chunk:
                bundle = bundle_ids.get(file_path)
                fields = (rev_id, page_id, parent, ts, bundle, off, length)
                buf.write("\t".join(NULL if v is None else str(v) for v in fields))
                buf.write("\n")
            buf.seek(0)
            c.execute("TRUNCATE temp_revisions")
            c.copy_expert(
                "COPY temp_revisions (revision_id, page_id, parent_revision_id, revision_timestamp, found_in_bundle, offset_begin, length) FROM STDIN",
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
            n += len(chunk)
    session.commit()
    return n


def load_to_postgres(session: Session, path: str, domain: str) -> dict:
    domain_id = ensure_domain(session, domain)
    bundles = load_bundles(session, path)
    log(f"{len(bundles)} bundle(s)")
    n_pages = load_pages(session, path, domain, domain_id)
    log(f"{n_pages} page_metadata rows")
    n_revs = load_revisions(session, path, domain_id, bundles)
    log(f"{n_revs} revisions")
    return {"domain_id": domain_id, "bundles": len(bundles), "pages": n_pages, "revisions": n_revs}


# ---------------------------------------------------------------------------
# Parquet
# ---------------------------------------------------------------------------

def write_revisions_parquet(path: str, domain: str, parquet_base: str) -> int:
    """``revisions/domain=<domain>/page_bucket=<n>/`` from the metadata file.
    Replaces that domain's partitions."""
    con = duckdb.connect()
    target = layout.dataset_dir(parquet_base, "revisions")
    target.mkdir(parents=True, exist_ok=True)
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
    args = ap.parse_args(argv)

    counts = metadata_counts(args.metadata)
    log(f"metadata: {counts.get('page', 0)} page rows, {counts.get('revision', 0)} revision rows")
    session = None
    if not args.no_postgres:
        session = make_session()
        try:
            load_to_postgres(session, args.metadata, args.domain)
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
