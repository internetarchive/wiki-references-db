"""Inter-stage validation gates (docs/design/proposed_data_integrity.md §4).

Each ``stageN(session, domain, parquet_base, …)`` returns a list of failure
messages (empty = pass). ``wcd gate <stage> --domain …`` runs one from the
command line and exits non-zero on failure; the stage drivers call them
directly at the end of their run.
"""
from __future__ import annotations

import argparse
import os
import random
import time

import duckdb
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from wcd_schema import parquet as layout
from wcd_schema.ids import part_sha1
from wcd_schema.postgres import (
    CitationInstance, Domain, NormalizedCitation, RefNameLink, Revision,
)

from .config import database_url, load_dotenv  # noqa: F401


def log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} [gate] {msg}", flush=True)


def make_session() -> Session:
    engine = create_engine(database_url(), poolclass=NullPool, hide_parameters=True)
    return sessionmaker(bind=engine)()


def _domain_id(session: Session, domain: str):
    return session.execute(select(Domain.id).where(Domain.value == domain)).scalar_one_or_none()


def _parquet_count(con, pattern: str) -> int:
    try:
        return con.execute(f"SELECT count(*) FROM read_parquet('{pattern}', hive_partitioning=true)").fetchone()[0]
    except duckdb.IOException:
        return 0


# ---------------------------------------------------------------------------
# Stage 0
# ---------------------------------------------------------------------------

def stage0(session: Session | None, domain: str, parquet_base: str | None) -> list[str]:
    """revisions has rows for the domain; every revision's page has page_metadata;
    revision_timestamp is a TIMESTAMP; the revisions Parquet matches Postgres."""
    failures = []
    pg_count = None
    if session is not None:
        domain_id = _domain_id(session, domain)
        if domain_id is None:
            return [f"domain {domain!r} is not in domains"]
        pg_count = session.execute(select(func.count()).select_from(Revision).where(Revision.domain_id == domain_id)).scalar_one()
        if pg_count == 0:
            failures.append("revisions has no rows for the domain")
        missing = session.execute(text(
            "SELECT count(*) FROM (SELECT DISTINCT page_id FROM revisions WHERE domain_id = :d) r "
            "LEFT JOIN page_metadata p ON p.domain_id = :d AND p.page_id = r.page_id WHERE p.page_id IS NULL"
        ), {"d": domain_id}).scalar_one()
        if missing:
            failures.append(f"{missing} page(s) in revisions have no page_metadata row")
        ts_type = session.execute(text(
            "SELECT data_type FROM information_schema.columns WHERE table_name = 'revisions' AND column_name = 'revision_timestamp'"
        )).scalar_one_or_none()
        if ts_type not in ("timestamp without time zone", "timestamp with time zone"):
            failures.append(f"revisions.revision_timestamp is {ts_type!r}, not TIMESTAMP")
    if parquet_base is not None:
        con = duckdb.connect()
        pq_count = _parquet_count(con, layout.partition_glob(parquet_base, "revisions", domain))
        if pq_count == 0:
            failures.append(f"no revisions Parquet for domain {domain} under {parquet_base}")
        elif pg_count is not None and pq_count != pg_count:
            failures.append(f"revisions Parquet has {pq_count} rows but Postgres has {pg_count} for the domain")
    return failures


# ---------------------------------------------------------------------------
# Stage 3 (after load)
# ---------------------------------------------------------------------------

def stage3(session: Session, domain: str, parquet_base: str, sample_buckets: int | None = 16,
           seed: int = 0) -> list[str]:
    """citation_instances and normalized_citations are non-empty for the domain;
    no NULL citation_type; every citation_history instance id (in a sample of
    buckets, or all) exists in citation_instances; the revisions Parquet
    matches Postgres."""
    failures = []
    domain_id = _domain_id(session, domain)
    if domain_id is None:
        return [f"domain {domain!r} is not in domains"]
    n_inst = session.execute(select(func.count()).select_from(CitationInstance)
                             .where(CitationInstance.domain_id == domain_id)).scalar_one()
    n_norm = session.execute(select(func.count()).select_from(NormalizedCitation)
                             .where(NormalizedCitation.appears_on_domain_id == domain_id)).scalar_one()
    if n_inst == 0:
        failures.append("citation_instances has no rows for the domain")
    if n_norm == 0:
        failures.append("normalized_citations has no rows for the domain")
    null_types = session.execute(select(func.count()).select_from(NormalizedCitation)
                                 .where(NormalizedCitation.citation_type.is_(None))).scalar_one()
    if null_types:
        failures.append(f"{null_types} normalized_citations rows have NULL citation_type")

    con = duckdb.connect()
    buckets = layout.list_buckets(parquet_base, "citation_history", domain)
    if not buckets:
        failures.append(f"no citation_history Parquet for domain {domain} under {parquet_base}")
    else:
        if sample_buckets is not None and len(buckets) > sample_buckets:
            buckets = sorted(random.Random(seed).sample(buckets, sample_buckets))
        missing_total = 0
        for b in buckets:
            ids = [r[0] for r in con.execute(
                f"SELECT DISTINCT citation_instance_id FROM read_parquet('{layout.partition_glob(parquet_base, 'citation_history', domain, b)}')"
            ).fetchall()]
            found = 0
            for i in range(0, len(ids), 50_000):
                chunk = ids[i:i + 50_000]
                found += session.execute(text(
                    "SELECT count(*) FROM citation_instances WHERE domain_id = :d AND id = ANY(:ids)"
                ), {"d": domain_id, "ids": chunk}).scalar_one()
            missing_total += len(ids) - found
        if missing_total:
            failures.append(f"{missing_total} citation_history instance id(s) in {len(buckets)} checked bucket(s) "
                            "have no citation_instances row")

    pg_rev = session.execute(select(func.count()).select_from(Revision).where(Revision.domain_id == domain_id)).scalar_one()
    pq_rev = _parquet_count(con, layout.partition_glob(parquet_base, "revisions", domain))
    if pq_rev != pg_rev:
        failures.append(f"revisions Parquet has {pq_rev} rows but Postgres has {pg_rev} for the domain")
    return failures


# ---------------------------------------------------------------------------
# Stage 4 (after parts)
# ---------------------------------------------------------------------------

def stage4(session: Session, domain: str, sample: int = 1000, seed: int = 0) -> list[str]:
    """every normalized citation of the domain has ≥ 1 part; bare_url /
    external_link parts carry extracted_url; template parts carry
    template_name; part_sha1 = sha1(content) on a sample."""
    failures = []
    domain_id = _domain_id(session, domain)
    if domain_id is None:
        return [f"domain {domain!r} is not in domains"]
    no_parts = session.execute(text(
        "SELECT count(*) FROM normalized_citations nc LEFT JOIN normalized_citation_parts p ON p.normalized_id = nc.id "
        "WHERE nc.appears_on_domain_id = :d AND p.normalized_id IS NULL"), {"d": domain_id}).scalar_one()
    if no_parts:
        failures.append(f"{no_parts} normalized_citations rows have no part")
    no_url = session.execute(text(
        "SELECT count(*) FROM normalized_citation_parts p JOIN normalized_citations nc ON nc.id = p.normalized_id "
        "WHERE nc.appears_on_domain_id = :d AND nc.citation_type IN ('bare_url', 'external_link') AND p.extracted_url IS NULL"),
        {"d": domain_id}).scalar_one()
    if no_url:
        failures.append(f"{no_url} bare_url/external_link parts have no extracted_url")
    no_tpl = session.execute(text(
        "SELECT count(*) FROM normalized_citation_parts p JOIN normalized_citations nc ON nc.id = p.normalized_id "
        "WHERE nc.appears_on_domain_id = :d AND p.part_type = 'template' AND p.template_name IS NULL"),
        {"d": domain_id}).scalar_one()
    if no_tpl:
        failures.append(f"{no_tpl} template parts have no template_name")
    rows = session.execute(text(
        "SELECT p.part_sha1, p.content FROM normalized_citation_parts p JOIN normalized_citations nc ON nc.id = p.normalized_id "
        "WHERE nc.appears_on_domain_id = :d ORDER BY p.normalized_id, p.part_index LIMIT :n OFFSET :o"),
        {"d": domain_id, "n": sample, "o": 0}).all()
    bad = sum(1 for sha, content in rows if part_sha1(content) != sha)
    if bad:
        failures.append(f"{bad} of {len(rows)} sampled parts have part_sha1 <> sha1(content)")
    return failures


# ---------------------------------------------------------------------------
# Stage 5 (after ref_name_links)
# ---------------------------------------------------------------------------

def stage5(session: Session, domain: str, parquet_base: str | None = None) -> list[str]:
    """no overlapping revision ranges per (domain_id, page_id, ref_name); every
    defining_instance_id exists; the Parquet copy matches Postgres."""
    failures = []
    domain_id = _domain_id(session, domain)
    if domain_id is None:
        return [f"domain {domain!r} is not in domains"]
    overlaps = session.execute(text("""
        SELECT count(*) FROM ref_name_links a JOIN ref_name_links b
          ON a.domain_id = b.domain_id AND a.page_id = b.page_id AND a.ref_name = b.ref_name
         AND a.first_revision_id < b.first_revision_id
         AND (a.last_revision_id IS NULL OR a.last_revision_id >= b.first_revision_id)
        WHERE a.domain_id = :d"""), {"d": domain_id}).scalar_one()
    if overlaps:
        failures.append(f"{overlaps} overlapping ref_name_links range pair(s)")
    dangling = session.execute(text(
        "SELECT count(*) FROM ref_name_links r LEFT JOIN citation_instances ci ON ci.id = r.defining_instance_id "
        "WHERE r.domain_id = :d AND ci.id IS NULL"), {"d": domain_id}).scalar_one()
    if dangling:
        failures.append(f"{dangling} ref_name_links rows point at a missing citation instance")
    if parquet_base is not None:
        pg_n = session.execute(select(func.count()).select_from(RefNameLink).where(RefNameLink.domain_id == domain_id)).scalar_one()
        pq_n = _parquet_count(duckdb.connect(), layout.partition_glob(parquet_base, "ref_name_links", domain))
        if pg_n != pq_n:
            failures.append(f"ref_name_links Parquet has {pq_n} rows but Postgres has {pg_n} for the domain")
    return failures


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

STAGES = ("0", "3", "4", "5", "6")


def run(stage: str, session: Session, domain: str, parquet_base: str | None, **kw) -> list[str]:
    if stage == "0":
        return stage0(session, domain, parquet_base)
    if stage == "3":
        return stage3(session, domain, parquet_base, **kw)
    if stage == "4":
        return stage4(session, domain)
    if stage == "5":
        return stage5(session, domain, parquet_base)
    if stage == "6":
        from .stage6 import gate as stage6
        return stage6(parquet_base, domain)
    raise ValueError(stage)


def report(stage: str, failures: list[str]) -> int:
    if failures:
        for f in failures:
            log(f"Stage {stage} GATE FAIL: {f}")
        return 1
    log(f"Stage {stage} gate: pass")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Run an inter-stage validation gate")
    ap.add_argument("stage", choices=STAGES)
    ap.add_argument("--domain", required=True)
    ap.add_argument("--parquet-dir", default=os.environ.get("PARQUET_DIR"), help="Parquet tier base directory (default: PARQUET_DIR)")
    ap.add_argument("--all-buckets", action="store_true", help="Stage 3: check every bucket instead of a sample of 16")
    args = ap.parse_args(argv)
    if args.stage in ("3", "6") and not args.parquet_dir:
        ap.error("--parquet-dir (or PARQUET_DIR) is required for this gate")
    session = make_session() if args.stage != "6" else None
    try:
        kw = {"sample_buckets": None} if (args.stage == "3" and args.all_buckets) else {}
        return report(args.stage, run(args.stage, session, args.domain, args.parquet_dir, **kw))
    finally:
        if session is not None:
            session.close()


if __name__ == "__main__":
    raise SystemExit(main())
