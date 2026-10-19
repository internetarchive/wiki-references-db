"""Stage 8: ``page_revision_stats`` (docs/design/proposed_page_revision_stats.md).

Per-revision citation metrics for every page — every revision of the
Stage-0 ``revisions`` dataset appears, zeros included. Inputs are the bucket
partitions of ``revisions``, ``citation_history``, ``named_ref_uses`` and the
*resolved* ``citation_instances_denorm`` (7c); output is
``page_revision_stats/domain=/page_bucket=/stats.parquet``. No Postgres.

``wcd revision-stats --domain D [--jobs N] [--buckets …]``
"""
from __future__ import annotations

import argparse
import os
import shutil
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

import duckdb

from wcd_schema import parquet as layout
from wcd_schema.duckdb import bind_bucket_views

from .config import load_dotenv  # noqa: F401

STATS_SQL = """
WITH resolved AS (
    SELECT dn.page_id, ch.revision_id AS rev_id, dn.citation_instance_id,
           dn.citation_type, dn.part_index, dn.global_source_id, dn.confidence
    FROM citation_history ch
    JOIN citation_instances_denorm dn ON dn.citation_instance_id = ch.citation_instance_id
),
rev_agg AS (
    SELECT page_id, rev_id,
        count(DISTINCT citation_instance_id)                                        AS n_total_citations,
        count(DISTINCT global_source_id)                                            AS n_unique_sources,
        count(DISTINCT citation_instance_id) FILTER (WHERE global_source_id IS NULL) AS n_unresolved,
        count(DISTINCT global_source_id) FILTER (WHERE confidence >= 0.85)          AS n_high_confidence,
        count(DISTINCT citation_instance_id) FILTER (WHERE citation_type = 'ref_tag')       AS n_ref_tag,
        count(DISTINCT citation_instance_id) FILTER (WHERE citation_type = 'template')      AS n_template,
        count(DISTINCT citation_instance_id) FILTER (WHERE citation_type = 'bare_url')      AS n_bare_url,
        count(DISTINCT citation_instance_id) FILTER (WHERE citation_type = 'endnote')       AS n_endnote,
        count(DISTINCT citation_instance_id) FILTER (WHERE citation_type = 'shorthand')     AS n_shorthand,
        count(DISTINCT citation_instance_id) FILTER (WHERE citation_type = 'external_link') AS n_external_link
    FROM resolved GROUP BY page_id, rev_id
),
self_closing AS (
    SELECT page_id, revision_id AS rev_id, count(*) AS n_self_closing
    FROM named_ref_uses GROUP BY page_id, revision_id
),
source_first AS (
    SELECT page_id, global_source_id, min(rev_id) AS first_rev_id
    FROM resolved WHERE global_source_id IS NOT NULL
    GROUP BY page_id, global_source_id
),
new_per_rev AS (
    SELECT page_id, first_rev_id AS rev_id, count(*) AS n_new_sources
    FROM source_first GROUP BY page_id, first_rev_id
)
SELECT
    r.page_id, r.revision_id AS rev_id, r.revision_timestamp AS timestamp,
    CAST(coalesce(a.n_total_citations, 0) AS INTEGER) AS n_total_citations,
    CAST(coalesce(sc.n_self_closing, 0)   AS INTEGER) AS n_self_closing,
    CAST(coalesce(a.n_unique_sources, 0)  AS INTEGER) AS n_unique_sources,
    CAST(coalesce(a.n_unresolved, 0)      AS INTEGER) AS n_unresolved,
    CAST(coalesce(a.n_high_confidence, 0) AS INTEGER) AS n_high_confidence,
    CAST(coalesce(n.n_new_sources, 0)     AS INTEGER) AS n_new_sources,
    round((coalesce(a.n_total_citations, 0) - coalesce(a.n_unresolved, 0))
          / greatest(coalesce(a.n_total_citations, 0), 1.0), 4) AS pct_resolved,
    CAST(coalesce(a.n_ref_tag, 0)       AS INTEGER) AS n_ref_tag,
    CAST(coalesce(a.n_template, 0)      AS INTEGER) AS n_template,
    CAST(coalesce(a.n_bare_url, 0)      AS INTEGER) AS n_bare_url,
    CAST(coalesce(a.n_endnote, 0)       AS INTEGER) AS n_endnote,
    CAST(coalesce(a.n_shorthand, 0)     AS INTEGER) AS n_shorthand,
    CAST(coalesce(a.n_external_link, 0) AS INTEGER) AS n_external_link
FROM revisions r
LEFT JOIN rev_agg a       ON a.page_id = r.page_id AND a.rev_id = r.revision_id
LEFT JOIN self_closing sc ON sc.page_id = r.page_id AND sc.rev_id = r.revision_id
LEFT JOIN new_per_rev n   ON n.page_id = r.page_id AND n.rev_id = r.revision_id
ORDER BY r.page_id, r.revision_id
"""


def log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} [revision_stats] {msg}", flush=True)


def build_bucket(parquet_base: str, domain: str, bucket: int) -> dict:
    t0 = time.time()
    con = duckdb.connect()
    try:
        bind_bucket_views(con, parquet_base, domain, bucket,
                          ("revisions", "citation_history", "named_ref_uses", "citation_instances_denorm"))
        d = layout.partition_dir(parquet_base, "page_revision_stats", domain, bucket)
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)
        con.execute(f"COPY ({STATS_SQL}) TO '{d / 'stats.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)")
        n = con.execute(f"SELECT count(*) FROM read_parquet('{d / 'stats.parquet'}')").fetchone()[0]
    finally:
        con.close()
    return {"bucket": bucket, "rows": n, "seconds": round(time.time() - t0, 1)}


def gate(parquet_base: str, domain: str) -> list[str]:
    """one stats row per revision of the domain."""
    con = duckdb.connect()
    try:
        n_rev = con.execute(f"SELECT count(*) FROM read_parquet('{layout.partition_glob(parquet_base, 'revisions', domain)}', hive_partitioning=true)").fetchone()[0]
        try:
            n_stats = con.execute(f"SELECT count(*) FROM read_parquet('{layout.partition_glob(parquet_base, 'page_revision_stats', domain)}', hive_partitioning=true)").fetchone()[0]
        except duckdb.IOException:
            n_stats = 0
    finally:
        con.close()
    return [] if n_stats == n_rev else [f"page_revision_stats has {n_stats} rows but revisions has {n_rev} for {domain}"]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Stage 8: per-revision citation stats (Parquet) for a domain")
    ap.add_argument("--domain", required=True)
    ap.add_argument("--parquet-dir", default=os.environ.get("PARQUET_DIR"), help="Parquet tier base directory (default: PARQUET_DIR)")
    ap.add_argument("--buckets", help="comma-separated page buckets (default: every bucket of revisions)")
    ap.add_argument("--jobs", type=int, default=max(1, (os.cpu_count() or 2) // 2))
    ap.add_argument("--no-gate", action="store_true")
    args = ap.parse_args(argv)
    if not args.parquet_dir:
        ap.error("--parquet-dir (or PARQUET_DIR) is required")
    buckets = [int(b) for b in args.buckets.split(",")] if args.buckets else layout.list_buckets(args.parquet_dir, "revisions", args.domain)
    if not buckets:
        ap.error(f"no revisions partitions for {args.domain} under {args.parquet_dir}")
    t0 = time.time()
    total = 0
    if args.jobs > 1 and len(buckets) > 1:
        with ProcessPoolExecutor(max_workers=args.jobs) as pool:
            for f in as_completed([pool.submit(build_bucket, args.parquet_dir, args.domain, b) for b in buckets]):
                r = f.result()
                total += r["rows"]
    else:
        for b in buckets:
            total += build_bucket(args.parquet_dir, args.domain, b)["rows"]
    log(f"{total} rows over {len(buckets)} bucket(s) in {time.time() - t0:.1f}s")
    if not args.no_gate and not args.buckets:
        failures = gate(args.parquet_dir, args.domain)
        if failures:
            for f in failures:
                log(f"Stage 8 GATE FAIL: {f}")
            return 1
        log("Stage 8 gate: pass")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
