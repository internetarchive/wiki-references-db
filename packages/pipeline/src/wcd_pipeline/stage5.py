"""Stage 5: ref-name link resolution — ``ref_name_links``.

For every ``(page, ref_name)`` the defining citation instance per revision is
the named instance (``citation_instances.reference_name``) present in that
revision (``citation_history``); walking the page's revisions in timestamp
order, a change of defining instance — or its disappearance — closes the
current range and opens a new one. A range whose definition is still present
in the page's latest revision has ``last_revision_id = NULL``.

Inputs: ``citation_history`` and ``revisions`` from the Parquet tier (per
bucket), the named instances from Postgres. Outputs: the ``ref_name_links``
table (domain replaced in one run) and the bucketed Parquet copy read by
Stage 7d. ``wcd ref-name-links --domain D``.
"""
from __future__ import annotations

import argparse
import os
import shutil
import time

import duckdb
import psycopg2
import psycopg2.extras

from wcd_schema import parquet as layout
from wcd_schema.duckdb import bind_bucket_views

from .config import load_dotenv, psycopg_dsn  # noqa: F401

COLUMNS = ("page_id", "ref_name", "defining_instance_id", "first_revision_id", "last_revision_id")

_BUCKET_SQL = """
WITH defs AS (
    SELECT h.page_id, h.revision_id, ni.ref_name, min(ni.id) AS inst
    FROM citation_history h
    JOIN named_instances ni ON ni.id = h.citation_instance_id
    WHERE ni.page_bucket = {bucket}
    GROUP BY 1, 2, 3
), names AS (
    SELECT DISTINCT page_id, ref_name FROM defs
), seq AS (
    SELECT n.page_id, n.ref_name, r.revision_id, d.inst,
           row_number() OVER (PARTITION BY n.page_id, n.ref_name ORDER BY r.revision_timestamp, r.revision_id) AS rn
    FROM names n
    JOIN revisions r ON r.page_id = n.page_id
    LEFT JOIN defs d ON d.page_id = n.page_id AND d.ref_name = n.ref_name AND d.revision_id = r.revision_id
), page_len AS (
    SELECT page_id, max(rn) AS last_rn FROM seq GROUP BY page_id
), islands AS (
    SELECT page_id, ref_name, revision_id, inst, rn,
           rn - row_number() OVER (PARTITION BY page_id, ref_name, inst ORDER BY rn) AS grp
    FROM seq WHERE inst IS NOT NULL
)
SELECT i.page_id, i.ref_name, i.inst AS defining_instance_id,
       arg_min(i.revision_id, i.rn) AS first_revision_id,
       CASE WHEN max(i.rn) = any_value(p.last_rn) THEN NULL ELSE arg_max(i.revision_id, i.rn) END AS last_revision_id
FROM islands i JOIN page_len p USING (page_id)
GROUP BY i.page_id, i.ref_name, i.inst, i.grp
ORDER BY i.page_id, i.ref_name, first_revision_id
"""


def log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} [ref_name_links] {msg}", flush=True)


def load_named_instances(con: duckdb.DuckDBPyConnection, pg, domain_id: int) -> int:
    """Copy the domain's named instances (id, page_id, ref_name) into DuckDB."""
    con.execute("CREATE OR REPLACE TABLE named_instances (id BIGINT, page_id INTEGER, ref_name VARCHAR, page_bucket INTEGER)")
    n = 0
    with pg.cursor(name="stage5_named") as cur:
        cur.itersize = 100_000
        cur.execute("SELECT id, page_id, reference_name FROM citation_instances "
                    "WHERE domain_id = %s AND reference_name IS NOT NULL", (domain_id,))
        while True:
            rows = cur.fetchmany(100_000)
            if not rows:
                break
            con.executemany("INSERT INTO named_instances VALUES (?, ?, ?, ? % " + str(layout.PAGE_BUCKETS) + ")",
                            [(i, p, r, p) for i, p, r in rows])
            n += len(rows)
    return n


def build_bucket(con: duckdb.DuckDBPyConnection, parquet_base: str, domain: str, bucket: int) -> list[tuple]:
    bind_bucket_views(con, parquet_base, domain, bucket, ("citation_history", "revisions"))
    return con.execute(_BUCKET_SQL.format(bucket=bucket)).fetchall()


def write_bucket_parquet(con: duckdb.DuckDBPyConnection, rows: list[tuple], parquet_base: str, domain: str, bucket: int) -> None:
    d = layout.partition_dir(parquet_base, "ref_name_links", domain, bucket)
    if d.exists():
        shutil.rmtree(d)
    if not rows:
        return
    d.mkdir(parents=True)
    con.execute("CREATE OR REPLACE TEMP TABLE out_rows (page_id INTEGER, ref_name VARCHAR, defining_instance_id BIGINT, "
                "first_revision_id BIGINT, last_revision_id BIGINT)")
    con.executemany("INSERT INTO out_rows VALUES (?, ?, ?, ?, ?)", rows)
    con.execute(f"COPY (SELECT * FROM out_rows ORDER BY page_id, ref_name, first_revision_id) TO '{d / 'data.parquet'}' "
                "(FORMAT PARQUET, COMPRESSION ZSTD)")


def run(dsn: str, domain: str, parquet_base: str) -> dict:
    pg = psycopg2.connect(dsn)
    con = duckdb.connect()
    t0 = time.time()
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT id FROM domains WHERE value = %s", (domain,))
            row = cur.fetchone()
            if row is None:
                raise SystemExit(f"domain {domain!r} is not in domains")
            domain_id = row[0]
        n_named = load_named_instances(con, pg, domain_id)
        log(f"{n_named} named instances for {domain}")
        buckets = layout.list_buckets(parquet_base, "citation_history", domain)
        if not buckets:
            raise SystemExit(f"no citation_history Parquet for {domain} under {parquet_base}")
        with pg.cursor() as cur:
            cur.execute("DELETE FROM ref_name_links WHERE domain_id = %s", (domain_id,))
        pg.commit()
        total = 0
        for b in buckets:
            rows = build_bucket(con, parquet_base, domain, b)
            write_bucket_parquet(con, rows, parquet_base, domain, b)
            if rows:
                with pg.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur, "INSERT INTO ref_name_links (domain_id, page_id, ref_name, defining_instance_id, "
                             "first_revision_id, last_revision_id) VALUES %s",
                        [(domain_id, *r) for r in rows], page_size=5000)
                pg.commit()
            total += len(rows)
        return {"buckets": len(buckets), "links": total, "seconds": round(time.time() - t0, 1)}
    except Exception:
        pg.rollback()
        raise
    finally:
        con.close()
        pg.close()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Stage 5: build ref_name_links (Postgres + Parquet) for a domain")
    ap.add_argument("--domain", required=True)
    ap.add_argument("--parquet-dir", default=os.environ.get("PARQUET_DIR"), help="Parquet tier base directory (default: PARQUET_DIR)")
    ap.add_argument("--no-gate", action="store_true")
    args = ap.parse_args(argv)
    if not args.parquet_dir:
        ap.error("--parquet-dir (or PARQUET_DIR) is required")
    dsn = psycopg_dsn()
    counts = run(dsn, args.domain, args.parquet_dir)
    log(f"done: {counts}")
    if not args.no_gate:
        from .gates import make_session, report, stage5
        session = make_session()
        try:
            return report("5", stage5(session, args.domain, args.parquet_dir))
        finally:
            session.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
