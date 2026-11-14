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
import io
import os
import shutil
import time

import duckdb
import psycopg2
import pyarrow as pa
import pyarrow.csv as pa_csv

from wcd_schema import parquet as layout
from wcd_schema.duckdb import bind_bucket_views

from . import fkeys
from .config import duckdb_connect, load_dotenv, psycopg_dsn  # noqa: F401
from .gates import make_session

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
    """Copy the domain's named instances (id, page_id, ref_name) into DuckDB,
    100k rows at a time as Arrow batches (row-by-row ``executemany`` was the
    slow part at enwiki scale)."""
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
            ids, pages, names = zip(*rows)
            batch = pa.table({"id": pa.array(ids, pa.int64()), "page_id": pa.array(pages, pa.int32()),
                              "ref_name": pa.array(names, pa.string())})
            con.register("named_batch", batch)
            con.execute(f"INSERT INTO named_instances SELECT id, page_id, ref_name, page_id % {layout.PAGE_BUCKETS} "
                        "FROM named_batch")
            con.unregister("named_batch")
            n += len(rows)
    return n


def build_bucket(con: duckdb.DuckDBPyConnection, parquet_base: str, domain: str, bucket: int) -> int:
    """The bucket's links into the DuckDB table ``out_rows``; returns their count."""
    bind_bucket_views(con, parquet_base, domain, bucket, ("citation_history", "revisions"))
    con.execute(f"""CREATE OR REPLACE TEMP TABLE out_rows AS
        SELECT CAST(page_id AS INTEGER) AS page_id, CAST(ref_name AS VARCHAR) AS ref_name,
               CAST(defining_instance_id AS BIGINT) AS defining_instance_id,
               CAST(first_revision_id AS BIGINT) AS first_revision_id,
               CAST(last_revision_id AS BIGINT) AS last_revision_id
        FROM ({_BUCKET_SQL.format(bucket=bucket)})""")
    return con.execute("SELECT count(*) FROM out_rows").fetchone()[0]


def write_bucket_parquet(con: duckdb.DuckDBPyConnection, n_rows: int, parquet_base: str, domain: str, bucket: int) -> None:
    d = layout.partition_dir(parquet_base, "ref_name_links", domain, bucket)
    if d.exists():
        shutil.rmtree(d)
    if not n_rows:
        return
    d.mkdir(parents=True)
    con.execute(f"COPY (SELECT * FROM out_rows ORDER BY page_id, ref_name, first_revision_id) TO '{d / 'data.parquet'}' "
                "(FORMAT PARQUET, COMPRESSION ZSTD)")


def write_bucket_postgres(con: duckdb.DuckDBPyConnection, cur, domain_id: int) -> None:
    """``COPY`` the bucket's ``out_rows`` into ``ref_name_links`` (NULLs as
    unquoted empty fields, every other value quoted)."""
    result = con.execute(f"SELECT CAST({int(domain_id)} AS INTEGER) AS domain_id, * FROM out_rows")
    table = (getattr(result, "to_arrow_table", None) or result.fetch_arrow_table)()
    buf = io.BytesIO()
    pa_csv.write_csv(table, buf, pa_csv.WriteOptions(include_header=False, quoting_style="all_valid"))
    buf.seek(0)
    cur.copy_expert("COPY ref_name_links (domain_id, " + ", ".join(COLUMNS) + ") FROM STDIN WITH (FORMAT csv)", buf)


def run(dsn: str, domain: str, parquet_base: str, defer_fks: bool = True) -> dict:
    pg = psycopg2.connect(dsn)
    con = duckdb_connect()
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
        # each row's foreign-key check would lock a citation_instances row (338M on enwiki)
        session = make_session()
        try:
            if defer_fks:
                fkeys.drop_if_empty(session, "ref_name_links", log)
            total = 0
            for i, b in enumerate(buckets, 1):
                n = build_bucket(con, parquet_base, domain, b)
                write_bucket_parquet(con, n, parquet_base, domain, b)
                if n:
                    with pg.cursor() as cur:
                        write_bucket_postgres(con, cur, domain_id)
                    pg.commit()
                total += n
                if i % 32 == 0:
                    log(f"{i}/{len(buckets)} buckets, {total} links ({time.time() - t0:.0f}s)")
            fkeys.restore(session, "ref_name_links", log)
        finally:
            session.close()
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
    ap.add_argument("--defer-fks", action=argparse.BooleanOptionalAction, default=True,
                    help="drop ref_name_links' foreign keys while it is empty, validate them after (default: on)")
    args = ap.parse_args(argv)
    if not args.parquet_dir:
        ap.error("--parquet-dir (or PARQUET_DIR) is required")
    dsn = psycopg_dsn()
    counts = run(dsn, args.domain, args.parquet_dir, args.defer_fks)
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
