"""Postgres → Parquet materializations (docs/design/proposed_citation_instances_denorm.md).

``export_citation_instances_denorm`` flattens ``citation_instances ⋈
normalized_citations ⋈ normalized_citation_parts`` (⋈ ``citation_source_resolution``
when ``resolved``) for one domain into the bucketed dataset the DuckDB stages
(6, 7d, 7h, 8) read, so none of them joins the big Postgres tables through a
scanner. It streams the join out of Postgres with a server-side cursor into
flat files and re-partitions them once (the same path Stage 1 uses), so it
needs no DuckDB extension. The unresolved build (Stage 4.6) carries NULL
resolution columns; the resolved build (Stage 7c) replaces it.

``wcd denorm-export --domain D [--resolved]``.
"""
from __future__ import annotations

import argparse
import os
import shutil
import time

import duckdb
import psycopg2

from wcd_schema import parquet as layout
from wcd_schema.parquet import SCHEMAS

from . import tier
from .config import load_dotenv, psycopg_dsn  # noqa: F401

DATASET = "citation_instances_denorm"
STEM = "denorm"

_COLUMNS = [f.name for f in SCHEMAS[DATASET]]

_SELECT = """
SELECT ci.id AS citation_instance_id, ci.page_id, ci.raw_sha1,
       nc.id AS normalized_id, nc.normalized_sha1,
       ci.reference_type, ci.reference_name, nc.citation_type,
       p.part_index, p.part_sha1, p.content, p.part_type,
       p.norm_subtype, p.norm_content, p.template_name, p.template_subtype,
       p.anchor_key, p.page_ref,
       p.extracted_doi, p.extracted_isbn, p.extracted_pmid, p.extracted_pmc,
       p.extracted_arxiv, p.extracted_wikidata_qid, p.extracted_librarybase_id,
       p.extracted_url, p.is_archive_url,
       p.author_key, p.title, p.year, p.year_reliable, p.id_tier, p.id_tier_label,
       {resolution}
FROM citation_instances ci
JOIN normalized_citations nc ON nc.id = ci.normalized_id
JOIN normalized_citation_parts p ON p.normalized_id = nc.id
{join}
WHERE ci.domain_id = %(d)s
ORDER BY ci.page_id, ci.id, p.part_index
"""
_UNRESOLVED = ("NULL::varchar AS global_source_id, NULL::varchar AS source_type, NULL::varchar AS match_type, "
               "NULL::double precision AS confidence", "")
_RESOLVED = ("csr.global_source_id, csr.source_type, csr.match_type, csr.confidence",
             "LEFT JOIN citation_source_resolution csr ON csr.normalized_id = p.normalized_id "
             "AND csr.part_index = p.part_index AND csr.domain_id = ci.domain_id AND csr.page_id = ci.page_id")


def log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} [denorm] {msg}", flush=True)


def expected_count(pg, domain_id: int) -> int:
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM citation_instances ci JOIN normalized_citation_parts p "
                    "ON p.normalized_id = ci.normalized_id WHERE ci.domain_id = %s", (domain_id,))
        return cur.fetchone()[0]


def export_citation_instances_denorm(dsn: str, domain: str, parquet_base: str, resolved: bool = False,
                                     scratch_dir: str | None = None) -> dict:
    """Replace the domain's ``citation_instances_denorm`` partitions. Returns counts;
    raises if the Parquet row count differs from the Postgres join count (the 4.6 / 7c gate)."""
    pg = psycopg2.connect(dsn)
    t0 = time.time()
    scratch = scratch_dir or str(layout.dataset_dir(parquet_base, DATASET) / f"_scratch_{domain}")
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT id FROM domains WHERE value = %s", (domain,))
            row = cur.fetchone()
            if row is None:
                raise SystemExit(f"domain {domain!r} is not in domains")
            domain_id = row[0]
        os.makedirs(scratch, exist_ok=True)
        writer = tier.FlatWriter(scratch, DATASET, STEM, rows_per_file=1_000_000, row_group=50_000)
        resolution, join = _RESOLVED if resolved else _UNRESOLVED
        with pg.cursor(name="denorm_export") as cur:
            cur.itersize = 50_000
            cur.execute(_SELECT.format(resolution=resolution, join=join), {"d": domain_id})
            while True:
                rows = cur.fetchmany(50_000)
                if not rows:
                    break
                writer.write(dict(zip(_COLUMNS, r)) for r in rows)
        writer.close()
        domain_dir = layout.partition_dir(parquet_base, DATASET, domain)
        if domain_dir.exists():
            shutil.rmtree(domain_dir)
        con = duckdb.connect()
        n = tier.repartition(con, writer.glob, parquet_base, DATASET, domain, STEM,
                             order_by="page_id, citation_instance_id, part_index")
        con.close()
        want = expected_count(pg, domain_id)
        if n != want:
            raise RuntimeError(f"{DATASET}: wrote {n} rows but citation_instances ⋈ parts has {want} for {domain}")
        return {"rows": n, "resolved": resolved, "seconds": round(time.time() - t0, 1)}
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
        pg.close()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Export citation_instances_denorm Parquet for a domain (Stage 4.6 unresolved, 7c resolved)")
    ap.add_argument("--domain", required=True)
    ap.add_argument("--parquet-dir", default=os.environ.get("PARQUET_DIR"), help="Parquet tier base directory (default: PARQUET_DIR)")
    ap.add_argument("--resolved", action="store_true", help="join citation_source_resolution (Stage 7c)")
    args = ap.parse_args(argv)
    if not args.parquet_dir:
        ap.error("--parquet-dir (or PARQUET_DIR) is required")
    counts = export_citation_instances_denorm(psycopg_dsn(), args.domain, args.parquet_dir, args.resolved)
    log(f"done: {counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
