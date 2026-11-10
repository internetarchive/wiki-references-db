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

import psycopg2

from wcd_schema import parquet as layout
from wcd_schema.parquet import SCHEMAS

from . import tier
from .config import duckdb_connect, load_dotenv, psycopg_dsn  # noqa: F401

DATASET = "citation_instances_denorm"
STEM = "denorm"

_COLUMNS = [f.name for f in SCHEMAS[DATASET]]

_SELECT = """
SELECT ci.id AS citation_instance_id, ci.page_id, ci.raw_sha1,
       nc.id AS normalized_id, nc.normalized_sha1,
       ci.reference_type, ci.reference_name, nc.citation_type,
       p.part_index, p.part_sha1, p.content, p.part_type,
       p.norm_subtype, p.norm_content, p.template_name, p.template_subtype,
       p.author_last, p.journal, p.pages,
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
"""
# No ORDER BY: tier.repartition sorts by (page_id, citation_instance_id,
# part_index), a unique key, so a Postgres sort of the whole join (one backend,
# ~100+ GB of wide rows on enwiki) would only be redone.
_UNRESOLVED = ("NULL::varchar AS global_source_id, NULL::varchar AS source_type, NULL::varchar AS match_type, "
               "NULL::double precision AS confidence", "")
_RESOLVED = ("csr.global_source_id, csr.source_type, csr.match_type, csr.confidence",
             "LEFT JOIN citation_source_resolution csr ON csr.normalized_id = p.normalized_id "
             "AND csr.part_index = p.part_index AND csr.domain_id = ci.domain_id AND csr.page_id = ci.page_id")


# Arrow type of a denorm column → the DuckDB type its CSV field is read as
_DUCK_TYPES = {"bool": "BOOLEAN", "double": "DOUBLE", "int8": "TINYINT", "int16": "SMALLINT",
               "int32": "INTEGER", "int64": "BIGINT", "string": "VARCHAR"}


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
        resolution, join = _RESOLVED if resolved else _UNRESOLVED
        csv_path = os.path.join(scratch, f"{STEM}.csv")
        with pg.cursor() as cur:
            # per process and doubled by hash_mem_multiplier: with 8 parallel workers
            # 1GB took Postgres past RAM on enwiki (the kernel killed a worker and the
            # server restarted); 256MB keeps the hash join under ~5 GB
            cur.execute(f"SET work_mem = '{os.environ.get('DENORM_WORK_MEM', '256MB')}'")
            # COPY (query) runs a parallel plan; a server-side cursor never does, and on
            # enwiki the serial 3-way join spilled 95 GB of hash batches before its first row
            sql = _SELECT.format(resolution=resolution, join=join).replace("%(d)s", str(int(domain_id)))
            with open(csv_path, "wb") as f:
                cur.copy_expert(f"COPY ({sql}) TO STDOUT WITH (FORMAT csv, HEADER true)", f)
        log(f"exported {os.path.getsize(csv_path) / 2**30:.1f} GiB of CSV ({time.time() - t0:.0f}s)")
        flat = os.path.join(scratch, f"{STEM}-{DATASET}-0000.flat.parquet")
        con = duckdb_connect()
        try:
            header = con.execute(f"SELECT Columns FROM sniff_csv('{csv_path}', header = true, sample_size = 1)").fetchone()[0]
            names = [c["name"] for c in header]
            if names != _COLUMNS:
                raise RuntimeError(f"{DATASET}: the export's columns are not in schema order: {names} vs {_COLUMNS}")
            # explicit types; an unquoted empty field is NULL, a quoted one an empty string
            types = ", ".join(f"'{f.name}': '{_DUCK_TYPES[str(f.type)]}'" for f in SCHEMAS[DATASET])
            con.execute(f"""COPY (SELECT * FROM read_csv('{csv_path}', header = true, columns = {{{types}}},
                                                       quote = '"', escape = '"', allow_quoted_nulls = false))
                            TO '{flat}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
            os.remove(csv_path)
            log(f"converted to Parquet ({time.time() - t0:.0f}s)")
            domain_dir = layout.partition_dir(parquet_base, DATASET, domain)
            if domain_dir.exists():
                shutil.rmtree(domain_dir)
            n = tier.repartition(con, os.path.join(scratch, "*.flat.parquet"), parquet_base, DATASET, domain, STEM,
                                 order_by="page_id, citation_instance_id, part_index")
        finally:
            con.close()
        log(f"repartitioned {n:,} rows ({time.time() - t0:.0f}s)")
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
