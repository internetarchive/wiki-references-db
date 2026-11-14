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
import glob
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


# Longest CSV row DuckDB accepts (DENORM_MAX_LINE_SIZE). enwiki's longest
# reference is 1.96 MB; a row holds its text a few times over and CSV doubles
# every quote, so 64 MB leaves a wide margin (the read buffer is twice this).
DEFAULT_MAX_LINE_SIZE = 64 * 2**20

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


def _repartition_in_groups(con, flat: str, groups_dir: str, parquet_base: str, domain: str) -> int:
    """``tier.repartition`` of one large flat file, ``DENORM_GROUPS`` (16) passes
    of PAGE_BUCKETS / groups buckets each. In one pass, the sort of enwiki's
    ~350M wide rows and the 512-way partitioned write ran DuckDB out of memory
    at 16 GB. First the file is split by bucket group (no sort, a handful of
    partitions, small row groups), then each group is sorted and partitioned
    on its own. Returns the row count."""
    groups = int(os.environ.get("DENORM_GROUPS", "16"))
    per = layout.PAGE_BUCKETS // groups
    shutil.rmtree(groups_dir, ignore_errors=True)
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"""COPY (SELECT *, (page_id % {layout.PAGE_BUCKETS}) // {per} AS bucket_group
                          FROM read_parquet('{flat}'))
                    TO '{groups_dir}' (FORMAT PARQUET, PARTITION_BY (bucket_group), COMPRESSION ZSTD,
                                       ROW_GROUP_SIZE 20000)""")
    con.execute("SET preserve_insertion_order = true")
    domain_dir = layout.partition_dir(parquet_base, DATASET, domain)
    if domain_dir.exists():
        shutil.rmtree(domain_dir)
    n = 0
    for g in sorted(os.listdir(groups_dir)):
        n += tier.repartition(con, os.path.join(groups_dir, g, "*.parquet"), parquet_base, DATASET, domain, STEM,
                              order_by="page_id, citation_instance_id, part_index")
        log(f"repartitioned {g}: {n:,} rows so far")
    shutil.rmtree(groups_dir, ignore_errors=True)
    return n


def export_citation_instances_denorm(dsn: str, domain: str, parquet_base: str, resolved: bool = False,
                                     scratch_dir: str | None = None, reuse_export: bool = False) -> dict:
    """Replace the domain's ``citation_instances_denorm`` partitions. Returns counts;
    raises if the Parquet row count differs from the Postgres join count (the 4.6 / 7c gate).

    The scratch directory (the exported CSV, ~200 GB for enwiki) is removed after
    a successful run and kept after a failed one; ``reuse_export`` converts a
    complete CSV left there instead of exporting again (only valid when the
    tables have not changed since)."""
    pg = psycopg2.connect(dsn)
    t0 = time.time()
    scratch = scratch_dir or str(layout.dataset_dir(parquet_base, DATASET) / f"_scratch_{domain}")
    ok = False
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
        if reuse_export and os.path.exists(csv_path):
            log(f"reusing the exported CSV ({os.path.getsize(csv_path) / 2**30:.1f} GiB)")
        else:
            with pg.cursor() as cur:
                # per process and doubled by hash_mem_multiplier: with 8 parallel workers
                # 1GB took Postgres past RAM on enwiki (the kernel killed a worker and the
                # server restarted); 256MB keeps the hash join under ~5 GB
                cur.execute(f"SET work_mem = '{os.environ.get('DENORM_WORK_MEM', '256MB')}'")
                # COPY (query) runs a parallel plan; a server-side cursor never does, and on
                # enwiki the serial 3-way join spilled 95 GB of hash batches before its first row
                sql = _SELECT.format(resolution=resolution, join=join).replace("%(d)s", str(int(domain_id)))
                with open(csv_path + ".partial", "wb") as f:
                    cur.copy_expert(f"COPY ({sql}) TO STDOUT WITH (FORMAT csv, HEADER true)", f)
            os.replace(csv_path + ".partial", csv_path)
            log(f"exported {os.path.getsize(csv_path) / 2**30:.1f} GiB of CSV ({time.time() - t0:.0f}s)")
        flat = os.path.join(scratch, f"{STEM}-{DATASET}-0000.flat.parquet")
        con = duckdb_connect()
        try:
            if reuse_export and os.path.exists(flat):
                log(f"reusing the converted Parquet ({os.path.getsize(flat) / 2**30:.1f} GiB)")
            else:
                with open(csv_path, encoding="utf-8") as f:
                    names = f.readline().rstrip("\n").split(",")   # COPY's header: bare column names
                if names != _COLUMNS:
                    raise RuntimeError(f"{DATASET}: the export's columns are not in schema order: {names} vs {_COLUMNS}")
                # explicit types; an unquoted empty field is NULL, a quoted one an empty string
                types = ", ".join(f"'{f.name}': '{_DUCK_TYPES[str(f.type)]}'" for f in SCHEMAS[DATASET])
                # a row carries a reference's text several times (content, norm_content,
                # title, …); enwiki has one of 2.8 MB, over DuckDB's 2 MB default
                max_line = int(os.environ.get("DENORM_MAX_LINE_SIZE", str(DEFAULT_MAX_LINE_SIZE)))
                con.execute(f"""COPY (SELECT * FROM read_csv('{csv_path}', header = true, columns = {{{types}}},
                                                           quote = '"', escape = '"', allow_quoted_nulls = false,
                                                           max_line_size = {max_line}, buffer_size = {2 * max_line}))
                                TO '{flat}.partial' (FORMAT PARQUET, COMPRESSION ZSTD)""")
                os.replace(flat + ".partial", flat)
                log(f"converted to Parquet ({time.time() - t0:.0f}s)")
            n = _repartition_in_groups(con, flat, os.path.join(scratch, "groups"), parquet_base, domain)
        finally:
            con.close()
        log(f"repartitioned {n:,} rows ({time.time() - t0:.0f}s)")
        want = expected_count(pg, domain_id)
        if n != want:
            raise RuntimeError(f"{DATASET}: wrote {n} rows but citation_instances ⋈ parts has {want} for {domain}")
        ok = True
        return {"rows": n, "resolved": resolved, "seconds": round(time.time() - t0, 1)}
    finally:
        if ok:
            shutil.rmtree(scratch, ignore_errors=True)
        else:
            for partial in glob.glob(os.path.join(glob.escape(scratch), "*.partial")):
                os.remove(partial)
        pg.close()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Export citation_instances_denorm Parquet for a domain (Stage 4.6 unresolved, 7c resolved)")
    ap.add_argument("--domain", required=True)
    ap.add_argument("--parquet-dir", default=os.environ.get("PARQUET_DIR"), help="Parquet tier base directory (default: PARQUET_DIR)")
    ap.add_argument("--resolved", action="store_true", help="join citation_source_resolution (Stage 7c)")
    ap.add_argument("--reuse-export", action="store_true",
                    help="convert the complete CSV a failed run left in the scratch directory instead of exporting "
                         "again (only if the tables have not changed since)")
    args = ap.parse_args(argv)
    if not args.parquet_dir:
        ap.error("--parquet-dir (or PARQUET_DIR) is required")
    counts = export_citation_instances_denorm(psycopg_dsn(), args.domain, args.parquet_dir, args.resolved,
                                              reuse_export=args.reuse_export)
    log(f"done: {counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
