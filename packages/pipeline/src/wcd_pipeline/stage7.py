"""Stage 7: materialize the matching results (docs/design/proposed_matching.md, Stage 7).

Steps, sequential, each replacing its output for the domain:

  7a  global_sources             ← matching_intermediate/global_sources_stage (upsert; counts/score NULL)
  7b  citation_source_resolution ← merged_citation_source_map ⋈ global_source_map ⋈ parts (⋈ instances on the page)
  7c  citation_instances_denorm  ← resolved re-export (wcd_pipeline.materialize)
  7d  page_source_summary        ← per bucket: denorm × history × revisions (+ named_ref_uses × ref_name_links)
  7e  global_sources counts      ← n_pages / n_total_citations from page_source_summary
  7f  reliability + verdict      ← wcd_semantic.scoring over the canonical member's fields
  7g  global_source_scores       ← Parquet export of 7e/7f
  7h  page_summary_cache         ← per bucket: the /summary response (wcd_semantic.profile)

DuckDB computes per bucket from the Parquet tier; psycopg2 writes to Postgres
(no DuckDB Postgres extension needed). 7d and 7h checkpoint per bucket in
``<parquet>/_checkpoints/<step>_<domain>.json`` and resume after a failure.

``wcd materialize --domain D [--steps a,b,…] [--force]``
"""
from __future__ import annotations

import argparse
import io
import json
import os
import shutil
import time
from pathlib import Path

import duckdb
import psycopg2
import psycopg2.extras
import pyarrow as pa
import pyarrow.csv as pa_csv

from wcd_schema import parquet as layout
from wcd_schema.duckdb import bind_bucket_views
from wcd_semantic.profile import build_source_profile
from wcd_semantic.scoring import compute_score

from .config import duckdb_connect, load_dotenv, psycopg_dsn  # noqa: F401
from .load import _CSV_OPTIONS, _copy_and_insert
from .materialize import export_citation_instances_denorm

MI = "matching_intermediate/"
STEPS = ("a", "b", "c", "d", "e", "f", "g", "h")
TOP_TEMPLATE_LIMIT = 10


def log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} [materialize] {msg}", flush=True)


def _domain_id(pg, domain: str) -> int:
    with pg.cursor() as cur:
        cur.execute("SELECT id FROM domains WHERE value = %s", (domain,))
        row = cur.fetchone()
    if row is None:
        raise SystemExit(f"domain {domain!r} is not in domains")
    return row[0]


class Checkpoint:
    def __init__(self, parquet_base: str, step: str, domain: str):
        self.path = Path(parquet_base) / "_checkpoints" / f"{step}_{domain}.json"
        self.done: set[int] = set()
        if self.path.exists():
            self.done = set(json.loads(self.path.read_text()))

    def mark(self, bucket: int) -> None:
        self.done.add(bucket)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(sorted(self.done)))

    def clear(self) -> None:
        self.done = set()
        if self.path.exists():
            self.path.unlink()


# ---------------------------------------------------------------------------
# 7a global_sources
# ---------------------------------------------------------------------------

_GS_COLS = ("global_source_id", "match_type", "confidence", "canonical_part_sha1", "source_type", "author_key",
            "year", "title", "extracted_doi", "extracted_isbn", "extracted_pmid", "extracted_pmc", "extracted_arxiv",
            "extracted_wikidata_qid", "extracted_url")


_GS_DDL = ("global_source_id varchar(16), match_type varchar, confidence double precision, canonical_part_sha1 char(40), "
           "source_type varchar, author_key varchar, year varchar, title text, extracted_doi varchar, "
           "extracted_isbn varchar, extracted_pmid varchar, extracted_pmc varchar, extracted_arxiv varchar, "
           "extracted_wikidata_qid varchar, extracted_url text")
# rows per COPY into a temp table and set-based upsert (one transaction each)
CHUNK_ROWS = int(os.getenv("MATERIALIZE_CHUNK_ROWS", "500000"))
# work_mem for 7e/7f's whole-table UPDATEs (single backend, no parallel workers)
STAGE7_WORK_MEM = os.getenv("STAGE7_WORK_MEM", "1GB")


def _batches(con, sql: str):
    """``sql``'s result from DuckDB as Arrow record batches of CHUNK_ROWS."""
    result = con.execute(sql)
    reader = (getattr(result, "to_arrow_reader", None) or result.fetch_record_batch)(CHUNK_ROWS)
    yield from reader


def step_a(pg, con, domain: str, parquet_base: str) -> int:
    """Upsert the domain's global sources (~131M on enwiki: streamed from DuckDB in
    chunks, COPY + one INSERT … ON CONFLICT per chunk, in id order so the primary
    key grows at its end)."""
    pattern = layout.partition_glob(parquet_base, MI + "global_sources_stage", domain)
    cols = ", ".join(_GS_COLS)
    upsert = (f"INSERT INTO global_sources ({cols}) SELECT {cols} FROM gs_in "
              "ON CONFLICT (global_source_id) DO UPDATE SET "
              + ", ".join(f"{c} = EXCLUDED.{c}" for c in _GS_COLS[1:])
              + ", n_pages = NULL, n_total_citations = NULL, reliability_score = NULL, verdict = NULL")
    n = 0
    with pg.cursor() as cur:
        for batch in _batches(con, f"SELECT {cols} FROM read_parquet('{pattern}') ORDER BY global_source_id"):
            _copy_and_insert(cur, batch, "gs_in", _GS_DDL, list(_GS_COLS), upsert)
            pg.commit()
            n += batch.num_rows
    return n


# ---------------------------------------------------------------------------
# 7b citation_source_resolution
# ---------------------------------------------------------------------------

_CSR_COLS = ["normalized_id", "part_index", "page_id", "global_source_id", "source_type", "match_type", "confidence",
             "is_canonical"]
_CSR_DDL = ("normalized_id integer, part_index integer, page_id integer, global_source_id varchar(16), "
            "source_type varchar, match_type varchar, confidence double precision, is_canonical boolean")
# A part_sha1 can belong to several normalized citations; the ones with an instance
# on that page are the denorm rows of (page_id, part_sha1) (citation_instances ⋈
# normalized_citations ⋈ parts for the domain), so the join runs per bucket in DuckDB
# instead of through the 295M-row parts table in Postgres.
_CSR_SQL = """
SELECT DISTINCT dn.normalized_id, dn.part_index, m.page_id, g.global_source_id,
       m.source_type, m.match_type, m.confidence, m.is_canonical
FROM read_parquet('{mmap}', hive_partitioning = false) m
JOIN read_parquet('{gmap}', hive_partitioning = false) g USING (merged_source_id)
JOIN citation_instances_denorm dn ON dn.page_id = m.page_id AND dn.part_sha1 = m.part_sha1
"""


def step_b(pg, con, domain: str, domain_id: int, parquet_base: str) -> int:
    """citation_source_resolution for the domain, per page bucket: the matches
    (merged_citation_source_map ⋈ global_source_map) joined to the denorm in
    DuckDB, COPY + INSERT … ON CONFLICT DO NOTHING per bucket, the table's
    foreign keys deferred while it is empty."""
    from . import fkeys
    from .gates import make_session

    gmap_all = layout.partition_glob(parquet_base, MI + "global_source_map", domain)
    split = Path(parquet_base) / MI / f"_scratch_7b_{domain}"
    shutil.rmtree(split, ignore_errors=True)
    # global_source_map is one file for the domain; split it by page bucket once
    con.execute(f"""COPY (SELECT merged_source_id, global_source_id, page_id % {layout.PAGE_BUCKETS} AS page_bucket
                          FROM read_parquet('{gmap_all}', hive_partitioning = false))
                    TO '{split}' (FORMAT PARQUET, PARTITION_BY (page_bucket))""")
    with pg.cursor() as cur:
        cur.execute("DELETE FROM citation_source_resolution WHERE domain_id = %s", (domain_id,))
    pg.commit()
    session = make_session()
    n = 0
    try:
        fkeys.drop_if_empty(session, "citation_source_resolution", log)
        insert = (f"INSERT INTO citation_source_resolution (domain_id, {', '.join(_CSR_COLS)}) "
                  f"SELECT {int(domain_id)}, {', '.join(_CSR_COLS)} FROM csr_in "
                  "ON CONFLICT (normalized_id, part_index, domain_id, page_id) DO NOTHING")
        buckets = layout.list_buckets(parquet_base, MI + "merged_citation_source_map", domain)
        for i, b in enumerate(buckets, 1):
            gmap = split / f"page_bucket={b}" / "*.parquet"
            if not list((split / f"page_bucket={b}").glob("*.parquet")):
                continue
            bind_bucket_views(con, parquet_base, domain, b, ("citation_instances_denorm",))
            mmap = layout.partition_glob(parquet_base, MI + "merged_citation_source_map", domain, b)
            result = con.execute(_CSR_SQL.format(mmap=mmap, gmap=gmap))
            rows = (getattr(result, "to_arrow_table", None) or result.fetch_arrow_table)()
            if rows.num_rows:
                with pg.cursor() as cur:
                    n += _copy_and_insert(cur, rows, "csr_in", _CSR_DDL, _CSR_COLS, insert)
                pg.commit()
            if i % 32 == 0:
                log(f"7b: {i}/{len(buckets)} buckets, {n:,} rows")
        fkeys.restore(session, "citation_source_resolution", log)
    finally:
        session.close()
        shutil.rmtree(split, ignore_errors=True)
    with pg.cursor() as cur:
        # global sources no longer referenced by any resolution row
        cur.execute("""DELETE FROM global_sources gs WHERE NOT EXISTS
                       (SELECT 1 FROM citation_source_resolution r WHERE r.global_source_id = gs.global_source_id)""")
        orphans = cur.rowcount
    pg.commit()
    if orphans:
        log(f"7b: removed {orphans} unreferenced global_sources")
    return n


# ---------------------------------------------------------------------------
# 7d page_source_summary
# ---------------------------------------------------------------------------

_PSS_SQL = """
WITH latest AS (
    SELECT page_id, arg_max(revision_id, (revision_timestamp, revision_id)) AS latest_rev_id FROM revisions GROUP BY page_id
),
temporal AS (
    SELECT dn.page_id, dn.global_source_id,
           min(r.revision_id)              AS first_rev_id,
           min(r.revision_timestamp)       AS first_seen,
           max(r.revision_id)              AS last_rev_id,
           max(r.revision_timestamp)       AS last_seen,
           count(DISTINCT dn.citation_instance_id) AS n_citations,
           count(DISTINCT ch.revision_id)  AS n_revisions_cited,
           bool_or(ch.revision_id = l.latest_rev_id) AS is_current,
           -- the strongest member's match on this page: (source_type, match_type, confidence) from
           -- one row, chosen by (confidence, match_type, source_type), so the triple is coherent and
           -- the choice deterministic (members of one source can match differently: doi via a
           -- template, author_year via a prose ref)
           arg_max(dn.source_type, (coalesce(dn.confidence, -1), dn.match_type, dn.source_type)) AS source_type,
           arg_max(dn.match_type,  (coalesce(dn.confidence, -1), dn.match_type, dn.source_type)) AS match_type,
           arg_max(dn.confidence,  (coalesce(dn.confidence, -1), dn.match_type, dn.source_type)) AS confidence
    FROM citation_instances_denorm dn
    JOIN citation_history ch ON ch.citation_instance_id = dn.citation_instance_id
    JOIN revisions r         ON r.revision_id = ch.revision_id
    JOIN latest l            ON l.page_id = dn.page_id
    WHERE dn.global_source_id IS NOT NULL
    GROUP BY dn.page_id, dn.global_source_id
),
reuse AS (
    SELECT dn.page_id, dn.global_source_id, count(*) AS n_self_closing_uses
    FROM named_ref_uses nru
    JOIN ref_name_links rnl
      ON rnl.page_id = nru.page_id AND rnl.ref_name = nru.ref_name
     AND nru.revision_id >= rnl.first_revision_id
     AND (rnl.last_revision_id IS NULL OR nru.revision_id <= rnl.last_revision_id)
    JOIN citation_instances_denorm dn ON dn.citation_instance_id = rnl.defining_instance_id
    WHERE dn.global_source_id IS NOT NULL
    GROUP BY dn.page_id, dn.global_source_id
)
SELECT t.page_id, t.global_source_id, t.source_type, t.match_type, t.confidence,
       t.first_rev_id, t.first_seen, t.last_rev_id, t.last_seen,
       t.n_citations, t.n_revisions_cited, coalesce(ru.n_self_closing_uses, 0) AS n_self_closing_uses, t.is_current
FROM temporal t
LEFT JOIN reuse ru USING (page_id, global_source_id)
ORDER BY t.page_id, t.global_source_id
"""

# global_sources columns page_source_summary carries (from global_sources_stage, as 7a loads them)
_PSS_GS_COLS = ["author_key", "year", "title", "extracted_doi", "extracted_isbn", "extracted_pmid",
                "extracted_pmc", "extracted_arxiv", "extracted_wikidata_qid", "extracted_url"]


_PSS_COLS = ["page_id", "global_source_id", "source_type", "match_type", "confidence", "first_rev_id", "first_seen",
             "last_rev_id", "last_seen", "n_citations", "n_revisions_cited", "n_self_closing_uses", "is_current"]
_PSS_DDL = ("page_id integer, global_source_id varchar(16), source_type varchar, match_type varchar, "
            "confidence double precision, first_rev_id bigint, first_seen timestamp, last_rev_id bigint, "
            "last_seen timestamp, n_citations integer, n_revisions_cited integer, n_self_closing_uses integer, "
            "is_current boolean")


def _pss_scratch(parquet_base: str, domain: str) -> Path:
    return Path(parquet_base) / "_scratch" / "7d" / f"domain={domain}"


def step_d(pg, con, domain: str, domain_id: int, parquet_base: str, force: bool) -> int:
    """page_source_summary for the domain. Each bucket's rows are computed in
    DuckDB and written to a scratch Parquet file (the file is the bucket's
    checkpoint); then one DuckDB join adds the global source's columns from
    ``global_sources_stage`` (what 7a loaded into ``global_sources``; nothing
    sets ``extracted_librarybase_id`` there, so it is NULL) and the rows go in
    by COPY, the domain's old rows deleted first and the table's foreign keys
    deferred while it is empty. (Joining ``global_sources`` in Postgres per
    bucket was one random primary-key read per row into a table larger than
    shared_buffers: ~2 min per bucket on enwiki.)"""
    from . import fkeys
    from .gates import make_session

    scratch = _pss_scratch(parquet_base, domain)
    Checkpoint(parquet_base, "7d", domain).clear()      # the per-bucket checkpoint of earlier versions
    if force and scratch.exists():
        shutil.rmtree(scratch)
    scratch.mkdir(parents=True, exist_ok=True)
    buckets = layout.list_buckets(parquet_base, "citation_instances_denorm", domain)
    todo = [b for b in buckets if not (scratch / f"bucket={b}.parquet").exists()]
    t0 = time.time()
    for i, b in enumerate(todo, 1):
        bind_bucket_views(con, parquet_base, domain, b,
                          ("citation_instances_denorm", "citation_history", "revisions", "named_ref_uses",
                           "ref_name_links"))
        tmp = scratch / f"bucket={b}.parquet.tmp"
        con.execute(f"COPY ({_PSS_SQL}) TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        tmp.rename(scratch / f"bucket={b}.parquet")
        if i % 32 == 0:
            log(f"7d: {i}/{len(todo)} buckets computed ({time.time() - t0:.0f}s)")

    gs_pattern = layout.partition_glob(parquet_base, MI + "global_sources_stage", domain)
    cols = ["domain_id", "page_id", "global_source_id"] + _PSS_GS_COLS + ["extracted_librarybase_id"] + _PSS_COLS[2:]
    select = ", ".join([f"CAST({int(domain_id)} AS INTEGER) AS domain_id", "t.page_id", "t.global_source_id"]
                       + [f"gs.{c}" for c in _PSS_GS_COLS] + ["CAST(NULL AS VARCHAR) AS extracted_librarybase_id"]
                       + [f"t.{c}" for c in _PSS_COLS[2:]])
    sql = (f"SELECT {select} FROM read_parquet('{scratch}/bucket=*.parquet') t "
           f"JOIN read_parquet('{gs_pattern}') gs USING (global_source_id) ORDER BY t.page_id, t.global_source_id")
    with pg.cursor() as cur:
        cur.execute("DELETE FROM page_source_summary WHERE domain_id = %s", (domain_id,))
    pg.commit()
    session = make_session()
    total = n_batches = 0
    try:
        fkeys.drop_if_empty(session, "page_source_summary", log)
        with pg.cursor() as cur:
            for batch in _batches(con, sql):
                buf = io.BytesIO()
                pa_csv.write_csv(batch, buf, write_options=_CSV_OPTIONS)
                buf.seek(0)
                cur.copy_expert(f"COPY page_source_summary ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv)", buf)
                pg.commit()
                total += batch.num_rows
                n_batches += 1
                if n_batches % 64 == 0:
                    log(f"7d: {total:,} rows loaded ({time.time() - t0:.0f}s)")
        n_scratch = con.execute(f"SELECT count(*) FROM read_parquet('{scratch}/bucket=*.parquet')").fetchone()[0]
        if n_scratch != total:
            raise RuntimeError(f"7d: {n_scratch} summary rows computed but {total} loaded "
                               "(a global source missing from global_sources_stage?)")
        fkeys.restore(session, "page_source_summary", log)
    finally:
        session.close()
    shutil.rmtree(scratch)
    return total


# ---------------------------------------------------------------------------
# 7e counts, 7f score/verdict, 7g export
# ---------------------------------------------------------------------------

def step_e(pg) -> int:
    """Each global source's page and citation counts from page_source_summary.
    Sort-based: the planner takes the ~121M groups on enwiki for ~16M and
    hash-aggregates and hash-joins them in work_mem, which spilled for over an
    hour without writing a row; sorted, the groups merge-join global_sources
    through its primary key (7a loads the table in key order)."""
    with pg.cursor() as cur:
        cur.execute(f"SET LOCAL work_mem = '{STAGE7_WORK_MEM}'")
        cur.execute("SET LOCAL enable_hashagg = off")
        cur.execute("SET LOCAL enable_hashjoin = off")
        cur.execute("""
            UPDATE global_sources gs
            SET n_pages = sub.n_pages, n_total_citations = sub.n_total_citations
            FROM (SELECT global_source_id, count(*) AS n_pages, sum(n_citations) AS n_total_citations
                  FROM page_source_summary GROUP BY global_source_id) sub
            WHERE gs.global_source_id = sub.global_source_id""")
        n = cur.rowcount
    pg.commit()
    return n


# wcd_semantic.scoring.compute_score as one UPDATE: the same terms, with Python's
# truthiness (NULL and '' are false) and its `x or ''` / `x or 0` defaults; the
# canonical part is the first by (normalized_id, part_index) with that sha1, as the
# per-row LATERAL lookup chose it. test_stage7_score holds it to compute_score.
_SCORE_SQL = """
WITH canon AS (
    SELECT DISTINCT ON (part_sha1) part_sha1, id_tier, year_reliable, norm_subtype
    FROM normalized_citation_parts
    WHERE part_sha1 IN (SELECT canonical_part_sha1 FROM global_sources)
    ORDER BY part_sha1, normalized_id, part_index
), s AS (
    SELECT gs.global_source_id,
           coalesce(gs.source_type, '') IN ('template', 'both') AS tmpl,
           coalesce(c.id_tier, 0) > 0 AS tiered,
           coalesce(c.norm_subtype, '') = 'noise' AS noise,
           CASE WHEN coalesce(gs.source_type, '') IN ('template', 'both') THEN 30 ELSE 0 END
         + CASE WHEN coalesce(c.id_tier, 0) > 0 THEN 25 ELSE 0 END
         + CASE WHEN coalesce(gs.author_key, '') <> '' THEN 15 ELSE 0 END
         + CASE WHEN coalesce(c.year_reliable, false) THEN 15 ELSE 0 END
         + CASE WHEN coalesce(gs.year, '') <> '' THEN 5 ELSE 0 END
         + CASE coalesce(gs.match_type, '')
               WHEN 'doi' THEN 10 WHEN 'isbn' THEN 10 WHEN 'pmid' THEN 10 WHEN 'pmc' THEN 10
               WHEN 'arxiv' THEN 10 WHEN 'wikidata_qid' THEN 10 WHEN 'url' THEN 8 WHEN 'author_year' THEN 5
               WHEN 'title_fuzzy' THEN 3 WHEN 'strong_author_fuzzy' THEN 3 WHEN 'author_title_fuzzy' THEN 3
               ELSE 0 END AS score
    FROM global_sources gs LEFT JOIN canon c ON c.part_sha1 = gs.canonical_part_sha1
)
UPDATE global_sources g
SET reliability_score = s.score::double precision,
    verdict = CASE WHEN s.tmpl THEN 'high_confidence' WHEN s.tiered THEN 'high_confidence'
                   WHEN s.noise THEN 'likely_not_citation' WHEN s.score >= 50 THEN 'high_confidence'
                   WHEN s.score >= 25 THEN 'medium_confidence' WHEN s.score >= 10 THEN 'low_confidence'
                   ELSE 'questionable' END
FROM s WHERE g.global_source_id = s.global_source_id
"""


def step_f(pg, dsn: str) -> int:
    """Score every global source from its own columns plus the canonical
    member's ``id_tier`` / ``year_reliable`` / ``norm_subtype`` (parts on
    ``canonical_part_sha1``; the same content has the same fields): one
    set-based UPDATE (on enwiki ~131M sources; a server cursor, a LATERAL
    lookup and a Python call per source would take hours)."""
    with pg.cursor() as cur:
        # its hash joins hold ~100M rows on enwiki: fewer, larger batches
        cur.execute(f"SET LOCAL work_mem = '{STAGE7_WORK_MEM}'")
        cur.execute(_SCORE_SQL)
        n = cur.rowcount
    pg.commit()
    return n


def _step_f_rowwise(pg, dsn: str, batch: int = 20000) -> int:
    """The previous, per-row implementation (compute_score in Python); kept as
    the reference ``test_stage7_score`` compares the UPDATE with."""
    n = 0
    # A second connection for the named (server-side) read cursor. Not pg.dsn: psycopg2
    # masks the password there ("password=xxx"), so it fails wherever auth needs one.
    read = psycopg2.connect(dsn)
    try:
        with read.cursor(name="score_read") as rc, pg.cursor() as wc:
            rc.itersize = batch
            rc.execute("""
                SELECT gs.global_source_id, gs.source_type, gs.match_type, gs.author_key, gs.year,
                       p.id_tier, p.year_reliable, p.norm_subtype
                FROM global_sources gs
                LEFT JOIN LATERAL (SELECT id_tier, year_reliable, norm_subtype FROM normalized_citation_parts
                                   WHERE part_sha1 = gs.canonical_part_sha1 ORDER BY normalized_id, part_index LIMIT 1) p ON true
                ORDER BY gs.global_source_id""")
            while True:
                chunk = rc.fetchmany(batch)
                if not chunk:
                    break
                updates = []
                for gid, source_type, match_type, author_key, year, id_tier, year_reliable, norm_subtype in chunk:
                    score, verdict = compute_score({
                        "source_type": source_type, "match_type": match_type, "author_key": author_key, "year": year,
                        "id_tier": id_tier or 0, "year_reliable": bool(year_reliable), "norm_subtype": norm_subtype,
                    })
                    updates.append((gid, score, verdict))
                psycopg2.extras.execute_values(
                    wc, "UPDATE global_sources gs SET reliability_score = v.score, verdict = v.verdict "
                        "FROM (VALUES %s) AS v(global_source_id, score, verdict) WHERE gs.global_source_id = v.global_source_id",
                    updates, template="(%s, %s::double precision, %s)", page_size=5000)
                n += len(updates)
                pg.commit()
    finally:
        read.close()
    return n


def step_g(pg, con, parquet_base: str) -> int:
    """global_source_scores: COPY out of Postgres as CSV, typed and written as
    Parquet by DuckDB (~131M rows on enwiki; fetchall + from_pylist would hold
    them all as Python objects)."""
    d = layout.dataset_dir(parquet_base, "global_source_scores")
    d.mkdir(parents=True, exist_ok=True)
    for old in d.glob("*.parquet*"):
        old.unlink()
    csv_path = d / "_scores.csv"
    try:
        with pg.cursor() as cur, open(csv_path, "wb") as f:
            cur.copy_expert("COPY (SELECT global_source_id, n_pages, n_total_citations, reliability_score, verdict "
                            "FROM global_sources ORDER BY global_source_id) TO STDOUT WITH (FORMAT csv)", f)
        con.execute(f"""COPY (SELECT * FROM read_csv('{csv_path}', header = false, quote = '"', escape = '"',
                                  allow_quoted_nulls = false,
                                  columns = {{'global_source_id': 'VARCHAR', 'n_pages': 'INTEGER',
                                             'n_total_citations': 'INTEGER', 'reliability_score': 'DOUBLE',
                                             'verdict': 'VARCHAR'}}))
                        TO '{d / 'data.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
        return con.execute(f"SELECT count(*) FROM read_parquet('{d / 'data.parquet'}')").fetchone()[0]
    finally:
        if csv_path.exists():
            csv_path.unlink()


# ---------------------------------------------------------------------------
# 7h page_summary_cache
# ---------------------------------------------------------------------------

_CACHE_REV_SQL = """
SELECT page_id, count(*) AS revision_count, min(revision_timestamp) AS first_ts, max(revision_timestamp) AS latest_ts,
       arg_max(revision_id, (revision_timestamp, revision_id)) AS latest_rev_id
FROM revisions GROUP BY page_id ORDER BY page_id
"""
# one row per citation instance at the page's latest revision, with its first part's fields
_CACHE_CIT_SQL = """
WITH latest AS (
    SELECT page_id, arg_max(revision_id, (revision_timestamp, revision_id)) AS latest_rev_id FROM revisions GROUP BY page_id
)
SELECT dn.page_id, dn.citation_instance_id, dn.citation_type, dn.template_name, dn.template_subtype, dn.norm_subtype,
       dn.extracted_isbn, dn.extracted_doi, dn.extracted_pmid, dn.extracted_pmc, dn.extracted_url
FROM citation_instances_denorm dn
JOIN latest l ON l.page_id = dn.page_id
JOIN citation_history ch ON ch.citation_instance_id = dn.citation_instance_id AND ch.revision_id = l.latest_rev_id
WHERE dn.part_index = 0
ORDER BY dn.page_id, dn.citation_instance_id
"""
# template names of every part of those instances (a bullet block counts each template once per instance)
_CACHE_TPL_SQL = """
WITH latest AS (
    SELECT page_id, arg_max(revision_id, (revision_timestamp, revision_id)) AS latest_rev_id FROM revisions GROUP BY page_id
)
SELECT dn.page_id, dn.template_name, count(DISTINCT dn.citation_instance_id) AS n
FROM citation_instances_denorm dn
JOIN latest l ON l.page_id = dn.page_id
JOIN citation_history ch ON ch.citation_instance_id = dn.citation_instance_id AND ch.revision_id = l.latest_rev_id
WHERE dn.template_name IS NOT NULL
GROUP BY dn.page_id, dn.template_name
ORDER BY dn.page_id, n DESC, dn.template_name
"""


def build_cache_rows(con, domain_id: int) -> list[tuple]:
    revs = con.execute(_CACHE_REV_SQL).fetchall()
    cits: dict[int, list[dict]] = {}
    for page_id, inst, ctype, tname, tsub, nsub, isbn, doi, pmid, pmc, url in con.execute(_CACHE_CIT_SQL).fetchall():
        cits.setdefault(page_id, []).append({
            "citation_type": ctype, "template_name": tname, "template_subtype": tsub, "norm_subtype": nsub,
            "extracted_isbn": isbn, "extracted_doi": doi, "extracted_pmid": pmid, "extracted_pmc": pmc, "extracted_url": url,
        })
    tpls: dict[int, list[dict]] = {}
    for page_id, tname, n in con.execute(_CACHE_TPL_SQL).fetchall():
        if len(tpls.setdefault(page_id, [])) < TOP_TEMPLATE_LIMIT:
            tpls[page_id].append({"template_name": tname, "count": int(n)})
    out = []
    for page_id, revision_count, first_ts, latest_ts, latest_rev_id in revs:
        rows = cits.get(page_id, [])
        type_counts: dict[str, int] = {}
        for r in rows:
            type_counts[r["citation_type"]] = type_counts.get(r["citation_type"], 0) + 1
        type_breakdown = [{"citation_type": t, "count": c}
                          for t, c in sorted(type_counts.items(), key=lambda kv: (-kv[1], kv[0]))]
        out.append((domain_id, page_id, int(revision_count), first_ts, latest_ts, int(latest_rev_id), len(rows),
                    psycopg2.extras.Json(type_breakdown), psycopg2.extras.Json(tpls.get(page_id, [])),
                    psycopg2.extras.Json(build_source_profile(rows, len(rows)))))
    return out


_CACHE_COLS = ["page_id", "revision_count", "first_revision_ts", "latest_revision_ts", "latest_rev_id",
               "reference_count", "type_breakdown", "top_templates", "source_profile"]
_CACHE_DDL = ("page_id integer, revision_count integer, first_revision_ts timestamp, latest_revision_ts timestamp, "
              "latest_rev_id bigint, reference_count integer, type_breakdown jsonb, top_templates jsonb, "
              "source_profile jsonb")


def step_h(pg, con, domain: str, domain_id: int, parquet_base: str, force: bool) -> int:
    ck = Checkpoint(parquet_base, "7h", domain)
    if force:
        ck.clear()
    buckets = layout.list_buckets(parquet_base, "revisions", domain)
    if not ck.done:
        with pg.cursor() as cur:
            cur.execute("DELETE FROM page_summary_cache WHERE domain_id = %s", (domain_id,))
        pg.commit()
    total = 0
    for i, b in enumerate(buckets, 1):
        if b in ck.done:
            continue
        bind_bucket_views(con, parquet_base, domain, b, ("revisions", "citation_history", "citation_instances_denorm"))
        rows = build_cache_rows(con, domain_id)
        with pg.cursor() as cur:
            # by page ids (the primary key leads with domain_id, page_id); a
            # page_id % 512 filter scanned the whole table once per bucket
            cur.execute("DELETE FROM page_summary_cache WHERE domain_id = %s AND page_id = ANY(%s)",
                        (domain_id, [r[1] for r in rows]))
            if rows:
                cols = list(zip(*rows))
                table = pa.table({
                    "page_id": pa.array(cols[1], pa.int32()), "revision_count": pa.array(cols[2], pa.int32()),
                    "first_revision_ts": pa.array(cols[3], pa.timestamp("us")),
                    "latest_revision_ts": pa.array(cols[4], pa.timestamp("us")),
                    "latest_rev_id": pa.array(cols[5], pa.int64()), "reference_count": pa.array(cols[6], pa.int32()),
                    # psycopg2's Json would dump the same objects with json.dumps
                    **{name: pa.array([json.dumps(j.adapted) for j in cols[k]], pa.string())
                       for name, k in (("type_breakdown", 7), ("top_templates", 8), ("source_profile", 9))},
                })
                _copy_and_insert(cur, table, "cache_in", _CACHE_DDL, _CACHE_COLS,
                                 f"INSERT INTO page_summary_cache (domain_id, {', '.join(_CACHE_COLS)}) "
                                 f"SELECT {int(domain_id)}, {', '.join(_CACHE_COLS)} FROM cache_in")
        pg.commit()
        ck.mark(b)
        total += len(rows)
        if i % 32 == 0:
            log(f"7h: {i}/{len(buckets)} buckets, {total:,} pages")
    ck.clear()
    return total


# ---------------------------------------------------------------------------
# Gate and CLI
# ---------------------------------------------------------------------------

def gate(pg, con, domain: str, domain_id: int, parquet_base: str) -> list[str]:
    """The 7a–7g checks of proposed_data_integrity.md §4 plus the referential
    checks, for one domain."""
    f = []
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM global_sources")
        if cur.fetchone()[0] == 0:
            f.append("7a: global_sources is empty")
        cur.execute("SELECT count(*) FROM citation_source_resolution WHERE domain_id = %s", (domain_id,))
        n_csr = cur.fetchone()[0]
        n_map = con.execute(f"SELECT count(*) FROM read_parquet('{layout.partition_glob(parquet_base, MI + 'merged_citation_source_map', domain)}')").fetchone()[0]
        if n_csr < n_map:
            f.append(f"7b: citation_source_resolution has {n_csr} rows for the domain but merged_citation_source_map has {n_map}")
        n_den = con.execute(f"SELECT count(DISTINCT (page_id, normalized_id, part_index)) FROM read_parquet("
                            f"'{layout.partition_glob(parquet_base, 'citation_instances_denorm', domain)}', hive_partitioning=true) "
                            "WHERE global_source_id IS NOT NULL").fetchone()[0]
        if n_den != n_csr:
            f.append(f"7c: resolved denorm has {n_den} resolved (page, part) rows but citation_source_resolution has {n_csr}")
        cur.execute("""SELECT count(*) FROM (SELECT DISTINCT page_id FROM citation_source_resolution WHERE domain_id = %s) r
                       LEFT JOIN (SELECT DISTINCT page_id FROM page_source_summary WHERE domain_id = %s) p USING (page_id)
                       WHERE p.page_id IS NULL""", (domain_id, domain_id))
        missing = cur.fetchone()[0]
        if missing:
            f.append(f"7d: {missing} resolved page(s) have no page_source_summary row")
        cur.execute("""SELECT count(*) FROM global_sources gs WHERE gs.n_pages IS NULL AND EXISTS
                       (SELECT 1 FROM page_source_summary p WHERE p.global_source_id = gs.global_source_id)""")
        if cur.fetchone()[0]:
            f.append("7e: global_sources referenced by page_source_summary with NULL n_pages")
        cur.execute("SELECT count(*) FROM global_sources WHERE reliability_score IS NULL OR verdict IS NULL")
        if cur.fetchone()[0]:
            f.append("7f: global_sources with NULL reliability_score / verdict")
        cur.execute("SELECT count(*) FROM global_sources")
        n_gs = cur.fetchone()[0]
        try:
            n_sc = con.execute(f"SELECT count(*) FROM read_parquet('{layout.dataset_glob(parquet_base, 'global_source_scores')}')").fetchone()[0]
        except duckdb.IOException:
            n_sc = 0
        if n_sc != n_gs:
            f.append(f"7g: global_source_scores has {n_sc} rows but global_sources has {n_gs}")
        for name, sql in (
            ("csr → global_sources", "SELECT count(*) FROM citation_source_resolution c LEFT JOIN global_sources g USING (global_source_id) WHERE g.global_source_id IS NULL"),
            ("pss → global_sources", "SELECT count(*) FROM page_source_summary p LEFT JOIN global_sources g USING (global_source_id) WHERE g.global_source_id IS NULL"),
            ("csr → parts", "SELECT count(*) FROM citation_source_resolution c LEFT JOIN normalized_citation_parts p USING (normalized_id, part_index) WHERE p.normalized_id IS NULL"),
            ("ref_name_links → instances", "SELECT count(*) FROM ref_name_links r LEFT JOIN citation_instances ci ON ci.id = r.defining_instance_id WHERE ci.id IS NULL"),
        ):
            cur.execute(sql)
            if cur.fetchone()[0]:
                f.append(f"referential: dangling {name}")
        cur.execute("SELECT count(*) FROM page_summary_cache WHERE domain_id = %s", (domain_id,))
        n_cache = cur.fetchone()[0]
        n_pages = con.execute(f"SELECT count(DISTINCT page_id) FROM read_parquet('{layout.partition_glob(parquet_base, 'revisions', domain)}', hive_partitioning=true)").fetchone()[0]
        if n_cache != n_pages:
            f.append(f"7h: page_summary_cache has {n_cache} rows for the domain but revisions has {n_pages} pages")
    return f


def run(dsn: str, domain: str, parquet_base: str, steps=STEPS, force: bool = False) -> dict:
    pg = psycopg2.connect(dsn)
    con = duckdb_connect()
    counts = {}
    try:
        domain_id = _domain_id(pg, domain)
        for s in steps:
            t0 = time.time()
            if s == "a":
                counts["7a global_sources"] = step_a(pg, con, domain, parquet_base)
            elif s == "b":
                counts["7b citation_source_resolution"] = step_b(pg, con, domain, domain_id, parquet_base)
            elif s == "c":
                counts["7c citation_instances_denorm"] = export_citation_instances_denorm(dsn, domain, parquet_base, resolved=True)["rows"]
            elif s == "d":
                counts["7d page_source_summary"] = step_d(pg, con, domain, domain_id, parquet_base, force)
            elif s == "e":
                counts["7e counts"] = step_e(pg)
            elif s == "f":
                counts["7f scored"] = step_f(pg, dsn)
            elif s == "g":
                counts["7g global_source_scores"] = step_g(pg, con, parquet_base)
            elif s == "h":
                counts["7h page_summary_cache"] = step_h(pg, con, domain, domain_id, parquet_base, force)
            else:
                raise ValueError(s)
            log(f"7{s}: {list(counts.values())[-1]} rows in {time.time() - t0:.1f}s")
        return counts
    except Exception:
        pg.rollback()
        raise
    finally:
        con.close()
        pg.close()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Stage 7: materialize matching results to Postgres and the Parquet exports")
    ap.add_argument("--domain", required=True)
    ap.add_argument("--parquet-dir", default=os.environ.get("PARQUET_DIR"), help="Parquet tier base directory (default: PARQUET_DIR)")
    ap.add_argument("--steps", default=",".join(STEPS), help="comma-separated subset of a,b,c,d,e,f,g,h (default: all, in order)")
    ap.add_argument("--force", action="store_true", help="ignore 7d/7h bucket checkpoints")
    ap.add_argument("--no-gate", action="store_true")
    args = ap.parse_args(argv)
    if not args.parquet_dir:
        ap.error("--parquet-dir (or PARQUET_DIR) is required")
    steps = [s.strip() for s in args.steps.split(",") if s.strip()]
    bad = [s for s in steps if s not in STEPS]
    if bad:
        ap.error(f"unknown step(s) {bad}")
    dsn = psycopg_dsn()
    counts = run(dsn, args.domain, args.parquet_dir, steps, args.force)
    log(f"done: {counts}")
    if not args.no_gate and steps == list(STEPS):
        pg = psycopg2.connect(dsn)
        con = duckdb_connect()
        try:
            failures = gate(pg, con, args.domain, _domain_id(pg, args.domain), args.parquet_dir)
        finally:
            con.close()
            pg.close()
        if failures:
            for f in failures:
                log(f"Stage 7 GATE FAIL: {f}")
            return 1
        log("Stage 7 gate: pass")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
