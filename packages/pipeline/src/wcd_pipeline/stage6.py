"""Stage 6: source matching (docs/design/proposed_matching.md).

Per ``(domain, page_bucket)`` — the unit of parallelism, checkpointing and
re-run — and per page inside it (largest first):

  6a  ref-tag parts      → ``sources`` + ``citation_source_map``          (wcd_semantic.match_reftag)
  6b  template parts     → ``template_sources`` + ``template_citation_source_map`` (match_template)
  6c  both               → ``merged_sources`` + ``merged_citation_source_map``     (merge_types)

then once per domain:

  6d  all merged sources → ``global_source_map`` + ``global_sources_stage``       (match_global)

Inputs are Parquet only: the unresolved ``citation_instances_denorm`` (Stage
4.6) and ``citation_history``, each read for one bucket partition. Outputs go
under ``matching_intermediate/<name>/domain=/page_bucket=/`` (6a–6c) and
``matching_intermediate/<name>/domain=/`` (6d), one file per bucket, replaced
on re-run; a bucket with a ``_SUCCESS`` marker is skipped unless ``--force``.
UVA's per-page functions run unchanged; this module only assembles their
inputs (the way upstream's ``load_*`` SQL did) and writes their outputs.

``wcd match --domain D [--buckets 3,17] [--jobs N] [--force] [--skip-global]``
"""
from __future__ import annotations

import argparse
import os
import shutil
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from wcd_schema import parquet as layout
from wcd_schema.duckdb import bind_bucket_views
from wcd_schema.parquet import SCHEMAS
from wcd_semantic import match_global, match_reftag, match_template, merge_types

from .config import load_dotenv  # noqa: F401

MI = "matching_intermediate/"
BUCKET_DATASETS = ("sources", "citation_source_map", "template_sources", "template_citation_source_map",
                   "merged_sources", "merged_citation_source_map")
DOMAIN_DATASETS = ("global_source_map", "global_sources_stage")
MARKER = "_SUCCESS"
_ADDED_IDS = ["extracted_pmc", "extracted_arxiv", "extracted_wikidata_qid"]

# ---------------------------------------------------------------------------
# Inputs (the design's 6a/6b query, both part types at once)
# ---------------------------------------------------------------------------

# One row per (page, part_sha1) and matcher: the part attributes are functions of the part
# text, so every occurrence agrees on them, except part_type when the same text occurs both as
# a bare-URL <ref> (ref_tag) and as a plain external link (external_link). Both go to the
# ref-tag matcher, which needs content_sha1 unique per page, so they are one input row,
# typed ref_tag, with revisions counted across both. Template parts stay separate rows
# (they go to the template matcher).
_INPUT_SQL = """
SELECT dn.page_id, dn.part_sha1,
       CASE WHEN bool_or(dn.part_type = 'ref_tag') THEN 'ref_tag' ELSE min(dn.part_type) END AS part_type,
       min(dn.content) AS content,
       min(dn.norm_content) AS norm_content,
       min(dn.norm_subtype) AS norm_subtype,
       min(dn.id_tier) AS id_tier,
       min(dn.id_tier_label) AS id_tier_label,
       min(dn.extracted_url) AS extracted_url,
       min(dn.extracted_isbn) AS extracted_isbn,
       min(dn.extracted_doi) AS extracted_doi,
       min(dn.extracted_pmid) AS extracted_pmid,
       min(dn.extracted_pmc) AS extracted_pmc,
       min(dn.extracted_arxiv) AS extracted_arxiv,
       min(dn.extracted_wikidata_qid) AS extracted_wikidata_qid,
       min(dn.anchor_key) AS anchor_key,
       min(dn.author_key) AS author_key,
       min(dn.year) AS year,
       min(dn.year_reliable) AS year_reliable,
       min(dn.page_ref) AS page_ref,
       min(dn.title) AS title,
       min(dn.template_name) AS template_name,
       min(dn.template_subtype) AS template_subtype,
       min(dn.author_last) AS author_last,
       min(dn.journal) AS journal,
       min(dn.pages) AS pages,
       count(DISTINCT ch.revision_id) AS n_revisions,
       min(ch.revision_id)            AS first_rev_id,
       min(dn.reference_name)         AS ref_name
FROM citation_instances_denorm dn
JOIN citation_history ch ON ch.citation_instance_id = dn.citation_instance_id
GROUP BY dn.page_id, dn.part_sha1, dn.part_type = 'template'
ORDER BY dn.page_id, dn.part_sha1
"""


def log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} [match] {msg}", flush=True)


def load_bucket_inputs(con: duckdb.DuckDBPyConnection, parquet_base: str, domain: str, bucket: int) -> pd.DataFrame:
    bind_bucket_views(con, parquet_base, domain, bucket, ("citation_instances_denorm", "citation_history"))
    df = con.execute(_INPUT_SQL).df()
    # pandas turns nullable ints into floats; the matchers compare raw values
    for c in ("id_tier", "n_revisions", "first_rev_id", "page_id"):
        if c in df.columns:
            df[c] = df[c].astype("Int64")
    df = df.astype(object).where(df.notna(), None)
    df["content_sha1"] = df["part_sha1"]
    df["raw_content"] = df["content"]
    return df


# ---------------------------------------------------------------------------
# One page: 6a, 6b, 6c
# ---------------------------------------------------------------------------

def _frame(rows: list[dict], columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)


def match_one_page(page_df: pd.DataFrame, page_id: int) -> dict[str, pd.DataFrame]:
    """Run 6a–6c for one page. Returns the six output frames (UVA column names)."""
    rt_in = page_df[page_df["part_type"].isin(["ref_tag", "external_link"])]
    tm_in = page_df[page_df["part_type"] == "template"]
    rt_in = rt_in[[c for c in match_reftag.INPUT_COLUMNS if c in rt_in.columns]].reset_index(drop=True)
    tm_in = tm_in[[c for c in match_template.INPUT_COLUMNS if c in tm_in.columns]].reset_index(drop=True)

    sources, cmap = (match_reftag.match_page(rt_in, page_id)[:2] if not rt_in.empty else (pd.DataFrame(), pd.DataFrame()))
    tsources, tmap = (match_template.match_page(tm_in, page_id)[:2] if not tm_in.empty else (pd.DataFrame(), pd.DataFrame()))

    rn_lookup = {r["content_sha1"]: r for r in rt_in.to_dict("records")}
    tn_lookup = {r["content_sha1"]: r for r in tm_in.to_dict("records")}

    # upstream load_sources: sources ⋈ canonical map row ⋈ ref_tag_normalized(norm_content, title)
    if not sources.empty:
        canon = cmap[cmap["is_canonical"]][["source_id", "content_sha1"]]
        rt = sources.merge(canon, on="source_id", how="left")
        rt["norm_content"] = rt["content_sha1"].map(lambda s: rn_lookup.get(s, {}).get("norm_content"))
        rt["ref_title"] = rt["content_sha1"].map(lambda s: rn_lookup.get(s, {}).get("title"))
        rt = rt[merge_types.RT_COLUMNS + _ADDED_IDS]
    else:
        rt = pd.DataFrame(columns=merge_types.RT_COLUMNS + _ADDED_IDS)
    if not tsources.empty:
        tm = tsources.copy()
        tm["author_key"] = tm["author_last"].map(lambda v: v.lower() if isinstance(v, str) else None)
        tm = tm[merge_types.TM_COLUMNS + _ADDED_IDS]
    else:
        tm = pd.DataFrame(columns=merge_types.TM_COLUMNS + _ADDED_IDS)
    rt, tm = merge_types.prepare_sources(rt, tm)
    merged = merge_types.merge_page(rt, tm, page_id) if not (rt.empty and tm.empty) else pd.DataFrame()

    merged_map_rows = []
    if not merged.empty:
        # the canonical member's fields for 6d and the scorer (design: via canonical_sha1)
        def _canon_field(sha, field, default=None):
            r = rn_lookup.get(sha) or tn_lookup.get(sha) or {}
            v = r.get(field)
            return default if v is None else v
        merged["id_tier"] = merged["canonical_sha1"].map(lambda s: int(_canon_field(s, "id_tier", 0) or 0))
        merged["year_reliable"] = merged["canonical_sha1"].map(lambda s: bool(_canon_field(s, "year_reliable", False)))
        merged["norm_subtype"] = merged["canonical_sha1"].map(lambda s: _canon_field(s, "norm_subtype"))

        by_rt = cmap.groupby("source_id") if not cmap.empty else None
        by_tm = tmap.groupby("source_id") if not tmap.empty else None
        for m in merged.to_dict("records"):
            for sid in m["rt_source_ids"]:
                if by_rt is not None and sid in by_rt.groups:
                    for r in by_rt.get_group(sid).to_dict("records"):
                        merged_map_rows.append({"content_sha1": r["content_sha1"], "page_id": page_id,
                                                "merged_source_id": m["merged_source_id"], "source_type": "ref_tag",
                                                "match_type": r["match_type"], "confidence": r["confidence"],
                                                "is_canonical": bool(r["is_canonical"])})
            for sid in m["tm_source_ids"]:
                if by_tm is not None and sid in by_tm.groups:
                    for r in by_tm.get_group(sid).to_dict("records"):
                        merged_map_rows.append({"content_sha1": r["content_sha1"], "page_id": page_id,
                                                "merged_source_id": m["merged_source_id"], "source_type": "template",
                                                "match_type": r["match_type"], "confidence": r["confidence"],
                                                "is_canonical": bool(r["is_canonical"])})
    return {
        "sources": sources, "citation_source_map": cmap,
        "template_sources": tsources, "template_citation_source_map": tmap,
        "merged_sources": merged.drop(columns=["rt_source_ids", "tm_source_ids"], errors="ignore") if not merged.empty else merged,
        "merged_citation_source_map": pd.DataFrame(merged_map_rows),
    }


# ---------------------------------------------------------------------------
# One bucket
# ---------------------------------------------------------------------------

_RENAME = {"content_sha1": "part_sha1", "canonical_sha1": "canonical_part_sha1"}


def _to_table(df: pd.DataFrame, dataset: str) -> pa.Table:
    schema = SCHEMAS[MI + dataset]
    df = df.rename(columns=_RENAME) if not df.empty else df
    rows = []
    names = [f.name for f in schema]
    for r in (df.to_dict("records") if not df.empty else []):
        row = {}
        for n in names:
            v = r.get(n)
            if v is not None and isinstance(v, float) and pd.isna(v):
                v = None
            row[n] = v
        rows.append(row)
    return pa.Table.from_pylist(rows, schema=schema)


def write_bucket(outputs: dict[str, pd.DataFrame], parquet_base: str, domain: str, bucket: int) -> None:
    for ds in BUCKET_DATASETS:
        d = layout.partition_dir(parquet_base, MI + ds, domain, bucket)
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)
        pq.write_table(_to_table(outputs.get(ds, pd.DataFrame()), ds), d / "data.parquet", compression="zstd")
    (layout.partition_dir(parquet_base, MI + "merged_citation_source_map", domain, bucket) / MARKER).touch()


def bucket_done(parquet_base: str, domain: str, bucket: int) -> bool:
    return (layout.partition_dir(parquet_base, MI + "merged_citation_source_map", domain, bucket) / MARKER).exists()


def run_bucket(parquet_base: str, domain: str, bucket: int) -> dict:
    t0 = time.time()
    con = duckdb.connect()
    try:
        df = load_bucket_inputs(con, parquet_base, domain, bucket)
    finally:
        con.close()
    outputs = {ds: [] for ds in BUCKET_DATASETS}
    n_pages = 0
    if not df.empty:
        sizes = df.groupby("page_id").size().sort_values(ascending=False)
        for page_id in sizes.index:
            page_df = df[df["page_id"] == page_id]
            res = match_one_page(page_df, int(page_id))
            for ds in BUCKET_DATASETS:
                if not res[ds].empty:
                    outputs[ds].append(res[ds])
            n_pages += 1
    frames = {ds: (pd.concat(v, ignore_index=True) if v else pd.DataFrame()) for ds, v in outputs.items()}
    write_bucket(frames, parquet_base, domain, bucket)
    return {"bucket": bucket, "pages": n_pages, "parts": int(len(df)),
            "merged_sources": int(len(frames["merged_sources"])), "seconds": round(time.time() - t0, 1)}


# ---------------------------------------------------------------------------
# 6d
# ---------------------------------------------------------------------------

def run_global(parquet_base: str, domain: str) -> dict:
    t0 = time.time()
    con = duckdb.connect()
    try:
        pattern = layout.partition_glob(parquet_base, MI + "merged_sources", domain)
        df = con.execute(f"SELECT * EXCLUDE (domain, page_bucket) FROM read_parquet('{pattern}', hive_partitioning=true) "
                         "ORDER BY page_id, merged_source_id").df()
    finally:
        con.close()
    for c in ("id_tier", "n_total_citations", "page_id"):
        df[c] = df[c].astype("Int64")
    df = df.astype(object).where(df.notna(), None)
    df = df.rename(columns={"canonical_part_sha1": "canonical_sha1"})
    df["year_reliable"] = df["year_reliable"].map(lambda v: bool(v) if v is not None else False)
    df["id_tier"] = df["id_tier"].map(lambda v: int(v) if v is not None else 0)
    df["n_total_citations"] = df["n_total_citations"].map(lambda v: int(v) if v is not None else 0)
    df = df[[c for c in match_global.INPUT_COLUMNS + ["extracted_pmc", "extracted_arxiv", "extracted_wikidata_qid"] if c in df.columns]]

    if df.empty:
        gs, gm, counts = pd.DataFrame(), pd.DataFrame(), {}
    else:
        gs, gm, counts = match_global.match_global(df)
        # the design's map carries the component's cross-page match_type / confidence
        gm = gm.merge(gs[["global_source_id", "match_type", "confidence"]], on="global_source_id", how="left")
        # source_type of the canonical member (7f needs it): the member whose
        # canonical_sha1 the global source carries
        canon_sha_to_type = {}
        for r in df.to_dict("records"):
            canon_sha_to_type.setdefault(r["canonical_sha1"], r["source_type"])
        gs["source_type"] = gs["canonical_sha1"].map(lambda s: canon_sha_to_type.get(s))

    for ds, frame in (("global_source_map", gm), ("global_sources_stage", gs)):
        d = layout.partition_dir(parquet_base, MI + ds, domain)
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)
        pq.write_table(_to_table(frame, ds), d / "data.parquet", compression="zstd")
    return {"merged_sources": int(len(df)), "global_sources": int(len(gs)), "merges": counts,
            "seconds": round(time.time() - t0, 1)}


# ---------------------------------------------------------------------------
# Gate (6d) and CLI
# ---------------------------------------------------------------------------

def gate(parquet_base: str, domain: str) -> list[str]:
    """all six intermediate datasets exist; merged_citation_source_map > 0; every
    merged_source_id in global_source_map exists in merged_sources."""
    failures = []
    con = duckdb.connect()
    for ds in BUCKET_DATASETS:
        if not layout.list_buckets(parquet_base, MI + ds, domain):
            failures.append(f"no {ds} partitions for {domain}")
    if failures:
        return failures
    n = con.execute(f"SELECT count(*) FROM read_parquet('{layout.partition_glob(parquet_base, MI + 'merged_citation_source_map', domain)}')").fetchone()[0]
    if n == 0:
        failures.append("merged_citation_source_map is empty")
    gmap = layout.partition_glob(parquet_base, MI + "global_source_map", domain)
    try:
        dangling = con.execute(
            f"SELECT count(*) FROM read_parquet('{gmap}') g LEFT JOIN "
            f"read_parquet('{layout.partition_glob(parquet_base, MI + 'merged_sources', domain)}') m USING (merged_source_id) "
            "WHERE m.merged_source_id IS NULL").fetchone()[0]
        unmapped = con.execute(
            f"SELECT count(*) FROM read_parquet('{layout.partition_glob(parquet_base, MI + 'merged_sources', domain)}') m "
            f"LEFT JOIN read_parquet('{gmap}') g USING (merged_source_id) WHERE g.merged_source_id IS NULL").fetchone()[0]
    except duckdb.IOException:
        return failures + [f"no global_source_map for {domain}"]
    if dangling:
        failures.append(f"{dangling} global_source_map rows reference a missing merged source")
    if unmapped:
        failures.append(f"{unmapped} merged sources have no global_source_map row")
    return failures


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Stage 6: per-page (6a-6c) and cross-page (6d) source matching for a domain")
    ap.add_argument("--domain", required=True)
    ap.add_argument("--parquet-dir", default=os.environ.get("PARQUET_DIR"), help="Parquet tier base directory (default: PARQUET_DIR)")
    ap.add_argument("--buckets", help="comma-separated page buckets to (re)run (default: all with denorm data)")
    ap.add_argument("--jobs", type=int, default=max(1, (os.cpu_count() or 2) // 2), help="buckets processed concurrently")
    ap.add_argument("--force", action="store_true", help="re-run buckets that already have a _SUCCESS marker")
    ap.add_argument("--skip-global", action="store_true", help="stop after 6c")
    ap.add_argument("--no-gate", action="store_true")
    args = ap.parse_args(argv)
    if not args.parquet_dir:
        ap.error("--parquet-dir (or PARQUET_DIR) is required")

    all_buckets = layout.list_buckets(args.parquet_dir, "citation_instances_denorm", args.domain)
    if not all_buckets:
        ap.error(f"no citation_instances_denorm partitions for {args.domain} under {args.parquet_dir} (run wcd denorm-export)")
    buckets = [int(b) for b in args.buckets.split(",")] if args.buckets else all_buckets
    todo = [b for b in buckets if args.force or not bucket_done(args.parquet_dir, args.domain, b)]
    log(f"{len(buckets)} bucket(s), {len(todo)} to run, {args.jobs} job(s)")
    t0 = time.time()
    if args.jobs > 1 and len(todo) > 1:
        with ProcessPoolExecutor(max_workers=args.jobs) as pool:
            futures = {pool.submit(run_bucket, args.parquet_dir, args.domain, b): b for b in todo}
            for f in as_completed(futures):
                log(f"bucket {f.result()}")
    else:
        for b in todo:
            log(f"bucket {run_bucket(args.parquet_dir, args.domain, b)}")
    log(f"6a-6c done in {time.time() - t0:.1f}s")
    if args.skip_global:
        return 0
    log(f"6d {run_global(args.parquet_dir, args.domain)}")
    if not args.no_gate:
        failures = gate(args.parquet_dir, args.domain)
        if failures:
            for f in failures:
                log(f"Stage 6 GATE FAIL: {f}")
            return 1
        log("Stage 6 gate: pass")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
