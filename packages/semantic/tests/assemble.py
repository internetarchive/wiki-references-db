"""Run the full semantic layer over the fixed corpus with a pluggable backend.

The same assembly runs against the upstream UVA modules (to generate the
golden files, see gen_golden.py) and against wcd_semantic (in the tests), so
the comparison holds the ported logic to upstream behaviour on identical
inputs. The frame-building steps mirror the SQL in upstream's load_* functions.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any, Callable

import pandas as pd

from corpus import CONTENTS, PAGES, REVISION_TEXTS

# Output fields wcd_semantic ADDS over upstream (identifier set extension,
# docs/design/decisions_2026_09_10.md item 7). They are stripped before the
# golden comparison and covered by test_additions.py instead.
ADDED_FIELDS = ("extracted_arxiv", "extracted_wikidata_qid")
ADDED_MATCHER_FIELDS = ("extracted_pmc", "extracted_arxiv", "extracted_wikidata_qid")


def strip_added(obj):
    """Remove ADDED keys recursively (dicts in lists in dicts …)."""
    if isinstance(obj, dict):
        return {k: strip_added(v) for k, v in obj.items()
                if k not in ADDED_FIELDS and k not in ADDED_MATCHER_FIELDS}
    if isinstance(obj, list):
        return [strip_added(x) for x in obj]
    return obj


@dataclass
class Backend:
    parse_citation: Callable       # (ref_name, content) -> dict | None
    iter_citations: Callable       # (ref_name, content) -> iterator of dict
    is_multi_block: Callable       # content -> bool
    is_citation: Callable          # content -> bool
    classify: Callable             # content -> dict
    extract_template: Callable     # (content, template_name, params) -> dict
    extract_flags: Callable        # text -> dict
    reftag_match: Callable         # (df, page_id) -> (sources, citation_map)
    template_match: Callable       # (df, page_id) -> (sources, citation_map)
    normalize_key: Callable        # str -> str
    merge: Callable                # (rt, tm, page_id) -> merged
    global_match: Callable         # df -> (global_sources, global_source_map)
    compute_score: Callable        # row -> (score, verdict)


def _clean(v: Any) -> Any:
    if isinstance(v, float) and math.isnan(v):
        return None
    if hasattr(v, "item"):          # numpy scalar
        v = v.item()
        return None if isinstance(v, float) and math.isnan(v) else v
    if isinstance(v, dict):
        return {k: _clean(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_clean(x) for x in v]
    return v


def records(df: pd.DataFrame, sort_by: list[str]) -> list[dict]:
    if df is None or df.empty:
        return []
    df = df.sort_values(sort_by, kind="stable").reset_index(drop=True)
    return [_clean(r) for r in df.to_dict("records")]


def run_contents(b: Backend) -> list[dict]:
    out = []
    for c in CONTENTS:
        parsed = b.parse_citation(None, c)
        item = {
            "content": c,
            "is_citation": b.is_citation(c),
            "is_multi_block": b.is_multi_block(c),
            "parsed": _clean(parsed),
            "iter": [_clean(x) for x in b.iter_citations("n", c)],
        }
        if parsed and parsed["citation_type"] == "template":
            item["template"] = _clean(b.extract_template(parsed["raw"], parsed["template_name"], parsed["params"]))
        # the ref-tag normalizer is also run on everything (Stage 4 may route
        # non-template content there regardless of the footnote filter)
        item["reftag"] = _clean(b.classify(c))
        out.append(item)
    return out


def run_flags(b: Backend) -> list[dict]:
    return [{"text": t, "flags": _clean(b.extract_flags(t))} for t in REVISION_TEXTS]


def build_page_frames(b: Backend, page_id: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Mirror upstream load_page_citations / load_page_templates for one page."""
    rt_rows, tm_rows = [], []
    for ref_name, content, n_rev, first_rev in PAGES[page_id]:
        parsed = b.parse_citation(ref_name, content)
        if parsed is None:
            continue
        extra = {"n_revisions": n_rev, "first_rev_id": first_rev, "ref_name": ref_name}
        if parsed["citation_type"] == "template":
            t = b.extract_template(parsed["raw"], parsed["template_name"], parsed["params"])
            tm_rows.append({
                **{k: t[k] for k in ADDED_FIELDS if k in t},
                "content_sha1": t["content_sha1"], "raw_content": parsed["raw"],
                "template_name": t["template_name"], "template_subtype": t["template_subtype"],
                "author_last": t["author_last"], "author_key": t["author_key"], "title": t["title"],
                "year": t["year"], "journal": t["journal"], "pages": t["pages"],
                "extracted_url": t["extracted_url"], "extracted_isbn": t["extracted_isbn"],
                "extracted_doi": t["extracted_doi"], "extracted_pmid": t["extracted_pmid"],
                "extracted_pmc": t["extracted_pmc"], "id_tier": t["id_tier"],
                "id_tier_label": t["id_tier_label"], **extra,
            })
        else:
            r = b.classify(parsed["content"])
            rt_rows.append({k: r[k] for k in (
                "content_sha1", "content", "norm_content", "norm_subtype", "id_tier", "id_tier_label",
                "extracted_url", "extracted_isbn", "extracted_doi", "extracted_pmid", "extracted_pmc",
                "anchor_key", "author_key", "year", "year_reliable", "page_ref", "title",
                *[k for k in ADDED_FIELDS if k in r])} | extra)
    rt = pd.DataFrame(rt_rows)
    tm = pd.DataFrame(tm_rows)
    if not rt.empty:
        rt = rt.sort_values("n_revisions", ascending=False, kind="stable").reset_index(drop=True)
    if not tm.empty:
        tm = tm.sort_values("n_revisions", ascending=False, kind="stable").reset_index(drop=True)
    return rt, tm


def run_pages(b: Backend) -> tuple[dict, pd.DataFrame, dict]:
    """6a, 6b, 6c per page. Returns (per-page outputs, concatenated merged_sources, lookup)."""
    per_page = {}
    merged_all = []
    rn_lookup: dict[str, dict] = {}     # content_sha1 -> ref_tag_normalized row
    for page_id in sorted(PAGES):
        rt_in, tm_in = build_page_frames(b, page_id)
        for r in rt_in.to_dict("records"):
            rn_lookup[r["content_sha1"]] = r
        sources, cmap = (b.reftag_match(rt_in, page_id) if not rt_in.empty else (pd.DataFrame(), pd.DataFrame()))
        tsources, tmap = (b.template_match(tm_in, page_id) if not tm_in.empty else (pd.DataFrame(), pd.DataFrame()))

        # upstream load_sources: sources ⋈ canonical map row ⋈ ref_tag_normalized (norm_content, title)
        if not sources.empty:
            canon = cmap[cmap["is_canonical"]][["source_id", "content_sha1"]]
            rt = sources.merge(canon, on="source_id", how="left")
            rt["norm_content"] = rt["content_sha1"].map(lambda s: rn_lookup.get(s, {}).get("norm_content"))
            rt["ref_title"] = rt["content_sha1"].map(lambda s: rn_lookup.get(s, {}).get("title"))
            rt = rt[["source_id", "canonical_sha1", "norm_subtype", "author_key", "year", "extracted_doi",
                     "extracted_isbn", "extracted_pmid", "extracted_url", "n_citations", "match_type",
                     "confidence", "norm_content", "ref_title"]]
            rt["author_key"] = rt["author_key"].apply(b.normalize_key)
        else:
            rt = pd.DataFrame()
        if not tsources.empty:
            tm = tsources.copy()
            tm["author_key"] = tm["author_last"].map(lambda v: v.lower() if isinstance(v, str) else None)
            tm = tm[["source_id", "canonical_sha1", "template_subtype", "author_key", "year", "title",
                     "extracted_doi", "extracted_isbn", "extracted_pmid", "extracted_url", "n_citations",
                     "match_type", "confidence"]]
            tm["author_key"] = tm["author_key"].apply(b.normalize_key)
        else:
            tm = pd.DataFrame()
        merged = b.merge(rt, tm, page_id) if not (rt.empty and tm.empty) else pd.DataFrame()

        per_page[str(page_id)] = {
            "reftag_input": records(rt_in, ["content_sha1"]),
            "template_input": records(tm_in, ["content_sha1"]),
            "sources": records(sources, ["source_id"]),
            "citation_map": records(cmap, ["source_id", "content_sha1"]),
            "template_sources": records(tsources, ["source_id"]),
            "template_map": records(tmap, ["source_id", "content_sha1"]),
            "merged": records(merged, ["merged_source_id"]),
        }
        if not merged.empty:
            m = merged.copy()
            # upstream load_merged_sources: ⋈ canonical ref map ⋈ ref_tag_normalized
            canon_sha = (cmap[cmap["is_canonical"]].set_index("source_id")["content_sha1"].to_dict()
                         if not cmap.empty else {})
            m["_rn_sha"] = m["ref_source_id"].map(canon_sha)
            m["id_tier"] = m["_rn_sha"].map(lambda s: rn_lookup.get(s, {}).get("id_tier")).fillna(0).astype(int)
            m["year_reliable"] = m["_rn_sha"].map(lambda s: bool(rn_lookup.get(s, {}).get("year_reliable") or False))
            m["norm_subtype"] = m["_rn_sha"].map(lambda s: rn_lookup.get(s, {}).get("norm_subtype"))
            merged_all.append(m.drop(columns="_rn_sha"))
    merged_df = pd.concat(merged_all, ignore_index=True) if merged_all else pd.DataFrame()
    if not merged_df.empty:
        merged_df = merged_df[["merged_source_id", "page_id", "author_key", "year", "title", "extracted_doi",
                               "extracted_isbn", "extracted_pmid", "extracted_url", "n_total_citations",
                               "match_type", "confidence", "canonical_sha1", "source_type", "id_tier",
                               "year_reliable", "norm_subtype"]]
        merged_df = merged_df.sort_values("merged_source_id", kind="stable").reset_index(drop=True)
    return per_page, merged_df, rn_lookup


def run_global(b: Backend, merged_df: pd.DataFrame) -> dict:
    gs, gm = b.global_match(merged_df)
    return {"global_sources": records(gs, ["global_source_id"]),
            "global_source_map": records(gm, ["merged_source_id"])}


def run_scores(b: Backend) -> list[dict]:
    rows = [
        {"source_type": "template", "id_tier": 1, "author_key": "a", "year": "2000", "year_reliable": True, "norm_subtype": None, "match_type": "doi"},
        {"source_type": "both", "id_tier": 0, "author_key": None, "year": None, "year_reliable": False, "norm_subtype": "prose_citation", "match_type": "author_year"},
        {"source_type": "ref_tag", "id_tier": 4, "author_key": "a", "year": "2000", "year_reliable": True, "norm_subtype": "prose_citation", "match_type": "doi"},
        {"source_type": "ref_tag", "id_tier": 0, "author_key": "a", "year": "2000", "year_reliable": True, "norm_subtype": "prose_citation", "match_type": "author_year"},
        {"source_type": "ref_tag", "id_tier": 0, "author_key": "a", "year": None, "year_reliable": False, "norm_subtype": "prose_citation", "match_type": "title_fuzzy"},
        {"source_type": "ref_tag", "id_tier": 0, "author_key": None, "year": "1999", "year_reliable": False, "norm_subtype": "prose_citation", "match_type": "singleton"},
        {"source_type": "ref_tag", "id_tier": 0, "author_key": None, "year": None, "year_reliable": False, "norm_subtype": "noise", "match_type": "singleton"},
        {"source_type": "ref_tag", "id_tier": 0, "author_key": None, "year": None, "year_reliable": False, "norm_subtype": "prose_citation", "match_type": "singleton"},
        {"source_type": "", "id_tier": None, "author_key": None, "year": None, "year_reliable": None, "norm_subtype": None, "match_type": None},
    ]
    return [{"row": r, "result": list(b.compute_score(r))} for r in rows]


def run_all(b: Backend) -> dict:
    per_page, merged_df, _ = run_pages(b)
    return {
        "contents": run_contents(b),
        "flags": run_flags(b),
        "pages": per_page,
        "global": run_global(b, merged_df) if not merged_df.empty else {},
        "scores": run_scores(b),
    }


def dumps(obj) -> str:
    return json.dumps(obj, indent=1, ensure_ascii=False, sort_keys=True)
