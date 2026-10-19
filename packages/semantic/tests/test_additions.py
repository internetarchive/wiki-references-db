"""Behaviour wcd_semantic adds over upstream: arXiv, Wikidata QID and PMC as
hard identifiers (docs/design/decisions_2026_09_10.md item 7)."""
from __future__ import annotations

import pandas as pd

from wcd_semantic import content, match_global, match_reftag, match_template, merge_types, reftag, scoring, template


def _tmpl(c):
    p = content.parse_template(c)
    return template.extract_template(p["raw"], p["template_name"], p["params"])


def test_template_arxiv_param():
    t = _tmpl("{{cite journal |last=Doe |title=T |journal=J |year=2020 |arxiv=2101.00001v2}}")
    assert t["extracted_arxiv"] == "2101.00001"          # version stripped
    assert _tmpl("{{cite arXiv |last=Doe |title=T |eprint=hep-th/9901001 |class=hep-th}}")["extracted_arxiv"] == "hep-th/9901001"
    # |eprint= is only an arXiv alias on {{cite arXiv}}
    assert _tmpl("{{cite journal |title=T |eprint=2101.00001}}")["extracted_arxiv"] is None
    assert _tmpl("{{cite journal |title=T |arxiv=not an id}}")["extracted_arxiv"] is None


def test_template_cite_q():
    assert _tmpl("{{Cite Q|Q12345678}}")["extracted_wikidata_qid"] == "Q12345678"
    assert _tmpl("{{cite Q |q12345678 |title=x}}")["extracted_wikidata_qid"] == "Q12345678"
    assert _tmpl("{{cite book |title=x |1=Q1}}")["extracted_wikidata_qid"] is None


def test_reftag_arxiv_from_url_or_text():
    assert reftag.classify("Doe, J. (2021). Preprint. https://arxiv.org/abs/2101.00001v1")["extracted_arxiv"] == "2101.00001"
    assert reftag.classify("Doe, J. (2021). Preprint. arXiv:hep-th/9901001.")["extracted_arxiv"] == "hep-th/9901001"
    r = reftag.classify("Doe, J. (2021). A paper with no identifiers.")
    assert r["extracted_arxiv"] is None and r["extracted_wikidata_qid"] is None
    # the upstream tier logic is untouched
    assert reftag.classify("https://arxiv.org/abs/2101.00001")["id_tier"] == 2


def _reftag_df(rows):
    base = {k: None for k in match_reftag.INPUT_COLUMNS}
    return pd.DataFrame([{**base, **r} for r in rows])


def test_reftag_matcher_unions_on_added_identifiers():
    df = _reftag_df([
        {"content_sha1": "a" * 40, "content": "x", "norm_content": "x", "extracted_arxiv": "2101.00001", "n_revisions": 2},
        {"content_sha1": "b" * 40, "content": "y", "norm_content": "y", "extracted_arxiv": "2101.00001", "n_revisions": 1},
        {"content_sha1": "c" * 40, "content": "z", "norm_content": "z", "extracted_pmc": "123", "n_revisions": 1},
        {"content_sha1": "d" * 40, "content": "w", "norm_content": "w", "extracted_pmc": "123", "n_revisions": 1},
        {"content_sha1": "e" * 40, "content": "v", "norm_content": "v", "extracted_wikidata_qid": "Q1", "n_revisions": 1},
        {"content_sha1": "f" * 40, "content": "u", "norm_content": "u", "extracted_wikidata_qid": "Q1", "n_revisions": 1},
    ])
    sources, cmap, _ = match_reftag.match_page(df, 1, fuzzy_threshold=101)   # no fuzzy merging
    assert len(sources) == 3
    assert set(sources["match_type"]) == {"arxiv", "pmc", "wikidata_qid"}
    assert (sources["confidence"] == 0.95).all()
    assert set(sources.columns) >= {"extracted_pmc", "extracted_arxiv", "extracted_wikidata_qid"}


def test_template_matcher_and_cross_type_and_global_on_added_identifiers():
    base = {k: None for k in match_template.INPUT_COLUMNS}
    tm_in = pd.DataFrame([
        {**base, "content_sha1": "1" * 40, "raw_content": "a", "template_name": "cite arxiv", "extracted_arxiv": "2101.00001", "n_revisions": 1},
        {**base, "content_sha1": "2" * 40, "raw_content": "b", "template_name": "cite journal", "extracted_arxiv": "2101.00001", "n_revisions": 1},
    ])
    tsources, _, _ = match_template.match_page(tm_in, 7)
    assert len(tsources) == 1 and tsources.iloc[0]["match_type"] == "arxiv"

    # cross-type: a prose ref with the same arXiv id merges with the template source
    rt = pd.DataFrame([{**{k: None for k in merge_types.RT_COLUMNS}, "source_id": "r1", "canonical_sha1": "9" * 40,
                        "n_citations": 1, "match_type": "singleton", "extracted_arxiv": "2101.00001"}])
    tm = pd.DataFrame([{**{k: None for k in merge_types.TM_COLUMNS}, "source_id": tsources.iloc[0]["source_id"],
                        "canonical_sha1": "1" * 40, "n_citations": 2, "match_type": "arxiv", "extracted_arxiv": "2101.00001"}])
    merged = merge_types.merge_page(rt, tm, 7)
    assert len(merged) == 1 and merged.iloc[0]["source_type"] == "both" and merged.iloc[0]["match_type"] == "arxiv"
    assert merged.iloc[0]["extracted_arxiv"] == "2101.00001"

    # cross-page: two pages sharing the id collapse to one global source; the fuzzy pass skips them
    g = pd.DataFrame([
        {**{k: None for k in match_global.INPUT_COLUMNS}, "merged_source_id": "m1", "page_id": 7, "extracted_arxiv": "2101.00001",
         "n_total_citations": 3, "match_type": "arxiv", "source_type": "both", "id_tier": 1, "year_reliable": False},
        {**{k: None for k in match_global.INPUT_COLUMNS}, "merged_source_id": "m2", "page_id": 8, "extracted_arxiv": "2101.00001",
         "n_total_citations": 1, "match_type": "unmerged", "source_type": "template", "id_tier": 0, "year_reliable": False},
    ])
    gs, gm, counts = match_global.match_global(g)
    assert len(gs) == 1 and gs.iloc[0]["match_type"] == "arxiv" and gs.iloc[0]["n_pages"] == 2
    assert counts["arxiv"] == 1 and counts["author_year_fuzzy"] == 0
    assert gs.iloc[0]["extracted_arxiv"] == "2101.00001"


def test_scoring_bonus_for_added_match_types():
    base = {"source_type": "ref_tag", "id_tier": 0, "author_key": None, "year": None, "year_reliable": False, "norm_subtype": None}
    for mt in ("pmc", "arxiv", "wikidata_qid"):
        assert scoring.compute_score({**base, "match_type": mt})[0] == 10


def test_merged_sources_list_every_member():
    # two ref-tag sources and one template source sharing a URL become one merged
    # source; upstream exposes only the first member of each side
    rt = pd.DataFrame([
        {**{k: None for k in merge_types.RT_COLUMNS}, "source_id": "r1", "canonical_sha1": "1" * 40, "n_citations": 1,
         "match_type": "singleton", "extracted_url": "https://e.org/x"},
        {**{k: None for k in merge_types.RT_COLUMNS}, "source_id": "r2", "canonical_sha1": "2" * 40, "n_citations": 1,
         "match_type": "singleton", "extracted_url": "https://e.org/x"},
        {**{k: None for k in merge_types.RT_COLUMNS}, "source_id": "r3", "canonical_sha1": "3" * 40, "n_citations": 1,
         "match_type": "singleton"},
    ])
    tm = pd.DataFrame([{**{k: None for k in merge_types.TM_COLUMNS}, "source_id": "t1", "canonical_sha1": "4" * 40,
                        "n_citations": 1, "match_type": "singleton", "extracted_url": "https://e.org/x"}])
    merged = merge_types.merge_page(rt, tm, 7).set_index("ref_source_id")
    both = merged[merged["source_type"] == "both"].iloc[0]
    assert sorted(both["rt_source_ids"]) == ["r1", "r2"] and both["tm_source_ids"] == ["t1"]
    assert both.name in ("r1", "r2")
    alone = merged.loc["r3"]
    assert alone["rt_source_ids"] == ["r3"] and alone["tm_source_ids"] == [] and alone["tmpl_source_id"] is None
