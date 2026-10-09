"""Generate fixtures/golden/semantic/expected.json from the UPSTREAM UVA code.

Run once (and only re-run deliberately):

    uv run python packages/semantic/tests/gen_golden.py upstream/uva/src

The upstream modules insert their own directory on sys.path and import
``pipeline_log``, so the ``src`` directory must be passed. duckdb/pandas must
be importable (they are, in the workspace venv).
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
GOLDEN = HERE.parents[2] / "fixtures" / "golden" / "semantic" / "expected.json"


def upstream_backend(src: Path):
    sys.path.insert(0, str(src))
    sys.path.insert(0, str(src / "citation_normalize"))
    sys.path.insert(0, str(src / "citation_matching"))
    sys.path.insert(0, str(src / "wcd_extract"))
    ce = importlib.import_module("citation_extractor")
    rn = importlib.import_module("reftag_normalizer")
    tn = importlib.import_module("template_normalizer")
    rm = importlib.import_module("ref_tag_matching")
    tm = importlib.import_module("template_matching")
    xm = importlib.import_module("cross_type_merger")
    cp = importlib.import_module("cross_page_matcher")
    from assemble import Backend

    # Upstream iterates a set of URL parameter names (hash-seed dependent);
    # apply the same ordered tuple wcd_semantic.reftag uses so the golden is
    # deterministic. This is the one documented deviation (see NOTICE).
    rn._ID_URL_PARAMS = ("url", "archive-url", "archiveurl", "chapter-url",
                         "chapterurl", "contribution-url", "article-url")

    def extract_flags(text):
        state = ce._new_state()
        for line in text.split("\n"):
            ce._update_signals(state, line)
        return ce._extract_flags(text, state)

    def reftag_match(df, page_id):       # upstream run_page without the DB
        sources, cmap = rm.build_source_map(df, page_id)
        singleton_df = (
            sources[sources["match_type"] == "singleton"][["canonical_sha1", "norm_subtype", "id_tier"]]
            .merge(df[["content_sha1", "norm_content"]], left_on="canonical_sha1", right_on="content_sha1", how="left")
            .drop(columns="content_sha1").reset_index(drop=True)
        )
        clusters = rm.fuzzy_cluster(singleton_df, rm.FUZZY_THRESHOLD)
        return rm.merge_fuzzy(sources, cmap, df, clusters, page_id)

    def global_match(df):
        uf = cp.UnionFind(df["merged_source_id"].tolist())
        cp.hard_identifier_pass(df, uf)
        cp.author_year_fuzzy_pass(df, uf, cp.AUTHOR_YEAR_THRESHOLD)
        return cp.build_global_sources(df, uf)

    return Backend(
        parse_citation=ce._parse_citation, iter_citations=ce.iter_citations,
        is_multi_block=ce._is_multi_block, is_citation=ce._is_citation,
        classify=rn.classify, extract_template=tn.extract_template, extract_flags=extract_flags,
        reftag_match=reftag_match, template_match=tm.build_source_map,
        normalize_key=xm._normalize_key, merge=xm.build_merged_sources,
        global_match=global_match, compute_score=cp._compute_score,
    )


if __name__ == "__main__":
    from assemble import dumps, run_all
    b = upstream_backend(Path(sys.argv[1]).resolve())
    GOLDEN.parent.mkdir(parents=True, exist_ok=True)
    GOLDEN.write_text(dumps(run_all(b)) + "\n", encoding="utf-8")
    print(f"wrote {GOLDEN}")
