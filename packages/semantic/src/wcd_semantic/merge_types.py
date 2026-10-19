"""Per-page cross-type merge of ref-tag and template sources (Stage 6c).

Vendored from UVAWikipediaCitationsDatabase (scatter-llc, commit 29f3ae319d54),
``src/citation_matching/cross_type_merger.py``: CONFIDENCE table, identifier index helpers, build_merged_sources (hard ids, author+year, three-pass fuzzy title bridge). The DuckDB loading/writing half of that module is
not carried over; wcd_pipeline supplies the inputs from Parquet instead.
Logic is kept verbatim so the golden tests in ``tests/`` can hold it to the
upstream behaviour; see NOTICE for the provenance rules.

Entry point: ``merge_page(rt, tm, page_id) -> merged``; ``rt``/``tm`` have the columns of
upstream ``load_sources`` (see ``RT_COLUMNS`` / ``TM_COLUMNS``). ``prepare_sources`` applies the
author-key normalisation that upstream did inside its loader.
"""
from __future__ import annotations

import hashlib
import unicodedata

import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process

from .unionfind import UnionFind


# Configuration

CONFIDENCE = {
    'doi':         1.00,
    'isbn':        0.95,
    'pmid':        0.95,
    'pmc':         0.95,   # ADDED (wcd)
    'arxiv':       0.95,   # ADDED (wcd)
    'wikidata_qid': 0.95,  # ADDED (wcd)
    'url':         0.85,
    'author_year': 0.65,
    'title_fuzzy': 0.60, 
    'author_title_fuzzy': 0.60,
    'strong_author_fuzzy':0.60,
}


def _id_priority(row: dict) -> int:
    """
    Rank a source row by identifier quality for canonical selection.
    Higher is better. Mirrors _richness() in the per-type matchers
    but returns a simple integer since revision count is not needed here.
    """
    if pd.notna(row.get('extracted_doi')):  return 4
    if pd.notna(row.get('extracted_isbn')): return 3
    if pd.notna(row.get('extracted_pmid')): return 2
    if pd.notna(row.get('extracted_url')):  return 1
    return 0


def _best_field(rows: list[dict], field: str):
    """Return the first non-null value of field across a list of source rows."""
    for r in rows:
        v = r.get(field)
        if v is not None and not (isinstance(v, float) and pd.isna(v)):
            return v
    return None


def _build_identifier_index(
    df: pd.DataFrame,
    prefix: str,
    col: str,
) -> dict[str, list[str]]:
    """
    Map identifier value -> list of prefixed source_ids.
    Used to find which sources on each side share a given identifier.
    """
    idx: dict = {}
    if col not in df.columns:
        return idx
    for _, row in df[df[col].notna()][['source_id', col]].iterrows():
        idx.setdefault(row[col], []).append(f"{prefix}:{row['source_id']}")
    return idx


def _normalize_key(k):
    if not k:
        return k
    return unicodedata.normalize('NFKD', k).encode('ascii', 'ignore').decode().strip()

# Load sources for a page

def build_merged_sources(
    rt: pd.DataFrame,
    tm: pd.DataFrame,
    page_id: int,
) -> pd.DataFrame:
    """
    Merge ref_tag and template sources into a unified merged_sources table.

    Keys are prefixed to distinguish the two sides:
      rt:<source_id>   ref_tag source
      tm:<source_id>   template source

    Signal passes union rt and tm sources that share common identifier values.
    After all passes, each component is flattened into one merged source row
    with fields propagated from both sides. Template fields take priority
    for structured metadata (title, author from params), each side fills in
    what the other is missing.
    """
    if rt.empty and tm.empty:
        return pd.DataFrame()

    rt_keys = [f"rt:{s}" for s in rt['source_id']] if not rt.empty else []
    tm_keys = [f"tm:{s}" for s in tm['source_id']] if not tm.empty else []
    uf = UnionFind(rt_keys + tm_keys)

    # Pass 1: Hard identifier signals
    # For each identifier, union any rt and tm sources sharing the same value.
    for sig, col in [
        ('doi',  'extracted_doi'),
        ('isbn', 'extracted_isbn'),
        ('pmid', 'extracted_pmid'),
        ('pmc',  'extracted_pmc'),          # ADDED (wcd)
        ('arxiv', 'extracted_arxiv'),       # ADDED (wcd)
        ('wikidata_qid', 'extracted_wikidata_qid'),  # ADDED (wcd)
        ('url',  'extracted_url'),
    ]:
        rt_idx = _build_identifier_index(rt, 'rt', col)
        tm_idx = _build_identifier_index(tm, 'tm', col)
        conf = CONFIDENCE[sig]
        for val in set(rt_idx) & set(tm_idx):
            for rt_key in rt_idx[val]:
                for tm_key in tm_idx[val]:
                    uf.union(rt_key, tm_key, sig, conf)

    # Pass 2: author+year signal
    # Only fires where both sides have author_key AND year.
    # Lowercased and stripped before comparison for case/whitespace tolerance.
    if not rt.empty and not tm.empty \
            and 'author_key' in rt.columns and 'author_key' in tm.columns:
        rt_ay = rt[rt['author_key'].notna() & rt['year'].notna()].copy()
        tm_ay = tm[tm['author_key'].notna() & tm['year'].notna()].copy()

        if not rt_ay.empty and not tm_ay.empty:
            rt_ay_idx: dict = {}
            for _, row in rt_ay.iterrows():
                k = (row['author_key'].lower().strip(), str(row['year']))
                rt_ay_idx.setdefault(k, []).append(f"rt:{row['source_id']}")

            tm_ay_idx: dict = {}
            for _, row in tm_ay.iterrows():
                k = (row['author_key'].lower().strip(), str(row['year']))
                tm_ay_idx.setdefault(k, []).append(f"tm:{row['source_id']}")

            conf = CONFIDENCE['author_year']
            for key in set(rt_ay_idx) & set(tm_ay_idx):
                for rt_key in rt_ay_idx[key]:
                    for tm_key in tm_ay_idx[key]:
                        uf.union(rt_key, tm_key, 'author_year', conf)

    #Pass 3: Fuzzy title matching for unresolved singletons
    # Catches cases where:
    #   - A ref_tag prose citation contains the book title verbatim
    #     (Haley, The Spirit of Japanese Law, page 63)
    #   - Both author and title fuzzy match despite typos (sim/sims)
    #   - Author is near-perfect but title is shortened informal form
    #     (Hoare, Britain and Japan vs Britain and Japan: Biographical...)
    # Only runs on rt sources not yet merged with any tm source.
    tm_with_titles = tm[tm['title'].notna()].copy()
    rt_with_content = rt[rt['norm_content'].notna()]
    if rt_with_content.empty or tm_with_titles.empty:
        pass  # skip Pass 3 entirely if no templates with titles or no ref_tags with out content
    else:
        # Find rt sources that are still unmerged (no tm partner yet)
        # merged_rt_keys = {
        #     k for k in uf.parent
        #     if k.startswith('rt:')
        #     and any(m.startswith('tm:') for m in uf.components().get(uf.find(k), []))
        # }
        _components = uf.components()
        merged_rt_keys = {
            k for k in uf.parent
            if k.startswith('rt:')
            and any(m.startswith('tm:') for m in _components.get(uf.find(k), []))
        }
        unmerged_rt = [
            f"rt:{row['source_id']}" for _, row in rt.iterrows()
            if f"rt:{row['source_id']}" not in merged_rt_keys
            and pd.notna(row.get('norm_content'))  # must have prose content to match against
        ]

        # Build rt content lookup
        rt_content = {
            f"rt:{row['source_id']}": str(row.get('norm_content') or '')
            for _, row in rt.iterrows()
        }
        rt_author = {
            f"rt:{row['source_id']}": str(row.get('author_key') or '').lower().strip()
            for _, row in rt.iterrows()
        }
        rt_title = {
            f"rt:{row['source_id']}": str(row.get('ref_title') or '').lower().strip()
            for _, row in rt.iterrows()
        }
        
        #####################################################################################
        # non-vectorized
        ####################################################################################
        
        for rt_key in unmerged_rt:
            content = rt_content.get(rt_key, '').lower()
            s_author = rt_author.get(rt_key, '')
            s_title   = rt_title.get(rt_key, '')
            if not content:
                continue

            best_score = 0
            best_tm_key = None
            best_match_t = None

            for _, tm_row in tm_with_titles.iterrows():
                tm_key = f"tm:{tm_row['source_id']}"
                t_title = str(tm_row['title']).lower().strip()
                t_author = str(tm_row.get('author_key') or '').lower().strip()
        
                # Title comparison: use extracted ref_title if available (title-to-title),
                # otherwise fall back to norm_content (title-to-full-content)
                if s_title:
                    title_score = fuzz.token_set_ratio(t_title, s_title)
                    title_threshold_a = 80   # tighter for comparing clean title to clean title
                    title_threshold_b = 75   # tighter for author+title combined
                    title_threshold_c = 65   # tighter for strong-author pass
                else:
                    title_score = fuzz.token_set_ratio(t_title, content)
                    title_threshold_a = 65   # original: comparing title to full prose content
                    title_threshold_b = 60   # original
                    title_threshold_c = 50   # original
        
                author_score = (fuzz.ratio(s_author, t_author)
                                if s_author and t_author else 0)
        
                # Sub-pass A: title alone
                if title_score >= title_threshold_a:
                    combined = title_score + (0.2 * author_score)
                    if combined > best_score:
                        best_score, best_tm_key = combined, tm_key
                        best_match_t = 'title_fuzzy'
        
                # Sub-pass B: author + title both above threshold
                if author_score >= 60 and title_score >= title_threshold_b:
                    combined = title_score + (0.5 * author_score)
                    if combined > best_score:
                        best_score, best_tm_key = combined, tm_key
                        best_match_t = 'author_title_fuzzy'
        
                # Sub-pass C: near-perfect author + weak title
                if author_score >= 90 and title_score >= title_threshold_c:
                    combined = title_score + (0.8 * author_score)
                    if combined > best_score:
                        best_score, best_tm_key = combined, tm_key
                        best_match_t = 'strong_author_fuzzy'
        
            if best_tm_key:
                uf.union(rt_key, best_tm_key, best_match_t, 0.60)

        #######################################################################
        
        ######################################################################
        # vectorized
        ########################################################################
        # if not unmerged_rt:
        #     pass
        # else:
        #     # Build flat lists for vectorized comparison
        #     rt_keys_list     = list(unmerged_rt)
        #     rt_contents_list = [rt_content.get(k, '').lower() for k in rt_keys_list]
        #     rt_authors_list  = [rt_author.get(k, '') for k in rt_keys_list]

        #     tm_keys_list     = [f"tm:{row['source_id']}"
        #                         for _, row in tm_with_titles.iterrows()]
        #     tm_titles_list   = [str(row['title']).lower().strip()
        #                         for _, row in tm_with_titles.iterrows()]
        #     tm_authors_list  = [str(row.get('author_key') or '').lower().strip()
        #                         for _, row in tm_with_titles.iterrows()]

        #     # Vectorized title score matrix — shape (n_rt, n_tm)
        #     title_matrix = process.cdist(
        #         rt_contents_list,
        #         tm_titles_list,
        #         scorer=fuzz.token_set_ratio,
        #         workers=-1,   # use all cores
        #     )

        #     # Vectorized author score matrix
        #     author_matrix = process.cdist(
        #         rt_authors_list,
        #         tm_authors_list,
        #         scorer=fuzz.ratio,
        #         workers=-1,
        #     )
        #     # Zero out author scores where either side is empty
        #     for i, s_author in enumerate(rt_authors_list):
        #         if not s_author:
        #             author_matrix[i, :] = 0
        #     for j, t_author in enumerate(tm_authors_list):
        #         if not t_author:
        #             author_matrix[:, j] = 0

        #     # For each rt source find best tm match across all three sub-passes
        #     for i, rt_key in enumerate(rt_keys_list):
        #         title_scores  = title_matrix[i]
        #         author_scores = author_matrix[i]

        #         best_score   = 0
        #         best_tm_key  = None
        #         best_match_t = None

        #         for j, tm_key in enumerate(tm_keys_list):
        #             ts = title_scores[j]
        #             as_ = author_scores[j]

        #             # Sub-pass A
        #             if ts >= 65:
        #                 combined = ts + (0.2 * as_)
        #                 if combined > best_score:
        #                     best_score, best_tm_key = combined, tm_key
        #                     best_match_t = 'title_fuzzy'

        #             # Sub-pass B
        #             if as_ >= 60 and ts >= 60:
        #                 combined = ts + (0.5 * as_)
        #                 if combined > best_score:
        #                     best_score, best_tm_key = combined, tm_key
        #                     best_match_t = 'author_title_fuzzy'

        #             # Sub-pass C
        #             if as_ >= 90 and ts >= 50:
        #                 combined = ts + (0.8 * as_)
        #                 if combined > best_score:
        #                     best_score, best_tm_key = combined, tm_key
        #                     best_match_t = 'strong_author_fuzzy'

        #         if best_tm_key:
        #             uf.union(rt_key, best_tm_key, best_match_t, 0.60)
        ####################################################################################

    # Build merged source rows from union-find components
    rt_by_key = {f"rt:{r['source_id']}": r.to_dict() for _, r in rt.iterrows()} \
        if not rt.empty else {}
    tm_by_key = {f"tm:{r['source_id']}": r.to_dict() for _, r in tm.iterrows()} \
        if not tm.empty else {}

    rows = []
    for root, members in uf.components().items():
        rt_members = [m for m in members if m.startswith('rt:')]
        tm_members = [m for m in members if m.startswith('tm:')]

        has_rt = bool(rt_members)
        has_tm = bool(tm_members)
        src_type = 'both' if (has_rt and has_tm) else \
                   'ref_tag' if has_rt else 'template'

        match_type, confidence = uf.edge_info.get(root, ('unmerged', None))
        if len(members) == 1:
            match_type, confidence = 'unmerged', None

        all_rt_rows = [rt_by_key[k] for k in rt_members if k in rt_by_key]
        all_tm_rows = [tm_by_key[k] for k in tm_members if k in tm_by_key]
        all_rows = all_rt_rows + all_tm_rows

        if not all_rows:
            continue

        # Canonical row: highest identifier priority wins
        canonical_row = max(all_rows, key=_id_priority)

        # Field propagation: template rows first (likely higher quality structured
        # params), then ref_tag rows fill in any remaining gaps
        ordered_rows = all_tm_rows + all_rt_rows

        n_rt = sum(r.get('n_citations', 0) for r in all_rt_rows)
        n_tm = sum(r.get('n_citations', 0) for r in all_tm_rows)

        merged_id = hashlib.sha1(
            f"{page_id}:merged:{'|'.join(sorted(members))}".encode()
        ).hexdigest()[:16]

        rows.append({
            'merged_source_id':  merged_id,
            'page_id':           page_id,
            'source_type':       src_type,
            'ref_source_id':     rt_members[0].replace('rt:', '') if rt_members else None,
            'tmpl_source_id':    tm_members[0].replace('tm:', '') if tm_members else None,
            # ADDED (wcd): every member, so the per-page citation maps can be joined to
            # merged sources (upstream exposes only the first member of each side)
            'rt_source_ids':     [m[3:] for m in rt_members],
            'tm_source_ids':     [m[3:] for m in tm_members],
            'canonical_sha1':    canonical_row.get('canonical_sha1'),
            'author_key':        _best_field(ordered_rows, 'author_key'),
            'year':              _best_field(ordered_rows, 'year'),
            'title':             _best_field(ordered_rows, 'title'),
            'extracted_doi':     _best_field(ordered_rows, 'extracted_doi'),
            'extracted_isbn':    _best_field(ordered_rows, 'extracted_isbn'),
            'extracted_pmid':    _best_field(ordered_rows, 'extracted_pmid'),
            'extracted_pmc':     _best_field(ordered_rows, 'extracted_pmc'),          # ADDED (wcd)
            'extracted_arxiv':   _best_field(ordered_rows, 'extracted_arxiv'),        # ADDED (wcd)
            'extracted_wikidata_qid': _best_field(ordered_rows, 'extracted_wikidata_qid'),  # ADDED (wcd)
            'extracted_url':     _best_field(ordered_rows, 'extracted_url'),
            'n_ref_citations':   int(n_rt),
            'n_tmpl_citations':  int(n_tm),
            'n_total_citations': int(n_rt + n_tm),
            'match_type':        match_type,
            'confidence':        confidence,
        })

    return pd.DataFrame(rows)


# Write to DuckDB


RT_COLUMNS = [
    "source_id", "canonical_sha1", "norm_subtype", "author_key", "year", "extracted_doi",
    "extracted_isbn", "extracted_pmid", "extracted_url", "n_citations", "match_type",
    "confidence", "norm_content", "ref_title",
    # optional, ADDED (wcd): "extracted_pmc", "extracted_arxiv", "extracted_wikidata_qid"
]
TM_COLUMNS = [
    "source_id", "canonical_sha1", "template_subtype", "author_key", "year", "title",
    "extracted_doi", "extracted_isbn", "extracted_pmid", "extracted_url", "n_citations",
    "match_type", "confidence",
]


def prepare_sources(rt: pd.DataFrame, tm: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """The non-SQL part of upstream ``load_sources``: NFKD/ASCII-fold the author keys.
    (Upstream's SQL also lower-cases ``author_last`` into ``tm.author_key``; callers do that.)"""
    rt = rt.copy(); tm = tm.copy()
    if not tm.empty:
        tm["author_key"] = tm["author_key"].apply(_normalize_key)
    if not rt.empty:
        rt["author_key"] = rt["author_key"].apply(_normalize_key)
    return rt, tm


def merge_page(rt: pd.DataFrame, tm: pd.DataFrame, page_id: int) -> pd.DataFrame:
    """Upstream ``run_page`` minus I/O (inputs already passed through ``prepare_sources``)."""
    if rt.empty and tm.empty:
        return pd.DataFrame()
    return build_merged_sources(rt, tm, page_id)
