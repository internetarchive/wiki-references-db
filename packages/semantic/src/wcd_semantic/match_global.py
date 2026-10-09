"""Cross-page source matching (Stage 6d).

Vendored from UVAWikipediaCitationsDatabase (scatter-llc, commit 29f3ae319d54),
``src/citation_matching/cross_page_matcher.py``: hard_identifier_pass, author_year_fuzzy_pass, build_global_sources, _id_priority. The DuckDB loading/writing half of that module is
not carried over; wcd_pipeline supplies the inputs from Parquet instead.
Logic is kept verbatim so the golden tests in ``tests/`` can hold it to the
upstream behaviour; see NOTICE for the provenance rules.

Entry point: ``match_global(df, threshold) -> (global_sources, global_source_map, counts)``;
``df`` has the columns of upstream ``load_merged_sources`` (see ``INPUT_COLUMNS``).
"""
from __future__ import annotations

import hashlib
import json
import re as _re
from typing import Optional

import pandas as pd
from rapidfuzz import fuzz

from .scoring import _compute_score
from .unionfind import UnionFind

# Configuration

AUTHOR_YEAR_THRESHOLD = 85   # cross-page is riskier, higher than within-page (70)

_TITLE_NORM_RE = _re.compile(r'[^\w\s]', _re.UNICODE)
_SPACE_RE = _re.compile(r'\s+')


def _norm_title(title: str) -> Optional[str]:
    """Normalize title for comparison: lowercase, strip punctuation."""
    if not title:
        return None
    s = _TITLE_NORM_RE.sub('', str(title).lower())
    s = _SPACE_RE.sub(' ', s).strip()
    return s if len(s) >= 5 else None


def _id_priority(row: dict) -> tuple:
    """Rank a source by identifier quality then citation count."""
    if pd.notna(row.get('extracted_doi')):  
        return (4, row.get('n_total_citations', 0))
    if pd.notna(row.get('extracted_isbn')): 
        return (3, row.get('n_total_citations', 0))
    if pd.notna(row.get('extracted_pmid')): 
        return (2, row.get('n_total_citations', 0))
    if pd.notna(row.get('extracted_url')):  
        return (1, row.get('n_total_citations', 0))
    return (0, row.get('n_total_citations', 0))


# Load merged sources


def hard_identifier_pass(
    df: pd.DataFrame,
    uf: UnionFind,
) -> dict[str, int]:
    """
    Union sources sharing the same DOI, ISBN, PMID, URL across pages.
    These identifiers are globally unique so any match is definitive.
    Returns count of merges per signal.
    """
    counts = {}

    for sig, col, conf in [
        ('doi',  'extracted_doi',  1.00),
        ('isbn', 'extracted_isbn', 0.95),
        ('pmid', 'extracted_pmid', 0.95),
        ('pmc',  'extracted_pmc',  0.95),          # ADDED (wcd)
        ('arxiv', 'extracted_arxiv', 0.95),        # ADDED (wcd)
        ('wikidata_qid', 'extracted_wikidata_qid', 0.95),  # ADDED (wcd)
        # ('url', 'extracted_url', 0.85),
    ]:
        if col not in df.columns:
            continue
        valid = df[df[col].notna()][['merged_source_id', col]]
        grouped = valid.groupby(col)['merged_source_id'].apply(list)
        n_merged = 0
        for val, members in grouped.items():
            if len(members) < 2:
                continue
            for i in range(1, len(members)):
                uf.union(members[0], members[i], sig, conf)
                n_merged += 1
        counts[sig] = n_merged

    return counts


# Pass 2: Author+year fuzzy blocking

def author_year_fuzzy_pass(
    df:        pd.DataFrame,
    uf:        UnionFind,
    threshold: int,
) -> int:
    """
    Group sources by (author_key, year) then fuzzy match on normalized title.
    Only processes sources with no hard identifier i.e. those that survived
    Pass 1 without being merged.

    When both sources have a title, token_sort_ratio must meet threshold.
    When neither has a title, author+year alone is sufficient at lower confidence.

    Returns count of merges made.
    """
    # ADDED (wcd): the extra hard identifiers also exclude a source from the fuzzy pass
    _no_extra = pd.Series(True, index=df.index)
    for _col in ('extracted_pmc', 'extracted_arxiv', 'extracted_wikidata_qid'):
        if _col in df.columns:
            _no_extra &= df[_col].isna()
    no_hard = df[
        df['extracted_doi'].isna()  &
        df['extracted_isbn'].isna() &
        df['extracted_pmid'].isna() &
        _no_extra &
        df['author_key'].notna()    &
        df['year'].notna() &
        df['title'].notna()
    ].copy()

    if no_hard.empty:
        return 0

    sha1_to_title = {
        row['merged_source_id']: _norm_title(row.get('title'))
        for _, row in no_hard.iterrows()
    }

    groups = no_hard.groupby(
        [no_hard['author_key'].str.lower().str.strip(), 'year']
    )['merged_source_id'].apply(list)

    n_merged = 0
    for (author, year), members in groups.items():
        if len(members) < 2:
            continue
        for i in range(len(members)):
            for j in range(i + 1, len(members)):
                a, b = members[i], members[j]
                ta, tb = sha1_to_title.get(a), sha1_to_title.get(b)

                if ta and tb:
                    score = fuzz.token_sort_ratio(ta, tb)
                    if score >= threshold:
                        conf = round(
                            0.60 + (score - threshold) / (100 - threshold) * 0.10, 4
                        )
                        uf.union(a, b, 'author_year_fuzzy', conf)
                        n_merged += 1
                # elif not ta and not tb:
                #     # Author+year only at lower confidence, no title to confirm
                #     uf.union(a, b, 'author_year_fuzzy', 0.55)
                #     n_merged += 1

    return n_merged

# Pass 3: URL passes

# def _extract_domain(url: str) -> Optional[str]:
#     """Extract bare domain from URL for blocking."""
#     if not url:
#         return None
#     # strip protocol and www
#     s = _re.sub(r'^https?://(www\.)?', '', str(url).lower())
#     return s.split('/')[0]   # e.g. 'nytimes.com'

# def url_domain_title_pass(
#     df: pd.DataFrame,
#     uf: UnionFind,
#     threshold: int,
# ) -> int:
#     """
#     Block sources by domain, then confirm with title fuzzy match.
#     Catches same-site citations that share domain but differ in URL path.
#     Only fires when both sources have a title and no hard identifier.
#     """
#     candidates = df[
#         df['extracted_doi'].isna()  &
#         df['extracted_isbn'].isna() &
#         df['extracted_pmid'].isna() &
#         df['extracted_url'].notna() &
#         df['title'].notna()
#     ].copy()

#     if candidates.empty:
#         return 0

#     candidates['domain'] = candidates['extracted_url'].apply(_extract_domain)
#     candidates = candidates[candidates['domain'].notna()]

#     sha1_to_title = {
#         row['merged_source_id']: _norm_title(row.get('title'))
#         for _, row in candidates.iterrows()
#     }

#     groups = candidates.groupby('domain')['merged_source_id'].apply(list)
#     n_merged = 0

#     for domain, members in groups.items():
#         if len(members) < 2:
#             continue
#         for i in range(len(members)):
#             for j in range(i + 1, len(members)):
#                 a, b = members[i], members[j]
#                 ta = sha1_to_title.get(a)
#                 tb = sha1_to_title.get(b)
#                 if not ta or not tb:
#                     continue
#                 # if len(ta) < 20 or len(tb) < 20: # filter for length
#                 #     continue
#                 score = fuzz.token_sort_ratio(ta, tb)
#                 if score >= threshold:
#                     conf = round(0.55 + (score - threshold) / (100 - threshold) * 0.10, 4)
#                     uf.union(a, b, 'url_domain_title_fuzzy', conf)
#                     n_merged += 1

#     return n_merged

# compute the reliability score of each source

def build_global_sources(
    df: pd.DataFrame,
    uf: UnionFind,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build global_sources and global_source_map from union-find components.
    Returns (global_sources_df, global_source_map_df).
    """
    components = uf.components()
    row_by_id = df.set_index('merged_source_id').to_dict('index')

    src_rows = []
    map_rows = []

    for root, members in components.items():
        match_type, confidence = uf.edge_info.get(root, ('singleton', None))
        is_singleton = len(members) == 1
        if is_singleton:
            match_type, confidence = 'singleton', None

        page_ids = sorted({
            row_by_id[m]['page_id'] for m in members if m in row_by_id
        })
        n_pages = len(page_ids)

        # Canonical: highest identifier priority, then most citations
        canonical_id = max(members, key=lambda m: _id_priority(row_by_id.get(m, {})))
        canonical = row_by_id.get(canonical_id, {})

        n_total = sum(
            row_by_id.get(m, {}).get('n_total_citations', 0) for m in members
        )

        global_id = hashlib.sha1(
            f"global:{'|'.join(sorted(members))}".encode()
        ).hexdigest()[:16]

        score, verdict = _compute_score(canonical)

        src_rows.append({
            'global_source_id':  global_id,
            'match_type':        match_type,
            'confidence':        confidence,
            'n_pages':           n_pages,
            'page_ids':          json.dumps(page_ids),
            'canonical_sha1':    canonical.get('canonical_sha1'),
            'author_key':        canonical.get('author_key'),
            'year':              canonical.get('year'),
            'title':             canonical.get('title'),
            'extracted_doi':     canonical.get('extracted_doi'),
            'extracted_isbn':    canonical.get('extracted_isbn'),
            'extracted_pmid':    canonical.get('extracted_pmid'),
            'extracted_pmc':     canonical.get('extracted_pmc'),          # ADDED (wcd)
            'extracted_arxiv':   canonical.get('extracted_arxiv'),        # ADDED (wcd)
            'extracted_wikidata_qid': canonical.get('extracted_wikidata_qid'),  # ADDED (wcd)
            'extracted_url':     canonical.get('extracted_url'),
            'n_total_citations': int(n_total),
            'reliability_score': score,
            'verdict':           verdict,  
        })
        for m in members:
            r = row_by_id.get(m, {})
            map_rows.append({
                'merged_source_id': m,
                'global_source_id': global_id,
                'page_id':          r.get('page_id'),
            })

    return pd.DataFrame(src_rows), pd.DataFrame(map_rows)


# Write to DuckDB


INPUT_COLUMNS = [
    "merged_source_id", "page_id", "author_key", "year", "title", "extracted_doi",
    "extracted_isbn", "extracted_pmid", "extracted_url", "n_total_citations", "match_type",
    "confidence", "canonical_sha1", "source_type", "id_tier", "year_reliable", "norm_subtype",
    # optional, ADDED (wcd): "extracted_pmc", "extracted_arxiv", "extracted_wikidata_qid"
]


def match_global(
    df: pd.DataFrame,
    threshold: int = AUTHOR_YEAR_THRESHOLD,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Upstream ``main`` minus I/O: hard-identifier pass, author+year fuzzy pass, build."""
    uf = UnionFind(df["merged_source_id"].tolist())
    hard_counts = hard_identifier_pass(df, uf)
    n_fuzzy = author_year_fuzzy_pass(df, uf, threshold)
    global_sources, global_source_map = build_global_sources(df, uf)
    return global_sources, global_source_map, {**hard_counts, "author_year_fuzzy": n_fuzzy}
