"""Per-page ref-tag source matching (Stage 6a).

Vendored from UVAWikipediaCitationsDatabase (scatter-llc, commit 29f3ae319d54),
``src/citation_matching/ref_tag_matching.py``: CONFIDENCE table, build_source_map (union-find signal passes), fuzzy_cluster and merge_fuzzy (singleton clustering). The DuckDB loading/writing half of that module is
not carried over; wcd_pipeline supplies the inputs from Parquet instead.
Logic is kept verbatim so the golden tests in ``tests/`` can hold it to the
upstream behaviour; see NOTICE for the provenance rules.

Entry point: ``match_page(df, page_id, fuzzy_threshold) -> (sources, citation_map, stats)``,
reproducing upstream ``run_page`` without the database. ``df`` has the columns of
upstream ``load_page_citations`` (see ``INPUT_COLUMNS``).
"""
from __future__ import annotations

import hashlib
import re as _re
import warnings

import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process

from .unionfind import UnionFind

# cannot seem to fix warning about pandas df concatenation issue deprication 
warnings.filterwarnings(
    'ignore',
    message='The behavior of DataFrame concatenation with empty or all-NA entries',
    category=FutureWarning,
)

# Configuration

CONFIDENCE = {
    'doi':               1.00,
    'isbn':              0.95,
    'pmid':              0.95,
    'ref_name':          0.90,
    'url':               0.85,
    'cite_isbn':         0.88,
    'anchor_key':        0.80,
    'content_sha1':      0.75,
    'norm_content':      0.65,
    'author_year_title': 0.60,
    'bert':              0.55,
}

FUZZY_THRESHOLD = 88
AUTHOR_YEAR_THRESHOLD = 70

# Union-Find

_PAGE_STRIP_RE = _re.compile(
    r'(?:pp?\.|pages?)\s*[\d\-–,\s]+'
    r'|(?<!\d)\d{1,4}\s*[-–]\s*\d{1,4}'
    r'|\bch(?:apter)?\.?\s*\d+'
    r'|\bloc\s*=\s*\S+'
    r'|\bvol\.?\s*\d+'
    r'|\bharvnb\b|\bharv\b|\bsfn\b|\bharvp\b',
    _re.IGNORECASE,
)

def _strip(text: str) -> str:
    """Strip page references and template markup for fuzzy comparison."""
    s = _PAGE_STRIP_RE.sub('', str(text or ''))
    s = _re.sub(r'[|{}\[\]]+', ' ', s)
    return _re.sub(r'\s+', ' ', s).strip(' ,;.')

def _richness(row: dict) -> tuple:
    """
    Rank a citation by identifier richness, then revision count.
    Used to select the canonical citation within a source cluster,
    the richest identifier wins, revision count breaks ties.
    """
    if pd.notna(row.get('extracted_doi')):  
        return (4, row.get('n_revisions', 0))
    if pd.notna(row.get('extracted_isbn')): 
        return (3, row.get('n_revisions', 0))
    if pd.notna(row.get('extracted_pmid')): 
        return (2, row.get('n_revisions', 0))
    if pd.notna(row.get('extracted_url')):  
        return (1, row.get('n_revisions', 0))
    return (0, row.get('n_revisions', 0))

def _author_containment_pass(
    df:   pd.DataFrame,
    uf:   UnionFind,
) -> int:
    """
    Match sources where one author_key is a whole-word substring of another,
    confirmed by matching year and (if available) fuzzy title similarity.
    
    Guards:
    - Whole-word boundary check prevents 'sam' matching 'samuel'
    - Year must match exactly (when both present)
    - Title fuzzy score >= 90 when both have titles
    - Short key must be >= 4 chars to avoid single-word noise.
    """
    candidates = df[df['author_key'].notna()].copy()
    if candidates.empty:
        return 0

    keys = candidates[['content_sha1', 'author_key', 'year', 'norm_content']].values.tolist()
    n_merged = 0

    for i in range(len(keys)):
        sha1_a, key_a, year_a, title_a = keys[i]
        if not key_a or len(key_a) < 4:
            continue
        for j in range(i + 1, len(keys)):
            sha1_b, key_b, year_b, title_b = keys[j]
            if not key_b:
                continue

            # Check containment in either direction
            short, long = (key_a, key_b) if len(key_a) <= len(key_b) else (key_b, key_a)
            if short == long:
                continue
            if not _re.search(rf'\b{_re.escape(short)}\b', long):
                continue

            # Year guard — must match if both present
            if year_a and year_b and year_a != year_b:
                continue

            # Title guard
            if title_a and title_b:
                score = fuzz.token_sort_ratio(title_a, title_b)
                if score < 90:
                    continue
                conf = round(0.85 + (score - 90) / 100 * 0.10, 4)
            elif year_a and year_b:
                # No title but year confirmed — lower confidence
                conf = 0.75
            else:
                # Only containment, no year or title — too weak
                continue

            uf.union(sha1_a, sha1_b, 'author_containment', conf)
            n_merged += 1

    return n_merged


def build_source_map(
    df: pd.DataFrame,
    page_id: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Group ref_tag citations into sources using union-find.

    Runs signal passes in confidence order. Each pass unions citations
    sharing the same identifier value. After all passes, builds one
    source row per component with richness-first canonical selection.

    Returns (sources, citation_map) DataFrames.
    """
    sha1s = df['content_sha1'].tolist()
    uf = UnionFind(sha1s)

    # Pass 1: Hard identifier signals
    for match_type, col in [
        ('doi',         'extracted_doi'),
        ('isbn',        'extracted_isbn'),
        ('pmid',        'extracted_pmid'),
        ('ref_name',    'ref_name'),
        ('url',         'extracted_url'),
        ('anchor_key',  'anchor_key'),
        ('norm_content','norm_content'),
    ]:
        if col not in df.columns:
            continue
        confidence = CONFIDENCE[match_type]
        signal_df = df[df[col].notna()][['content_sha1', col]]
        grouped = signal_df.groupby(col)['content_sha1'].apply(list)
        for val, members in grouped.items():
            if len(members) < 2:
                continue
            for i in range(1, len(members)):
                uf.union(members[0], members[i], match_type, confidence)

    # Pass 2: cite_isbn signal
    # Union cite_html citations (which have ISBNs) with other citations
    # on the same page that share (author_key, year) but lack an identifier.
    # Bridges the gap between <cite> bibliography entries and inline shorthands.
    if 'norm_subtype' in df.columns and 'extracted_isbn' in df.columns:
        cite_html = df[
            (df['norm_subtype'] == 'cite_html') &
            df['extracted_isbn'].notna() &
            df['author_key'].notna() &
            df['year'].notna()
        ]
        for _, cite_row in cite_html.iterrows():
            same_ay = df[
                (df['author_key'] == cite_row['author_key']) &
                (df['year'] == cite_row['year']) &
                (df['extracted_isbn'].isna()) &
                (df['content_sha1'] != cite_row['content_sha1'])
            ]
            for _, other_row in same_ay.iterrows():
                uf.union(
                    cite_row['content_sha1'],
                    other_row['content_sha1'],
                    'cite_isbn',
                    CONFIDENCE['cite_isbn'],
                )

    # Pass 3: author+year+title (normalizer fields)
    # Groups citations sharing (author_key, year) where norm_content is
    # similar after stripping page refs. At least one citation in each pair
    # must have year_reliable=True to guard against subject-matter years.
    ay_df = df[df['author_key'].notna() & df['year'].notna()].copy()
    if not ay_df.empty:
        sha1_to_norm = df.set_index('content_sha1')['norm_content'].to_dict()
        sha1_to_title = (
            df.set_index('content_sha1')['title'].to_dict()
            if 'title' in df.columns else {}
        )
        sha1_to_reliable = (
            df.set_index('content_sha1')['year_reliable'].to_dict()
            if 'year_reliable' in df.columns else {}
        )
        groups = ay_df.groupby(['author_key', 'year'])['content_sha1'].apply(list)
        for (author, year), members in groups.items():
            if len(members) < 2:
                continue

            ##############################################################################
            # Non-vectorized
            ##############################################################################
            for i in range(len(members)):
                for j in range(i + 1, len(members)):
                    a, b = members[i], members[j]
                    if sha1_to_reliable and \
                            not sha1_to_reliable.get(a, True) and \
                            not sha1_to_reliable.get(b, True):
                        continue

                    # Use extracted title when available, else fall back to norm_content
                    ta = sha1_to_title.get(a)
                    tb = sha1_to_title.get(b)
                    if ta and tb:
                        # title-to-title: tighter threshold
                        cmp_a, cmp_b = ta.lower(), tb.lower()
                        threshold = 85
                    else:
                        cmp_a = _strip(sha1_to_norm.get(a, ''))
                        cmp_b = _strip(sha1_to_norm.get(b, ''))
                        threshold = AUTHOR_YEAR_THRESHOLD

                    if cmp_a and cmp_b and \
                            fuzz.token_sort_ratio(cmp_a, cmp_b) >= threshold:
                        uf.union(a, b, 'author_year_title',
                                 CONFIDENCE['author_year_title'])

            #############################################################################
            # Vectorized
            # groups = ay_df.groupby(['author_key', 'year'])['content_sha1'].apply(list)
            # for (author, year), members in groups.items():
            #     if len(members) < 2:
            #         continue
            #     norms = [_strip(sha1_to_norm.get(s, '')) for s in members]
            #     if len(members) == 2:
            #         # Skip cdist overhead for the common case
            #         scores = [[0, fuzz.token_sort_ratio(norms[0], norms[1])],
            #                   [0, 0]]
            #     else:
            #         scores = process.cdist(
            #             norms, norms,
            #             scorer=fuzz.token_sort_ratio,
            #             workers=-1,
            #         )
            #     for i in range(len(members)):
            #         for j in range(i + 1, len(members)):
            #             if scores[i][j] >= AUTHOR_YEAR_THRESHOLD:
            #                 a, b = members[i], members[j]
            #                 if sha1_to_reliable and \
            #                         not sha1_to_reliable.get(a, True) and \
            #                         not sha1_to_reliable.get(b, True):
            #                     continue
            #                 uf.union(a, b, 'author_year_title',
            #                          CONFIDENCE['author_year_title'])
            #####################################################################################
                        
    # Pass 4: author containment
    # Matches sources where one author_key is a whole-word substring of
    # another (e.g. 'sabatini' within 'peter sabatini'), confirmed by
    # matching year and fuzzy title similarity >= 90.
    _author_containment_pass(df, uf)

    # Pass 5: BERT author+year+title
    # Same logic as pass 3 but using BERT-extracted fields for Tier 0
    # prose citations where the normalizer couldn't extract author/year.
    if 'ml_author' in df.columns and 'ml_year' in df.columns:
        ml_df = df[df['ml_author'].notna() & df['ml_year'].notna()].copy()
        if not ml_df.empty:
            sha1_to_ml_title = (
                df.set_index('content_sha1')['ml_title'].to_dict()
                if 'ml_title' in df.columns else {}
            )
            sha1_to_reliable = (
                df.set_index('content_sha1')['year_reliable'].to_dict()
                if 'year_reliable' in df.columns else {}
            )
            groups = ml_df.groupby(['ml_author', 'ml_year'])['content_sha1'].apply(list)
            for (author, year), members in groups.items():
                if len(members) < 2:
                    continue
                titles = {s: sha1_to_ml_title.get(s) for s in members}
                for i in range(len(members)):
                    for j in range(i + 1, len(members)):
                        a, b = members[i], members[j]
                        if sha1_to_reliable and \
                                not sha1_to_reliable.get(a, True) and \
                                not sha1_to_reliable.get(b, True):
                            continue
                        ta, tb = titles.get(a), titles.get(b)
                        if ta and tb:
                            if fuzz.token_sort_ratio(
                                ta.lower(), tb.lower()
                            ) >= AUTHOR_YEAR_THRESHOLD:
                                uf.union(a, b, 'bert', CONFIDENCE['bert'])
                        elif not ta and not tb:
                            uf.union(a, b, 'bert', CONFIDENCE['bert'])

    # Build source and map rows from union-find components
    components = uf.components()
    sha1_to_row = df.set_index('content_sha1').to_dict('index')
    src_rows, map_rows = [], []

    for root, members in components.items():
        source_id = hashlib.sha1(
            f"{page_id}:{root}".encode()
        ).hexdigest()[:16]
        match_type, confidence = uf.edge_info.get(
            root, ('content_sha1', CONFIDENCE['content_sha1'])
        )
        is_singleton = len(members) == 1
        canonical = max(members, key=lambda s: _richness(sha1_to_row[s]))
        can = sha1_to_row[canonical]

        src_rows.append({
            'source_id':         source_id,
            'page_id':           page_id,
            'canonical_sha1':    canonical,
            'norm_subtype':      can.get('norm_subtype'),
            'id_tier':           can.get('id_tier'),
            'extracted_doi':     can.get('extracted_doi'),
            'extracted_isbn':    can.get('extracted_isbn'),
            'extracted_pmid':    can.get('extracted_pmid'),
            'extracted_url':     can.get('extracted_url'),
            'anchor_key':        can.get('anchor_key'),
            'author_key':        can.get('author_key'),
            'year':              can.get('year'),
            'title':             can.get('title'),
            'n_citations':       len(members),
            'match_type':        'singleton' if is_singleton else match_type,
            'confidence':        None if is_singleton else confidence,
        })
        for sha1 in members:
            map_rows.append({
                'content_sha1': sha1,
                'source_id':    source_id,
                'page_id':      page_id,
                'match_type':   'singleton' if is_singleton else match_type,
                'confidence':   None if is_singleton else confidence,
                'is_canonical': sha1 == canonical,
            })

    return pd.DataFrame(src_rows), pd.DataFrame(map_rows)


# Fuzzy clustering: greedy singleton pass

_YEAR_RE = _re.compile(r'\b(1[5-9]\d{2}|20[0-2]\d)\b')
_PAGE_REF_RE = _re.compile(
    r'(?:pp?\.|pages?)\s*[\d\-–,\s]+'
    r'|(?<!\d)\d{1,4}\s*[-–]\s*\d{1,4}'
    r'|\bch(?:apter)?\.?\s*\d+'
    r'|\bvol\.?\s*\d+',
    _re.IGNORECASE,
)

def strip_page(text):
    return _PAGE_REF_RE.sub('', str(text or '')).strip(' ,;.')

def get_years(text):
    return set(_YEAR_RE.findall(str(text or '')))

def years_compatible(a, b):
    ya = get_years(a.get('norm_content'))
    yb = get_years(b.get('norm_content'))
    return not (ya and yb) or bool(ya & yb)

#########################################################################
# non-vectorized
##########################################################################
def fuzzy_cluster(df: pd.DataFrame, threshold: int) -> list[dict]:
    """
    Greedy fuzzy clustering on norm_content with year-mismatch guard.

    Applied to singletons after the union-find pass. Groups citations
    whose stripped norm_content scores above threshold on token_sort_ratio.
    Citations with incompatible years are never compared.
    """

    records = df.to_dict('records')
    clusters = []

    for row in records:
        norm = str(row.get('norm_content') or '')
        norm_bare = strip_page(norm)
        if not norm.strip():
            clusters.append({'members': [row], 'best_score': 100})
            continue
        best_idx, best_score = None, 0
        for idx, cl in enumerate(clusters):
            if not all(years_compatible(row, m) for m in cl['members']):
                continue
            score = max(
                fuzz.token_sort_ratio(
                    norm_bare,
                    strip_page(str(
                        m.get('norm_content') or ''
                    ))
                )
                for m in cl['members']
            )
            if score > threshold and score > best_score:
                best_score, best_idx = score, idx
        if best_idx is not None:
            clusters[best_idx]['members'].append(row)
            clusters[best_idx]['best_score'] = max(
                clusters[best_idx]['best_score'], best_score
            )
        else:
            clusters.append({'members': [row], 'best_score': 100})
    return clusters
##############################################################################

#############################################################################
# Vectorized
###########################################################################
# def fuzzy_cluster(df: pd.DataFrame, threshold: int) -> list[dict]:
#     """
#     Vectorized fuzzy clustering on norm_content with year-mismatch guard.
#     Uses cdist for the full similarity matrix then UnionFind for grouping.
#     """
#     records = df.to_dict('records')
#     if len(records) < 2:
#         return [{'members': [r], 'best_score': 100} for r in records]

#     sha1s = [r['canonical_sha1'] for r in records]
#     norms = [strip_page(str(r.get('norm_content') or '')) for r in records]

#     # Vectorized similarity matrix
#     matrix = process.cdist(
#         norms, norms,
#         scorer=fuzz.token_sort_ratio,
#         workers=-1,
#     )

#     uf = UnionFind(sha1s)
#     best_scores = {s: 100 for s in sha1s}

#     # Union all pairs above threshold with year compatibility guard
#     n = len(records)
#     for i in range(n):
#         for j in range(i + 1, n):
#             score = matrix[i, j]
#             if score < threshold:
#                 continue
#             if not years_compatible(records[i], records[j]):
#                 continue
#             uf.union(sha1s[i], sha1s[j], 'fuzzy', score)
#             root = uf.find(sha1s[i])
#             best_scores[root] = max(best_scores.get(root, 0), score)

#     # Build cluster list from components
#     components = uf.components()
#     idx_lookup = {s: r for s, r in zip(sha1s, records)}

#     return [
#         {
#             'members':    [idx_lookup[s] for s in members],
#             'best_score': best_scores.get(root, 100),
#         }
#         for root, members in components.items()
#     ]
####################################################################################

def merge_fuzzy(
    sources:        pd.DataFrame,
    citation_map:   pd.DataFrame,
    page_citations: pd.DataFrame,
    fuzzy_clusters: list[dict],
    page_id:        int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Merge fuzzy-matched singleton clusters into sources and citation_map."""
    matched = [c for c in fuzzy_clusters if len(c['members']) > 1]
    if not matched:
        return sources, citation_map

    sha1_to_cit = page_citations.set_index('content_sha1').to_dict('index')
    new_src, new_map, merged = [], [], set()

    for cl in matched:
        members = cl['members']
        score = cl['best_score']
        confidence = round(0.60 + (score - 88) / (100 - 88) * 0.10, 4)
        canonical = max(
            members,
            key=lambda m: _richness(sha1_to_cit.get(m['canonical_sha1'], {}))
        )['canonical_sha1']
        can = sha1_to_cit.get(canonical, {})
        sha1s = sorted(m['canonical_sha1'] for m in members)
        source_id = hashlib.sha1(
            f"{page_id}:fuzzy:{'|'.join(sha1s)}".encode()
        ).hexdigest()[:16]

        new_src.append({
            'source_id':         source_id,
            'page_id':           page_id,
            'canonical_sha1':    canonical,
            'norm_subtype':      can.get('norm_subtype'),
            'id_tier':           can.get('id_tier'),
            'extracted_doi':     can.get('extracted_doi'),
            'extracted_isbn':    can.get('extracted_isbn'),
            'extracted_pmid':    can.get('extracted_pmid'),
            'extracted_url':     can.get('extracted_url'),
            'anchor_key':        can.get('anchor_key'),
            'author_key':        can.get('author_key'),
            'year':              can.get('year'),
            'title':             can.get('title'),
            'n_citations':       len(members),
            'match_type':        'fuzzy',
            'confidence':        confidence,
        })
        for m in members:
            sha1 = m['canonical_sha1']
            merged.add(sha1)
            new_map.append({
                'content_sha1': sha1,
                'source_id':    source_id,
                'page_id':      page_id,
                'match_type':   'fuzzy',
                'confidence':   confidence,
                'is_canonical': sha1 == canonical,
            })

    return (
        pd.concat(
            [sources[~sources['canonical_sha1'].isin(merged)],
             pd.DataFrame(new_src)],
            ignore_index=True,
        ),
        pd.concat(
            [citation_map[~citation_map['content_sha1'].isin(merged)],
             pd.DataFrame(new_map)],
            ignore_index=True,
        ),
    )


INPUT_COLUMNS = [
    "content_sha1", "content", "norm_content", "norm_subtype", "id_tier", "id_tier_label",
    "extracted_url", "extracted_isbn", "extracted_doi", "extracted_pmid", "extracted_pmc",
    "anchor_key", "author_key", "year", "year_reliable", "page_ref", "title",
    "n_revisions", "first_rev_id", "ref_name",
    # optional (BERT enrichment): "ml_author", "ml_year", "ml_title"
]


def match_page(
    df: pd.DataFrame,
    page_id: int,
    fuzzy_threshold: int = FUZZY_THRESHOLD,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Upstream ``run_page`` minus I/O: union-find passes, then fuzzy clustering of singletons."""
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), {
            "page_id": page_id, "n_citations": 0, "n_sources": 0, "skipped": True,
        }
    sources, citation_map = build_source_map(df, page_id)
    singleton_df = (
        sources[sources["match_type"] == "singleton"]
        [["canonical_sha1", "norm_subtype", "id_tier"]]
        .merge(
            df[["content_sha1", "norm_content"]],
            left_on="canonical_sha1", right_on="content_sha1", how="left",
        )
        .drop(columns="content_sha1")
        .reset_index(drop=True)
    )
    clusters = fuzzy_cluster(singleton_df, fuzzy_threshold)
    sources, citation_map = merge_fuzzy(sources, citation_map, df, clusters, page_id)
    stats = {
        "page_id":        page_id,
        "n_citations":    len(df),
        "n_sources":      len(sources),
        "n_singletons":   int((sources["match_type"] == "singleton").sum()),
        "n_fuzzy_groups": int((sources["match_type"] == "fuzzy").sum()),
        "n_bert":         int((sources["match_type"] == "bert").sum()),
        "skipped":        False,
    }
    return sources, citation_map, stats
