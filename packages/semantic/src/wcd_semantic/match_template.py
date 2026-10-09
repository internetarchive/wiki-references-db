"""Per-page template source matching (Stage 6b).

Vendored from UVAWikipediaCitationsDatabase (scatter-llc, commit 29f3ae319d54),
``src/citation_matching/template_matching.py``: CONFIDENCE table, build_source_map (union-find signal passes). The DuckDB loading/writing half of that module is
not carried over; wcd_pipeline supplies the inputs from Parquet instead.
Logic is kept verbatim so the golden tests in ``tests/`` can hold it to the
upstream behaviour; see NOTICE for the provenance rules.

Entry point: ``match_page(df, page_id) -> (sources, citation_map, stats)``; ``df`` has the
columns of upstream ``load_page_templates`` (see ``INPUT_COLUMNS``).
"""
from __future__ import annotations

import hashlib
import re
from typing import Optional

import pandas as pd
from rapidfuzz import fuzz

from .unionfind import UnionFind

# Configuration

CONFIDENCE = {
    'doi':        1.00,
    'isbn':       0.95,
    'pmid':       0.95,
    'ref_name':   0.90,
    'url':        0.85,
    'title_year': 0.70,
    'bert':       0.55,
}


_TITLE_NORM_RE = re.compile(r'[^\w\s]', re.UNICODE)
_SPACE_RE = re.compile(r'\s+')


def _norm_title(title: str) -> Optional[str]:
    """Normalize title for comparison — lowercase, strip punctuation."""
    if not title:
        return None
    s = _TITLE_NORM_RE.sub('', title.lower())
    s = _SPACE_RE.sub(' ', s).strip()
    return s if len(s) >= 5 else None


def _richness(row: dict) -> tuple:
    """
    Rank a citation by identifier richness, then revision count.
    Used for canonical selection within a source cluster —
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


# Load template citations for a page


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
    - Short key must be >= 4 chars to avoid single-word noise like 'lee'
    """
    candidates = df[df['author_key'].notna()].copy()
    if candidates.empty:
        return 0

    keys = candidates[['content_sha1', 'author_key', 'year', 'title']].values.tolist()
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
            if not re.search(rf'\b{re.escape(short)}\b', long):
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

# Source mapping

def build_source_map(
    df:      pd.DataFrame,
    page_id: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Group template citations into sources using union-find.

    Pass 1: Hard identifier signals (doi, isbn, pmid, ref_name, url)
    Pass 2: title_year: same normalized title + same year
    Pass 3: BERT: ML-extracted author+year+title (Tier 0 templates only,
            fires only if bert_results was joined in load_page_templates)

    Returns (sources, citation_map) DataFrames.
    """
    sha1s = df['content_sha1'].tolist()
    uf = UnionFind(sha1s)

    # Pass 1: Hard identifier signals
    for match_type, col in [
        ('doi',      'extracted_doi'),
        ('isbn',     'extracted_isbn'),
        ('pmid',     'extracted_pmid'),
        ('ref_name', 'ref_name'),
        ('url',      'extracted_url'),
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

    # Pass 2: title_year signal
    # Templates have structured title params, normalize and group by
    # (norm_title, year). Higher confidence than the BERT pass because
    # template params are already structured and editor-verified.
    df['_norm_title'] = df['title'].apply(_norm_title)
    ty_df = df[df['_norm_title'].notna() & df['year'].notna()][
        ['content_sha1', '_norm_title', 'year']
    ]
    for (norm_title, year), members in \
            ty_df.groupby(['_norm_title', 'year'])['content_sha1'].apply(list).items():
        if len(members) < 2:
            continue
        for i in range(1, len(members)):
            uf.union(members[0], members[i], 'title_year', CONFIDENCE['title_year'])

    df.drop(columns=['_norm_title'], inplace=True, errors='ignore')

    # Pass 2.5 matching author containments
    _author_containment_pass(df, uf)

    # Pass 3: BERT author+year+title
    # Only fires for Tier 0 templates where structured params had no identifier.
    # Uses ML-extracted fields as a last-resort grouping signal.
    # When both citations have an ml_title, exact normalized title match is
    # required. When neither has a title, author+year alone is sufficient.
    if 'ml_author' in df.columns and 'ml_year' in df.columns:
        ml_df = df[df['ml_author'].notna() & df['ml_year'].notna()].copy()
        if not ml_df.empty:
            sha1_to_ml_title = (
                df.set_index('content_sha1')['ml_title'].to_dict()
                if 'ml_title' in df.columns else {}
            )
            groups = ml_df.groupby(['ml_author', 'ml_year'])['content_sha1'].apply(list)
            for (author, year), members in groups.items():
                if len(members) < 2:
                    continue
                titles = {s: sha1_to_ml_title.get(s) for s in members}
                for i in range(len(members)):
                    for j in range(i + 1, len(members)):
                        a, b = members[i], members[j]
                        ta, tb = titles.get(a), titles.get(b)
                        if ta and tb:
                            if _norm_title(ta) == _norm_title(tb):
                                uf.union(a, b, 'bert', CONFIDENCE['bert'])
                        elif not ta and not tb:
                            uf.union(a, b, 'bert', CONFIDENCE['bert'])

    # Build source and map rows from union-find components
    components = uf.components()
    sha1_to_row = df.set_index('content_sha1').to_dict('index')
    source_rows, map_rows = [], []

    for root, members in components.items():
        source_id = hashlib.sha1(
            f"{page_id}:tmpl:{root}".encode()
        ).hexdigest()[:16]

        match_type, confidence = uf.edge_info.get(root, ('singleton', None))
        is_singleton = len(members) == 1
        canonical_sha1 = max(members, key=lambda s: _richness(sha1_to_row[s]))
        canonical = sha1_to_row[canonical_sha1]

        source_rows.append({
            'source_id':        source_id,
            'page_id':          page_id,
            'canonical_sha1':   canonical_sha1,
            'template_subtype': canonical.get('template_subtype'),
            'template_name':    canonical.get('template_name'),
            'author_last':      canonical.get('author_last'),
            'title':            canonical.get('title'),
            'year':             canonical.get('year'),
            'extracted_doi':    canonical.get('extracted_doi'),
            'extracted_isbn':   canonical.get('extracted_isbn'),
            'extracted_pmid':   canonical.get('extracted_pmid'),
            'extracted_url':    canonical.get('extracted_url'),
            'id_tier':          canonical.get('id_tier'),
            'n_citations':      len(members),
            'match_type':       'singleton' if is_singleton else match_type,
            'confidence':       None        if is_singleton else confidence,
        })
        for sha1 in members:
            map_rows.append({
                'content_sha1': sha1,
                'source_id':    source_id,
                'page_id':      page_id,
                'match_type':   'singleton' if is_singleton else match_type,
                'confidence':   None        if is_singleton else confidence,
                'is_canonical': sha1 == canonical_sha1,
            })

    return pd.DataFrame(source_rows), pd.DataFrame(map_rows)


# Summary printing


INPUT_COLUMNS = [
    "content_sha1", "raw_content", "template_name", "template_subtype", "author_last",
    "author_key", "title", "year", "journal", "pages", "extracted_url", "extracted_isbn",
    "extracted_doi", "extracted_pmid", "extracted_pmc", "id_tier", "id_tier_label",
    "n_revisions", "first_rev_id", "ref_name",
    # optional (BERT enrichment): "ml_author", "ml_year", "ml_title"
]


def match_page(df: pd.DataFrame, page_id: int) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Upstream ``run_page`` minus I/O."""
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), {
            "page_id": page_id, "n_citations": 0, "n_sources": 0, "skipped": True,
        }
    sources, citation_map = build_source_map(df, page_id)
    stats = {
        "page_id":      page_id,
        "n_citations":  len(df),
        "n_sources":    len(sources),
        "n_singletons": int((sources["match_type"] == "singleton").sum()),
        "n_bert":       int((sources["match_type"] == "bert").sum()),
        "skipped":      False,
    }
    return sources, citation_map, stats
