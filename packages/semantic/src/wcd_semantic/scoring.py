"""Reliability score and verdict per global source (Stage 7f).

Vendored from UVAWikipediaCitationsDatabase (scatter-llc, commit 29f3ae319d54),
``src/citation_matching/cross_page_matcher.py``: _compute_score. The DuckDB loading/writing half of that module is
not carried over; wcd_pipeline supplies the inputs from Parquet instead.
Logic is kept verbatim so the golden tests in ``tests/`` can hold it to the
upstream behaviour; see NOTICE for the provenance rules.

Entry point: ``compute_score(row) -> (score, verdict)``.
"""
from __future__ import annotations


def _compute_score(row: dict) -> tuple[float, str]:
    """Compute reliability score and verdict from a canonical source row."""
    source_type = row.get('source_type', '') or ''
    id_tier = int(row.get('id_tier', 0) or 0)
    year_reliable = bool(row.get('year_reliable', False))
    norm_subtype = row.get('norm_subtype', '') or ''
    match_type = row.get('match_type', '') or ''
    author_key = row.get('author_key')
    year = row.get('year')

    score = round(sum([
        30 if source_type in ('template', 'both') else 0,
        25 if id_tier > 0                         else 0,
        15 if author_key                          else 0,
        15 if year_reliable                       else 0,
        5  if year                                else 0,
        {'doi': 10, 'isbn': 10, 'pmid': 10,
         'pmc': 10, 'arxiv': 10, 'wikidata_qid': 10,   # ADDED (wcd)
         'url': 8, 'author_year': 5,
         'title_fuzzy': 3, 'strong_author_fuzzy': 3,
         'author_title_fuzzy': 3}.get(match_type, 0),
    ]), 1)

    if source_type in ('template', 'both'):
        verdict = 'high_confidence'
    elif id_tier > 0:
        verdict = 'high_confidence'
    elif norm_subtype == 'noise':
        verdict = 'likely_not_citation'
    elif score >= 50:
        verdict = 'high_confidence'
    elif score >= 25:
        verdict = 'medium_confidence'
    elif score >= 10:
        verdict = 'low_confidence'
    else:
        verdict = 'questionable'

    return score, verdict


compute_score = _compute_score
