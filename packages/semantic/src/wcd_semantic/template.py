"""Template semantic normalizer.

Vendored from UVAWikipediaCitationsDatabase (scatter-llc, commit 29f3ae319d54),
``src/citation_normalize/template_normalizer.py``: extract_template(content, template_name, params) → structured bibliographic fields, extracted identifiers and id_tier. The DuckDB loading/writing half of that module is
not carried over; wcd_pipeline supplies the inputs from Parquet instead.
Logic is kept verbatim so the golden tests in ``tests/`` can hold it to the
upstream behaviour; see NOTICE for the provenance rules.

Entry point: ``extract_template(content, template_name, params_json) -> dict``
(keys = ``OUTPUT_COLS``). ``params`` come from ``wcd_semantic.content.parse_template``.
"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from functools import lru_cache
from typing import Optional

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

# Identifier normalization. Mirrors reftag_normalizer

def _norm_isbn(raw: str) -> Optional[str]:
    if not raw:
        return None
    s = re.sub(r'(?i)isbn(?:-1[03])?:?\s*', '', raw.strip())
    s = re.sub(r'[\s\-]+', '', s).upper()
    return s if re.match(r'^([0-9]{9}[0-9X]|[0-9]{13})$', s) else None


def _norm_doi(raw: str) -> Optional[str]:
    if not raw:
        return None
    s = re.sub(r'(?i)^doi:\s*', '', raw.strip())
    s = re.sub(r'(?i)^https?://(?:dx\.)?doi\.org/', '', s)
    s = re.sub(r'[\s.,;:\)\]]+$', '', s.lower())
    return s if s.startswith('10.') else None


def _norm_url(raw: str) -> Optional[str]:
    if not raw:
        return None
    s = raw.strip().rstrip('.,;>|')
    s = re.sub(r'^http://', 'https://', s, flags=re.IGNORECASE)
    return s if re.match(r'^https?://', s, re.IGNORECASE) else None


def _is_archive_url(url: str) -> bool:
    if not url:
        return False
    return bool(re.search(
        r'web\.archive\.org|webcitation\.org|archive\.ph|archive\.today',
        url, re.IGNORECASE
    ))

# Wikimarkup cleaning for param values

_WIKILINK_RE = re.compile(r'\[\[(?:[^|\]]*\|)?([^\]]*)\]\]')
_ITALIC_BOLD_RE = re.compile(r"'{2,3}")
_HTML_TAG_RE = re.compile(r'<[^>]{0,200}>')
_HTML_COMMENT_RE = re.compile(r'<!--.*?-->', re.DOTALL)
_MULTI_SPACE_RE = re.compile(r'\s+')

def _clean_param(val: str) -> str:
    """Strip wikimarkup from a param value."""
    if not val:
        return val
    s = _HTML_COMMENT_RE.sub('', val)
    s = _WIKILINK_RE.sub(r'\1', s)
    s = _ITALIC_BOLD_RE.sub('', s)
    s = _HTML_TAG_RE.sub('', s)
    return _MULTI_SPACE_RE.sub(' ', s).strip()

# Param field aliases
# Templates use many different param names for the same field

# Author surname aliases
_LAST_PARAMS = ['last', 'last1', 'surname', 'surname1', 'author',
                 'author1', 'authors', 'last-first', 'author-last']
# Author first name aliases
_FIRST_PARAMS = ['first', 'first1', 'given', 'given1', 'author-first']
# Title aliases
_TITLE_PARAMS = ['title', 'chapter', 'article', 'entry', 'contribution']
# Journal/periodical aliases
_JOURNAL_PARAMS = ['journal', 'periodical', 'newspaper', 'magazine',
                   'work', 'website', 'publisher']
# Year aliases
_YEAR_PARAMS = ['year', 'date', 'publication-date', 'pubdate']
# Publisher aliases
_PUBLISHER_PARAMS = ['publisher', 'institution', 'distributor']
# URL aliases
_URL_PARAMS = ['url', 'archive-url', 'archiveurl', 'chapter-url',
                 'chapterurl', 'contribution-url']
# ISBN aliases
_ISBN_PARAMS = ['isbn', 'ISBN', 'isbn13', 'isbn10']
# DOI aliases
_DOI_PARAMS = ['doi', 'DOI']
# PMID/PMC
_PMID_PARAMS = ['pmid', 'PMID']
_PMC_PARAMS = ['pmc', 'PMC']
# ADDED (wcd): arXiv and Wikidata QID — see docs/design/decisions_2026_09_10.md item 7
_ARXIV_PARAMS = ['arxiv', 'arXiv', 'eprint']
_ARXIV_RE = re.compile(r'^(?:arxiv:\s*)?((?:\d{4}\.\d{4,5})|(?:[a-z\-]+(?:\.[A-Z]{2})?/\d{7}))(?:v\d+)?$', re.IGNORECASE)
_QID_RE = re.compile(r'^\s*(Q\d+)\s*$', re.IGNORECASE)


def _norm_arxiv(raw: str) -> Optional[str]:
    """arXiv identifier without the 'arXiv:' prefix or a version suffix (ADDED)."""
    if not raw:
        return None
    m = _ARXIV_RE.match(raw.strip())
    return m.group(1) if m else None


def _norm_qid(raw: str) -> Optional[str]:
    """Wikidata item id, upper-cased (ADDED)."""
    if not raw:
        return None
    m = _QID_RE.match(raw)
    return m.group(1).upper() if m else None
# Location aliases
_LOCATION_PARAMS = ['location', 'place', 'publication-place', 'city']
# Pages aliases
_PAGE_PARAMS = ['pages', 'page', 'pp', 'p']

_YEAR_RE = re.compile(r'\b(1[5-9]\d{2}|20[0-2]\d)\b')

def _get_first(params: dict, keys: list) -> Optional[str]:
    """Return cleaned value of first matching key in params."""
    for k in keys:
        v = params.get(k) or params.get(k.lower())
        if v and v.strip():
            return _clean_param(v.strip())
    return None

# Template subtype classification

_CITE_WEB_RE = re.compile(r'cite\s+web|cite\s+news|cite\s+podcast|cite\s+tweet', re.I)
_CITE_JOURNAL_RE = re.compile(r'cite\s+journal|cite\s+article|cite\s+magazine|cite\s+periodical', re.I)
_CITE_BOOK_RE = re.compile(r'cite\s+book|cite\s+encyclop|cite\s+dictionary|cite\s+thesis', re.I)
_CITE_NEWS_RE = re.compile(r'cite\s+news', re.I)


def _classify_subtype(template_name: str, params: dict) -> str:
    """Classify template into a subtype based on name and params."""
    tn = (template_name or '').lower().strip()

    if _CITE_NEWS_RE.search(tn):
        return 'cite_news'
    if _CITE_WEB_RE.search(tn):
        return 'cite_web'
    if _CITE_JOURNAL_RE.search(tn):
        return 'cite_journal'
    if _CITE_BOOK_RE.search(tn):
        return 'cite_book'

    # Fall back to param-based detection
    has_journal = any(params.get(k) for k in ['journal', 'periodical', 'magazine'])
    has_isbn = any(params.get(k) for k in _ISBN_PARAMS)
    has_url = any(params.get(k) for k in ['url', 'archiveurl'])

    if has_journal:
        return 'cite_journal'
    if has_isbn:
        return 'cite_book'
    if has_url:
        return 'cite_web'

    return 'cite_other'

# Main extraction

def extract_template(
    content: str,
    template_name: Optional[str],
    params_json,
) -> dict:
    """
    Extract and normalize fields from a template citation row.
    params_json is either a dict (already parsed) or a JSON string.
    """
    
    # Parse params
    if isinstance(params_json, str):
        try:
            params = json.loads(params_json) or {}
        except Exception:
            params = {}
    elif isinstance(params_json, dict):
        params = params_json or {}
    else:
        params = {}

    sha1 = _sha1(content)
    tn = (template_name or '').strip().lower()
    subtype = _classify_subtype(tn, params)

    # Author
    author_last = _get_first(params, _LAST_PARAMS)
    author_first = _get_first(params, _FIRST_PARAMS)

    # If author= contains a full name (no last= present), split on comma
    if not author_last:
        author_raw = params.get('author') or params.get('author1') or params.get('authors', '')
        if author_raw and ',' in author_raw:
            parts = author_raw.split(',', 1)
            author_last = _clean_param(parts[0].strip())
            author_first = _clean_param(parts[1].strip()) if len(parts) > 1 else None
        elif author_raw:
            # No comma, take last word as surname
            words = _clean_param(author_raw).split()
            author_last = words[-1] if words else None

    author_key = author_last.lower() if author_last else None
    # Unicode normalization: collapse diacritic variants for matching  
    if author_key:
        author_key = (
            unicodedata.normalize('NFKD', author_key)
            .encode('ascii', 'ignore')
            .decode('ascii')
            .strip()
        ) or author_key  # fallback to original if normalization empties it
        
    # Title
    title = _get_first(params, _TITLE_PARAMS)

    # Journal
    journal = _get_first(params, _JOURNAL_PARAMS)
    # Don't let publisher bleed into journal for non-journal types
    if subtype not in ('cite_journal', 'cite_news') and journal == params.get('publisher'):
        journal = None

    # Year
    year = None
    for k in _YEAR_PARAMS:
        v = params.get(k, '')
        if v:
            m = _YEAR_RE.search(str(v))
            if m:
                year = m.group(1)
                break

    date_raw = _get_first(params, ['date', 'publication-date', 'pubdate', 'access-date'])

    # Publisher / Location
    publisher = _get_first(params, _PUBLISHER_PARAMS)
    location = _get_first(params, _LOCATION_PARAMS)
    edition = _clean_param(params.get('edition', '') or '')  or None

    # Volume / Issue / Pages
    volume = _clean_param(params.get('volume', '') or '') or None
    issue = _clean_param(params.get('issue',  '') or params.get('number', '') or '') or None
    pages = _get_first(params, _PAGE_PARAMS)

    # Identifiers
    # URL
    url = None
    for k in _URL_PARAMS:
        v = params.get(k, '')
        if v:
            url = _norm_url(v)
            if url:
                break

    # ISBN
    isbn = None
    for k in _ISBN_PARAMS:
        v = params.get(k, '')
        if v:
            isbn = _norm_isbn(v)
            if isbn:
                break
    # ADDED (wcd): arXiv — |arxiv= on any template, |eprint= only on {{cite arXiv}}
    arxiv = None
    for k in _ARXIV_PARAMS:
        if k == 'eprint' and not tn.startswith('cite arxiv'):
            continue
        v = params.get(k, '')
        if v:
            arxiv = _norm_arxiv(v)
            if arxiv:
                break
    # ADDED (wcd): Wikidata QID — {{Cite Q}} first positional parameter
    qid = None
    if tn in ('cite q', 'citeq'):
        qid = _norm_qid(params.get('1', '') or '')

    # Also check id= field
    id_field = params.get('id', '')
    if not isbn and id_field and re.match(r'(?i)isbn', id_field):
        isbn = _norm_isbn(id_field)

    # DOI
    doi = None
    for k in _DOI_PARAMS:
        v = params.get(k, '')
        if v:
            doi = _norm_doi(v)
            if doi:
                break
    if not doi and id_field and re.match(r'(?i)doi', id_field):
        doi = _norm_doi(id_field)

    # PMID / PMC
    pmid = _get_first(params, _PMID_PARAMS)
    pmc = _get_first(params, _PMC_PARAMS)
    if pmc:
        pmc = re.sub(r'(?i)^pmc\s*', '', pmc).strip()

    # id_tier: trust templates as 1
    # mark as tier 0 if nothing extracted at all
    has_identifier = any([url, isbn, doi, pmid, pmc])
    id_tier = 1 if has_identifier else 0
    id_tier_label = 'template_wrapped' if has_identifier else 'no_identifier'

    # normalization rule
    if subtype == 'cite_book':
        norm_rule = 'book_params_extracted'
    elif subtype == 'cite_journal':
        norm_rule = 'journal_params_extracted'
    elif subtype == 'cite_web':
        norm_rule = 'web_params_extracted'
    elif subtype == 'cite_news':
        norm_rule = 'news_params_extracted'
    else:
        norm_rule = 'template_params_extracted'

    return {
        'content_sha1':     sha1,
        'content':          content,
        'template_name':    tn,
        'template_subtype': subtype,
        'normalization_rule': norm_rule,
        'author_last':      author_last,
        'author_first':     author_first,
        'author_key':       author_key,
        'title':            title,
        'journal':          journal,
        'year':             year,
        'date':             date_raw,
        'publisher':        publisher,
        'location':         location,
        'edition':          edition,
        'volume':           volume,
        'issue':            issue,
        'pages':            pages,
        'extracted_url':    url,
        'extracted_isbn':   isbn,
        'extracted_doi':    doi,
        'extracted_pmid':   pmid,
        'extracted_pmc':    pmc,
        'extracted_arxiv':  arxiv,            # ADDED
        'extracted_wikidata_qid': qid,      # ADDED
        'is_archive_url':   _is_archive_url(url) if url else False,
        'id_tier':          id_tier,
        'id_tier_label':    id_tier_label,
    }

# Schema

_SCHEMA = {
    'content_sha1':       'CHAR(40)',
    'content':            'VARCHAR',
    'template_name':      'VARCHAR',
    'template_subtype':   'VARCHAR',
    'normalization_rule': 'VARCHAR',
    'author_last':        'VARCHAR',
    'author_first':       'VARCHAR',
    'author_key':         'VARCHAR',
    'title':              'VARCHAR',
    'journal':            'VARCHAR',
    'year':               'VARCHAR',
    'date':               'VARCHAR',
    'publisher':          'VARCHAR',
    'location':           'VARCHAR',
    'edition':            'VARCHAR',
    'volume':             'VARCHAR',
    'issue':              'VARCHAR',
    'pages':              'VARCHAR',
    'extracted_url':      'VARCHAR',
    'extracted_isbn':     'VARCHAR',
    'extracted_doi':      'VARCHAR',
    'extracted_pmid':     'VARCHAR',
    'extracted_pmc':      'VARCHAR',
    'extracted_arxiv':    'VARCHAR',   # ADDED
    'extracted_wikidata_qid': 'VARCHAR',  # ADDED
    'is_archive_url':     'BOOLEAN',
    'id_tier':            'INTEGER',
    'id_tier_label':      'VARCHAR',
}

_OUTPUT_COLS = list(_SCHEMA.keys())
