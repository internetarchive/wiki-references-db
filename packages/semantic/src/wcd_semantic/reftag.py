"""Ref-tag semantic normalizer.

Vendored from UVAWikipediaCitationsDatabase (scatter-llc, commit 29f3ae319d54),
``src/citation_normalize/reftag_normalizer.py``: classify(content) → norm_subtype, anchor/author/title/year/page_ref fields, norm_content fingerprint, extracted identifiers and id_tier. The DuckDB loading/writing half of that module is
not carried over; wcd_pipeline supplies the inputs from Parquet instead.
Logic is kept verbatim so the golden tests in ``tests/`` can hold it to the
upstream behaviour; see NOTICE for the provenance rules.

Entry point: ``classify(content: str) -> dict`` (keys = ``OUTPUT_COLS``).
"""
from __future__ import annotations

import hashlib
import html
import re
import unicodedata
from functools import lru_cache
from typing import Optional

import mwparserfromhell


# Wikimarkup cleaning
_HTML_ENTITY_RE = re.compile(r"&(?:#\d+|#x[\da-fA-F]+|[a-zA-Z]+);")
_WIKILINK_RE = re.compile(r"\[\[(?:[^|\]]*\|)?([^\]]*)\]\]")
_ITALIC_BOLD_RE = re.compile(r"'{2,3}")
_HTML_TAG_RE = re.compile(r"<[^>]{0,200}>")
_MULTI_SPACE_RE = re.compile(r"\s+")
_HTML_COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)

# Common HTML entities to decode
_ENTITY_MAP = {
    "&ndash;": "–", "&mdash;": "—", "&nbsp;": " ",
    "&amp;": "&",   "&lt;": "<",    "&gt;": ">",
    "&quot;": '"',  "&apos;": "'",
}

def _decode_entities(s: str) -> str:
    for ent, char in _ENTITY_MAP.items():
        s = s.replace(ent, char)
    return html.unescape(s)

# SHA1 content key
def _sha1(s: str) -> str:
    """40-char hex SHA1 of UTF-8 encoded content string."""
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

# Typographic normalization. Second pass after markup stripping
# Collapses cosmetic variants so norm_content works as a dedup fingerprint

# page space cleanup
_PAGE_SPACE_RE = re.compile(r'\bpp?\.\s+(\d)', re.IGNORECASE)

# Years in parenthesis with training punctions
_YEAR_TRAIL_RE = re.compile(r'(\(\d{4}[a-z]?\))\s*[.,;:]+')

# Trailing punctuation at end of string
_TRAIL_PUNCT_RE = re.compile(r'[.,;:\s]+$')

# En/em dash normalisation
_DASH_DIGIT_RE = re.compile(r'(\d)\s*[–—]\s*(\d)')
_DASH_RE = re.compile(r'\s*[–—]\s*')

# Multiple spaces that survived
_MULTI_SPACE2_RE = re.compile(r'  +')

def _typographic_normalize(s: str) -> str:
    """
    Moderate typographic normalization for fingerprinting.
    Applied after markup stripping, collapses cosmetic variants only,
    preserves enough structure to remain human-readable.
    """
    # Normalize page ref spacing
    s = _PAGE_SPACE_RE.sub(lambda m: m.group(0).replace(' ', ''), s)

    # Normalize year trailing punctuation
    s = _YEAR_TRAIL_RE.sub(r'\1 ', s)

    # Normalize dashes
    s = _DASH_DIGIT_RE.sub(r'\1-\2', s)
    s = _DASH_RE.sub(' - ', s)

    # Collapse any new double spaces introduced above
    s = _MULTI_SPACE2_RE.sub(' ', s)

    # Strip trailing punctuation and whitespace
    s = _TRAIL_PUNCT_RE.sub('', s)

    return s.strip()


def normalize_content(raw: str) -> str:
    """
    Strip wikimarkup and apply typographic normalization to produce a
    clean, fingerprint-ready string suitable for deduplication.

    Steps (in order):
      1. Strip error/maintenance templates ({{Citation not found}})
      2. Strip HTML comments
      3. Decode HTML entities
      4. Collapse wikilinks to their display text
      5. Strip italic/bold markers ('' and ''')
      6. Strip HTML tags
      7. Collapse whitespace
      8. Typographic normalization (page refs, year punctuation, dashes,
         trailing punctuation)
    """
    # Strip Wikipedia error/maintenance templates that editors append to
    # citations.
    s = re.sub(
        r'\{\{(?:'
        r'[Cc]itation\s+not\s+found'
        r'|[Dd]ead\s+link'
        r'|[Bb]are\s+URL'
        r'|[Ff]ull\s+citation\s+needed'
        r'|[Cc]itation\s+needed'
        r'|[Vv]erify\s+source'
        r'|[Ff]ailed\s+verification'
        r')[^}]*\}\}',
        '',
        raw,
    )
    s = _HTML_COMMENT_RE.sub("", s)
    s = s.replace("\\'", "'")    # unescape escaped apostrophes before markup stripping
    s = _decode_entities(s)
    s = _WIKILINK_RE.sub(r"\1", s)
    s = _ITALIC_BOLD_RE.sub("", s)
    s = _HTML_TAG_RE.sub("", s)
    s = _MULTI_SPACE_RE.sub(" ", s).strip()
    s = _typographic_normalize(s)
    return s

# Sub-type detection regexes

# Wikilink refs: [[#anchor|Label]] optionally followed by page info
# Captures: anchor key, display label
_WIKILINK_ANCHOR_RE = re.compile(
    r"^\s*\[\[\s*#([^\]|{}\s]+)\s*\|?\s*([^\]]*?)\s*\]\]",
    re.IGNORECASE,
)

# Bare URL ref: [http(s)://... optional label text]
# Also matches raw http URLs not in brackets
_BARE_URL_RE = re.compile(
    r"^\s*\[\s*(https?://[^\s\]]+)\s*(.*?)\s*\]",
    re.IGNORECASE,
)

_URL_STRIP_RE = re.compile(r'https?://[^\s\]}\)]+', re.IGNORECASE)

# Shorthand templates: {{sfn|...}}, {{harvnb|...}}, {{r|...}}, {{rp|...}}
_SHORTHAND_TEMPLATE_RE = re.compile(
    r"^\s*\{\{\s*(sfn|harv|harvnb|harvp|r|rp)\b",
    re.IGNORECASE,
)

# Author-date pattern matches:
#   Donald (1995) p. 555
#   Beveridge 1: 428-33
#   Thomas (1952) 32-34
#   Basler (1946) p. 551
# Requires: word starting with uppercase + (year) or vol:page immediately after
# prone to errors
_AUTHOR_DATE_RE = re.compile(
    r"^([A-Z][a-zA-Z'\-]+(?:\s+(?:Jr\.|Sr\.|[A-Z][a-z]+))?)"  # author surname
    r"\s*"
    r"(?:"
        r"\((\d{4}[a-z]?)\)"            # (year) optional letter suffix
        r"|(\d{1,2})\s*:\s*[\d\-]+"    # vol:pages (Beveridge 1: 428)
    r")",
    re.IGNORECASE,
)

# Page reference: extract raw page string for any sub-type
# Matches: p. 45 / pp. 3-10 / pp.3–10 / p.45 / pages 3-10 / 1: 428-33
_PAGE_REF_RE = re.compile(
    r"(?:"
        r"pp?\.\s*[\d\-–,\s]+"         # p. / pp. with digits
        r"|pages?\s+[\d\-–,\s]+"       # pages N / page N
        r"|\b\d{1,2}\s*:\s*[\d\-–]+"  # vol:pages
    r")",
    re.IGNORECASE,
)

# Year: standalone 4-digit year (not part of a larger number)
_YEAR_RE = re.compile(r"\b(1[5-9]\d{2}|20[0-2]\d)\b")

# Reliable year patterns: year is unambiguously a publication year, not
# embedded in a title or historical date context.
_RELIABLE_YEAR_RE = re.compile(
    # Author (YYYY): capital name + paren year: "Smith (1995)"
    r'[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?\s*\(\s*(1[5-9]\d{2}|20[0-2]\d)\s*[,\.\)]'
    # (Month/Season YYYY)
    r'|\((?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|'
    r'Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|'
    r'Dec(?:ember)?|Spring|Summer|Fall|Autumn|Winter)\s+'
    r'(1[5-9]\d{2}|20[0-2]\d)\s*[,\.\)]'
    # Standalone (YYYY)
    r'|(?<!\d)\(\s*(1[5-9]\d{2}|20[0-2]\d)\s*[,\.\)]'
    # harvnb/sfn positional year param
    r'|\{\{(?:harvnb|sfn|harv|harvp|sfnp)\s*\|[^}]*\|\s*(1[5-9]\d{2}|20[0-2]\d)\s*[|\}]'
    # Surname YYYY: bare author-date: "Sandburg 1974,"
    r'|[A-Z][a-z]{2,}\s+(1[5-9]\d{2}|20[0-2]\d)\s*[,\.]',
    re.IGNORECASE
)

# Author surname from prose: first capitalized word that looks like a surname
# Used as a weak signal when AUTHOR_DATE_RE doesn't match
# _PROSE_AUTHOR_RE = re.compile(
#     r"^([A-Z][a-zA-Z'\-]{2,})"   # Capitalized word of 3+ chars
#     r"(?:\s+[A-Z]\.|\s+[A-Z][a-z]+)*"  # optional initials / given name
#     r"\s*[,;]",                   # followed by comma or semicolon
# )

_PROSE_AUTHOR_RE = re.compile(
    r"^((?:[A-Z][a-zA-Z'\-]{2,})(?:\s+[A-Z]\.|\s+[A-Z][a-zA-Z'\-]+)*)"
    r"\s*[,;]",
)

# Italicized title citation: Author, ''Title'', Publisher, Year, pages
# Handles both "First Last," and "Last, First." name formats
_ITALIC_CITATION_RE = re.compile(
    r"^"
    r"(?P<author>"
        r"[A-ZÁÉÍÓÚÑÜ][a-záéíóúñü\-\.]+"           # first capitalized word
        r"(?:,\s*[A-ZÁÉÍÓÚÑÜ][a-záéíóúñü\-\.]+)?"  # optional: , Word
        r"(?:\s+[A-ZÁÉÍÓÚÑÜ][a-záéíóúñü\-\.]+)?"   # optional: second word
    r")"
    r"[,\.]\s*"
    r"['']{2}(?P<title>[^'']{5,200})['']{2}"         # ''Title'' Limited to 200 characters
    r"(?:[,\.]\s*(?P<publisher>[^,]{3,60}))?"         # optional publisher
    r"(?:[,\.]\s*(?P<year>1[0-9]{3}|20[0-2][0-9]))?" # optional year
    r"(?:[,\.]\s*pp?\.\s*[\d\-]+)?",                  # optional pages
    re.UNICODE
)

# Known Org name indicators. Used to try and clean up author errors. 
# Multi-word matches containing these are kept as-is
# rather than extracting just the last word as surname
_ORG_INDICATORS = re.compile(
    r'\b(group|organization|organisation|press|university|college|institute|'
    r'foundation|committee|collective|association|society|federation|union|'
    r'network|project|coalition|alliance|council|bureau|agency|department|'
    r'office|center|centre|library|archive|archives|magazine|journal|'
    r'tribune|times|gazette|post|news|review|quarterly|forum)\b',
    re.IGNORECASE,
)

def _normalize_author_key(raw: str) -> Optional[str]:
    """
    Normalize an author key extracted from prose:
      1. Strip whitespace and trailing punctuation
      2. Assume "Last, First" format, take part before comma as surname
      3. Multi-word org names (contain org indicator words) are kept as-is
      4. Multi-word person names assume extract last word as surname
      5. Unicode normalization to collapse diacritic variants
    """
    if not raw:
        return None

    key = raw.strip().strip('.,;:')
    if not key or len(key) < 2:
        return None

    words = key.split()

    # "Last, First" format: take everything before first comma as surname
    if ',' in key and not _ORG_INDICATORS.search(key):
        surname = key.split(',')[0].strip()
    elif len(words) > 1:
        # Org name indicators: keep full name
        if _ORG_INDICATORS.search(key):
            surname = key
        # Parentheses: likely org or title, keep as-is
        elif '(' in key or ')' in key:
            surname = key
        # All-caps abbreviation (NEFAC, etc.): keep as-is
        elif all(w.isupper() for w in words):
            surname = key
        else:
            # Person name: take last word as surname
            surname = words[-1]
    else:
        surname = key

    # Unicode normalization: decompose then re-encode as ASCII
    normalized = (
        unicodedata.normalize('NFKD', surname)
        .encode('ascii', 'ignore')
        .decode('ascii')
        .strip()
    )

    return normalized.lower() if normalized else None

# Noise: too short or clearly non-citation
_NOISE_RE = re.compile(r"^[\s\W]{0,4}$")

# Wiki table syntax:lines starting with {|, |-, |}, or ! (header cells)
# These are wikitext table markup that slipped through the footnote filter
_WIKI_TABLE_RE = re.compile(r"^\s*(?:\{\||\|\-|\|\}|![ \t])", re.MULTILINE)

# Field extractors shared across sub-types

def _extract_year(s: str) -> Optional[str]:
    """
    Extract 4-digit year from string, preferring reliable publication year
    patterns over bare year scanning.

    Priority:
      1. Reliable pattern match (Author (YYYY), (YYYY), harvnb param, etc.)
      2. Bare scan fallback: any 4-digit year in range
    """
    # strip urls, years extracts from them are not reliable
    text = _URL_STRIP_RE.sub('', s)

    # Try reliable patterns first
    m = _RELIABLE_YEAR_RE.search(text)
    if m:
        # Return first non-None group
        for g in m.groups():
            if g is not None:
                return g
    # Fallback to bare scan
    m = _YEAR_RE.search(text)
    return m.group(1) if m else None


def _year_reliable(s: str) -> bool:
    """True if the year in s is reliably a publication year (not title/date context)."""
    if not s:
        return False
    return bool(_RELIABLE_YEAR_RE.search(s))


def _extract_page_ref(s: str) -> Optional[str]:
    m = _PAGE_REF_RE.search(s)
    if not m:
        return None
    return _MULTI_SPACE_RE.sub(" ", m.group(0)).strip().rstrip(",;")

# Sub-type classifiers

def _classify_wikilink(content: str, norm: str) -> Optional[dict]:
    """[[#anchor|Label]], p. N style internal bibliography references."""
    m = _WIKILINK_ANCHOR_RE.match(content)
    if not m:
        return None

    anchor_key = m.group(1).strip().lstrip("#")   # strip extra # from ##anchor
    anchor_label = m.group(2).strip() or anchor_key

    # Author key = first word of anchor key (e.g. "Donald2001" -> "Donald")
    author_key = re.match(r"[A-Za-z'\-]+", anchor_key)
    author_key = author_key.group(0) if author_key else None

    return {
        "norm_subtype":    "wikilink_ref",
        "anchor_key":      anchor_key,
        "anchor_label":    anchor_label,
        "url_anchor_text": None,
        "author_key":      author_key,
        "title":           None,
        "year":            _extract_year(norm),
        "year_reliable":   _year_reliable(content),
        "page_ref":        _extract_page_ref(norm),
    }


def _classify_bare_url(content: str, norm: str) -> Optional[dict]:
    """[http://... Label text] bare external link refs."""
    m = _BARE_URL_RE.match(content)
    if not m:
        return None

    label = m.group(2).strip() or None

    return {
        "norm_subtype":    "bare_url",
        "anchor_key":      None,
        "anchor_label":    None,
        "url_anchor_text": label,
        "author_key":      None,
        "title":           None,
        "year":            _extract_year(norm),
        "year_reliable":   _year_reliable(content),
        "page_ref":        _extract_page_ref(norm),
    }


def _classify_shorthand_template(content: str, norm: str) -> Optional[dict]:
    """ Shorthand citation templates: {{sfn}}, {{harvnb}}, {{r}}, {{rp}}, etc.

    Extracts author_key, year, and page_ref directly from template params
    rather than relying on regex over norm_content. The param structure is:
      unnamed positional: [Author1, Author2, ..., Year]  (year = last non-empty)
      named: p / pp = page ref
    """
    if not _SHORTHAND_TEMPLATE_RE.match(content):
        return None

    author_key = None
    year = None
    page_ref = None

    try:
        # Parse only the template portion, calls mwparser
        wc = mwparserfromhell.parse(content)
        tpl = next(wc.ifilter_templates(recursive=False))

        unnamed = [
            str(p.value).strip()
            for p in tpl.params
            if str(p.name).strip().isdigit()
        ]
        named = {
            str(p.name).strip(): str(p.value).strip()
            for p in tpl.params
            if not str(p.name).strip().isdigit()
        }

        # Last non-empty unnamed param that looks like a year, then
        # everything before it is considered author surnames
        non_empty = [u for u in unnamed if u]
        if non_empty and re.match(r'^\d{4}[a-z]?$', non_empty[-1]):
            year = non_empty[-1]
            authors = non_empty[:-1]
            author_key = authors[0] if authors else None
        else:
            # No year found: assume all positionals are authors
            author_key = non_empty[0] if non_empty else None

        # Page ref from named p= / pp= params
        page_val = named.get('p') or named.get('pp')
        if page_val:
            prefix = 'pp.' if named.get('pp') else 'p.'
            page_ref = f"{prefix} {page_val}"

    except Exception:
        # Fall back to regex extraction if mwparserfromhell fails
        year = _extract_year(norm)
        page_ref = _extract_page_ref(norm)

    return {
        "norm_subtype":    "shorthand_template",
        "anchor_key":      None,
        "anchor_label":    None,
        "url_anchor_text": None,
        "author_key":      author_key,
        "title":           None,
        "year":            year,
        "year_reliable":   True,   # harvnb/sfn years are always positional params
        "page_ref":        page_ref,
    }


def _classify_author_date(content: str, norm: str) -> Optional[dict]:
    """Author (year) p. N or Author vol:pages style shorthand prose refs.

    Guard: if the raw content starts with '' (italic markup) the leading token
    is a likely a title, not an author surname, go to prose.
    """
    # Italic-title-first guard: ''Title'' (year) patterns are prose, not author-date
    if content.lstrip().startswith("''"):
        return None

    # HTML <cite> tag guard: these are prose citations wrapped in semantic HTML
    if content.lstrip().startswith("<cite"):
        return None

    m = _AUTHOR_DATE_RE.match(norm)
    if not m:
        return None

    author_key = m.group(1).strip().split()[-1]   # last word = surname
    year = m.group(2) if m.group(2) else None

    return {
        "norm_subtype":    "author_date",
        "anchor_key":      None,
        "anchor_label":    None,
        "url_anchor_text": None,
        "author_key":      author_key,
        "title":           None,
        "year":            year or _extract_year(norm),
        "year_reliable":   True,   # author_date format always has Author (YYYY)
        "page_ref":        _extract_page_ref(norm),
    }


def _classify_noise(content: str, norm: str) -> Optional[dict]:
    """Clearly non-citation content: too short, only punctuation, or wiki table markup."""
    if _NOISE_RE.match(norm) or len(norm) < 5:
        return {
            "norm_subtype":    "noise",
            "anchor_key":      None,
            "anchor_label":    None,
            "url_anchor_text": None,
            "author_key":      None,
            "title":           None,
            "year":            None,
            "year_reliable":   False,
            "page_ref":        None,
        }
    # Wiki table markup: {|, |-, |}, or ! header cells
    if _WIKI_TABLE_RE.search(content):
        return {
            "norm_subtype":    "noise",
            "anchor_key":      None,
            "anchor_label":    None,
            "url_anchor_text": None,
            "author_key":      None,
            "title":           None,
            "year":            None,
            "year_reliable":   False,
            "page_ref":        None,
        }
    return None

# Fake title guard for when  et al. captured as title when italic markup misused
_FAKE_TITLE_RE = re.compile(r'^et\.?\s+al\.?$', re.IGNORECASE)

# Title cleaning patterns applied after extraction
_TITLE_WIKILINK_RE = re.compile(r'\[\[(?:[^|\]]*\|)?([^\]]*)\]\]')  # [[Link|Label]] → Label
_TITLE_URL_RE = re.compile(r'\[https?://[^\s\]]+\s+([^\]]+)\]') # [url Label] → Label
_TITLE_BARE_URL_RE = re.compile(r'https?://\S+') # bare URLs → strip

def _classify_italic_citation(content: str, norm: str) -> Optional[dict]:
    """Author, ''Title'', Publisher, Year, pages formatted citations.

    Covers both name formats:
      Osvaldo Bayer, ''Los anarquistas...''    (First Last,)
      Heider, Ulrike. ''Anarchism...''          (Last, First.)

    Title cleaning applied after extraction:
      - Wikilink markup stripped: [[Title]] -> Title
      - Bracketed URLs stripped: [http://... Label] -> Label
      - Bare URLs removed
      - Trailing punctuation stripped
      - et al. guard: falls through to prose if title is et al.
    """
    m = _ITALIC_CITATION_RE.match(content.strip())
    if not m:
        return None

    author_raw = m.group('author')
    year = m.group('year') or _extract_year(norm)
    title_raw  = m.group('title')

    if not title_raw:
        return None

    # Guard: et al. is not a title — fall through to prose
    if _FAKE_TITLE_RE.match(title_raw.strip()):
        return None

    # Clean the extracted title
    title = title_raw.strip()
    title = _TITLE_WIKILINK_RE.sub(r'\1', title)    # [[Link|Label]] or [[Label]] → Label
    title = _TITLE_URL_RE.sub(r'\1', title)          # [http://... Label] → Label
    title = _TITLE_BARE_URL_RE.sub('', title)        # bare http://... → remove
    title = title.strip(' ,;.')                        # trailing punctuation and whitespace

    if not title:
        return None

    # Normalize author: "Osvaldo Bayer" -> "bayer", "Heider, Ulrike" -> "heider"
    if ',' in author_raw:
        author_key = author_raw.split(',')[0].strip().split()[-1].lower()
    else:
        author_key = author_raw.strip().split()[-1].lower()

    return {
        "norm_subtype":    "italicized_citation",
        "anchor_key":      None,
        "anchor_label":    None,
        "url_anchor_text": None,
        "author_key":      author_key,
        "title":           title,
        "year":            year,
        "year_reliable":   _year_reliable(content),
        "page_ref":        _extract_page_ref(norm),
    }

def _classify_cite_html(content: str, norm: str) -> Optional[dict]:
    """
    VisualEditor-generated <cite class="citation ..."> HTML bibliography entries.

    These have a predictable structure:
      <cite class="citation book">Last, First (YYYY). [url ''Title'']. Publisher.
      ISBN [[Special:BookSources/...]].
      </cite>

    Extracts author_key and year from the Last, First (YYYY) pattern inside
    the tag. Identifier extraction (ISBN/DOI/URL) is handled separately by
    _extract_identifiers() which already handles wikilink-encoded forms.
    """
    if not content.lstrip().startswith('<cite'):
        return None

    # Author: Last, First (YYYY) pattern inside the cite tag
    # The content after <cite ...> starts with the author name
    _CITE_AUTHOR_RE = re.compile(
        r'>\s*'                                    # end of opening tag
        r'(?:\[\[[^\]]*\|)?'                       # optional wikilink prefix [[...|
        r'([A-Z][a-zA-Z\'\-]{1,30})'              # surname
        r'(?:,\s*[A-Z][a-zA-Z\s\.\-]{0,40})?'    # optional first name
        r'\s*(?:\]\])?\s*'                         # optional closing ]] for wikilinks
        r'\(\s*(1[5-9]\d{2}|20[0-2]\d)'           # (YYYY
        r'(?:-\d{2}-\d{2})?\s*\)',                 # optional -MM-DD)
    )
    m = _CITE_AUTHOR_RE.search(content)
    author_key = m.group(1).strip() if m else None
    year = m.group(2).strip() if m else None

    # If no author found, try extracting from norm (stripped content)
    if not author_key:
        _CITE_NORM_AUTHOR_RE = re.compile(
            r'^([A-Z][a-zA-Z\'\-]{1,30})'
            r'(?:,\s*[A-Z][a-zA-Z\s\.\-]{0,40})?'
            r'\s*\(\s*(1[5-9]\d{2}|20[0-2]\d)',
        )
        mn = _CITE_NORM_AUTHOR_RE.match(norm)
        if mn:
            author_key = mn.group(1).strip()
            year = mn.group(2).strip()

    if not year:
        year = _extract_year(content)

    return {
        "norm_subtype":    "cite_html",
        "anchor_key":      None,
        "anchor_label":    None,
        "url_anchor_text": None,
        "author_key":      author_key,
        "title":           None,
        "year":            year,
        "year_reliable":   True,   # (YYYY) in cite tags is always publication year
        "page_ref":        _extract_page_ref(norm),
    }


def _classify_prose(content: str, norm: str) -> dict:
    """
    Catch-all: manually formatted citation, free prose.
    Attempts weak author/year extraction as a best-effort signal
    for downstream matching, but does not commit to a parse.
    """
    author_key = None
    m = _PROSE_AUTHOR_RE.match(norm)
    # if m:
    #     # Take the last word of the matched name as the surname
    #     author_key = m.group(1).strip().split()[-1]
    if m:
        full_match = m.group(1).strip()
        # Reject if match looks like a title (3+ words) rather than a name
        if len(full_match.split()) <= 3:
            author_key = full_match.split()[-1]

    return {
        "norm_subtype":    "prose_citation",
        "anchor_key":      None,
        "anchor_label":    None,
        "url_anchor_text": None,
        "author_key":      author_key,
        "title":           None,
        "year":            _extract_year(norm),
        "year_reliable":   _year_reliable(content),
        "page_ref":        _extract_page_ref(norm),
    }


# Identifier extraction

# Normalization helpers

def _norm_isbn(raw: str) -> Optional[str]:
    s = re.sub(r'(?i)isbn(?:-1[03])?:?\s*', '', raw.strip())
    s = re.sub(r'[\s\-]+', '', s).upper()
    return s if re.match(r'^([0-9]{9}[0-9X]|[0-9]{13})$', s) else None


def _norm_doi(raw: str) -> Optional[str]:
    s = re.sub(r'(?i)^doi:\s*', '', raw.strip())
    s = re.sub(r'(?i)^https?://(?:dx\.)?doi\.org/', '', s)
    s = re.sub(r'[\s.,;:\)\]]+$', '', s.lower())
    return s if s.startswith('10.') else None


def _norm_url(raw: str) -> Optional[str]:
    s = raw.strip().rstrip('.,;>|')
    s = re.sub(r'^http://', 'https://', s, flags=re.IGNORECASE)
    return s if re.match(r'^https?://', s, re.IGNORECASE) else None


def _is_archive_url(url: str) -> bool:
    return bool(re.search(
        r'web\.archive\.org|webcitation\.org|archive\.ph|archive\.today',
        url, re.IGNORECASE
    ))


# Regex patterns

_ID_URL_RE = re.compile(r'\[*(https?://[^\s\]|<>"\']+)', re.IGNORECASE)
# _ID_ISBN_RE = re.compile(r'ISBN[:\s-]*([\dX\-]{9,17})', re.IGNORECASE)
_ID_ISBN_RE = re.compile(
    r'(?:'
    r'ISBN(?:-1[03])?[\s:\-]*(97[89][\d\-]{10,16})'   # ISBN-13 with prefix
    r'|ISBN(?:-1[03])?[\s:\-]*((?!97)[0-9][0-9\-]{8,11}[0-9Xx])'  # ISBN-10 with prefix
    r'|\b(97[89][\d\-]{10,14})\b'                      # bare 978/979 digits
    r')',
    re.IGNORECASE
)
_ID_DOI_RE = re.compile(r'\b(10\.\d{4,9}/[^\s\]|<>"\']+)', re.IGNORECASE)
_ID_PMID_RE = re.compile(r'\bPMID[:\s=]?\s*(\d+)', re.IGNORECASE)
_ID_PMC_RE = re.compile(r'\bPMC[:\s=]?\s*(\d+)', re.IGNORECASE)
_ID_COMMENT_RE = re.compile(r'<!--.*?-->', re.DOTALL)
_ID_CITE_PREFIX_RE = re.compile(r'^\{\{\s*(cite\s+\w|citation\b)', re.IGNORECASE)
# DEVIATION FROM UPSTREAM (determinism): upstream iterates a *set* of parameter
# names here, so which URL a {{cite}} inside a ref yields depends on the hash
# seed. An ordered tuple with the original URL first (as in template.py).
_ID_URL_PARAMS = ("url", "archive-url", "archiveurl", "chapter-url",
                  "chapterurl", "contribution-url", "article-url")

# Wikilink-encoded identifiers: appear in <cite> HTML:
#   [[Special:BookSources/9781439126455|9781439126455]] -> ISBN
#   [[doi:10.1353/cwh.2000.0073|10.1353/...]] -> DOI
_ID_WIKI_ISBN_RE = re.compile(
    r'\[\[Special:BookSources/([0-9X\-]{9,17})', re.IGNORECASE
)
_ID_WIKI_DOI_RE = re.compile(
    r'\[\[doi:(10\.\d{4,9}/[^\]|]+)', re.IGNORECASE
)

_ID_EMPTY = {
    "extracted_url":  None, "extracted_isbn": None,
    "extracted_doi":  None, "extracted_pmid": None,
    "extracted_pmc":  None, "is_archive_url": False,
}


@lru_cache(maxsize=8192)
def _parse_template_ids(content: str) -> Optional[dict]:
    """Parse cite/citation template for identifier params (Tier 1)."""
    if not _ID_CITE_PREFIX_RE.match(content):
        return None
    try:
        wikicode = mwparserfromhell.parse(content)
        tpl = next(wikicode.ifilter_templates(recursive=False))
        params = {
            str(p.name).strip(): _ID_COMMENT_RE.sub('', str(p.value)).strip()
            for p in tpl.params
        }
        return {k: v for k, v in params.items() if v}
    except Exception:
        return None


def _extract_identifiers(content: str) -> dict:
    """
    Extract hard identifiers from a ref_tag content string.
    Returns tier (0-5), tier_label, and extracted identifier fields.

    Tier 1: template-wrapped  : {{cite ...}} with url/doi/isbn/pmid/pmc params
    Tier 2: URL               : http(s):// regex scan
    Tier 3: ISBN              : ISBN prefix + digit pattern
    Tier 4: DOI               : 10.xxxx/ pattern
    Tier 5: PMID / PMC        : PMID/PMC prefix + digits
    
    Tier 0: no identifier found
    """
    c = content.strip()

    # Tier 1: template
    params = _parse_template_ids(c)
    if params is not None:
        url = None
        for k in _ID_URL_PARAMS:
            if k in params:
                url = _norm_url(params[k])
                if url:
                    break
        doi = _norm_doi(params.get("doi", "")  or params.get("DOI", ""))
        isbn = _norm_isbn(params.get("isbn", "") or params.get("ISBN", ""))
        pmid = params.get("pmid") or params.get("PMID")
        pmc = params.get("pmc")  or params.get("PMC")
        id_field = params.get("id", "")
        if id_field:
            if not isbn and re.match(r'(?i)isbn', id_field):
                isbn = _norm_isbn(id_field)
            if not doi and re.match(r'(?i)doi', id_field):
                doi = _norm_doi(id_field)
        if any([url, doi, isbn, pmid, pmc]):
            return {
                "id_tier": 1, "id_tier_label": "template_wrapped",
                "extracted_url":  url,  "extracted_isbn": isbn,
                "extracted_doi":  doi,  "extracted_pmid": pmid,
                "extracted_pmc":  pmc,
                "is_archive_url": _is_archive_url(url) if url else False,
            }

    # Pre-check: wikilink-encoded ISBN/DOI (from <cite> HTML generated by VisualEditor).
    # These contain both a URL and an ISBN/DOI, but the structured identifier is
    # more stable than a URL
    wiki_isbn_m = _ID_WIKI_ISBN_RE.search(c)
    if wiki_isbn_m:
        isbn = _norm_isbn(wiki_isbn_m.group(1))
        if isbn:
            # Also capture the URL if present
            urls = [_norm_url(u) for u in _ID_URL_RE.findall(c)]
            normed = [u for u in urls if u]
            url = normed[0] if normed else None
            return {
                "id_tier": 3, "id_tier_label": "isbn_ref",
                **_ID_EMPTY,
                "extracted_isbn": isbn,
                "extracted_url":  url,
                "is_archive_url": _is_archive_url(url) if url else False,
            }

    wiki_doi_m = _ID_WIKI_DOI_RE.search(c)
    if wiki_doi_m:
        doi = _norm_doi(wiki_doi_m.group(1))
        if doi:
            urls = [_norm_url(u) for u in _ID_URL_RE.findall(c)]
            normed = [u for u in urls if u]
            url = normed[0] if normed else None
            return {
                "id_tier": 4, "id_tier_label": "doi_ref",
                **_ID_EMPTY,
                "extracted_doi": doi,
                "extracted_url": url,
                "is_archive_url": _is_archive_url(url) if url else False,
            }

    # Tier 2: URL
    urls = [_norm_url(u) for u in _ID_URL_RE.findall(c)]
    normed = [u for u in urls if u]
    if normed:
        non_archive = [u for u in normed if not _is_archive_url(u)]
        url = non_archive[0] if non_archive else normed[0]
        return {
            "id_tier": 2, "id_tier_label": "url_ref",
            **_ID_EMPTY,
            "extracted_url":  url,
            "is_archive_url": _is_archive_url(url),
        }

    # Tier 3: ISBN (plain prefix first, then wikilink-encoded)
    m = _ID_ISBN_RE.search(c) or _ID_WIKI_ISBN_RE.search(c)
    if m:
        raw_isbn = next((g for g in m.groups() if g is not None), None)
        isbn = _norm_isbn(raw_isbn) if raw_isbn else None
        if isbn:
            return {
                "id_tier": 3, "id_tier_label": "isbn_ref",
                **_ID_EMPTY, "extracted_isbn": isbn,
            }

    # Tier 4: DOI (bare 10.xxx first, then wikilink-encoded [[doi:...]])
    m = _ID_DOI_RE.search(c) or _ID_WIKI_DOI_RE.search(c)
    if m:
        doi = _norm_doi(m.group(1))
        if doi:
            return {
                "id_tier": 4, "id_tier_label": "doi_ref",
                **_ID_EMPTY, "extracted_doi": doi,
            }

    # Tier 5: PMID / PMC
    pmid_m = _ID_PMID_RE.search(c)
    pmc_m = _ID_PMC_RE.search(c)
    if pmid_m or pmc_m:
        return {
            "id_tier": 5, "id_tier_label": "pmid_pmc_ref",
            **_ID_EMPTY,
            "extracted_pmid": pmid_m.group(1) if pmid_m else None,
            "extracted_pmc":  pmc_m.group(1)  if pmc_m  else None,
        }

    # Tier 0: no identifier
    return {
        "id_tier": 0, "id_tier_label": "no_identifier",
        **_ID_EMPTY,
    }

# Main classifier dispatch

# Priority order matters: wikilink and bare_url are syntactically unambiguous,
# shorthand templates next, then structured author-date, then prose catch-all.
_CLASSIFIERS = [
    _classify_noise,
    _classify_wikilink,
    _classify_bare_url,
    _classify_shorthand_template,
    _classify_author_date,
    _classify_italic_citation,
    _classify_cite_html,
    _classify_prose,
]

def classify(content: str) -> dict:
    """
    Classify a single unique ref_tag content string.
    Returns a dict of all normalized fields including content_sha1
    and merged identifier extraction results (id_tier, extracted_url, etc.)
    """
    norm = normalize_content(content)
    sha1 = _sha1(content)

    # Structural classification
    structure = None
    for fn in _CLASSIFIERS:
        structure = fn(content, norm)
        if structure is not None:
            break

    if structure is None:
        structure = {
            "norm_subtype":    "prose_citation",
            "anchor_key":      None,
            "anchor_label":    None,
            "url_anchor_text": None,
            "author_key":      None,
            "title":           None,
            "year":            _extract_year(norm),
            "year_reliable":   _year_reliable(content),
            "page_ref":        _extract_page_ref(norm),
        }

    # Identifier extraction, runs independently of structural classification
    identifiers = _extract_identifiers(content)

    # Normalize author_key uniformly, single choke point regardless of classifier.
    # Collapses case variants, diacritics, and extracts surname from full names.
    if structure.get('author_key'):
        structure['author_key'] = _normalize_author_key(structure['author_key'])

    return {
        "content_sha1": sha1,
        "content":      content,
        "norm_content": norm,
        **structure,
        **identifiers,
    }

# DuckDB I/O

_SCHEMA = {
    "content_sha1":       "CHAR(40)",
    "content":            "VARCHAR",
    "norm_content":       "VARCHAR",
    "norm_subtype":       "VARCHAR",
    "normalization_rule": "VARCHAR",
    "anchor_key":         "VARCHAR",
    "anchor_label":       "VARCHAR",
    "url_anchor_text":    "VARCHAR",
    "author_key":         "VARCHAR",
    "title":              "VARCHAR",
    "year":               "VARCHAR",
    "year_reliable":      "BOOLEAN",
    "page_ref":           "VARCHAR",
    "id_tier":            "INTEGER",
    "id_tier_label":      "VARCHAR",
    "extracted_url":      "VARCHAR",
    "extracted_isbn":     "VARCHAR",
    "extracted_doi":      "VARCHAR",
    "extracted_pmid":     "VARCHAR",
    "extracted_pmc":      "VARCHAR",
    "is_archive_url":     "BOOLEAN",
}

_OUTPUT_COLS = list(_SCHEMA.keys())
