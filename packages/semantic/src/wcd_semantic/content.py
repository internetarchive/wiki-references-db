"""Ref content classification (template vs ref_tag), bullet-block splitting, footnote filter.

Vendored from UVAWikipediaCitationsDatabase (scatter-llc, commit 29f3ae319d54),
``src/wcd_extract/citation_extractor.py``: the per-ref parsing half of the UVA extractor: _is_citation (footnote filter), _is_multi_block / iter_citations (bullet-block split), _parse_cached / _parse_citation (cite-template parse with mwparserfromhell, embedded-template and shorthand handling). The DuckDB loading/writing half of that module is
not carried over; wcd_pipeline supplies the inputs from Parquet instead.
Logic is kept verbatim so the golden tests in ``tests/`` can hold it to the
upstream behaviour; see NOTICE for the provenance rules.

Entry points: ``iter_citations(ref_name, content)``, ``parse_citation(ref_name, content)``,
``parse_template(content)``, ``is_citation(content)``, ``is_multi_block(content)``.
The UVA revision-scanning loop (``_extract_refs``, ``RevisionProcessor``) is NOT
vendored: reference discovery is ``refs_extractor``'s job.
"""
from __future__ import annotations

import re
from functools import lru_cache
from typing import Iterator, Optional

import mwparserfromhell

_REF_NAME_RE = re.compile(
    r"""<ref[^>]*\bname\s*=\s*(?:"([^"]+)"|'([^']+)'|([^'">\s/]+))""",
    re.I,
)

_CITE_PREFIX_RE = re.compile(r"^\{\{\s*(cite\s+\w|citation\b)", re.I)

_EMBEDDED_CITE_RE = re.compile(
    r'(\{\{\s*(?:cite\s+\w+|citation)\b[^}]*\}\})',
    re.IGNORECASE | re.DOTALL
)

# Separate pattern for shorthands, these are handled neatly by ref_tag normalizer
_EMBEDDED_SHORTHAND_RE = re.compile(
    r'\{\{\s*(?:harvnb|sfn|harvp|harv)\b[^}]*\}\}',
    re.IGNORECASE | re.DOTALL
)

# strip HTML comments from param values
_HTML_COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)

# Refs that start with these patterns are editorial notes, not citations
_FOOTNOTE_PATTERNS = [
    re.compile(r"^&",                        re.I),  # HTML entities
    re.compile(r"^\[\[[^#][^\]]*\]\][^{[h]", re.I),  # bare wikilinks
    re.compile(r"^<",                        re.I),  # raw HTML tags
]

# Refs containing these signals are real citations even if they look like prose
_CITATION_SIGNALS = re.compile(
    r"https?://|ISBN|\b10\.\d{4,}/|PMID|PMC|\{\{cite|\{\{citation",
    re.I,
)

# Regex for identifying bullet pointed references
_BULLET_LINE_RE = re.compile(
    r'^[ \t]*\*+[ \t]+(.+)',       # capture content after bullet marker
    re.MULTILINE,
)

def _is_multi_block(content: str) -> bool:
    """
    True if content looks like a bullet list of multiple citations.
    Requires 2+ bullet lines each with at least 30 chars of content to
    guards against single-bullet footnotes and short list items.
    """
    bullets = [
        b for b in _BULLET_LINE_RE.findall(content)
        if len(b.strip()) >= 30
    ]
    if len(bullets) < 2:
        return False
    # At least one bullet must look like a citation
    # Guards against data tables formatted with bullets
    return any(_is_citation(b) for b in bullets)

def iter_citations(
    ref_name: Optional[str],
    content:  str,
) -> Iterator[dict]:
    """
    Main entry point for citation extraction from a single ref tag.
    Replaces direct calls to _parse_citation in the extraction loop.

    Detects multi-citation bullet blocks and processes each bullet
    independently through the full _parse_citation pipeline, template
    detection, embedded template extraction, shorthand skipping, etc.
    Each bullet is treated exactly as if it were its own ref tag.

    For single citations, falls straight through to _parse_citation
    with no overhead.
    """
    if _is_multi_block(content):
        bullets = [
            b.strip()
            for b in _BULLET_LINE_RE.findall(content)
            if len(b.strip()) >= 30
        ]

        for bullet in bullets:
            split_ref_name = ref_name if ref_name else None
            result = _parse_citation(split_ref_name, bullet)
            if result is not None:
                yield result
    else:
        result = _parse_citation(ref_name, content)
        if result is not None:
            yield result
            

def _is_citation(content: str) -> bool:
    """Return True if content looks like a real citation rather than an editorial note."""
    if _CITATION_SIGNALS.search(content):
        return True
    for pat in _FOOTNOTE_PATTERNS:
        if pat.match(content):
            return False
    return True



def _parse_cached(content: str) -> Optional[dict]:
    """
    Parse a {{cite ...}} or {{citation}} template with mwparserfromhell.
    Cached -> the same wikitext string appearing in thousands of revisions
    is only parsed once.
    Returns None if the content is not a cite/citation template.
    """
    if not _CITE_PREFIX_RE.match(content):
        return None
    try:
        wikicode = mwparserfromhell.parse(content)
        tpl = next(wikicode.ifilter_templates(recursive=False))
        tname = str(tpl.name).strip().lower()
        params = {str(p.name).strip(): str(p.value).strip() for p in tpl.params}
        # Strip HTML comments from param values, then drop empty params
        params = {
            k: _HTML_COMMENT_RE.sub("", v).strip()
            for k, v in params.items()
        }
        params = {k: v for k, v in params.items() if v}
        return {
            "citation_type": "template",
            "template_name": tname,
            "params":        params,
            "raw":           content,
            "content":       None,
        }
    except (StopIteration, Exception):
        return None


def _parse_citation(ref_name: Optional[str], content: str) -> Optional[dict]:
    """
    Classify a single ref tag's content as a template or ref_tag citation.
    Returns None if the content is a non-citation footnote.
    """
    if '{{' not in content:
        if not _is_citation(content):
            return None
        return {
            "citation_type": "ref_tag",
            "template_name": None,
            "params":        None,
            "content":       content,
            "ref_name":      ref_name,
        }

    parsed = _parse_cached(content)
    if parsed:
        return {**parsed, "ref_name": ref_name}

    # adding embedded citation template recognizer. Templates embedded by prose
    embedded = _EMBEDDED_CITE_RE.search(content)
    if embedded:
        embedded_parsed = _parse_cached(embedded.group(1).strip())
        if embedded_parsed:
            return {
                "citation_type": embedded_parsed["citation_type"],
                "template_name": embedded_parsed["template_name"],
                "params":        embedded_parsed["params"],
                "raw":           embedded_parsed["raw"],
                "content":       None,
                "ref_name":      ref_name,
            }
    
    if _EMBEDDED_SHORTHAND_RE.search(content):
    # Store full prose as content for reftag_normalizer's shorthand classifier
    # will find the {{harvnb}} and other shorthand within it and extract author/year correctly
        if not _is_citation(content):
            return None
        return {
            "citation_type": "ref_tag",
            "template_name": None,
            "params":        None,
            "content":       content,
            "ref_name":      ref_name,
        }
        
    # Has {{ but not a cite/citation template, or parse failed
    if not _is_citation(content):
        return None
    return {
        "citation_type": "ref_tag",
        "template_name": None,
        "params":        None,
        "content":       content,
        "ref_name":      ref_name,
    }


# Public aliases
is_citation = _is_citation
is_multi_block = _is_multi_block
parse_citation = _parse_citation
parse_template = _parse_cached
