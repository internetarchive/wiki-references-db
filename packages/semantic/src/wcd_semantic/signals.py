"""Per-revision structural signal flags.

Vendored from UVAWikipediaCitationsDatabase (scatter-llc, commit 29f3ae319d54),
``src/wcd_extract/citation_extractor.py``: _FLAG_RES and the cheap substring pre-filter + regex confirmation that produce the seven has_* booleans (revision_signals in the unified model). The DuckDB loading/writing half of that module is
not carried over; wcd_pipeline supplies the inputs from Parquet instead.
Logic is kept verbatim so the golden tests in ``tests/`` can hold it to the
upstream behaviour; see NOTICE for the provenance rules.

Entry point: ``extract_flags(text) -> dict``.
"""
from __future__ import annotations

import re

_FLAG_RES = {
    "has_named_ref":          re.compile(r"<ref[^>]*\bname\s*=",       re.I),
    "has_self_closing_ref":   re.compile(r"<ref[^>]*/\s*>",            re.I),
    "has_references_tag":     re.compile(r"<references\b",             re.I),
    "has_cite_template":      re.compile(r"\{\{\s*cite\s+",            re.I),
    "has_citation_template":  re.compile(r"\{\{\s*citation\b",         re.I),
    "has_shorthand_template": re.compile(
        r"\{\{\s*(sfn|harv|harvnb|harvp|r|rp)\b", re.I),
}

def _update_signals(state: dict, line: str) -> None:
    """Update revision state flags from a single line using cheap substring checks."""
    if "<ref" in line or "<REF" in line:
        state["saw_ref"] = state["interesting"] = state["buffer"] = True
    if "<references" in line or "<REFERENCES" in line:
        state["saw_references"] = state["interesting"] = state["buffer"] = True
    if "{{cite " in line or "{{Cite " in line or "{{cite\t" in line:
        state["saw_cite"] = state["interesting"] = state["buffer"] = True
    if "{{citation" in line or "{{Citation" in line:
        state["saw_citation"] = state["interesting"] = state["buffer"] = True
    if ("{{sfn" in line or "{{Sfn" in line or "{{harv" in line
            or "{{Harv" in line or "{{harvnb" in line or "{{harvp" in line
            or "{{rp" in line or "{{r|" in line):
        state["saw_shorthand"] = state["interesting"] = state["buffer"] = True


def _new_state() -> dict:
    return {
        "interesting": False, "buffer": False,
        "saw_ref": False, "saw_references": False,
        "saw_cite": False, "saw_citation": False,
        "saw_shorthand": False,
    }

# Flag extraction — runs on the full buffered revision text

def _extract_flags(text: str, state: dict) -> dict:
    flags = {"has_ref_tag": state["saw_ref"]}
    if state["saw_ref"]:
        flags["has_named_ref"] = bool(_FLAG_RES["has_named_ref"].search(text))
        flags["has_self_closing_ref"] = bool(_FLAG_RES["has_self_closing_ref"].search(text))
    else:
        flags["has_named_ref"] = flags["has_self_closing_ref"] = False
    flags["has_references_tag"] = bool(_FLAG_RES["has_references_tag"].search(text)) if state["saw_references"] else False
    flags["has_cite_template"] = bool(_FLAG_RES["has_cite_template"].search(text)) if state["saw_cite"] else False
    flags["has_citation_template"] = bool(_FLAG_RES["has_citation_template"].search(text)) if state["saw_citation"] else False
    flags["has_shorthand_template"] = bool(_FLAG_RES["has_shorthand_template"].search(text)) if state["saw_shorthand"] else False
    return flags


def extract_flags(text: str) -> dict:
    """Compute the seven structural flags for one revision text."""
    state = _new_state()
    for line in text.split("\n"):
        _update_signals(state, line)
    return _extract_flags(text, state)
