"""Group 2: global sources — ``/sources/{id}``, ``/sources/{id}/pages`` and the
lookup by identifier, ``/sources?doi=…`` (or ``key=…``).

``global_source_id`` is an interim per-build handle (``<run id>.<hash>``, bare for
the build before run ids); ``source_key`` is the durable name: the standardized
identifier, else the canonical part's hash."""
from __future__ import annotations

from sqlalchemy import text

from ..db import Database
import re

from wcd_schema.ids import split_global_source_id

from ..models import CanonicalPart, GlobalSourceResponse, SourceLookupResponse, SourceUsagePage, SourceUsagePagesResponse
from . import BadRequest, NotFound
from .common import SOURCE_KEY_FALLBACK, SOURCE_KEY_ORDER, page_url, source_key, source_label

_GS_COLS = ("global_source_id, source_type, match_type, confidence, author_key, year, title, extracted_doi, "
            "extracted_isbn, extracted_pmid, extracted_pmc, extracted_arxiv, extracted_wikidata_qid, "
            "extracted_librarybase_id, extracted_url, n_pages, n_total_citations, reliability_score, verdict, "
            "canonical_part_sha1")


def _global_source(con, global_source_id: str):
    row = con.execute(text(f"SELECT {_GS_COLS} FROM global_sources WHERE global_source_id = :g"),
                      {"g": global_source_id}).mappings().first()
    if row is None:
        raise NotFound(f"Unknown global source {global_source_id!r}")
    return row


def _run_id(db: Database, global_source_id: str) -> str | None:
    run_id, _ = split_global_source_id(global_source_id)
    return run_id or db.settings.base_run_id


def _source_response(db: Database, row, canonical: CanonicalPart | None = None) -> GlobalSourceResponse:
    return GlobalSourceResponse(source_label=source_label(row), source_key=source_key(row),
                                run_id=_run_id(db, row["global_source_id"]), canonical=canonical,
                                **{k: row[k] for k in row.keys()})


def get_source(db: Database, global_source_id: str) -> GlobalSourceResponse:
    with db.connect() as con:
        row = _global_source(con, global_source_id)
        canon = con.execute(text("""
            SELECT d.value, r.page_id, nc.normalized_sha1, r.part_index
            FROM citation_source_resolution r
            JOIN normalized_citations nc ON nc.id = r.normalized_id
            JOIN domains d ON d.id = r.domain_id
            JOIN normalized_citation_parts p ON p.normalized_id = r.normalized_id AND p.part_index = r.part_index
            WHERE r.global_source_id = :g AND p.part_sha1 = :sha
            ORDER BY r.is_canonical DESC, r.domain_id, r.page_id, r.normalized_id, r.part_index LIMIT 1"""),
            {"g": global_source_id, "sha": row["canonical_part_sha1"]}).first()
    return _source_response(
        db, row, CanonicalPart(domain=canon[0], page_id=canon[1], normalized_sha1=canon[2], part_index=canon[3]) if canon else None)


# ---------------------------------------------------------------------------
# lookup by standardized identifier (or source key)
# ---------------------------------------------------------------------------

LOOKUP_COLUMNS = dict(SOURCE_KEY_ORDER + (SOURCE_KEY_FALLBACK,))
_QID = re.compile(r"^Q[1-9][0-9]*$")


def _isbn_forms(value: str) -> list[str]:
    """The ISBN as given (digits and X) plus its other form: ISBN-10 ↔ ISBN-13 (978 prefix);
    the data keeps whichever form the citation used."""
    v = re.sub(r"[^0-9X]", "", value.upper())
    forms = [v]
    if len(v) == 10 and v[:9].isdigit():
        body = "978" + v[:9]
        check = (10 - sum(int(d) * (1 if i % 2 == 0 else 3) for i, d in enumerate(body)) % 10) % 10
        forms.append(body + str(check))
    elif len(v) == 13 and v.startswith("978") and v.isdigit():
        body = v[3:12]
        s = sum(int(d) * (10 - i) for i, d in enumerate(body))
        check = (11 - s % 11) % 11
        forms.append(body + ("X" if check == 10 else str(check)))
    return forms


def normalize_identifier(scheme: str, value: str) -> list[str]:
    """Candidate stored values for ``scheme:value`` (the forms the extractor writes)."""
    v = (value or "").strip()
    if not v:
        raise BadRequest(f"{scheme} is empty")
    if scheme == "doi":
        v = re.sub(r"^(doi:|https?://(dx\.)?doi\.org/)", "", v, flags=re.I)
        return [v.lower()]
    if scheme == "isbn":
        return _isbn_forms(v)
    if scheme == "pmid":
        return [re.sub(r"^pmid:?", "", v, flags=re.I).strip()]
    if scheme == "pmc":
        return [re.sub(r"^pmc", "", v, flags=re.I).strip()]
    if scheme == "arxiv":
        return [re.sub(r"^arxiv:", "", v, flags=re.I).strip()]
    if scheme in ("wikidata", "librarybase"):
        # both are Wikibase item ids (Q…): which knowledge base is part of the key, never inferred
        v = v.upper()
        if not _QID.match(v):
            raise BadRequest(f"{scheme} must be an item id like Q42")
        return [v]
    if scheme == "part":
        if not re.fullmatch(r"[0-9a-fA-F]{40}", v):
            raise BadRequest("part must be a 40-character sha1")
        return [v.lower()]
    return [v]   # url: exact


def lookup_sources(db: Database, identifiers: dict[str, str | None], limit: int = 20, offset: int = 0) -> SourceLookupResponse:
    """Global sources whose standardized identifier (the representative value the
    source carries) equals the one given. Exactly one of ``identifiers`` (doi,
    isbn, pmid, pmc, arxiv, wikidata, librarybase, url, key) must be set; ``key``
    takes a ``source_key`` (``scheme:value``)."""
    given = {k: v for k, v in identifiers.items() if v is not None}
    if len(given) != 1:
        raise BadRequest("Give exactly one of: " + ", ".join(list(LOOKUP_COLUMNS) [:-1] + ["key"]))
    (scheme, value), = given.items()
    if scheme == "key":
        scheme, sep, value = value.partition(":")
        if not sep or scheme not in LOOKUP_COLUMNS:
            raise BadRequest("key must be scheme:value with scheme one of " + ", ".join(LOOKUP_COLUMNS))
    col = LOOKUP_COLUMNS[scheme]
    candidates = normalize_identifier(scheme, value)
    rows, total = [], 0
    with db.connect() as con:
        # one equality per candidate form: every lookup column has an index usable for "="
        # (the DOI and URL ones are hash indexes)
        for cand in candidates:
            total += con.execute(text(f"SELECT count(*) FROM global_sources WHERE {col} = :v"), {"v": cand}).scalar() or 0
            rows += con.execute(text(f"SELECT {_GS_COLS} FROM global_sources WHERE {col} = :v "
                                     "ORDER BY n_total_citations DESC NULLS LAST, global_source_id"),
                                {"v": cand}).mappings().all()
    return SourceLookupResponse(identifier=scheme, value=candidates[0], total=int(total),
                                sources=[_source_response(db, r) for r in rows[offset:offset + limit]])


_PAGES_SQL = """
SELECT d.value AS domain, p.page_id, m.page_title, m.page_namespace,
       p.n_citations AS citation_count, p.n_revisions_cited, p.is_current, p.first_seen, p.last_seen
FROM page_source_summary p
JOIN domains d ON d.id = p.domain_id
LEFT JOIN page_metadata m ON m.domain_id = p.domain_id AND m.page_id = p.page_id
WHERE p.global_source_id = :g {exclude}
ORDER BY p.n_citations DESC, p.last_seen DESC NULLS LAST, d.value, p.page_id
LIMIT :limit OFFSET :offset
"""


def get_source_pages(db: Database, global_source_id: str, exclude_domain: str | None, exclude_page_id: int | None,
                     limit: int = 10, offset: int = 0) -> SourceUsagePagesResponse:
    with db.connect() as con:
        row = _global_source(con, global_source_id)
        params = {"g": global_source_id, "limit": limit, "offset": offset}
        exclude = ""
        if exclude_page_id is not None:
            if exclude_domain:
                exclude = "AND NOT (d.value = :xd AND p.page_id = :xp)"
                params.update({"xd": exclude_domain, "xp": exclude_page_id})
            else:
                exclude = "AND p.page_id <> :xp"
                params["xp"] = exclude_page_id
        rows = con.execute(text(_PAGES_SQL.format(exclude=exclude)), params).mappings().all()
        total = con.execute(text("SELECT count(*) FROM page_source_summary WHERE global_source_id = :g"),
                            {"g": global_source_id}).scalar() or 0
    pages = [SourceUsagePage(url=page_url(r["domain"], r["page_id"]), **{k: r[k] for k in r.keys()}) for r in rows]
    return SourceUsagePagesResponse(global_source_id=global_source_id, source_label=source_label(row),
                                    total_pages=int(row["n_pages"] if row["n_pages"] is not None else total), pages=pages)
