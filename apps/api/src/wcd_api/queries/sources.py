"""Group 2: global sources — ``/sources/{id}`` and ``/sources/{id}/pages``."""
from __future__ import annotations

from sqlalchemy import text

from ..db import Database
from ..models import CanonicalPart, GlobalSourceResponse, SourceUsagePage, SourceUsagePagesResponse
from . import NotFound
from .common import page_url, source_label

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
    return GlobalSourceResponse(
        source_label=source_label(row), **{k: row[k] for k in row.keys()},
        canonical=CanonicalPart(domain=canon[0], page_id=canon[1], normalized_sha1=canon[2], part_index=canon[3]) if canon else None,
    )


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
