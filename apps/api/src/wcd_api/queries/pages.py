"""Group 2: page analytics — summary (page_summary_cache), timeline
(page_revision_stats Parquet), top sources (page_source_summary ⋈ global_sources)."""
from __future__ import annotations

from sqlalchemy import text

from ..db import Database
from ..models import (
    CitationTypeCount, LatestRevisionSummary, PageSummaryResponse, PageTimelineResponse, PageTopSourcesResponse,
    SourceProfile, TemplateCount, TimelinePoint, TopSource,
)
from . import NotFound, Unavailable
from .common import latest_revision, page_header, require_domain_id, source_label

TYPE_COLUMNS = ("ref_tag", "template", "bare_url", "endnote", "shorthand", "external_link")


def _summary_from_row(db, con, domain, domain_id, row) -> PageSummaryResponse:
    header = page_header(con, domain, domain_id, row["page_id"])
    latest = latest_revision(con, domain_id, row["page_id"])
    return PageSummaryResponse(
        **header.model_dump(),
        latest_revision=LatestRevisionSummary(
            rev_id=int(row["latest_rev_id"]),
            timestamp=latest[1] if latest and latest[0] == row["latest_rev_id"] else row["latest_revision_ts"],
            reference_count=int(row["reference_count"]),
            citation_type_breakdown=[CitationTypeCount(**x) for x in (row["type_breakdown"] or [])],
            top_templates=[TemplateCount(**x) for x in (row["top_templates"] or [])],
        ),
        source_profile=SourceProfile(**row["source_profile"]),
    )


_CACHE_COLS = ("domain_id, page_id, revision_count, first_revision_ts, latest_revision_ts, latest_rev_id, "
               "reference_count, type_breakdown, top_templates, source_profile")


def get_summary(db: Database, domain: str, page_id: int) -> PageSummaryResponse:
    with db.connect() as con:
        domain_id = require_domain_id(db, con, domain)
        row = con.execute(text(f"SELECT {_CACHE_COLS} FROM page_summary_cache WHERE domain_id = :d AND page_id = :p"),
                          {"d": domain_id, "p": page_id}).mappings().first()
        if row is None:
            page_header(con, domain, domain_id, page_id)  # 404 if unknown page
            raise Unavailable(f"No page summary has been built for page {page_id} on {domain} (Stage 7h)")
        return _summary_from_row(db, con, domain, domain_id, row)


def get_random_summary(db: Database, domain: str | None) -> PageSummaryResponse:
    with db.connect() as con:
        where, params = "", {}
        if domain:
            domain_id = require_domain_id(db, con, domain)
            where, params = "WHERE domain_id = :d", {"d": domain_id}
        row = con.execute(text(f"SELECT {_CACHE_COLS} FROM page_summary_cache TABLESAMPLE SYSTEM (1) {where} LIMIT 1"),
                          params).mappings().first()
        if row is None:  # small tables: the sample can come back empty
            row = con.execute(text(f"SELECT {_CACHE_COLS} FROM page_summary_cache {where} ORDER BY random() LIMIT 1"),
                              params).mappings().first()
        if row is None:
            raise NotFound("No page summaries have been built")
        dom = domain or db.domain_value(con, row["domain_id"])
        return _summary_from_row(db, con, dom, row["domain_id"], row)


_STATS_COLS = ("rev_id, timestamp, n_total_citations, n_template, n_self_closing, n_high_confidence, "
               + ", ".join(f"n_{t}" for t in TYPE_COLUMNS))


def get_timeline(db: Database, domain: str, page_id: int) -> PageTimelineResponse:
    with db.connect() as con:
        domain_id = require_domain_id(db, con, domain)
        header = page_header(con, domain, domain_id, page_id)
    duck = db.duck()
    try:
        if db.has_dataset("page_revision_stats", domain):
            db.bind(duck, domain, page_id, ("page_revision_stats",))
            rows = duck.execute(f"SELECT {_STATS_COLS} FROM page_revision_stats WHERE page_id = ? ORDER BY timestamp, rev_id",
                                [page_id]).fetchall()
        elif db.has_dataset("citation_instances_denorm", domain) and db.has_dataset("revisions", domain):
            # fallback: the Stage-8 query on this page's partitions
            from wcd_pipeline.stage8 import STATS_SQL
            db.bind(duck, domain, page_id, ("revisions", "citation_history", "named_ref_uses", "citation_instances_denorm"))
            rows = duck.execute(f"SELECT {_STATS_COLS} FROM ({STATS_SQL}) WHERE page_id = ? ORDER BY timestamp, rev_id",
                                [page_id]).fetchall()
        else:
            raise Unavailable(f"No page_revision_stats for {domain} (Stage 8)")
    finally:
        duck.close()
    points = []
    for r in rows:
        counts = dict(zip(TYPE_COLUMNS, r[6:]))
        points.append(TimelinePoint(
            rev_id=int(r[0]), timestamp=r[1], reference_count=int(r[2]), template_count=int(r[3]),
            named_ref_count=int(r[4]), high_confidence_count=int(r[5]),
            citation_type_breakdown=[CitationTypeCount(citation_type=t, count=int(c)) for t, c in counts.items() if c],
        ))
    return PageTimelineResponse(**header.model_dump(), points=points)


_TOP_SQL = """
SELECT p.global_source_id, gs.author_key, gs.year, gs.title,
       gs.extracted_doi, gs.extracted_isbn, gs.extracted_pmid, gs.extracted_pmc, gs.extracted_arxiv,
       gs.extracted_wikidata_qid, gs.extracted_librarybase_id, gs.extracted_url,
       p.source_type, p.n_citations AS page_citation_count, gs.n_pages AS cross_page_count,
       p.match_type, p.confidence, gs.reliability_score, gs.verdict,
       p.first_seen, p.last_seen, p.n_revisions_cited, p.n_self_closing_uses, p.is_current
FROM page_source_summary p JOIN global_sources gs USING (global_source_id)
WHERE p.domain_id = :d AND p.page_id = :p
ORDER BY p.n_citations DESC, coalesce(gs.n_pages, 0) DESC, p.global_source_id
LIMIT :limit
"""


def get_top_sources(db: Database, domain: str, page_id: int, limit: int = 10) -> PageTopSourcesResponse:
    with db.connect() as con:
        domain_id = require_domain_id(db, con, domain)
        header = page_header(con, domain, domain_id, page_id)
        total = con.execute(text("SELECT count(*) FROM page_source_summary WHERE domain_id = :d AND page_id = :p"),
                            {"d": domain_id, "p": page_id}).scalar() or 0
        rows = con.execute(text(_TOP_SQL), {"d": domain_id, "p": page_id, "limit": limit}).mappings().all()
    sources = [TopSource(source_label=source_label(r), **{k: r[k] for k in r.keys()}) for r in rows]
    return PageTopSourcesResponse(**header.model_dump(), total_sources=int(total), sources=sources)
