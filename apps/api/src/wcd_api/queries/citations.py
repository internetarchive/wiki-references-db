"""Group 3: revisions, citations at a revision, citation detail and history.

The revision dimension lives in ``citation_history`` (Parquet); everything
descriptive is in Postgres. Each endpoint reads the page's single bucket
partition and then fetches the Postgres rows for the ids it found.
"""
from __future__ import annotations

from collections import defaultdict

from sqlalchemy import text

from wcd_schema.ids import page_bucket

from ..db import Database
from ..models import (
    CitationAtRevision, CitationDetailResponse, CitationHistoryResponse, ExtractedLink, GlobalPart, HistoryEntry,
    InstanceRef, PageCitationsResponse, PageRef, PageRevisionsResponse, Part, PartResolution, RevisionItem,
    RevisionRef, TemplateUse,
)
from . import NotFound
from .common import TYPE_LABELS, require_domain_id

PART_COLS = ("part_index", "part_type", "norm_subtype", "template_name", "template_subtype", "author_key", "title",
             "year", "extracted_doi", "extracted_isbn", "extracted_pmid", "extracted_pmc", "extracted_arxiv",
             "extracted_wikidata_qid", "extracted_librarybase_id", "extracted_url")


# ---------------------------------------------------------------------------
# revisions
# ---------------------------------------------------------------------------

def get_revisions(db: Database, domain: str, page_id: int, limit: int = 100, offset: int = 0) -> PageRevisionsResponse:
    with db.connect() as con:
        domain_id = require_domain_id(db, con, domain)
        total = con.execute(text("SELECT count(*) FROM revisions WHERE domain_id = :d AND page_id = :p"),
                            {"d": domain_id, "p": page_id}).scalar() or 0
        if total == 0:
            raise NotFound(f"Page {page_id} on {domain} not found")
        revs = con.execute(text(
            "SELECT revision_id, revision_timestamp, parent_revision_id FROM revisions "
            "WHERE domain_id = :d AND page_id = :p ORDER BY revision_timestamp, revision_id LIMIT :l OFFSET :o"),
            {"d": domain_id, "p": page_id, "l": limit, "o": offset}).all()
    counts: dict[int, int] = {}
    if revs and db.has_dataset("citation_history", domain):
        duck = db.duck()
        try:
            db.bind(duck, domain, page_id, ("citation_history",))
            ids = [r[0] for r in revs]
            counts = dict(duck.execute(
                "SELECT revision_id, count(DISTINCT citation_instance_id) FROM citation_history "
                "WHERE page_id = ? AND revision_id IN (SELECT unnest(?::BIGINT[])) GROUP BY 1", [page_id, ids]).fetchall())
        finally:
            duck.close()
    return PageRevisionsResponse(
        domain=domain, page_id=page_id, total=int(total),
        revisions=[RevisionItem(revision_id=r[0], revision_timestamp=r[1], parent_revision_id=r[2],
                                citation_count=int(counts.get(r[0], 0))) for r in revs])


# ---------------------------------------------------------------------------
# shared Postgres detail fetches
# ---------------------------------------------------------------------------

def _links(con, nc_ids: list[int]) -> dict[int, list[ExtractedLink]]:
    out = defaultdict(list)
    if nc_ids:
        for nid, wid, url in con.execute(text(
                "SELECT l.normalized_id, w.id, w.url FROM normalized_citation_web_resources l "
                "JOIN web_resources w ON w.id = l.web_resource_id WHERE l.normalized_id = ANY(:ids) ORDER BY w.url"),
                {"ids": nc_ids}).all():
            out[nid].append(ExtractedLink(web_resource_id=wid, url=url))
    return out


def _templates(con, nc_ids: list[int]) -> dict[int, list[TemplateUse]]:
    out = defaultdict(list)
    if nc_ids:
        grouped: dict[tuple, dict] = {}
        for nid, wt_id, name, key, value, offset in con.execute(text(
                "SELECT t.normalized_id, w.id, w.name, t.parameter_key, t.parameter_value, t.offset_start "
                "FROM template_data t JOIN wiki_templates w ON w.id = t.wiki_template_id "
                "WHERE t.normalized_id = ANY(:ids) ORDER BY t.normalized_id, t.offset_start, t.parameter_key"),
                {"ids": nc_ids}).all():
            grouped.setdefault((nid, wt_id, name, offset), {})[key] = value
        for (nid, wt_id, name, _off), params in grouped.items():
            out[nid].append(TemplateUse(wiki_template_id=wt_id, template_name=name, parameters=params))
    return out


def _parts(con, nc_ids: list[int], domain_id: int | None, page_id: int | None) -> dict[int, list[dict]]:
    """Parts of the normalized citations, with the resolution on ``(domain_id, page_id)`` when given."""
    out = defaultdict(list)
    if not nc_ids:
        return out
    cols = ", ".join(f"p.{c}" for c in PART_COLS)
    if domain_id is not None:
        sql = f"""SELECT p.normalized_id, {cols}, r.global_source_id, r.source_type, r.match_type, r.confidence,
                         gs.reliability_score, gs.verdict
                  FROM normalized_citation_parts p
                  LEFT JOIN citation_source_resolution r ON r.normalized_id = p.normalized_id AND r.part_index = p.part_index
                       AND r.domain_id = :d AND r.page_id = :pg
                  LEFT JOIN global_sources gs ON gs.global_source_id = r.global_source_id
                  WHERE p.normalized_id = ANY(:ids) ORDER BY p.normalized_id, p.part_index"""
        params = {"ids": nc_ids, "d": domain_id, "pg": page_id}
    else:
        sql = f"SELECT p.normalized_id, {cols} FROM normalized_citation_parts p WHERE p.normalized_id = ANY(:ids) ORDER BY p.normalized_id, p.part_index"
        params = {"ids": nc_ids}
    for row in con.execute(text(sql), params).mappings().all():
        out[row["normalized_id"]].append(dict(row))
    return out


def _resolutions(con, nc_ids: list[int]) -> dict[tuple[int, int], list[PartResolution]]:
    out = defaultdict(list)
    if nc_ids:
        for row in con.execute(text(
                "SELECT r.normalized_id, r.part_index, d.value AS domain, r.page_id, r.global_source_id, r.source_type, "
                "r.match_type, r.confidence, gs.reliability_score, gs.verdict FROM citation_source_resolution r "
                "JOIN domains d ON d.id = r.domain_id LEFT JOIN global_sources gs USING (global_source_id) "
                "WHERE r.normalized_id = ANY(:ids) ORDER BY r.normalized_id, r.part_index, d.value, r.page_id"),
                {"ids": nc_ids}).mappings().all():
            r = dict(row)
            out[(r.pop("normalized_id"), r.pop("part_index"))].append(PartResolution(**r))
    return out


def _other_articles(con, nc_ids: list[int]) -> dict[int, list[PageRef]]:
    out = defaultdict(list)
    if nc_ids:
        for nid, dom, pid in con.execute(text(
                "SELECT DISTINCT ci.normalized_id, d.value, ci.page_id FROM citation_instances ci JOIN domains d ON d.id = ci.domain_id "
                "WHERE ci.normalized_id = ANY(:ids) ORDER BY 1, 2, 3"), {"ids": nc_ids}).all():
            out[nid].append(PageRef(domain=dom, page_id=pid))
    return out


# ---------------------------------------------------------------------------
# citations at a revision
# ---------------------------------------------------------------------------

def get_citations(db: Database, domain: str, page_id: int, revision_id: int | None, raw: bool = False,
                  limit: int = 100, offset: int = 0) -> PageCitationsResponse:
    with db.connect() as con:
        domain_id = require_domain_id(db, con, domain)
        revs = con.execute(text("SELECT revision_id, revision_timestamp FROM revisions WHERE domain_id = :d AND page_id = :p "
                                "ORDER BY revision_timestamp, revision_id"), {"d": domain_id, "p": page_id}).all()
        if not revs:
            raise NotFound(f"Page {page_id} on {domain} has no revisions")
        if revision_id is None:
            revision_id = revs[-1][0]
        ts = {r[0]: r[1] for r in revs}
        if revision_id not in ts:
            raise NotFound(f"Revision {revision_id} is not a revision of page {page_id} on {domain}")
        latest_rev_id = revs[-1][0]
        order = [r[0] for r in revs]
        pos = order.index(revision_id)
        next_rev = order[pos + 1] if pos + 1 < len(order) else None

        if not db.has_dataset("citation_history", domain):
            raise NotFound(f"No citation_history for {domain}")
        duck = db.duck()
        try:
            db.bind(duck, domain, page_id, ("citation_history",))
            # every instance present at the revision, with its appearance range on the page
            present = duck.execute("""
                WITH present AS (SELECT DISTINCT citation_instance_id FROM citation_history WHERE page_id = ? AND revision_id = ?)
                SELECT h.citation_instance_id, min(h.revision_id), max(h.revision_id), count(DISTINCT h.revision_id),
                       bool_or(h.revision_id = ?) AS in_next
                FROM citation_history h JOIN present USING (citation_instance_id)
                WHERE h.page_id = ? GROUP BY 1 ORDER BY 1""", [page_id, revision_id, next_rev or -1, page_id]).fetchall()
        finally:
            duck.close()
        # order: most recently seen first (as v1), then page through
        present.sort(key=lambda r: (ts.get(r[2]) or 0, r[0]), reverse=True)
        total = len(present)
        page_rows = present[offset:offset + limit]
        ids = [r[0] for r in page_rows]
        inst = {}
        if ids:
            for row in con.execute(text(
                    "SELECT ci.id, ci.raw_sha1, ci.reference_type, ci.reference_name, nc.id AS nc_id, nc.normalized_sha1, "
                    "nc.reference_normalized, nc.citation_type FROM citation_instances ci "
                    "JOIN normalized_citations nc ON nc.id = ci.normalized_id WHERE ci.id = ANY(:ids)"), {"ids": ids}).mappings().all():
                inst[row["id"]] = dict(row)
        nc_ids = sorted({v["nc_id"] for v in inst.values()})
        links = templates = parts = others = {}
        if not raw:
            links, templates, parts, others = _links(con, nc_ids), _templates(con, nc_ids), _parts(con, nc_ids, domain_id, page_id), _other_articles(con, nc_ids)

    citations = []
    for cid, first_id, last_id, n, in_next in page_rows:
        i = inst.get(cid)
        if i is None:
            continue
        removed = None
        if next_rev is not None and not in_next:
            removed = RevisionRef(revision_id=next_rev, revision_timestamp=ts.get(next_rev))
        item = CitationAtRevision(
            citation_instance_id=cid, raw_sha1=i["raw_sha1"],
            reference_type=TYPE_LABELS.get(i["reference_type"], str(i["reference_type"])), reference_name=i["reference_name"],
            first_seen=RevisionRef(revision_id=first_id, revision_timestamp=ts.get(first_id)),
            last_seen=RevisionRef(revision_id=last_id, revision_timestamp=ts.get(last_id)),
            removed_at=removed, currently_visible=(last_id == latest_rev_id), appearance_count=int(n),
        )
        if not raw:
            item.normalized_sha1 = i["normalized_sha1"]
            item.reference_normalized = i["reference_normalized"]
            item.citation_type = i["citation_type"]
            item.other_articles = [p for p in others.get(i["nc_id"], []) if not (p.domain == domain and p.page_id == page_id)]
            item.extracted_links = links.get(i["nc_id"], [])
            item.templates = templates.get(i["nc_id"], [])
            item.parts = [Part(**{k: v for k, v in p.items() if k != "normalized_id"}) for p in parts.get(i["nc_id"], [])]
        citations.append(item)
    return PageCitationsResponse(domain=domain, page_id=page_id, revision_id=revision_id, revision_timestamp=ts[revision_id],
                                 citation_count=total, citations=citations)


# ---------------------------------------------------------------------------
# citation detail and history
# ---------------------------------------------------------------------------

def _history(db: Database, con, instances: list[dict], page_filter: tuple[str, int] | None) -> list[HistoryEntry]:
    """citation_history rows of the given instances, with timestamps from the revisions Parquet (same bucket)."""
    by_part: dict[tuple[str, int], list[int]] = defaultdict(list)
    for i in instances:
        if page_filter and (i["domain"], i["page_id"]) != page_filter:
            continue
        by_part[(i["domain"], page_bucket(i["page_id"]))].append(i["id"])
    out = []
    if not by_part:
        return out
    duck = db.duck()
    try:
        for (dom, bucket), ids in by_part.items():
            if not db.has_dataset("citation_history", dom):
                continue
            from wcd_schema.duckdb import bind_bucket_views
            bind_bucket_views(duck, db.parquet_dir, dom, bucket, ("citation_history", "revisions"))
            for pid, rid, rts in duck.execute("""
                    SELECT h.page_id, h.revision_id, r.revision_timestamp
                    FROM (SELECT DISTINCT page_id, revision_id FROM citation_history WHERE citation_instance_id IN (SELECT unnest(?::BIGINT[]))) h
                    LEFT JOIN revisions r ON r.revision_id = h.revision_id
                    ORDER BY r.revision_timestamp, h.revision_id""", [ids]).fetchall():
                out.append(HistoryEntry(domain=dom, page_id=pid, revision_id=rid, revision_timestamp=rts))
    finally:
        duck.close()
    out.sort(key=lambda h: ((h.revision_timestamp.timestamp() if h.revision_timestamp else 0), h.revision_id))
    return out


def _instances(con, nc_id: int) -> list[dict]:
    return [dict(r) for r in con.execute(text(
        "SELECT ci.id, d.value AS domain, ci.page_id, ci.reference_name, ci.reference_type FROM citation_instances ci "
        "JOIN domains d ON d.id = ci.domain_id WHERE ci.normalized_id = :n ORDER BY d.value, ci.page_id, ci.id"),
        {"n": nc_id}).mappings().all()]


def get_citation(db: Database, normalized_sha1: str) -> CitationDetailResponse:
    with db.connect() as con:
        nc = con.execute(text("SELECT id, normalized_sha1, reference_normalized, citation_type FROM normalized_citations "
                              "WHERE normalized_sha1 = :s"), {"s": normalized_sha1}).mappings().first()
        if nc is None:
            raise NotFound("Citation not found")
        instances = _instances(con, nc["id"])
        links, templates = _links(con, [nc["id"]]), _templates(con, [nc["id"]])
        parts, resolutions = _parts(con, [nc["id"]], None, None), _resolutions(con, [nc["id"]])
        history = _history(db, con, instances, None)
    pages = sorted({(i["domain"], i["page_id"]) for i in instances})
    return CitationDetailResponse(
        normalized_sha1=nc["normalized_sha1"], reference_normalized=nc["reference_normalized"], citation_type=nc["citation_type"],
        appears_on_articles=[PageRef(domain=d, page_id=p) for d, p in pages],
        instances=[InstanceRef(domain=i["domain"], page_id=i["page_id"], citation_instance_id=i["id"], reference_name=i["reference_name"],
                               reference_type=TYPE_LABELS.get(i["reference_type"], str(i["reference_type"]))) for i in instances],
        extracted_links=links.get(nc["id"], []), templates=templates.get(nc["id"], []),
        parts=[GlobalPart(**{k: v for k, v in p.items() if k != "normalized_id"},
                          resolutions=resolutions.get((nc["id"], p["part_index"]), [])) for p in parts.get(nc["id"], [])],
        history=history,
    )


def get_citation_history(db: Database, normalized_sha1: str, domain: str | None, page_id: int | None) -> CitationHistoryResponse:
    with db.connect() as con:
        nc_id = con.execute(text("SELECT id FROM normalized_citations WHERE normalized_sha1 = :s"), {"s": normalized_sha1}).scalar()
        if nc_id is None:
            raise NotFound("Citation not found")
        instances = _instances(con, nc_id)
        page_filter = (domain, page_id) if (domain and page_id is not None) else None
        if page_id is not None and not domain:
            instances = [i for i in instances if i["page_id"] == page_id]
        history = _history(db, con, instances, page_filter)
    return CitationHistoryResponse(normalized_sha1=normalized_sha1, revisions=history)
