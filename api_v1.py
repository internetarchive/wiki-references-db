from flask import Blueprint, request, jsonify
from sqlalchemy import func, and_, select
from sqlalchemy.orm import Session
from models import (
    WebResource, Document, Citation, CitationHistory, NormalizedCitation,
    Revision, NormalizedCitationWebResource, WikiTemplate, TemplateData, Domain,
)

api_v1 = Blueprint('api_v1', __name__, url_prefix='/api/v1')

TYPE_LABELS = {0: "other", 1: "inline", 2: "endnote"}


def _get_engine():
    from app import engine
    return engine


def _error(msg, code):
    return jsonify({"error": msg, "code": code}), code


def _paginate(q, limit, offset):
    return q.limit(limit).offset(offset)


@api_v1.route("/article", methods=["GET"])
def get_article():
    url = request.args.get("url")
    if not url:
        return _error("url parameter is required", 400)

    with Session(_get_engine()) as session:
        wr = session.query(WebResource).filter(WebResource.url == url).first()
        if not wr:
            return _error("Article not found", 404)

        page_id = wr.numeric_page_id
        if page_id is None:
            return _error("Article has no page ID", 404)

        revisions = session.execute(
            select(Revision.revision_id, Revision.revision_timestamp, Revision.parent_revision_id)
            .where(Revision.page_id == page_id)
            .order_by(Revision.revision_timestamp)
        ).all()

        return jsonify({
            "page_id": page_id,
            "url": wr.url,
            "document_id": wr.instance_of_document,
            "revisions": [
                {
                    "revision_id": r.revision_id,
                    "revision_timestamp": r.revision_timestamp,
                    "parent_revision_id": r.parent_revision_id,
                }
                for r in revisions
            ],
            "revision_count": len(revisions),
            "latest_revision_id": revisions[-1].revision_id if revisions else None,
        })


@api_v1.route("/article/<int:page_id>/revisions", methods=["GET"])
def get_article_revisions(page_id):
    limit = min(request.args.get("limit", 100, type=int), 1000)
    offset = request.args.get("offset", 0, type=int)

    with Session(_get_engine()) as session:
        total = session.execute(
            select(func.count()).select_from(Revision).where(Revision.page_id == page_id)
        ).scalar()

        rows = session.execute(
            select(
                Revision.revision_id,
                Revision.revision_timestamp,
                Revision.parent_revision_id,
                func.count(CitationHistory.record_sha1).label("citation_count"),
            )
            .outerjoin(CitationHistory, CitationHistory.revision_id == Revision.revision_id)
            .where(Revision.page_id == page_id)
            .group_by(Revision.revision_id, Revision.revision_timestamp, Revision.parent_revision_id)
            .order_by(Revision.revision_timestamp)
            .limit(limit).offset(offset)
        ).all()

        return jsonify({
            "page_id": page_id,
            "revisions": [
                {
                    "revision_id": r.revision_id,
                    "revision_timestamp": r.revision_timestamp,
                    "parent_revision_id": r.parent_revision_id,
                    "citation_count": r.citation_count,
                }
                for r in rows
            ],
            "total": total,
        })


@api_v1.route("/article/<int:page_id>/citations", methods=["GET"])
def get_article_citations(page_id):
    raw = request.args.get("raw", "false").lower() == "true"
    limit = min(request.args.get("limit", 100, type=int), 1000)
    offset = request.args.get("offset", 0, type=int)

    with Session(_get_engine()) as session:
        # Resolve revision_id
        revision_id = request.args.get("revision_id", type=int)
        if revision_id is None:
            revision_id = session.execute(
                select(func.max(Revision.revision_id)).where(Revision.page_id == page_id)
            ).scalar()
        if revision_id is None:
            return _error("No revisions found for this article", 404)

        rev_ts = session.execute(
            select(Revision.revision_timestamp).where(Revision.revision_id == revision_id)
        ).scalar()
        if rev_ts is None:
            return _error("Revision not found", 404)

        # Latest revision for currently_visible check
        latest_rev_id = session.execute(
            select(func.max(Revision.revision_id)).where(Revision.page_id == page_id)
        ).scalar()

        # Records present at this revision
        present_sha1s = (
            select(CitationHistory.record_sha1, CitationHistory.reference_raw_sha1,
                   CitationHistory.reference_normalized_sha1)
            .where(CitationHistory.revision_id == revision_id)
            .subquery()
        )

        if raw:
            stmt = (
                select(
                    Citation.record_sha1,
                    Citation.reference_raw_sha1,
                    Citation.offset_start,
                    Citation.length,
                    Citation.reference_type,
                    Citation.reference_name,
                    func.min(Revision.revision_timestamp).label("first_seen_ts"),
                    func.max(Revision.revision_timestamp).label("last_seen_ts"),
                    func.min(Revision.revision_id).label("first_seen_id"),
                    func.max(Revision.revision_id).label("last_seen_id"),
                    func.count(Revision.revision_id).label("appearance_count"),
                )
                .select_from(present_sha1s)
                .join(Citation, and_(
                    Citation.record_sha1 == present_sha1s.c.record_sha1,
                    Citation.reference_raw_sha1 == present_sha1s.c.reference_raw_sha1,
                ))
                .join(CitationHistory, and_(
                    CitationHistory.record_sha1 == Citation.record_sha1,
                    CitationHistory.reference_raw_sha1 == Citation.reference_raw_sha1,
                ))
                .join(Revision, Revision.revision_id == CitationHistory.revision_id)
                .group_by(
                    Citation.record_sha1, Citation.reference_raw_sha1,
                    Citation.offset_start, Citation.length,
                    Citation.reference_type, Citation.reference_name,
                )
                .order_by(func.max(Revision.revision_timestamp).desc())
                .limit(limit).offset(offset)
            )
            rows = session.execute(stmt).all()
            citations = []
            for r in rows:
                citations.append({
                    "record_sha1": r.record_sha1,
                    "reference_raw_sha1": r.reference_raw_sha1,
                    "offset_start": r.offset_start,
                    "length": r.length,
                    "reference_type": TYPE_LABELS.get(r.reference_type, str(r.reference_type)),
                    "reference_name": r.reference_name,
                    "first_seen": {"revision_id": r.first_seen_id, "revision_timestamp": r.first_seen_ts},
                    "last_seen": {"revision_id": r.last_seen_id, "revision_timestamp": r.last_seen_ts},
                    "currently_visible": r.last_seen_id == latest_rev_id,
                    "appearance_count": r.appearance_count,
                })
        else:
            stmt = (
                select(
                    NormalizedCitation.record_sha1,
                    NormalizedCitation.reference_normalized_sha1,
                    NormalizedCitation.reference_normalized,
                    Citation.reference_type,
                    func.coalesce(
                        func.nullif(func.array_agg(func.distinct(Citation.reference_name)), '{NULL}'),
                        '{}'
                    ).label("reference_names"),
                    func.min(Revision.revision_timestamp).label("first_seen_ts"),
                    func.max(Revision.revision_timestamp).label("last_seen_ts"),
                    func.min(Revision.revision_id).label("first_seen_id"),
                    func.max(Revision.revision_id).label("last_seen_id"),
                    func.count(Revision.revision_id).label("appearance_count"),
                )
                .select_from(present_sha1s)
                .join(Citation, and_(
                    Citation.record_sha1 == present_sha1s.c.record_sha1,
                    Citation.reference_raw_sha1 == present_sha1s.c.reference_raw_sha1,
                ))
                .join(CitationHistory, and_(
                    CitationHistory.record_sha1 == Citation.record_sha1,
                    CitationHistory.reference_raw_sha1 == Citation.reference_raw_sha1,
                ))
                .join(Revision, Revision.revision_id == CitationHistory.revision_id)
                .join(NormalizedCitation, NormalizedCitation.record_sha1 == Citation.record_sha1)
                .group_by(
                    NormalizedCitation.record_sha1,
                    NormalizedCitation.reference_normalized_sha1,
                    NormalizedCitation.reference_normalized,
                    Citation.reference_type,
                )
                .order_by(func.max(Revision.revision_timestamp).desc())
                .limit(limit).offset(offset)
            )
            rows = session.execute(stmt).all()

            # Check next revision for removed_at
            next_rev = session.execute(
                select(Revision.revision_id, Revision.revision_timestamp)
                .where(Revision.page_id == page_id)
                .where(Revision.revision_id > revision_id)
                .order_by(Revision.revision_id)
                .limit(1)
            ).first()

            next_rev_sha1s = set()
            if next_rev:
                next_rev_sha1s = set(session.execute(
                    select(CitationHistory.record_sha1)
                    .where(CitationHistory.revision_id == next_rev.revision_id)
                ).scalars().all())

            citations = []
            for r in rows:
                # Other articles
                other_articles = session.execute(
                    select(NormalizedCitation.appears_on_article, Document.id)
                    .outerjoin(Document, Document.id == NormalizedCitation.appears_on_article)
                    .where(NormalizedCitation.reference_normalized_sha1 == r.reference_normalized_sha1)
                    .where(NormalizedCitation.record_sha1 != r.record_sha1)
                ).all()

                # Extracted links
                links = session.execute(
                    select(WebResource.id, WebResource.url)
                    .join(NormalizedCitationWebResource,
                          NormalizedCitationWebResource.web_resource_id == WebResource.id)
                    .where(NormalizedCitationWebResource.reference_normalized_sha1 == r.reference_normalized_sha1)
                ).all()

                # Templates
                templates_raw = session.execute(
                    select(WikiTemplate.id, WikiTemplate.name,
                           TemplateData.parameter_key, TemplateData.parameter_value,
                           TemplateData.offset_start)
                    .join(TemplateData, TemplateData.wiki_template_id == WikiTemplate.id)
                    .where(TemplateData.reference_normalized_sha1 == r.reference_normalized_sha1)
                    .order_by(TemplateData.offset_start, TemplateData.parameter_key)
                ).all()

                # Group template params by (template_id, offset_start)
                tmpl_map = {}
                for t in templates_raw:
                    key = (t.id, t.name, t.offset_start)
                    if key not in tmpl_map:
                        tmpl_map[key] = {}
                    tmpl_map[key][t.parameter_key] = t.parameter_value
                templates = [
                    {"wiki_template_id": k[0], "template_name": k[1], "parameters": v}
                    for k, v in tmpl_map.items()
                ]

                removed_at = None
                if next_rev and r.record_sha1 not in next_rev_sha1s:
                    removed_at = {
                        "revision_id": next_rev.revision_id,
                        "revision_timestamp": next_rev.revision_timestamp,
                    }

                ref_names = r.reference_names
                if isinstance(ref_names, str):
                    ref_names = [n for n in ref_names.strip("{}").split(",") if n] if ref_names != "{}" else []

                citations.append({
                    "record_sha1": r.record_sha1,
                    "reference_normalized_sha1": r.reference_normalized_sha1,
                    "reference_normalized": r.reference_normalized,
                    "reference_type": TYPE_LABELS.get(r.reference_type, str(r.reference_type)),
                    "reference_names": ref_names,
                    "first_seen": {"revision_id": r.first_seen_id, "revision_timestamp": r.first_seen_ts},
                    "last_seen": {"revision_id": r.last_seen_id, "revision_timestamp": r.last_seen_ts},
                    "removed_at": removed_at,
                    "currently_visible": r.last_seen_id == latest_rev_id,
                    "appearance_count": r.appearance_count,
                    "other_articles": [
                        {"page_id": a.appears_on_article, "document_id": a.id}
                        for a in other_articles
                    ],
                    "extracted_links": [
                        {"web_resource_id": l.id, "url": l.url}
                        for l in links
                    ],
                    "templates": templates,
                })

        return jsonify({
            "page_id": page_id,
            "revision_id": revision_id,
            "revision_timestamp": rev_ts,
            "citation_count": len(citations),
            "citations": citations,
        })


@api_v1.route("/citation/<record_sha1>", methods=["GET"])
def get_citation(record_sha1):
    with Session(_get_engine()) as session:
        nc = session.query(NormalizedCitation).filter(
            NormalizedCitation.record_sha1 == record_sha1
        ).first()
        if not nc:
            return _error("Citation not found", 404)

        # Articles
        articles = session.execute(
            select(NormalizedCitation.appears_on_article, Document.id)
            .outerjoin(Document, Document.id == NormalizedCitation.appears_on_article)
            .where(NormalizedCitation.reference_normalized_sha1 == nc.reference_normalized_sha1)
        ).all()

        # Links
        links = session.execute(
            select(WebResource.id, WebResource.url)
            .join(NormalizedCitationWebResource,
                  NormalizedCitationWebResource.web_resource_id == WebResource.id)
            .where(NormalizedCitationWebResource.reference_normalized_sha1 == nc.reference_normalized_sha1)
        ).all()

        # Templates
        templates_raw = session.execute(
            select(WikiTemplate.id, WikiTemplate.name,
                   TemplateData.parameter_key, TemplateData.parameter_value,
                   TemplateData.offset_start)
            .join(TemplateData, TemplateData.wiki_template_id == WikiTemplate.id)
            .where(TemplateData.reference_normalized_sha1 == nc.reference_normalized_sha1)
            .order_by(TemplateData.offset_start, TemplateData.parameter_key)
        ).all()
        tmpl_map = {}
        for t in templates_raw:
            key = (t.id, t.name, t.offset_start)
            if key not in tmpl_map:
                tmpl_map[key] = {}
            tmpl_map[key][t.parameter_key] = t.parameter_value
        templates = [
            {"wiki_template_id": k[0], "template_name": k[1], "parameters": v}
            for k, v in tmpl_map.items()
        ]

        # History
        history = session.execute(
            select(CitationHistory.revision_id, Revision.revision_timestamp, Revision.page_id)
            .join(Revision, Revision.revision_id == CitationHistory.revision_id)
            .where(CitationHistory.record_sha1 == record_sha1)
            .order_by(Revision.revision_timestamp)
        ).all()

        return jsonify({
            "record_sha1": nc.record_sha1,
            "reference_normalized_sha1": nc.reference_normalized_sha1,
            "reference_normalized": nc.reference_normalized,
            "appears_on_articles": [
                {"page_id": a.appears_on_article, "document_id": a.id}
                for a in articles
            ],
            "extracted_links": [
                {"web_resource_id": l.id, "url": l.url}
                for l in links
            ],
            "templates": templates,
            "history": [
                {
                    "revision_id": h.revision_id,
                    "revision_timestamp": h.revision_timestamp,
                    "page_id": h.page_id,
                }
                for h in history
            ],
        })


@api_v1.route("/citation/<record_sha1>/history", methods=["GET"])
def get_citation_history(record_sha1):
    with Session(_get_engine()) as session:
        nc = session.query(NormalizedCitation).filter(
            NormalizedCitation.record_sha1 == record_sha1
        ).first()
        if not nc:
            return _error("Citation not found", 404)

        page_id = request.args.get("page_id", type=int)
        stmt = (
            select(CitationHistory.revision_id, Revision.revision_timestamp, Revision.page_id)
            .join(Revision, Revision.revision_id == CitationHistory.revision_id)
            .where(CitationHistory.record_sha1 == record_sha1)
        )
        if page_id is not None:
            stmt = stmt.where(Revision.page_id == page_id)
        stmt = stmt.order_by(Revision.revision_timestamp)

        rows = session.execute(stmt).all()
        return jsonify({
            "record_sha1": record_sha1,
            "revisions": [
                {
                    "revision_id": r.revision_id,
                    "revision_timestamp": r.revision_timestamp,
                    "page_id": r.page_id,
                }
                for r in rows
            ],
        })


@api_v1.route("/template/<int:wiki_template_id>/report", methods=["GET"])
def get_template_report(wiki_template_id):
    parameter_key = request.args.get("parameter_key")
    parameter_value = request.args.get("parameter_value")
    if not parameter_key or parameter_value is None:
        return _error("parameter_key and parameter_value are required", 400)

    limit = min(request.args.get("limit", 100, type=int), 1000)
    offset = request.args.get("offset", 0, type=int)

    with Session(_get_engine()) as session:
        tmpl = session.query(WikiTemplate).filter(WikiTemplate.id == wiki_template_id).first()
        if not tmpl:
            return _error("Template not found", 404)

        stmt = (
            select(
                NormalizedCitation.record_sha1,
                NormalizedCitation.reference_normalized_sha1,
                NormalizedCitation.reference_normalized,
                NormalizedCitation.appears_on_article,
            )
            .join(TemplateData,
                  TemplateData.reference_normalized_sha1 == NormalizedCitation.reference_normalized_sha1)
            .where(TemplateData.wiki_template_id == wiki_template_id)
            .where(TemplateData.parameter_key == parameter_key)
            .where(TemplateData.parameter_value == parameter_value)
            .distinct()
        )

        total = session.execute(
            select(func.count()).select_from(stmt.subquery())
        ).scalar()

        rows = session.execute(stmt.limit(limit).offset(offset)).all()

        return jsonify({
            "wiki_template_id": wiki_template_id,
            "template_name": tmpl.name,
            "parameter_key": parameter_key,
            "parameter_value": parameter_value,
            "citations": [
                {
                    "record_sha1": r.record_sha1,
                    "reference_normalized_sha1": r.reference_normalized_sha1,
                    "reference_normalized": r.reference_normalized,
                    "appears_on_article": r.appears_on_article,
                }
                for r in rows
            ],
            "total": total,
        })


@api_v1.route("/web_resource", methods=["GET"])
def get_web_resource():
    url = request.args.get("url")
    if not url:
        return _error("url parameter is required", 400)

    with Session(_get_engine()) as session:
        wr = session.query(WebResource).filter(WebResource.url == url).first()
        if not wr:
            return _error("Web resource not found", 404)

        domain_val = None
        if wr.domain_id:
            domain_val = session.execute(
                select(Domain.value).where(Domain.id == wr.domain_id)
            ).scalar()

        refs = session.execute(
            select(
                NormalizedCitationWebResource.reference_normalized_sha1,
                NormalizedCitation.record_sha1,
                NormalizedCitation.appears_on_article,
            )
            .join(NormalizedCitation,
                  NormalizedCitation.reference_normalized_sha1 == NormalizedCitationWebResource.reference_normalized_sha1)
            .where(NormalizedCitationWebResource.web_resource_id == wr.id)
        ).all()

        return jsonify({
            "web_resource_id": wr.id,
            "url": wr.url,
            "domain": domain_val,
            "numeric_page_id": wr.numeric_page_id,
            "referenced_by": [
                {
                    "reference_normalized_sha1": r.reference_normalized_sha1,
                    "record_sha1": r.record_sha1,
                    "appears_on_article": r.appears_on_article,
                }
                for r in refs
            ],
        })
