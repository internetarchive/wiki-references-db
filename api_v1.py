from pathlib import Path

import yaml
from flask import Blueprint, request, jsonify, Response
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from models import (
    WebResource, Document, CitationInstance, CitationHistory, NormalizedCitation,
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


def _load_openapi_spec() -> dict:
    spec_path = Path(__file__).with_name("openapi.yaml")
    with spec_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@api_v1.route("/openapi.json", methods=["GET"])
def openapi_spec():
    return jsonify(_load_openapi_spec())


@api_v1.route("/docs", methods=["GET"])
def openapi_docs():
    html = """<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"UTF-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Wiki References DB API Docs</title>
  <link rel=\"stylesheet\" href=\"https://unpkg.com/swagger-ui-dist@5/swagger-ui.css\" />
</head>
<body>
  <div id=\"swagger-ui\"></div>
  <script src=\"https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js\"></script>
  <script>
    window.ui = SwaggerUIBundle({
      url: '/api/v1/openapi.json',
      dom_id: '#swagger-ui',
      deepLinking: true,
      presets: [SwaggerUIBundle.presets.apis],
    });
  </script>
</body>
</html>
"""
    return Response(html, mimetype="text/html")


@api_v1.route("/article", methods=["GET"])
def get_article():
    url = request.args.get("url")
    if not url:
        return _error("url parameter is required", 400)

    with Session(_get_engine()) as session:
        url_hash = WebResource.compute_url_hash(url)
        wr = session.query(WebResource).filter(WebResource.url_hash == url_hash).first()
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
                func.count(CitationHistory.citation_instance_id).label("citation_count"),
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

        # Citation instance IDs present at this revision
        present_ci_ids = (
            select(CitationHistory.citation_instance_id)
            .where(CitationHistory.revision_id == revision_id)
            .subquery()
        )

        if raw:
            # Raw mode: return per-instance data
            stmt = (
                select(
                    CitationInstance.id.label('ci_id'),
                    CitationInstance.raw_sha1,
                    CitationInstance.reference_type,
                    CitationInstance.reference_name,
                    func.min(Revision.revision_timestamp).label("first_seen_ts"),
                    func.max(Revision.revision_timestamp).label("last_seen_ts"),
                    func.min(Revision.revision_id).label("first_seen_id"),
                    func.max(Revision.revision_id).label("last_seen_id"),
                    func.count(Revision.revision_id).label("appearance_count"),
                )
                .join(CitationHistory, CitationHistory.citation_instance_id == CitationInstance.id)
                .join(Revision, Revision.revision_id == CitationHistory.revision_id)
                .where(CitationInstance.id.in_(select(present_ci_ids.c.citation_instance_id)))
                .group_by(
                    CitationInstance.id, CitationInstance.raw_sha1,
                    CitationInstance.reference_type, CitationInstance.reference_name,
                )
                .order_by(func.max(Revision.revision_timestamp).desc())
                .limit(limit).offset(offset)
            )
            rows = session.execute(stmt).all()
            citations = []
            for r in rows:
                citations.append({
                    "citation_instance_id": r.ci_id,
                    "raw_sha1": r.raw_sha1,
                    "reference_type": TYPE_LABELS.get(r.reference_type, str(r.reference_type)),
                    "reference_name": r.reference_name,
                    "first_seen": {"revision_id": r.first_seen_id, "revision_timestamp": r.first_seen_ts},
                    "last_seen": {"revision_id": r.last_seen_id, "revision_timestamp": r.last_seen_ts},
                    "currently_visible": r.last_seen_id == latest_rev_id,
                    "appearance_count": r.appearance_count,
                })
        else:
            # Normalized mode: group by normalized citation
            stmt = (
                select(
                    CitationInstance.id.label('ci_id'),
                    NormalizedCitation.id.label('nc_id'),
                    NormalizedCitation.normalized_sha1,
                    NormalizedCitation.reference_normalized,
                    CitationInstance.reference_type,
                    CitationInstance.reference_name,
                    func.min(Revision.revision_timestamp).label("first_seen_ts"),
                    func.max(Revision.revision_timestamp).label("last_seen_ts"),
                    func.min(Revision.revision_id).label("first_seen_id"),
                    func.max(Revision.revision_id).label("last_seen_id"),
                    func.count(Revision.revision_id).label("appearance_count"),
                )
                .join(NormalizedCitation, NormalizedCitation.id == CitationInstance.normalized_id)
                .join(CitationHistory, CitationHistory.citation_instance_id == CitationInstance.id)
                .join(Revision, Revision.revision_id == CitationHistory.revision_id)
                .where(CitationInstance.id.in_(select(present_ci_ids.c.citation_instance_id)))
                .group_by(
                    CitationInstance.id,
                    NormalizedCitation.id,
                    NormalizedCitation.normalized_sha1,
                    NormalizedCitation.reference_normalized,
                    CitationInstance.reference_type,
                    CitationInstance.reference_name,
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

            next_rev_ci_ids = set()
            if next_rev:
                next_rev_ci_ids = set(session.execute(
                    select(CitationHistory.citation_instance_id)
                    .where(CitationHistory.revision_id == next_rev.revision_id)
                ).scalars().all())

            # Batch-fetch related data
            nc_ids = list(set(r.nc_id for r in rows))
            ci_ids = [r.ci_id for r in rows]

            # Other articles
            other_articles_map = {}
            if nc_ids:
                oa_rows = session.execute(
                    select(NormalizedCitation.id.label('nc_id'),
                           NormalizedCitation.appears_on_article, Document.id.label('doc_id'))
                    .outerjoin(Document, Document.id == NormalizedCitation.appears_on_article)
                    .where(NormalizedCitation.id.in_(nc_ids))
                ).all()
                for oa in oa_rows:
                    other_articles_map.setdefault(oa.nc_id, []).append(oa)

            # Links
            links_map = {}
            if nc_ids:
                lk_rows = session.execute(
                    select(NormalizedCitationWebResource.normalized_id,
                           WebResource.id.label('wr_id'), WebResource.url)
                    .join(WebResource, WebResource.id == NormalizedCitationWebResource.web_resource_id)
                    .where(NormalizedCitationWebResource.normalized_id.in_(nc_ids))
                ).all()
                for lk in lk_rows:
                    links_map.setdefault(lk.normalized_id, []).append(lk)

            # Templates
            templates_map = {}
            if nc_ids:
                tpl_rows = session.execute(
                    select(TemplateData.normalized_id,
                           WikiTemplate.id.label('wt_id'), WikiTemplate.name,
                           TemplateData.parameter_key, TemplateData.parameter_value,
                           TemplateData.offset_start)
                    .join(WikiTemplate, WikiTemplate.id == TemplateData.wiki_template_id)
                    .where(TemplateData.normalized_id.in_(nc_ids))
                    .order_by(TemplateData.offset_start, TemplateData.parameter_key)
                ).all()
                for t in tpl_rows:
                    templates_map.setdefault(t.normalized_id, []).append(t)

            citations = []
            for r in rows:
                # Other articles
                other_articles = [
                    {"page_id": a.appears_on_article, "document_id": a.doc_id}
                    for a in other_articles_map.get(r.nc_id, [])
                ]

                # Links
                links = [
                    {"web_resource_id": lk.wr_id, "url": lk.url}
                    for lk in links_map.get(r.nc_id, [])
                ]

                # Templates
                tmpl_raw = templates_map.get(r.nc_id, [])
                tmpl_map = {}
                for t in tmpl_raw:
                    key = (t.wt_id, t.name, t.offset_start)
                    if key not in tmpl_map:
                        tmpl_map[key] = {}
                    tmpl_map[key][t.parameter_key] = t.parameter_value
                templates = [
                    {"wiki_template_id": k[0], "template_name": k[1], "parameters": v}
                    for k, v in tmpl_map.items()
                ]

                removed_at = None
                if next_rev and r.ci_id not in next_rev_ci_ids:
                    removed_at = {
                        "revision_id": next_rev.revision_id,
                        "revision_timestamp": next_rev.revision_timestamp,
                    }

                citations.append({
                    "citation_instance_id": r.ci_id,
                    "normalized_sha1": r.normalized_sha1,
                    "reference_normalized": r.reference_normalized,
                    "reference_type": TYPE_LABELS.get(r.reference_type, str(r.reference_type)),
                    "reference_name": r.reference_name,
                    "first_seen": {"revision_id": r.first_seen_id, "revision_timestamp": r.first_seen_ts},
                    "last_seen": {"revision_id": r.last_seen_id, "revision_timestamp": r.last_seen_ts},
                    "removed_at": removed_at,
                    "currently_visible": r.last_seen_id == latest_rev_id,
                    "appearance_count": r.appearance_count,
                    "other_articles": other_articles,
                    "extracted_links": links,
                    "templates": templates,
                })

        return jsonify({
            "page_id": page_id,
            "revision_id": revision_id,
            "revision_timestamp": rev_ts,
            "citation_count": len(citations),
            "citations": citations,
        })


@api_v1.route("/citation/<normalized_sha1>", methods=["GET"])
def get_citation(normalized_sha1):
    """Look up a citation by its normalized_sha1 (content-addressed hash)."""
    with Session(_get_engine()) as session:
        nc = session.query(NormalizedCitation).filter(
            NormalizedCitation.normalized_sha1 == normalized_sha1
        ).first()
        if not nc:
            return _error("Citation not found", 404)

        # Articles
        articles = session.execute(
            select(NormalizedCitation.appears_on_article, Document.id.label('doc_id'))
            .outerjoin(Document, Document.id == NormalizedCitation.appears_on_article)
            .where(NormalizedCitation.id == nc.id)
        ).all()

        # Links
        links = session.execute(
            select(WebResource.id, WebResource.url)
            .join(NormalizedCitationWebResource,
                  NormalizedCitationWebResource.web_resource_id == WebResource.id)
            .where(NormalizedCitationWebResource.normalized_id == nc.id)
        ).all()

        # Templates
        templates_raw = session.execute(
            select(WikiTemplate.id, WikiTemplate.name,
                   TemplateData.parameter_key, TemplateData.parameter_value,
                   TemplateData.offset_start)
            .join(TemplateData, TemplateData.wiki_template_id == WikiTemplate.id)
            .where(TemplateData.normalized_id == nc.id)
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

        # History via citation instances
        history = session.execute(
            select(CitationHistory.revision_id, Revision.revision_timestamp, Revision.page_id)
            .join(Revision, Revision.revision_id == CitationHistory.revision_id)
            .join(CitationInstance, CitationInstance.id == CitationHistory.citation_instance_id)
            .where(CitationInstance.normalized_id == nc.id)
            .order_by(Revision.revision_timestamp)
        ).all()

        return jsonify({
            "normalized_sha1": nc.normalized_sha1,
            "reference_normalized": nc.reference_normalized,
            "appears_on_articles": [
                {"page_id": a.appears_on_article, "document_id": a.doc_id}
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


@api_v1.route("/citation/<normalized_sha1>/history", methods=["GET"])
def get_citation_history(normalized_sha1):
    """Get revision history for a citation identified by normalized_sha1."""
    with Session(_get_engine()) as session:
        nc = session.query(NormalizedCitation).filter(
            NormalizedCitation.normalized_sha1 == normalized_sha1
        ).first()
        if not nc:
            return _error("Citation not found", 404)

        page_id = request.args.get("page_id", type=int)
        stmt = (
            select(CitationHistory.revision_id, Revision.revision_timestamp, Revision.page_id)
            .join(Revision, Revision.revision_id == CitationHistory.revision_id)
            .join(CitationInstance, CitationInstance.id == CitationHistory.citation_instance_id)
            .where(CitationInstance.normalized_id == nc.id)
        )
        if page_id is not None:
            stmt = stmt.where(Revision.page_id == page_id)
        stmt = stmt.order_by(Revision.revision_timestamp)

        rows = session.execute(stmt).all()
        return jsonify({
            "normalized_sha1": normalized_sha1,
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
                NormalizedCitation.normalized_sha1,
                NormalizedCitation.reference_normalized,
                NormalizedCitation.appears_on_article,
            )
            .join(TemplateData,
                  TemplateData.normalized_id == NormalizedCitation.id)
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
                    "normalized_sha1": r.normalized_sha1,
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
        url_hash = WebResource.compute_url_hash(url)
        wr = session.query(WebResource).filter(WebResource.url_hash == url_hash).first()
        if not wr:
            return _error("Web resource not found", 404)

        domain_val = None
        if wr.domain_id:
            domain_val = session.execute(
                select(Domain.value).where(Domain.id == wr.domain_id)
            ).scalar()

        refs = session.execute(
            select(
                NormalizedCitation.normalized_sha1,
                NormalizedCitation.appears_on_article,
            )
            .join(NormalizedCitationWebResource,
                  NormalizedCitationWebResource.normalized_id == NormalizedCitation.id)
            .where(NormalizedCitationWebResource.web_resource_id == wr.id)
        ).all()

        return jsonify({
            "web_resource_id": wr.id,
            "url": wr.url,
            "domain": domain_val,
            "numeric_page_id": wr.numeric_page_id,
            "referenced_by": [
                {
                    "normalized_sha1": r.normalized_sha1,
                    "appears_on_article": r.appears_on_article,
                }
                for r in refs
            ],
        })
