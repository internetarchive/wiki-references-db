import json
from flask import Blueprint, request, render_template
from sqlalchemy import func, and_, select
from sqlalchemy.orm import Session
from models import (
    WebResource, Document, Citation, CitationHistory, NormalizedCitation,
    Revision, NormalizedCitationWebResource, WikiTemplate, TemplateData,
)

explorer = Blueprint('explorer', __name__, url_prefix='/explorer')

TYPE_LABELS = {0: "other", 1: "inline", 2: "endnote"}


def _get_engine():
    from app import engine
    return engine


@explorer.route("/", methods=["GET"])
def index():
    return render_template("explorer_index.html")


@explorer.route("/article", methods=["GET"])
def article_view():
    url = request.args.get("url")
    if not url:
        return render_template("explorer_index.html", error="Please enter a URL.")

    with Session(_get_engine()) as session:
        wr = session.query(WebResource).filter(WebResource.url == url).first()
        if not wr:
            return render_template("explorer_index.html", error="Article not found in database.")

        page_id = wr.numeric_page_id
        if page_id is None:
            return render_template("explorer_index.html", error="Article has no page ID.")

        revisions = session.execute(
            select(Revision.revision_id, Revision.revision_timestamp, Revision.parent_revision_id)
            .where(Revision.page_id == page_id)
            .order_by(Revision.revision_timestamp)
        ).all()

        revisions_list = [
            {
                "revision_id": r.revision_id,
                "revision_timestamp": r.revision_timestamp,
                "parent_revision_id": r.parent_revision_id,
            }
            for r in revisions
        ]

    return render_template(
        "explorer_article.html",
        url=url,
        page_id=page_id,
        revisions=revisions_list,
        revisions_json=json.dumps(revisions_list),
    )


@explorer.route("/partials/citations", methods=["GET"])
def partials_citations():
    """Return an HTML partial of citation cards for a given page_id + revision_id."""
    page_id = request.args.get("page_id", type=int)
    revision_id = request.args.get("revision_id", type=int)
    if page_id is None or revision_id is None:
        return "<p>Missing page_id or revision_id.</p>", 400

    with Session(_get_engine()) as session:
        rev_ts = session.execute(
            select(Revision.revision_timestamp).where(Revision.revision_id == revision_id)
        ).scalar()
        if rev_ts is None:
            return "<p>Revision not found.</p>", 404

        latest_rev_id = session.execute(
            select(func.max(Revision.revision_id)).where(Revision.page_id == page_id)
        ).scalar()

        present_sha1s = (
            select(CitationHistory.record_sha1, CitationHistory.reference_raw_sha1,
                   CitationHistory.reference_normalized_sha1)
            .where(CitationHistory.revision_id == revision_id)
            .subquery()
        )

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

    return render_template(
        "partials/citations.html",
        citations=citations,
        citation_count=len(citations),
        revision_id=revision_id,
        revision_timestamp=rev_ts,
    )


@explorer.route("/template/<int:wiki_template_id>/report", methods=["GET"])
def template_report(wiki_template_id):
    parameter_key = request.args.get("parameter_key", "")
    parameter_value = request.args.get("parameter_value", "")

    with Session(_get_engine()) as session:
        tmpl = session.query(WikiTemplate).filter(WikiTemplate.id == wiki_template_id).first()
        template_name = tmpl.name if tmpl else "Unknown"

        stmt = (
            select(
                NormalizedCitation.reference_normalized,
                NormalizedCitation.appears_on_article,
                TemplateData.reference_normalized_sha1,
            )
            .join(TemplateData,
                  TemplateData.reference_normalized_sha1 == NormalizedCitation.reference_normalized_sha1)
            .where(TemplateData.wiki_template_id == wiki_template_id)
            .where(TemplateData.parameter_key == parameter_key)
            .where(TemplateData.parameter_value == parameter_value)
        )
        rows = session.execute(stmt).all()

    citations = [
        {
            "reference_normalized": r.reference_normalized,
            "appears_on_article": r.appears_on_article,
            "reference_normalized_sha1": r.reference_normalized_sha1,
        }
        for r in rows
    ]

    return render_template(
        "explorer_template_report.html",
        template_name=template_name,
        parameter_key=parameter_key,
        parameter_value=parameter_value,
        citations=citations,
        total=len(citations),
    )
