import json
import os
import re
from functools import lru_cache
from urllib.parse import urlparse, parse_qs

import requests as http_requests
from flask import Blueprint, request, render_template
from sqlalchemy import func, and_, select
from sqlalchemy.orm import Session
from models import (
    WebResource, Document, CitationInstance, CitationHistory, NormalizedCitation,
    Revision, NormalizedCitationWebResource, WikiTemplate, TemplateData,
)

explorer = Blueprint('explorer', __name__, url_prefix='/explorer')

TYPE_LABELS = {0: "other", 1: "inline", 2: "endnote"}


def _get_wikipedia_api_headers() -> dict[str, str]:
    contact_email = (os.getenv("WIKIPEDIA_API_CONTACT_EMAIL") or "").strip()
    primary_token = (os.getenv("WIKIPEDIA_API_USER_AGENT") or "WikiReferencesDB/1.0").strip()
    secondary_token = (os.getenv("WIKIPEDIA_API_SECONDARY_USER_AGENT") or "").strip()

    user_agent = primary_token
    if contact_email:
        user_agent = f"{user_agent} ({contact_email})"
    if secondary_token:
        user_agent = f"{user_agent} {secondary_token}"

    return {"User-Agent": user_agent}


@lru_cache(maxsize=1024)
def _resolve_wikipedia_title_to_curid(domain: str, title: str, follow_redirects: bool) -> str | None:
    """Resolve a Wikipedia page title to a curid-based URL via the MediaWiki API.

    Results are cached (LRU, 1024 entries) to avoid repeated API calls.
    """
    api_url = f"https://{domain}/w/api.php"
    params = {
        "action": "query",
        "titles": title,
        "format": "json",
    }
    if follow_redirects:
        params["redirects"] = 1
    try:
        resp = http_requests.get(api_url, params=params, headers=_get_wikipedia_api_headers(), timeout=10)
        data = resp.json()
    except Exception:
        return None
    pages = data.get("query", {}).get("pages", {})
    for page_id, page_info in pages.items():
        if page_id == "-1":
            return None
        return f"https://{domain}/w/index.php?curid={page_id}"
    return None


def resolve_wikipedia_url_to_curid(url: str, follow_redirects: bool = True) -> str | None:
    """If *url* is a title-based Wikipedia URL, resolve it to the canonical
    curid-based URL.  Returns ``None`` when the URL is not recognised or
    resolution fails."""
    parsed = urlparse(url)
    domain = parsed.netloc
    title = None

    # /wiki/PageTitle
    wiki_match = re.match(r'^/wiki/(.+)$', parsed.path)
    if wiki_match:
        title = wiki_match.group(1)

    # /w/index.php?title=PageTitle  or  /wiki/index.php?title=PageTitle
    if parsed.path in ('/w/index.php', '/wiki/index.php'):
        qs = parse_qs(parsed.query)
        if 'title' in qs:
            title = qs['title'][0]
        elif 'curid' in qs:
            return url  # already in curid format

    if not title or not domain:
        return None

    return _resolve_wikipedia_title_to_curid(domain, title, follow_redirects)


def _get_engine():
    from app import engine
    return engine


@explorer.route("/", methods=["GET"])
def index():
    return render_template("explorer_index.html")


@explorer.route("/article", methods=["GET"])
def article_view():
    url = request.args.get("url")
    follow_redirects = "follow_redirects" in request.args
    if not url:
        return render_template("explorer_index.html", error="Please enter a URL.")

    # Normalise title-based Wikipedia URLs to curid format
    resolved = resolve_wikipedia_url_to_curid(url, follow_redirects=follow_redirects)
    if resolved:
        url = resolved

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
    """Return an HTML partial of citation cards for a given page_id + revision_id.

    Uses integer FK joins throughout for efficiency. The main query retrieves
    all citations present at a given revision in a single statement, then
    batch-fetches related data (other articles, links, templates) to avoid
    the N+1 query pattern.
    """
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

        # Find the document_id for the current article so we can exclude it from "other articles"
        current_doc_id = session.execute(
            select(WebResource.instance_of_document)
            .where(WebResource.numeric_page_id == page_id)
            .limit(1)
        ).scalar()

        latest_rev_id = session.execute(
            select(func.max(Revision.revision_id)).where(Revision.page_id == page_id)
        ).scalar()

        # Get all citation_instance_ids present at this revision
        present_instances = (
            select(CitationHistory.citation_instance_id)
            .where(CitationHistory.revision_id == revision_id)
            .subquery()
        )

        # Main query: join through integer FKs
        stmt = (
            select(
                CitationInstance.id.label('ci_id'),
                CitationInstance.raw_sha1,
                CitationInstance.reference_name,
                CitationInstance.reference_type,
                NormalizedCitation.id.label('nc_id'),
                NormalizedCitation.normalized_sha1,
                NormalizedCitation.reference_normalized,
            )
            .join(NormalizedCitation, NormalizedCitation.id == CitationInstance.normalized_id)
            .where(CitationInstance.id.in_(select(present_instances.c.citation_instance_id)))
        )
        instance_rows = session.execute(stmt).all()

        if not instance_rows:
            return render_template(
                "partials/citations.html",
                citations=[],
                citation_count=0,
                revision_id=revision_id,
                revision_timestamp=rev_ts,
            )

        ci_ids = [r.ci_id for r in instance_rows]
        nc_ids = list(set(r.nc_id for r in instance_rows))

        # Batch: history stats per citation instance
        history_stats = {}
        hist_stmt = (
            select(
                CitationHistory.citation_instance_id,
                func.min(Revision.revision_timestamp).label("first_seen_ts"),
                func.max(Revision.revision_timestamp).label("last_seen_ts"),
                func.min(Revision.revision_id).label("first_seen_id"),
                func.max(Revision.revision_id).label("last_seen_id"),
                func.count(Revision.revision_id).label("appearance_count"),
            )
            .join(Revision, Revision.revision_id == CitationHistory.revision_id)
            .where(CitationHistory.citation_instance_id.in_(ci_ids))
            .group_by(CitationHistory.citation_instance_id)
        )
        for hs in session.execute(hist_stmt).all():
            history_stats[hs.citation_instance_id] = hs

        # Batch: other articles sharing the same normalized citation
        # Join through to WebResource to get the article URL and Document for the title
        other_articles_map = {}
        if nc_ids:
            article_wr = (
                select(
                    WebResource.instance_of_document,
                    WebResource.url.label('article_url'),
                )
                .where(WebResource.instance_of_document.isnot(None))
                .distinct(WebResource.instance_of_document)
                .subquery()
            )
            oa_stmt = (
                select(
                    NormalizedCitation.id.label('nc_id'),
                    NormalizedCitation.appears_on_article,
                    Document.id.label('doc_id'),
                    Document.title.label('doc_title'),
                    article_wr.c.article_url,
                )
                .outerjoin(Document, Document.id == NormalizedCitation.appears_on_article)
                .outerjoin(article_wr, article_wr.c.instance_of_document == NormalizedCitation.appears_on_article)
                .where(NormalizedCitation.id.in_(nc_ids))
            )
            for oa in session.execute(oa_stmt).all():
                other_articles_map.setdefault(oa.nc_id, []).append(oa)

        # Batch: extracted links per normalized citation
        links_map = {}
        if nc_ids:
            links_stmt = (
                select(
                    NormalizedCitationWebResource.normalized_id,
                    WebResource.id.label('wr_id'),
                    WebResource.url,
                )
                .join(WebResource, WebResource.id == NormalizedCitationWebResource.web_resource_id)
                .where(NormalizedCitationWebResource.normalized_id.in_(nc_ids))
            )
            for lk in session.execute(links_stmt).all():
                links_map.setdefault(lk.normalized_id, []).append(lk)

        # Batch: templates per normalized citation
        templates_map = {}
        if nc_ids:
            tpl_stmt = (
                select(
                    TemplateData.normalized_id,
                    WikiTemplate.id.label('wt_id'),
                    WikiTemplate.name,
                    TemplateData.parameter_key,
                    TemplateData.parameter_value,
                    TemplateData.offset_start,
                )
                .join(WikiTemplate, WikiTemplate.id == TemplateData.wiki_template_id)
                .where(TemplateData.normalized_id.in_(nc_ids))
                .order_by(TemplateData.offset_start, TemplateData.parameter_key)
            )
            for t in session.execute(tpl_stmt).all():
                templates_map.setdefault(t.normalized_id, []).append(t)

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

        # Build response
        citations = []
        for r in instance_rows:
            hs = history_stats.get(r.ci_id)

            # Other articles (exclude self)
            other_articles = [
                {
                    "page_id": a.appears_on_article,
                    "document_id": a.doc_id,
                    "title": a.doc_title,
                    "url": a.article_url,
                }
                for a in other_articles_map.get(r.nc_id, [])
                if a.appears_on_article != current_doc_id  # exclude self using document ID
            ]

            # Extracted links
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

            ref_names = [r.reference_name] if r.reference_name else []

            citations.append({
                "citation_instance_id": r.ci_id,
                "normalized_sha1": r.normalized_sha1,
                "reference_normalized": r.reference_normalized,
                "reference_type": TYPE_LABELS.get(r.reference_type, str(r.reference_type)),
                "reference_names": ref_names,
                "first_seen": {
                    "revision_id": hs.first_seen_id if hs else None,
                    "revision_timestamp": hs.first_seen_ts if hs else None,
                },
                "last_seen": {
                    "revision_id": hs.last_seen_id if hs else None,
                    "revision_timestamp": hs.last_seen_ts if hs else None,
                },
                "removed_at": removed_at,
                "currently_visible": (hs.last_seen_id == latest_rev_id) if hs else False,
                "appearance_count": hs.appearance_count if hs else 0,
                "other_articles": other_articles,
                "extracted_links": links,
                "templates": templates,
            })

        # Sort by last seen descending
        citations.sort(key=lambda c: c["last_seen"]["revision_timestamp"] or "", reverse=True)

    return render_template(
        "partials/citations.html",
        citations=citations,
        citation_count=len(citations),
        page_id=page_id,
        revision_id=revision_id,
        revision_timestamp=rev_ts,
    )


@explorer.route("/citation/<normalized_sha1>/report", methods=["GET"])
def citation_report(normalized_sha1):
    page_id = request.args.get("page_id", type=int)

    with Session(_get_engine()) as session:
        nc = session.query(NormalizedCitation).filter(
            NormalizedCitation.normalized_sha1 == normalized_sha1
        ).first()
        if not nc:
            return render_template(
                "explorer_citation_report.html",
                normalized_sha1=normalized_sha1,
                reference_normalized="Citation not found",
                page_id=page_id,
                revisions=[],
                total=0,
            )

        article_wr = (
            select(
                WebResource.instance_of_document,
                WebResource.url.label("article_url"),
            )
            .where(WebResource.instance_of_document.isnot(None))
            .distinct(WebResource.instance_of_document)
            .subquery()
        )

        stmt = (
            select(
                CitationHistory.revision_id,
                Revision.revision_timestamp,
                Revision.page_id,
                Document.id.label("doc_id"),
                Document.title.label("doc_title"),
                article_wr.c.article_url,
            )
            .join(Revision, Revision.revision_id == CitationHistory.revision_id)
            .join(CitationInstance, CitationInstance.id == CitationHistory.citation_instance_id)
            .outerjoin(Document, Document.id == Revision.page_id)
            .outerjoin(article_wr, article_wr.c.instance_of_document == Revision.page_id)
            .where(CitationInstance.normalized_id == nc.id)
        )
        if page_id is not None:
            stmt = stmt.where(Revision.page_id == page_id)
        stmt = stmt.order_by(Revision.revision_timestamp)

        rows = session.execute(stmt).all()

    revisions = [
        {
            "revision_id": r.revision_id,
            "revision_timestamp": r.revision_timestamp,
            "page_id": r.page_id,
            "document_id": r.doc_id,
            "title": r.doc_title,
            "url": r.article_url,
        }
        for r in rows
    ]

    return render_template(
        "explorer_citation_report.html",
        normalized_sha1=normalized_sha1,
        reference_normalized=nc.reference_normalized,
        page_id=page_id,
        revisions=revisions,
        total=len(revisions),
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
                NormalizedCitation.normalized_sha1,
            )
            .join(TemplateData,
                  TemplateData.normalized_id == NormalizedCitation.id)
            .where(TemplateData.wiki_template_id == wiki_template_id)
            .where(TemplateData.parameter_key == parameter_key)
            .where(TemplateData.parameter_value == parameter_value)
        )
        rows = session.execute(stmt).all()

    citations = [
        {
            "reference_normalized": r.reference_normalized,
            "appears_on_article": r.appears_on_article,
            "normalized_sha1": r.normalized_sha1,
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
