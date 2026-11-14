"""``/api/v1`` aliases: the v1 paths over the v2 handlers, with the domain
defaulted (``API_DEFAULT_DOMAIN``, en.wikipedia.org). ``document_id`` is gone
from ``/article``; ``appears_on_article`` is kept beside the new fields.

v1 also keeps ``citation_instance_id`` (a string), and its template ids are the
v1 database's: ``wiki_template_id`` in responses and ``/template/{id}/report``
are translated by name through the v1 id table (``API_LEGACY_TEMPLATE_IDS``).
``web_resource_id`` is gone (URLs are the key). Remove this module, and the
``V1…`` models, when v1 is retired."""
from __future__ import annotations

from fastapi import APIRouter, Path, Query, Request

from .. import models
from ..queries import NotFound, article, citations, lookup

router = APIRouter(prefix="/api/v1", tags=["v1 (aliases)"])


def _db(request: Request):
    return request.app.state.db


def _domain(request: Request) -> str:
    return request.app.state.settings.default_domain


def _v1_template_ids(request: Request, templates) -> None:
    """Set each template use's v1 database id (by name on the default domain); None if unknown."""
    by_name = request.app.state.legacy_template_ids_by_name
    for t in templates:
        t.wiki_template_id = by_name.get(t.template_name)


@router.get("/article", response_model=models.ArticleResponse)
def v1_article(request: Request, url: str = Query(...)):
    return article.get_article(_db(request), url, resolver=request.app.state.title_resolver)


@router.get("/article/{page_id}/revisions", response_model=models.PageRevisionsResponse)
def v1_revisions(request: Request, page_id: int = Path(..., ge=1), limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    return citations.get_revisions(_db(request), _domain(request), page_id, limit, offset)


@router.get("/article/{page_id}/citations", response_model=models.V1PageCitationsResponse)
def v1_citations(request: Request, page_id: int = Path(..., ge=1), revision_id: int | None = Query(None), raw: bool = Query(False),
                 limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    res = citations.get_citations(_db(request), _domain(request), page_id, revision_id, raw, limit, offset)
    for c in res.citations:
        _v1_template_ids(request, c.templates)
    return res


@router.get("/citation/{normalized_sha1}", response_model=models.V1CitationDetailResponse)
def v1_citation(request: Request, normalized_sha1: str):
    res = citations.get_citation(_db(request), normalized_sha1)
    _v1_template_ids(request, res.templates)
    return res


@router.get("/citation/{normalized_sha1}/history", response_model=models.CitationHistoryResponse)
def v1_citation_history(request: Request, normalized_sha1: str, page_id: int | None = Query(None),
                        limit: int = Query(10000, ge=1, le=10000), offset: int = Query(0, ge=0)):
    # v1 returned every row; with full revision history that can be millions, so it is capped (and pageable)
    return citations.get_citation_history(_db(request), normalized_sha1, _domain(request) if page_id is not None else None, page_id,
                                          limit, offset)


@router.get("/template/{wiki_template_id}/report", response_model=models.V1TemplateReportResponse)
def v1_template_report(request: Request, wiki_template_id: int, parameter_key: str = Query(...), parameter_value: str = Query(...),
                       limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    """``wiki_template_id`` is the v1 database's id, translated to the template's name."""
    name = request.app.state.legacy_template_names.get(wiki_template_id)
    if name is None:
        raise NotFound(f"Unknown v1 template id {wiki_template_id} (v1 ids are translated through API_LEGACY_TEMPLATE_IDS); "
                       "use /api/v2/templates/report?domain=&template=<name>")
    res = lookup.get_template_report(_db(request), _domain(request), name, parameter_key, parameter_value, limit, offset)
    return models.V1TemplateReportResponse(**res.model_dump(), wiki_template_id=wiki_template_id)


@router.get("/web_resource", response_model=models.WebResourceResponse)
def v1_web_resource(request: Request, url: str = Query(...)):
    return lookup.get_web_resource(_db(request), url)
