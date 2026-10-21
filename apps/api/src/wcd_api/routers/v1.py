"""``/api/v1`` aliases: the v1 paths over the v2 handlers, with the domain
defaulted (``API_DEFAULT_DOMAIN``, en.wikipedia.org). ``document_id`` is gone
from ``/article``; ``appears_on_article`` is kept beside the new fields."""
from __future__ import annotations

from fastapi import APIRouter, Path, Query, Request

from .. import models
from ..queries import article, citations, lookup

router = APIRouter(prefix="/api/v1", tags=["v1 (aliases)"])


def _db(request: Request):
    return request.app.state.db


def _domain(request: Request) -> str:
    return request.app.state.settings.default_domain


@router.get("/article", response_model=models.ArticleResponse)
def v1_article(request: Request, url: str = Query(...)):
    return article.get_article(_db(request), url, resolver=request.app.state.title_resolver)


@router.get("/article/{page_id}/revisions", response_model=models.PageRevisionsResponse)
def v1_revisions(request: Request, page_id: int = Path(..., ge=1), limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    return citations.get_revisions(_db(request), _domain(request), page_id, limit, offset)


@router.get("/article/{page_id}/citations", response_model=models.PageCitationsResponse)
def v1_citations(request: Request, page_id: int = Path(..., ge=1), revision_id: int | None = Query(None), raw: bool = Query(False),
                 limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    return citations.get_citations(_db(request), _domain(request), page_id, revision_id, raw, limit, offset)


@router.get("/citation/{normalized_sha1}", response_model=models.CitationDetailResponse)
def v1_citation(request: Request, normalized_sha1: str):
    return citations.get_citation(_db(request), normalized_sha1)


@router.get("/citation/{normalized_sha1}/history", response_model=models.CitationHistoryResponse)
def v1_citation_history(request: Request, normalized_sha1: str, page_id: int | None = Query(None)):
    return citations.get_citation_history(_db(request), normalized_sha1, _domain(request) if page_id is not None else None, page_id)


@router.get("/template/{wiki_template_id}/report", response_model=models.TemplateReportResponse)
def v1_template_report(request: Request, wiki_template_id: int, parameter_key: str = Query(...), parameter_value: str = Query(...),
                       limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    return lookup.get_template_report(_db(request), wiki_template_id, parameter_key, parameter_value, limit, offset)


@router.get("/web_resource", response_model=models.WebResourceResponse)
def v1_web_resource(request: Request, url: str = Query(...)):
    return lookup.get_web_resource(_db(request), url)
