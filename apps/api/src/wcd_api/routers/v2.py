"""``/api/v2`` — the endpoint catalog of docs/design/proposed_api.md."""
from __future__ import annotations

from fastapi import APIRouter, Path, Query, Request

from .. import models
from ..queries import article, citations, lookup, pages, sources

router = APIRouter(prefix="/api/v2")


def _db(request: Request):
    return request.app.state.db


# Group 1 -------------------------------------------------------------------

@router.get("/article", response_model=models.ArticleResponse, tags=["article"])
def get_article(request: Request, url: str = Query(..., description="curid or /wiki/Title URL of an article")):
    return article.get_article(_db(request), url, resolver=request.app.state.title_resolver)


# Group 2 -------------------------------------------------------------------

@router.get("/pages/random/summary", response_model=models.PageSummaryResponse, tags=["pages"])
def random_summary(request: Request, domain: str | None = Query(None)):
    return pages.get_random_summary(_db(request), domain)


@router.get("/pages/{domain}/{page_id}/summary", response_model=models.PageSummaryResponse, tags=["pages"])
def page_summary(request: Request, domain: str, page_id: int = Path(..., ge=1)):
    return pages.get_summary(_db(request), domain, page_id)


@router.get("/pages/{domain}/{page_id}/timeline", response_model=models.PageTimelineResponse, tags=["pages"])
def page_timeline(request: Request, domain: str, page_id: int = Path(..., ge=1)):
    return pages.get_timeline(_db(request), domain, page_id)


@router.get("/pages/{domain}/{page_id}/top-sources", response_model=models.PageTopSourcesResponse, tags=["pages"])
def page_top_sources(request: Request, domain: str, page_id: int = Path(..., ge=1), limit: int = Query(10, ge=1, le=50)):
    return pages.get_top_sources(_db(request), domain, page_id, limit)


@router.get("/sources/{global_source_id}", response_model=models.GlobalSourceResponse, tags=["sources"])
def source(request: Request, global_source_id: str = Path(..., min_length=1)):
    return sources.get_source(_db(request), global_source_id)


@router.get("/sources/{global_source_id}/pages", response_model=models.SourceUsagePagesResponse, tags=["sources"])
def source_pages(request: Request, global_source_id: str = Path(..., min_length=1),
                 exclude_domain: str | None = Query(None), exclude_page_id: int | None = Query(None, ge=1),
                 limit: int = Query(10, ge=1, le=50), offset: int = Query(0, ge=0)):
    return sources.get_source_pages(_db(request), global_source_id, exclude_domain, exclude_page_id, limit, offset)


# Group 3 -------------------------------------------------------------------

@router.get("/pages/{domain}/{page_id}/revisions", response_model=models.PageRevisionsResponse, tags=["revisions"])
def page_revisions(request: Request, domain: str, page_id: int = Path(..., ge=1),
                   limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    return citations.get_revisions(_db(request), domain, page_id, limit, offset)


@router.get("/pages/{domain}/{page_id}/citations", response_model=models.PageCitationsResponse,
            response_model_exclude_none=False, tags=["citations"])
def page_citations(request: Request, domain: str, page_id: int = Path(..., ge=1),
                   revision_id: int | None = Query(None), raw: bool = Query(False),
                   limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    return citations.get_citations(_db(request), domain, page_id, revision_id, raw, limit, offset)


@router.get("/citations/{normalized_sha1}", response_model=models.CitationDetailResponse, tags=["citations"])
def citation(request: Request, normalized_sha1: str = Path(..., min_length=40, max_length=40)):
    return citations.get_citation(_db(request), normalized_sha1)


@router.get("/citations/{normalized_sha1}/history", response_model=models.CitationHistoryResponse, tags=["citations"])
def citation_history(request: Request, normalized_sha1: str = Path(..., min_length=40, max_length=40),
                     domain: str | None = Query(None), page_id: int | None = Query(None)):
    return citations.get_citation_history(_db(request), normalized_sha1, domain, page_id)


# Group 4 -------------------------------------------------------------------

@router.get("/templates/{wiki_template_id}/report", response_model=models.TemplateReportResponse, tags=["lookup"])
def template_report(request: Request, wiki_template_id: int, parameter_key: str = Query(...), parameter_value: str = Query(...),
                    limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    return lookup.get_template_report(_db(request), wiki_template_id, parameter_key, parameter_value, limit, offset)


@router.get("/web-resources", response_model=models.WebResourceResponse, tags=["lookup"])
def web_resource(request: Request, url: str = Query(...)):
    return lookup.get_web_resource(_db(request), url)
