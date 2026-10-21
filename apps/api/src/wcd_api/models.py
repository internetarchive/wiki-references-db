"""Response models. The Group-2 shapes are UVA's (``analytics_service/main.py``)
plus ``domain`` / ``url`` and the new identifier and resolution fields; the
Group-3/4 shapes are v1's plus ``citation_type`` / ``parts`` (proposed_api.md).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

IDENTIFIER_FIELDS = ("extracted_doi", "extracted_isbn", "extracted_pmid", "extracted_pmc", "extracted_arxiv",
                     "extracted_wikidata_qid", "extracted_librarybase_id", "extracted_url")


# ---------------------------------------------------------------------------
# Group 1
# ---------------------------------------------------------------------------

class ArticleResponse(BaseModel):
    domain: str
    page_id: int
    url: str
    page_title: str | None = None
    page_namespace: int | None = None
    revision_count: int = Field(..., ge=0)
    latest_revision_id: int | None = None


# ---------------------------------------------------------------------------
# Group 2 (UVA dashboard)
# ---------------------------------------------------------------------------

class PageHeader(BaseModel):
    domain: str
    page_id: int
    page_title: str | None = None
    page_namespace: int | None = None
    url: str
    revision_count: int = Field(..., ge=0)
    first_revision_timestamp: datetime | None = None
    latest_revision_timestamp: datetime | None = None


class CitationTypeCount(BaseModel):
    citation_type: str
    count: int = Field(..., ge=0)


class TemplateCount(BaseModel):
    template_name: str
    count: int = Field(..., ge=0)


class LatestRevisionSummary(BaseModel):
    rev_id: int
    timestamp: datetime | None
    reference_count: int = Field(..., ge=0)
    citation_type_breakdown: list[CitationTypeCount]
    top_templates: list[TemplateCount]


class SourceProfileBucket(BaseModel):
    label: str
    count: int = Field(..., ge=0)
    percentage: float = Field(..., ge=0.0)


class SourceProfile(BaseModel):
    total_citation_uses: int = Field(..., ge=0)
    format_breakdown: list[SourceProfileBucket]
    source_kind_breakdown: list[SourceProfileBucket]
    summary: str


class PageSummaryResponse(PageHeader):
    latest_revision: LatestRevisionSummary
    source_profile: SourceProfile


class TimelinePoint(BaseModel):
    rev_id: int
    timestamp: datetime | None
    reference_count: int = Field(..., ge=0)
    template_count: int = Field(..., ge=0)
    #: self-closing <ref name="X"/> reuse events (the frontend labels this "reuses")
    named_ref_count: int = Field(..., ge=0)
    high_confidence_count: int = Field(..., ge=0)
    citation_type_breakdown: list[CitationTypeCount]


class PageTimelineResponse(PageHeader):
    points: list[TimelinePoint]


class Identifiers(BaseModel):
    extracted_doi: str | None = None
    extracted_isbn: str | None = None
    extracted_pmid: str | None = None
    extracted_pmc: str | None = None
    extracted_arxiv: str | None = None
    extracted_wikidata_qid: str | None = None
    extracted_librarybase_id: str | None = None
    extracted_url: str | None = None


class TopSource(Identifiers):
    global_source_id: str
    source_label: str
    author_key: str | None = None
    year: str | None = None
    title: str | None = None
    source_type: str | None = None
    page_citation_count: int | None = Field(default=None, ge=0)
    cross_page_count: int | None = Field(default=None, ge=0)
    match_type: str | None = None
    confidence: float | None = Field(default=None, ge=0.0)
    reliability_score: float | None = Field(default=None, ge=0.0)
    verdict: str | None = None
    first_seen: datetime | None = None
    last_seen: datetime | None = None
    n_revisions_cited: int = Field(..., ge=0)
    n_self_closing_uses: int = Field(..., ge=0)
    is_current: bool


class PageTopSourcesResponse(PageHeader):
    total_sources: int = Field(..., ge=0)
    sources: list[TopSource]


class SourceUsagePage(BaseModel):
    domain: str
    page_id: int
    page_title: str | None = None
    page_namespace: int | None = None
    url: str
    citation_count: int | None = Field(default=None, ge=0)
    n_revisions_cited: int | None = Field(default=None, ge=0)
    is_current: bool | None = None
    first_seen: datetime | None = None
    last_seen: datetime | None = None


class SourceUsagePagesResponse(BaseModel):
    global_source_id: str
    source_label: str
    total_pages: int = Field(..., ge=0)
    pages: list[SourceUsagePage]


class CanonicalPart(BaseModel):
    domain: str
    page_id: int
    normalized_sha1: str
    part_index: int


class GlobalSourceResponse(Identifiers):
    global_source_id: str
    source_label: str
    source_type: str | None = None
    match_type: str | None = None
    confidence: float | None = None
    author_key: str | None = None
    year: str | None = None
    title: str | None = None
    n_pages: int | None = None
    n_total_citations: int | None = None
    reliability_score: float | None = None
    verdict: str | None = None
    canonical_part_sha1: str | None = None
    canonical: CanonicalPart | None = None


# ---------------------------------------------------------------------------
# Group 3
# ---------------------------------------------------------------------------

class RevisionRef(BaseModel):
    revision_id: int
    revision_timestamp: datetime | None


class RevisionItem(RevisionRef):
    parent_revision_id: int | None = None
    citation_count: int = Field(..., ge=0)


class PageRevisionsResponse(BaseModel):
    domain: str
    page_id: int
    revisions: list[RevisionItem]
    total: int


class Part(Identifiers):
    part_index: int
    part_type: str
    norm_subtype: str | None = None
    template_name: str | None = None
    template_subtype: str | None = None
    author_key: str | None = None
    title: str | None = None
    year: str | None = None
    # resolution on the page in question (None on the global citation detail)
    global_source_id: str | None = None
    source_type: str | None = None
    match_type: str | None = None
    confidence: float | None = None
    reliability_score: float | None = None
    verdict: str | None = None


class PartResolution(BaseModel):
    domain: str
    page_id: int
    global_source_id: str
    source_type: str | None = None
    match_type: str | None = None
    confidence: float | None = None
    reliability_score: float | None = None
    verdict: str | None = None


class GlobalPart(Part):
    resolutions: list[PartResolution] = []


class ExtractedLink(BaseModel):
    web_resource_id: int
    url: str


class TemplateUse(BaseModel):
    wiki_template_id: int
    template_name: str
    parameters: dict[str, Any]


class PageRef(BaseModel):
    domain: str
    page_id: int


class CitationAtRevision(BaseModel):
    citation_instance_id: int
    normalized_sha1: str | None = None
    reference_normalized: str | None = None
    raw_sha1: str | None = None
    reference_type: str
    reference_name: str | None = None
    citation_type: str | None = None
    first_seen: RevisionRef
    last_seen: RevisionRef
    removed_at: RevisionRef | None = None
    currently_visible: bool
    appearance_count: int
    other_articles: list[PageRef] = []
    extracted_links: list[ExtractedLink] = []
    templates: list[TemplateUse] = []
    parts: list[Part] = []


class PageCitationsResponse(BaseModel):
    domain: str
    page_id: int
    revision_id: int
    revision_timestamp: datetime | None
    citation_count: int
    citations: list[CitationAtRevision]


class InstanceRef(BaseModel):
    domain: str
    page_id: int
    citation_instance_id: int
    reference_name: str | None = None
    reference_type: str


class HistoryEntry(RevisionRef):
    domain: str
    page_id: int


class CitationDetailResponse(BaseModel):
    normalized_sha1: str
    reference_normalized: str
    citation_type: str | None = None
    appears_on_articles: list[PageRef]
    instances: list[InstanceRef]
    extracted_links: list[ExtractedLink]
    templates: list[TemplateUse]
    parts: list[GlobalPart]
    history: list[HistoryEntry]


class CitationHistoryResponse(BaseModel):
    normalized_sha1: str
    revisions: list[HistoryEntry]


# ---------------------------------------------------------------------------
# Group 4
# ---------------------------------------------------------------------------

class TemplateReportCitation(BaseModel):
    normalized_sha1: str
    reference_normalized: str
    appears_on_domain: str | None = None
    appears_on_page_id: int | None = None
    appears_on_article: int | None = None   # v1 name for appears_on_page_id


class TemplateReportResponse(BaseModel):
    wiki_template_id: int
    template_name: str
    domain: str | None = None
    parameter_key: str
    parameter_value: str
    citations: list[TemplateReportCitation]
    total: int


class WebResourceReference(BaseModel):
    normalized_sha1: str
    appears_on_domain: str | None = None
    appears_on_page_id: int | None = None
    appears_on_article: int | None = None
    global_source_id: str | None = None
    reliability_score: float | None = None
    verdict: str | None = None


class WebResourceResponse(BaseModel):
    web_resource_id: int
    url: str
    domain: str | None = None
    numeric_page_id: int | None = None
    referenced_by: list[WebResourceReference]


class ErrorResponse(BaseModel):
    error: str
    code: int
