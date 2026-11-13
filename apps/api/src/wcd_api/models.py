"""Response models. The Group-2 shapes are UVA's (``analytics_service/main.py``)
plus ``domain`` / ``url`` and the new identifier and resolution fields; the
Group-3/4 shapes are v1's plus ``citation_type`` / ``parts`` (proposed_api.md).

**No build-dependent ids in /api/v2.** Things are named by upstream ids (domain,
page id, revision id), content hashes (``normalized_sha1``, ``raw_sha1``,
``part_sha1``), names (templates) and URLs (web resources). Database row ids
(``wiki_templates.id``, ``web_resources.id``) are never returned, and a citation
instance is ``(domain, page_id, raw_sha1)``. The one interim id is
``global_source_id``, a per-build handle (see ``source_key`` for the durable one).

The ``V1…`` subclasses at the end add back the fields the v1 contract carried
(``citation_instance_id``, the v1 database's ``wiki_template_id``). Queries build
those supersets; a v2 route's ``response_model`` drops the v1-only fields.
"""
from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

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


SOURCE_KEY_DESCRIPTION = (
    "Durable key of the source: its standardized identifier as `scheme:value`, in the order "
    "doi, isbn, pmid, pmc, arxiv, wikidata, librarybase, url (intrinsic identifiers first, so the key does not "
    "change when a Wikidata or LibraryBase item is created later), else `part:<canonical_part_sha1>` (a content "
    "hash, the fallback). Look a source up by it with /api/v2/sources?key=.")
GLOBAL_SOURCE_ID_DESCRIPTION = (
    "Interim, per-build handle of the global source: `<run id>.<hash>`, or a bare hash for the build that predates "
    "run ids. It changes when the source's membership changes between builds; store `source_key` instead.")


class TopSource(Identifiers):
    global_source_id: str = Field(..., description=GLOBAL_SOURCE_ID_DESCRIPTION)
    source_key: str | None = Field(None, description=SOURCE_KEY_DESCRIPTION)
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
    global_source_id: str = Field(..., description=GLOBAL_SOURCE_ID_DESCRIPTION)
    run_id: str | None = Field(None, description="the build run the global_source_id belongs to (API_BASE_RUN_ID for bare ids)")
    source_key: str = Field(..., description=SOURCE_KEY_DESCRIPTION)
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


class SourceLookupResponse(BaseModel):
    identifier: str = Field(..., description="doi, isbn, pmid, pmc, arxiv, wikidata, librarybase, url or part")
    value: str = Field(..., description="the value as looked up (normalized: lowercase DOI, bare PMC number, …)")
    total: int
    sources: list[GlobalSourceResponse]


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
    url: str


class TemplateUse(BaseModel):
    template_name: str = Field(..., description="as normalized at extraction (underscores → spaces, first letter capitalized); "
                                                 "with the page's domain it names the template")
    parameters: dict[str, Any]


class PageRef(BaseModel):
    domain: str
    page_id: int
    page_title: str | None = None


#: citation_instances.id is a 64-bit hash: past 2**53, so JSON numbers lose digits in
#: JavaScript. It is served as a decimal string.
InstanceId = Annotated[str, Field(description="citation_instances.id as a decimal string (64-bit; exceeds JavaScript's safe integers)",
                                  examples=["9172276549252580379"])]


class CitationAtRevision(BaseModel):
    raw_sha1: str = Field(..., description="sha1 of the raw citation text; with domain and page_id it identifies the citation instance")
    normalized_sha1: str | None = None
    reference_normalized: str | None = None
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
    raw_sha1: str
    reference_name: str | None = None
    reference_type: str


class HistoryEntry(RevisionRef):
    domain: str
    page_id: int
    page_title: str | None = None


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
    total: int = Field(0, description="revisions in the whole history (before limit/offset)")
    revisions: list[HistoryEntry]


class CitationPage(BaseModel):
    domain: str
    page_id: int
    page_title: str | None = None
    url: str
    n_instances: int
    reference_names: list[str] = []


class CitationPagesResponse(BaseModel):
    normalized_sha1: str
    reference_normalized: str
    citation_type: str | None = None
    total: int
    pages: list[CitationPage]


# ---------------------------------------------------------------------------
# Group 4
# ---------------------------------------------------------------------------

class TemplateReportCitation(BaseModel):
    normalized_sha1: str
    reference_normalized: str
    appears_on_domain: str | None = None
    appears_on_page_id: int | None = None
    appears_on_article: int | None = None   # v1 name for appears_on_page_id
    appears_on_page_title: str | None = None


class TemplateReportResponse(BaseModel):
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
    url: str
    domain: str | None = None
    numeric_page_id: int | None = None
    referenced_by: list[WebResourceReference]


class ErrorResponse(BaseModel):
    error: str
    code: int


# ---------------------------------------------------------------------------
# /api/v1 supersets: the fields the v1 contract had that v2 dropped. Remove
# with the v1 aliases.
# ---------------------------------------------------------------------------

class V1TemplateUse(TemplateUse):
    wiki_template_id: int | None = Field(None, description="the template's id in the v1 database (API_LEGACY_TEMPLATE_IDS); "
                                                           "null when unknown")


class V1CitationAtRevision(CitationAtRevision):
    citation_instance_id: InstanceId
    templates: list[V1TemplateUse] = []


class V1PageCitationsResponse(PageCitationsResponse):
    citations: list[V1CitationAtRevision]


class V1InstanceRef(InstanceRef):
    citation_instance_id: InstanceId


class V1CitationDetailResponse(CitationDetailResponse):
    instances: list[V1InstanceRef]
    templates: list[V1TemplateUse]


class V1TemplateReportResponse(TemplateReportResponse):
    wiki_template_id: int = Field(..., description="the v1 template id that was asked for")
