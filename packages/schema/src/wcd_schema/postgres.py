"""The Postgres tier of the Wikipedia Citations Database (SQLAlchemy models).

This is the rebuild schema of ``docs/design/proposed_data_model.md``: pages are
identified by ``(domain_id, page_id)``, ``revisions`` and ``page_metadata`` are
written by RevisionChest (Stage 0), ``citation_history`` lives in Parquet only,
and the semantic layer (``normalized_citation_parts``, ``global_sources``,
``citation_source_resolution``, ``page_source_summary``, ``ref_name_links``,
``page_summary_cache``) sits beside the syntactic tables.

Tables and who writes them:

======================================  =============================================
table                                   writer
======================================  =============================================
containers, domains                     Stage 3 load; RevisionChest inserts the domain
revision_bundles, revisions,            RevisionChest (Stage 0), or ``wcd stage0-load``
page_metadata, web_resources (articles) from its Parquet metadata
web_resources (cited URLs),             Stage 3 load
normalized_citation_web_resources
wiki_templates, template_data           Stage 3 load
normalized_citations, citation_instances Stage 3 load
normalized_citation_parts               Stage 4
ref_name_links                          Stage 5
global_sources, citation_source_resolution, page_source_summary, page_summary_cache  Stage 7
documents                               nothing in Stages 0–8 (citation-graph phase)
======================================  =============================================

The ``upsert`` / ``bulk_upsert`` helpers are what the loaders use; they sort
by the conflict key so concurrent loaders take locks in the same order.
"""

import hashlib

from sqlalchemy import (
    BigInteger,
    Boolean,
    CHAR,
    Column,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.orm import Session, declarative_base, relationship
from sqlalchemy.types import SmallInteger

Base = declarative_base()


def _merge_rows(rows, key_fields, drop=("id",)):
    """Deduplicate ``rows`` on ``key_fields`` (later non-None values win) and
    return them sorted by key, so one multi-row INSERT never hits the same
    conflict key twice and concurrent loaders lock in the same order."""
    merged = {}
    for r in rows:
        base = {k: v for k, v in r.items() if k not in drop}
        key = tuple(base.get(k) for k in key_fields)
        if any(k is None for k in key):
            continue
        if key in merged:
            cur = merged[key]
            for k, v in base.items():
                if v is not None:
                    cur[k] = v
        else:
            merged[key] = dict(base)
    cleaned = sorted(merged.values(), key=lambda r: tuple(str(r.get(k, "")) for k in key_fields))
    # every row must carry every column that any row carries (multi-VALUES rule)
    keys = set()
    for r in cleaned:
        keys.update(r)
    for r in cleaned:
        for k in keys:
            r.setdefault(k, None)
    return cleaned


# ---------------------------------------------------------------------------
# Entity tables
# ---------------------------------------------------------------------------

class Container(Base):
    """Periodicals, journals, sites: things Documents appear in."""
    __tablename__ = 'containers'
    id = Column(Integer, primary_key=True, nullable=False)
    label = Column(String)
    wikidata_id = Column(Integer, unique=True)
    librarybase_id = Column(Integer, unique=True)
    __table_args__ = (UniqueConstraint('label', name='uix_container_label'),)

    @staticmethod
    def bulk_upsert(session: Session, rows):
        cleaned = _merge_rows(rows, ('label',))
        if not cleaned:
            return
        stmt = insert(Container).values(cleaned).on_conflict_do_update(
            index_elements=['label'],
            set_={'wikidata_id': insert(Container).excluded.wikidata_id,
                  'librarybase_id': insert(Container).excluded.librarybase_id},
        )
        session.execute(stmt)


class Domain(Base):
    """Domain names. ``value`` (e.g. ``en.wikipedia.org``) is the site identifier
    throughout the model; ``domains.id`` is what page-keyed tables carry."""
    __tablename__ = 'domains'
    id = Column(Integer, primary_key=True, nullable=False)
    value = Column(String, nullable=False, unique=True)
    top_level_domain = Column(String)
    parent_domain = Column(Integer, ForeignKey('domains.id'))
    for_container = Column(Integer, ForeignKey('containers.id'))
    internet_domains_id = Column(Integer, unique=True)
    container = relationship("Container", foreign_keys=[for_container])

    @staticmethod
    def upsert(session: Session, **kwargs):
        values = {k: v for k, v in kwargs.items() if k != 'id'}
        stmt = insert(Domain).values(values)
        set_values = {k: v for k, v in values.items() if v is not None}
        stmt = stmt.on_conflict_do_update(index_elements=['value'], set_=set_values)
        session.execute(stmt)

    @staticmethod
    def bulk_upsert(session: Session, rows):
        cleaned = _merge_rows(rows, ('value',))
        if not cleaned:
            return
        excluded = insert(Domain).excluded
        stmt = insert(Domain).values(cleaned).on_conflict_do_update(
            index_elements=['value'],
            set_={'top_level_domain': func.coalesce(excluded.top_level_domain, Domain.top_level_domain),
                  'parent_domain': func.coalesce(excluded.parent_domain, Domain.parent_domain),
                  'for_container': func.coalesce(excluded.for_container, Domain.for_container),
                  'internet_domains_id': func.coalesce(excluded.internet_domains_id, Domain.internet_domains_id)},
        )
        session.execute(stmt)


class Document(Base):
    """The abstract document node (roughly a FRBR edition): articles and cited
    works alike. Not used for page identity; populated in the citation-graph
    phase, by nothing in Stages 0–8."""
    __tablename__ = 'documents'
    id = Column(Integer, primary_key=True, nullable=False)
    language_code = Column(String)
    has_container = Column(Integer, ForeignKey('containers.id'))
    part_of_larger_work = Column(Integer, ForeignKey('documents.id'))
    title = Column(String)
    wikidata_id = Column(Integer, unique=True)
    librarybase_id = Column(Integer, unique=True)
    container = relationship("Container", foreign_keys=[has_container])
    part_of = relationship("Document", remote_side=[id], foreign_keys=[part_of_larger_work])


class WebResource(Base):
    """A URL: a cited web page, or an article's own curid URL
    (``numeric_page_id`` / ``numeric_namespace_id`` set, written by RevisionChest)."""
    __tablename__ = 'web_resources'
    id = Column(BigInteger, primary_key=True, nullable=False)
    url = Column(String, nullable=False)
    url_hash = Column(CHAR(32), nullable=False, unique=True)
    instance_of_document = Column(Integer, ForeignKey('documents.id'))
    availability_status = Column(Integer)
    is_archive_of = Column(BigInteger, ForeignKey('web_resources.id'))
    domain_id = Column(Integer, ForeignKey('domains.id'))
    numeric_page_id = Column(Integer)
    numeric_namespace_id = Column(Integer)
    document = relationship("Document", foreign_keys=[instance_of_document])
    original_resource = relationship("WebResource", foreign_keys=[is_archive_of])
    domain = relationship("Domain", foreign_keys=[domain_id])

    __table_args__ = (
        Index('idx_wr_instance_doc', 'instance_of_document', postgresql_where=text('instance_of_document IS NOT NULL')),
        Index('idx_wr_numeric_page', 'domain_id', 'numeric_page_id', postgresql_where=text('numeric_page_id IS NOT NULL')),
    )

    @staticmethod
    def compute_url_hash(url: str) -> str:
        return hashlib.md5(url.encode('utf-8')).hexdigest()

    @staticmethod
    def bulk_upsert(session: Session, rows):
        prepared = []
        for r in rows:
            if r.get('url') is None:
                continue
            r2 = dict(r)
            r2.setdefault('url_hash', WebResource.compute_url_hash(r2['url']))
            prepared.append(r2)
        cleaned = _merge_rows(prepared, ('url_hash',))
        if not cleaned:
            return
        excluded = insert(WebResource).excluded
        stmt = insert(WebResource).values(cleaned).on_conflict_do_update(
            index_elements=['url_hash'],
            set_={
                'url': excluded.url,
                'instance_of_document': func.coalesce(excluded.instance_of_document, WebResource.instance_of_document),
                'availability_status': func.coalesce(excluded.availability_status, WebResource.availability_status),
                'is_archive_of': func.coalesce(excluded.is_archive_of, WebResource.is_archive_of),
                'domain_id': func.coalesce(excluded.domain_id, WebResource.domain_id),
                'numeric_page_id': func.coalesce(excluded.numeric_page_id, WebResource.numeric_page_id),
                'numeric_namespace_id': func.coalesce(excluded.numeric_namespace_id, WebResource.numeric_namespace_id),
            },
        )
        session.execute(stmt)


# ---------------------------------------------------------------------------
# Stage 0: RevisionChest
# ---------------------------------------------------------------------------

class RevisionBundle(Base):
    """A ``.mwrev.zst`` file. ``file_path`` is relative to the bundles directory."""
    __tablename__ = 'revision_bundles'
    id = Column(Integer, primary_key=True, nullable=False)
    file_path = Column(String, nullable=False, unique=True)

    @staticmethod
    def bulk_upsert(session: Session, rows):
        cleaned = _merge_rows(rows, ('file_path',))
        if not cleaned:
            return
        stmt = insert(RevisionBundle).values(cleaned).on_conflict_do_nothing(index_elements=['file_path'])
        session.execute(stmt)


class Revision(Base):
    """Every revision of every page, with where its text is in the bundles.
    Written by RevisionChest for every revision, not only those with references."""
    __tablename__ = 'revisions'
    revision_id = Column(BigInteger, primary_key=True, autoincrement=False)  # MediaWiki rev_id
    domain_id = Column(Integer, ForeignKey('domains.id'), nullable=False)
    page_id = Column(Integer, nullable=False)
    parent_revision_id = Column(BigInteger)
    revision_timestamp = Column(DateTime(timezone=False), nullable=False)
    found_in_bundle = Column(Integer, ForeignKey('revision_bundles.id'))
    offset_begin = Column(BigInteger)
    length = Column(BigInteger)
    bundle = relationship("RevisionBundle", foreign_keys=[found_in_bundle])

    __table_args__ = (
        Index('idx_revisions_page_ts', 'domain_id', 'page_id', 'revision_timestamp'),
        Index('idx_revisions_page_revid', 'domain_id', 'page_id', 'revision_id'),
    )


class PageMetadata(Base):
    """Title and namespace of every page, from RevisionChest's page metadata."""
    __tablename__ = 'page_metadata'
    domain_id = Column(Integer, ForeignKey('domains.id'), nullable=False)
    page_id = Column(Integer, nullable=False)
    page_title = Column(String)
    page_namespace = Column(Integer)
    __table_args__ = (PrimaryKeyConstraint('domain_id', 'page_id', name='pk_page_metadata'),)

    @staticmethod
    def bulk_upsert(session: Session, rows):
        cleaned = _merge_rows(rows, ('domain_id', 'page_id'))
        if not cleaned:
            return
        excluded = insert(PageMetadata).excluded
        stmt = insert(PageMetadata).values(cleaned).on_conflict_do_update(
            index_elements=['domain_id', 'page_id'],
            set_={'page_title': excluded.page_title, 'page_namespace': excluded.page_namespace},
        )
        session.execute(stmt)


# ---------------------------------------------------------------------------
# Stages 1–3: citations
# ---------------------------------------------------------------------------

# citation_type values, set at extraction (Stage 1) from the extractor's candidate class.
CITATION_TYPES = ('ref_tag', 'template', 'bare_url', 'endnote', 'shorthand', 'external_link')


class NormalizedCitation(Base):
    """One row per distinct normalized reference text (the ``<ref …>`` wrapper
    included). Semantic columns are on ``normalized_citation_parts``."""
    __tablename__ = 'normalized_citations'
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    normalized_sha1 = Column(CHAR(40), nullable=False, unique=True)
    reference_normalized = Column(Text, nullable=False)
    appears_on_domain_id = Column(Integer, ForeignKey('domains.id'), nullable=False)
    appears_on_page_id = Column(Integer, nullable=False)
    # NULL until Stage 1 emits it (Phase 1); the Stage-3 gate then requires it.
    citation_type = Column(String)

    @staticmethod
    def bulk_upsert(session: Session, rows):
        cleaned = _merge_rows(rows, ('normalized_sha1',))
        if not cleaned:
            return
        excluded = insert(NormalizedCitation).excluded
        stmt = insert(NormalizedCitation).values(cleaned).on_conflict_do_update(
            index_elements=['normalized_sha1'],
            set_={'reference_normalized': excluded.reference_normalized,
                  'appears_on_domain_id': excluded.appears_on_domain_id,
                  'appears_on_page_id': excluded.appears_on_page_id,
                  'citation_type': func.coalesce(excluded.citation_type, NormalizedCitation.citation_type)},
        )
        session.execute(stmt)


class CitationInstance(Base):
    """One row per distinct raw reference text on a page. ``id`` is the
    deterministic ``wcd_schema.ids.instance_id(domain, page_id, raw_sha1)``
    (first 8 bytes of a SHA-1 as a signed BIGINT), so ``citation_history``
    Parquet can carry it without a Postgres round trip. Self-closing
    ``<ref name="x"/>`` reuses are not instances (see ``named_ref_uses``)."""
    __tablename__ = 'citation_instances'
    id = Column(BigInteger, primary_key=True, nullable=False, autoincrement=False)
    domain_id = Column(Integer, ForeignKey('domains.id'), nullable=False)
    page_id = Column(Integer, nullable=False)
    normalized_id = Column(Integer, ForeignKey('normalized_citations.id'), nullable=False)
    raw_sha1 = Column(CHAR(40), nullable=False)
    # 0=other, 1=inline, 2=endnote (the extractor's positional class)
    reference_type = Column(SmallInteger, nullable=False, server_default='0')
    reference_name = Column(String)
    normalized_citation = relationship("NormalizedCitation", foreign_keys=[normalized_id])

    __table_args__ = (
        UniqueConstraint('domain_id', 'page_id', 'raw_sha1', name='uix_ci_domain_page_raw'),
        Index('idx_ci_page', 'domain_id', 'page_id'),
        Index('idx_ci_normalized', 'normalized_id'),
        Index('idx_ci_norm_named', 'normalized_id', postgresql_where=text("reference_name IS NOT NULL AND reference_name <> ''")),
    )

    @staticmethod
    def bulk_upsert(session: Session, rows):
        cleaned = _merge_rows(rows, ('id',), drop=())
        if not cleaned:
            return
        excluded = insert(CitationInstance).excluded
        stmt = insert(CitationInstance).values(cleaned).on_conflict_do_update(
            index_elements=['id'],
            set_={'normalized_id': excluded.normalized_id,
                  'reference_type': excluded.reference_type,
                  'reference_name': excluded.reference_name},
        )
        session.execute(stmt)


class NormalizedCitationWebResource(Base):
    """Which cited URLs appear in which normalized citations."""
    __tablename__ = 'normalized_citation_web_resources'
    normalized_id = Column(Integer, ForeignKey('normalized_citations.id'), nullable=False)
    web_resource_id = Column(BigInteger, ForeignKey('web_resources.id'), nullable=False)
    __table_args__ = (PrimaryKeyConstraint('normalized_id', 'web_resource_id', name='pk_normcit_webresource'),)

    @staticmethod
    def bulk_upsert(session: Session, rows):
        cleaned = _merge_rows(rows, ('normalized_id', 'web_resource_id'))
        if not cleaned:
            return
        session.execute(insert(NormalizedCitationWebResource).values(cleaned).on_conflict_do_nothing())


class WikiTemplate(Base):
    """Template names per domain, stored first-letter-capitalised with spaces."""
    __tablename__ = 'wiki_templates'
    id = Column(Integer, primary_key=True, nullable=False)
    domain = Column(Integer, ForeignKey('domains.id'), nullable=False)
    name = Column(String, nullable=False)
    wikidata_id = Column(Integer, unique=True)
    librarybase_id = Column(Integer, unique=True)
    domain_row = relationship("Domain", foreign_keys=[domain])
    __table_args__ = (Index('uix_template_domain_name', 'domain', func.md5(name), unique=True),)

    @staticmethod
    def normalize_name(raw: str) -> str:
        if not raw:
            return raw
        norm = raw.replace('_', ' ').strip()
        return norm[0].upper() + norm[1:] if norm else norm

    @staticmethod
    def bulk_upsert(session: Session, rows):
        prepared = []
        for r in rows:
            r2 = dict(r)
            if r2.get('name'):
                r2['name'] = WikiTemplate.normalize_name(r2['name'])
            prepared.append(r2)
        cleaned = _merge_rows(prepared, ('domain', 'name'))
        if not cleaned:
            return
        excluded = insert(WikiTemplate).excluded
        stmt = insert(WikiTemplate).values(cleaned).on_conflict_do_update(
            index_elements=[WikiTemplate.domain, func.md5(WikiTemplate.name)],
            set_={'wikidata_id': excluded.wikidata_id, 'librarybase_id': excluded.librarybase_id},
        )
        session.execute(stmt)


class TemplateData(Base):
    """One row per template parameter inside a normalized citation."""
    __tablename__ = 'template_data'
    wiki_template_id = Column(Integer, ForeignKey('wiki_templates.id'), nullable=False)
    normalized_id = Column(Integer, ForeignKey('normalized_citations.id'), nullable=False)
    offset_start = Column(Integer, nullable=False)
    parameter_key = Column(String, nullable=False)
    parameter_key_md5 = Column(CHAR(32), nullable=False)
    parameter_value = Column(Text, nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint('wiki_template_id', 'normalized_id', 'offset_start', 'parameter_key_md5', name='pk_template_param'),
        Index('idx_template_lookup_md5', 'wiki_template_id', 'parameter_key', func.md5(parameter_value), 'normalized_id',
              postgresql_where=text('parameter_value IS NOT NULL')),
        Index('idx_td_norm_offset_key', 'normalized_id', 'offset_start', 'parameter_key'),
    )

    @staticmethod
    def bulk_upsert(session: Session, rows):
        prepared = []
        for r in rows:
            r2 = dict(r)
            if r2.get('parameter_key') is not None:
                r2['parameter_key_md5'] = hashlib.md5(r2['parameter_key'].encode()).hexdigest()
            prepared.append(r2)
        cleaned = _merge_rows(prepared, ('wiki_template_id', 'normalized_id', 'offset_start', 'parameter_key_md5'))
        if not cleaned:
            return
        stmt = insert(TemplateData).values(cleaned).on_conflict_do_update(
            index_elements=['wiki_template_id', 'normalized_id', 'offset_start', 'parameter_key_md5'],
            set_={'parameter_value': insert(TemplateData).excluded.parameter_value},
        )
        session.execute(stmt)


# ---------------------------------------------------------------------------
# Stage 4: semantic parts
# ---------------------------------------------------------------------------

# Which normalizer path handled a part.
PART_TYPES = ('template', 'ref_tag', 'external_link')


class NormalizedCitationPart(Base):
    """Stage 4 output: one row per semantic part of a normalized citation. A
    ``<ref>`` holding a bullet list of N citations yields N parts; every other
    citation has one part (``part_index`` 0). ``part_sha1`` = SHA1(content) is
    UVA's ``content_sha1``; the remaining columns are the UVA normalizers'
    outputs (``wcd_semantic.reftag`` / ``wcd_semantic.template``)."""
    __tablename__ = 'normalized_citation_parts'
    normalized_id = Column(Integer, ForeignKey('normalized_citations.id'), nullable=False)
    part_index = Column(SmallInteger, nullable=False, server_default='0')
    part_sha1 = Column(CHAR(40), nullable=False)
    content = Column(Text, nullable=False)
    part_type = Column(String, nullable=False)
    # ref_tag path
    norm_subtype = Column(String)
    norm_content = Column(Text)
    anchor_key = Column(String)
    anchor_label = Column(String)
    url_anchor_text = Column(String)
    page_ref = Column(String)
    # template path
    template_name = Column(String)
    template_subtype = Column(String)
    params_json = Column(JSONB)
    author_last = Column(String)
    author_first = Column(String)
    journal = Column(String)
    date = Column(String)
    publisher = Column(String)
    location = Column(String)
    edition = Column(String)
    volume = Column(String)
    issue = Column(String)
    pages = Column(String)
    # shared
    author_key = Column(String)
    title = Column(Text)
    year = Column(String)
    year_reliable = Column(Boolean)
    extracted_doi = Column(String)
    extracted_isbn = Column(String)
    extracted_pmid = Column(String)
    extracted_pmc = Column(String)
    extracted_arxiv = Column(String)
    extracted_wikidata_qid = Column(String)
    extracted_librarybase_id = Column(String)
    extracted_url = Column(Text)
    is_archive_url = Column(Boolean)
    # UVA id_tier: 0 none, 1 template_wrapped, 2 url, 3 isbn, 4 doi, 5 pmid/pmc (a branch label)
    id_tier = Column(SmallInteger)
    id_tier_label = Column(String)

    __table_args__ = (
        PrimaryKeyConstraint('normalized_id', 'part_index', name='pk_normalized_citation_parts'),
        Index('idx_ncp_part_sha1', 'part_sha1'),
        Index('idx_ncp_doi', 'extracted_doi', postgresql_where=text('extracted_doi IS NOT NULL')),
        Index('idx_ncp_isbn', 'extracted_isbn', postgresql_where=text('extracted_isbn IS NOT NULL')),
        Index('idx_ncp_pmid', 'extracted_pmid', postgresql_where=text('extracted_pmid IS NOT NULL')),
        Index('idx_ncp_pmc', 'extracted_pmc', postgresql_where=text('extracted_pmc IS NOT NULL')),
        Index('idx_ncp_arxiv', 'extracted_arxiv', postgresql_where=text('extracted_arxiv IS NOT NULL')),
        Index('idx_ncp_qid', 'extracted_wikidata_qid', postgresql_where=text('extracted_wikidata_qid IS NOT NULL')),
        Index('idx_ncp_id_tier', 'id_tier'),
    )


# ---------------------------------------------------------------------------
# Stages 5–7: resolution and serving
# ---------------------------------------------------------------------------

class RefNameLink(Base):
    """Stage 5: ``<ref name="X">`` → the defining citation instance, per
    revision range. A redefinition mid-history closes a range and opens a new one."""
    __tablename__ = 'ref_name_links'
    domain_id = Column(Integer, ForeignKey('domains.id'), nullable=False)
    page_id = Column(Integer, nullable=False)
    ref_name = Column(String, nullable=False)
    defining_instance_id = Column(BigInteger, ForeignKey('citation_instances.id'), nullable=False)
    first_revision_id = Column(BigInteger, nullable=False)
    last_revision_id = Column(BigInteger)  # NULL = still current
    __table_args__ = (
        PrimaryKeyConstraint('domain_id', 'page_id', 'ref_name', 'first_revision_id', name='pk_ref_name_links'),
        Index('idx_rnl_instance', 'defining_instance_id'),
    )


class GlobalSource(Base):
    """Stage 7a/7e/7f: one row per real-world source cited anywhere.
    ``global_source_id`` is UVA's ``sha1("global:" + sorted members)[:16]``."""
    __tablename__ = 'global_sources'
    global_source_id = Column(String(16), primary_key=True)
    match_type = Column(String)        # doi, isbn, pmid, pmc, arxiv, wikidata_qid, author_year_fuzzy, singleton
    confidence = Column(Float)         # NULL for singletons
    canonical_part_sha1 = Column(CHAR(40))
    source_type = Column(String)       # ref_tag | template | both (of the canonical member)
    author_key = Column(String)
    year = Column(String)
    title = Column(Text)
    extracted_doi = Column(String)
    extracted_isbn = Column(String)
    extracted_pmid = Column(String)
    extracted_pmc = Column(String)
    extracted_arxiv = Column(String)
    extracted_wikidata_qid = Column(String)
    extracted_librarybase_id = Column(String)
    extracted_url = Column(Text)
    n_pages = Column(Integer)          # Stage 7e
    n_total_citations = Column(Integer)  # Stage 7e
    reliability_score = Column(Float)  # Stage 7f (UVA scorer)
    verdict = Column(String)           # high_confidence | medium_confidence | low_confidence | questionable | likely_not_citation
    __table_args__ = (
        Index('idx_gs_doi', 'extracted_doi', postgresql_where=text('extracted_doi IS NOT NULL')),
        Index('idx_gs_isbn', 'extracted_isbn', postgresql_where=text('extracted_isbn IS NOT NULL')),
        Index('idx_gs_pmid', 'extracted_pmid', postgresql_where=text('extracted_pmid IS NOT NULL')),
        Index('idx_gs_pmc', 'extracted_pmc', postgresql_where=text('extracted_pmc IS NOT NULL')),
        Index('idx_gs_arxiv', 'extracted_arxiv', postgresql_where=text('extracted_arxiv IS NOT NULL')),
        Index('idx_gs_qid', 'extracted_wikidata_qid', postgresql_where=text('extracted_wikidata_qid IS NOT NULL')),
        Index('idx_gs_verdict', 'verdict'),
        Index('idx_gs_score', 'reliability_score'),
    )


class CitationSourceResolution(Base):
    """Stage 7b: part → global source, flattened. ``match_type``/``confidence``
    are the per-page values (UVA ``citations_resolved`` semantics)."""
    __tablename__ = 'citation_source_resolution'
    normalized_id = Column(Integer, nullable=False)
    part_index = Column(SmallInteger, nullable=False)
    domain_id = Column(Integer, ForeignKey('domains.id'), nullable=False)
    page_id = Column(Integer, nullable=False)
    global_source_id = Column(String(16), ForeignKey('global_sources.global_source_id'), nullable=False)
    source_type = Column(String)
    match_type = Column(String)
    confidence = Column(Float)
    is_canonical = Column(Boolean)
    __table_args__ = (
        PrimaryKeyConstraint('normalized_id', 'part_index', 'domain_id', 'page_id', name='pk_citation_source_resolution'),
        ForeignKeyConstraint(['normalized_id', 'part_index'],
                             ['normalized_citation_parts.normalized_id', 'normalized_citation_parts.part_index'],
                             name='fk_csr_part'),
        Index('idx_csr_global', 'global_source_id'),
        Index('idx_csr_page', 'domain_id', 'page_id'),
    )


class PageSourceSummary(Base):
    """Stage 7d: per (page, global source) — what the frontend's top-sources
    and source-pages endpoints read. Reliability lives on ``global_sources``."""
    __tablename__ = 'page_source_summary'
    domain_id = Column(Integer, ForeignKey('domains.id'), nullable=False)
    page_id = Column(Integer, nullable=False)
    global_source_id = Column(String(16), ForeignKey('global_sources.global_source_id'), nullable=False)
    author_key = Column(String)
    year = Column(String)
    title = Column(Text)
    extracted_doi = Column(String)
    extracted_isbn = Column(String)
    extracted_pmid = Column(String)
    extracted_pmc = Column(String)
    extracted_arxiv = Column(String)
    extracted_wikidata_qid = Column(String)
    extracted_librarybase_id = Column(String)
    extracted_url = Column(Text)
    source_type = Column(String)
    match_type = Column(String)
    confidence = Column(Float)
    first_rev_id = Column(BigInteger)
    first_seen = Column(DateTime(timezone=False))
    last_rev_id = Column(BigInteger)
    last_seen = Column(DateTime(timezone=False))
    n_citations = Column(Integer)
    n_revisions_cited = Column(Integer)
    n_self_closing_uses = Column(Integer)
    is_current = Column(Boolean)
    __table_args__ = (
        PrimaryKeyConstraint('domain_id', 'page_id', 'global_source_id', name='pk_page_source_summary'),
        Index('idx_pss_global', 'global_source_id'),
        Index('idx_pss_current', 'domain_id', 'page_id', postgresql_where=text('is_current')),
    )


class PageSummaryCache(Base):
    """Stage 7h: the pre-computed ``/pages/{domain}/{page_id}/summary`` response."""
    __tablename__ = 'page_summary_cache'
    domain_id = Column(Integer, ForeignKey('domains.id'), nullable=False)
    page_id = Column(Integer, nullable=False)
    revision_count = Column(Integer, nullable=False)
    first_revision_ts = Column(DateTime(timezone=False))
    latest_revision_ts = Column(DateTime(timezone=False))
    latest_rev_id = Column(BigInteger)
    reference_count = Column(Integer, nullable=False)
    type_breakdown = Column(JSONB)   # [{citation_type, count}]
    top_templates = Column(JSONB)    # [{template_name, count}]
    source_profile = Column(JSONB)   # {total_citation_uses, format_breakdown, source_kind_breakdown, summary}
    updated_at = Column(DateTime(timezone=False), nullable=False, server_default=func.now())
    __table_args__ = (PrimaryKeyConstraint('domain_id', 'page_id', name='pk_page_summary_cache'),)


# Tables grouped by the stage that writes them, for `wcd init-db --table` and the gates.
STAGE_TABLES = {
    0: ('domains', 'revision_bundles', 'revisions', 'page_metadata', 'web_resources'),
    3: ('containers', 'domains', 'web_resources', 'wiki_templates', 'normalized_citations',
        'citation_instances', 'normalized_citation_web_resources', 'template_data'),
    4: ('normalized_citation_parts',),
    5: ('ref_name_links',),
    7: ('global_sources', 'citation_source_resolution', 'page_source_summary', 'page_summary_cache'),
}
