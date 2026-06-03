import hashlib
from sqlalchemy import Column, Index, Integer, BigInteger, String, CHAR, ForeignKey, Text, UniqueConstraint, PrimaryKeyConstraint, select, func
from sqlalchemy.types import SmallInteger
from sqlalchemy.orm import relationship, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import insert

Base = declarative_base()

# "Documents" are the basic publication type. It is roughly analogous to a FRBR "edition".
# An individual Wikipedia article is a Document. The sources it cites are Documents.
# A Document can be made available online as one or more Web Resources. For example, an
# online news article would be an individual Document, the original publication of that
# article at a given URL would be a Web Resource, and a web archive of that article would
# be a second Web Resource of the same underlying Document. Documents can be part of other
# Documents (like chapters in a book). Documents can also appear in bigger works called
# Containers (periodicals, etc).
class Document(Base):
    __tablename__               =  'documents'
    id                          =  Column(Integer, primary_key=True, nullable=False)
    language_code               =  Column(String)
    has_container               =  Column(Integer, ForeignKey('containers.id'))
    part_of_larger_work         =  Column(Integer, ForeignKey('documents.id'))
    title                       =  Column(String)
    wikidata_id                 =  Column(Integer, unique=True)
    librarybase_id              =  Column(Integer, unique=True)
    container                   =  relationship("Container", foreign_keys=[has_container])
    part_of                     =  relationship("Document", remote_side=[id], foreign_keys=[part_of_larger_work])
    __table_args__              =  ()

    @staticmethod
    def upsert(session: Session, **kwargs):
        # Create a new Document and return its id. Deduplication should be handled by callers.
        # Do not commit here; caller manages transaction.
        stmt = insert(Document).values(**kwargs).returning(Document.id)
        result = session.execute(stmt).scalar_one()
        return result

# "Web Resources" are individual web pages. Ideally, a Web Resource corresponds to a Document,
# but in the initial step of building the database, a Web Resource may not necessarily be
# correlated with a Document. Web archives are Web Resources of other Web Resources.
class WebResource(Base):
    __tablename__              =  'web_resources'
    id                         =  Column(BigInteger, primary_key=True, nullable=False)
    url                        =  Column(String, nullable=False)
    url_hash                   =  Column(CHAR(32), nullable=False, unique=True)
    instance_of_document       =  Column(Integer, ForeignKey('documents.id'))
    availability_status        =  Column(Integer)
    is_archive_of              =  Column(BigInteger, ForeignKey('web_resources.id'))
    domain_id                  =  Column(Integer, ForeignKey('domains.id'))
    numeric_page_id            =  Column(Integer)
    numeric_namespace_id       =  Column(Integer)
    document                   =  relationship("Document", foreign_keys=[instance_of_document])
    original_resource          =  relationship("WebResource", foreign_keys=[is_archive_of])
    domain                     =  relationship("Domain", foreign_keys=[domain_id])

    @staticmethod
    def compute_url_hash(url: str) -> str:
        return hashlib.md5(url.encode('utf-8')).hexdigest()

    @staticmethod
    def upsert(session: Session, **kwargs):
        values = {k: v for k, v in kwargs.items() if k != 'id'}
        if 'url' in values and 'url_hash' not in values:
            values['url_hash'] = WebResource.compute_url_hash(values['url'])
        stmt = insert(WebResource).values(values)
        set_values = {k: v for k, v in kwargs.items() if v is not None and k not in ('id', 'url_hash')}
        stmt = stmt.on_conflict_do_update(
            index_elements=['url_hash'],
            set_=set_values
        )
        session.execute(stmt)

    @staticmethod
    def bulk_upsert(session: Session, rows):
        if not rows:
            return
        # Deduplicate rows by conflict key (url) within the same command to avoid
        # PostgreSQL "ON CONFLICT DO UPDATE command cannot affect row a second time".
        # When duplicates exist in the input, merge them preferring non-None values.
        # Then, normalize rows for a multi-values INSERT: every row must provide a
        # Python value (or None) for all potentially present columns. If some
        # rows omit a key entirely, SQLAlchemy 2.x may attempt to use an
        # internal BindParameter for that slot which is not allowed in a
        # multiparams VALUES clause, leading to a CompileError. Ensure uniform
        # keys across all rows and default missing ones to None.
        deduped = {}
        # Columns we may upsert for WebResource in this bulk path
        expected_keys = {
            'url',
            'url_hash',
            'domain_id',
            'numeric_page_id',
            'numeric_namespace_id',
            'instance_of_document',
            'is_archive_of',
            'availability_status',
        }
        for r in rows:
            base = {k: v for k, v in r.items() if k != 'id'}
            key = base.get('url')
            if key is None:
                # Skip rows without a URL; they would violate NOT NULL/UNIQUE
                continue
            # Compute url_hash if not present
            if 'url_hash' not in base:
                base['url_hash'] = WebResource.compute_url_hash(key)
            if key in deduped:
                # Merge, preferring non-None from the newer row
                current = deduped[key]
                for k, v in base.items():
                    if v is not None:
                        current[k] = v
            else:
                deduped[key] = dict(base)
        # Now normalize keys for INSERT
        cleaned = []
        for base in deduped.values():
            for k in expected_keys:
                base.setdefault(k, None)
            cleaned.append(base)
        if not cleaned:
            return
        # Sort by conflict key to ensure consistent lock ordering and prevent deadlocks
        cleaned.sort(key=lambda r: r.get('url_hash', ''))
        # Build SET clause that only updates columns with non-null values using COALESCE
        excluded = insert(WebResource).excluded
        set_clause = {
            'url': excluded.url,
            'instance_of_document': func.coalesce(excluded.instance_of_document, WebResource.instance_of_document),
            'availability_status': func.coalesce(excluded.availability_status, WebResource.availability_status),
            'is_archive_of': func.coalesce(excluded.is_archive_of, WebResource.is_archive_of),
            'domain_id': func.coalesce(excluded.domain_id, WebResource.domain_id),
            'numeric_page_id': func.coalesce(excluded.numeric_page_id, WebResource.numeric_page_id),
            'numeric_namespace_id': func.coalesce(excluded.numeric_namespace_id, WebResource.numeric_namespace_id),
        }
        stmt = insert(WebResource).values(cleaned).on_conflict_do_update(
            index_elements=['url_hash'],
            set_=set_clause
        )
        session.execute(stmt)

# "Domains" are domain names, like example.com, archive.org, or fremont.k12.ca.us. Web Resources
# have exactly one Domain.
class Domain(Base):
    __tablename__              =  'domains'
    id                         =  Column(Integer, primary_key=True, nullable=False)
    value                      =  Column(String, nullable=False, unique=True)
    top_level_domain           =  Column(String)
    parent_domain              =  Column(Integer, ForeignKey('domains.id'))
    for_container              =  Column(Integer, ForeignKey('containers.id'))
    internet_domains_id        =  Column(Integer, unique=True)
    container                  =  relationship("Container", foreign_keys=[for_container])

    @staticmethod
    def upsert(session: Session, **kwargs):
        values = {k: v for k, v in kwargs.items() if k != 'id'}
        stmt = insert(Domain).values(values)
        set_values = {k: v for k, v in values.items() if v is not None}
        if len(set_values) > 0:
            stmt = stmt.on_conflict_do_update(
                index_elements=['value'],
                set_=set_values
            )
        session.execute(stmt)

    @staticmethod
    def bulk_upsert(session: Session, rows):
        if not rows:
            return
        # Deduplicate by unique key 'value' and merge non-None values
        merged = {}
        for r in rows:
            base = {k: v for k, v in r.items() if k != 'id'}
            key = base.get('value')
            if key is None:
                continue
            if key in merged:
                cur = merged[key]
                for k, v in base.items():
                    if v is not None:
                        cur[k] = v
            else:
                merged[key] = dict(base)
        if not merged:
            return
        # Sort by conflict key to ensure consistent lock ordering and prevent deadlocks
        cleaned = sorted(merged.values(), key=lambda r: r.get('value', ''))
        stmt = insert(Domain).values(cleaned)
        stmt = stmt.on_conflict_do_update(
            index_elements=['value'],
            set_={
                'top_level_domain': insert(Domain).excluded.top_level_domain,
                'parent_domain': insert(Domain).excluded.parent_domain,
                'for_container': insert(Domain).excluded.for_container,
                'internet_domains_id': insert(Domain).excluded.internet_domains_id,
            }
        )
        session.execute(stmt)

# "Containers" are periodicals, journals, etc. Containers contain multiple Documents.
class Container(Base):
    __tablename__              =  'containers'
    id                         =  Column(Integer, primary_key=True, nullable=False)
    label                      =  Column(String)
    wikidata_id                =  Column(Integer, unique=True)
    librarybase_id             =  Column(Integer, unique=True)
    __table_args__             =  (
        UniqueConstraint('label', name='uix_container_label'),
    )

    @staticmethod
    def upsert(session: Session, **kwargs):
        values = {k: v for k, v in kwargs.items()}
        stmt = insert(Container).values(values)
        set_values = {k: v for k, v in values.items() if v is not None}
        stmt = stmt.on_conflict_do_update(
            index_elements=['label'],
            set_=set_values
        )
        session.execute(stmt)

    @staticmethod
    def bulk_upsert(session: Session, rows):
        if not rows:
            return
        # Deduplicate by unique key 'label' and merge non-None values
        merged = {}
        for r in rows:
            key = r.get('label')
            if key is None:
                continue
            if key in merged:
                cur = merged[key]
                for k, v in r.items():
                    if v is not None:
                        cur[k] = v
            else:
                merged[key] = dict(r)
        if not merged:
            return
        # Sort by conflict key to ensure consistent lock ordering and prevent deadlocks
        stmt = insert(Container).values(sorted(merged.values(), key=lambda r: r.get('label', ''))).on_conflict_do_update(
            index_elements=['label'],
            set_={
                'wikidata_id': insert(Container).excluded.wikidata_id,
                'librarybase_id': insert(Container).excluded.librarybase_id,
            }
        )
        session.execute(stmt)

# "Citations" appear on Wikipedia articles and other documents. Citations can have one or more
# Referenced Documents. This table tracks the earliest and latest revisions the citation
# appears in, while each individual revision containing the reference is stored as Citation History.
# Instead of storing the raw text, we store the start offset and length of the reference in the wikitext.
class Citation(Base):
    __tablename__ = 'citations'
    record_sha1 = Column(String, nullable=False)
    reference_raw_sha1 = Column(String, nullable=False)
    offset_start = Column(Integer, nullable=True)
    length = Column(Integer, nullable=True)
    # reference_type is an application-level small int enum: 0=other, 1=inline, 2=endnote (extensible)
    reference_type = Column(SmallInteger, nullable=False, server_default='0')
    reference_normalized_sha1 = Column(String, nullable=False)
    reference_name = Column(String)
    wiki_article_id = Column(Integer, nullable=False)

    __table_args__ = (
        UniqueConstraint('record_sha1', 'reference_raw_sha1', name='uix_record_raw'),
        PrimaryKeyConstraint('record_sha1', 'reference_raw_sha1', name='pk_citation'),
    )

    @staticmethod
    def upsert(session: Session, **kwargs):
        stmt = insert(Citation).values(**kwargs)
        stmt = stmt.on_conflict_do_update(
            index_elements=['record_sha1', 'reference_raw_sha1'],
            set_={
                'offset_start': kwargs.get('offset_start', Citation.offset_start),
                'length': kwargs.get('length', Citation.length),
                'reference_type': kwargs.get('reference_type', Citation.reference_type),
                'reference_normalized_sha1': kwargs.get('reference_normalized_sha1', Citation.reference_normalized_sha1),
                'reference_name': kwargs.get('reference_name', Citation.reference_name),
                'wiki_article_id': kwargs.get('wiki_article_id', Citation.wiki_article_id),
            }
        )
        session.execute(stmt)

# "Citation History" tracks the individual article revisions in which a given Citation appears.
class CitationHistory(Base):
    __tablename__ = 'citation_history'
    record_sha1 = Column(String, nullable=False, primary_key=True)
    revision_id = Column(BigInteger, nullable=False, primary_key=True)
    reference_normalized_sha1 = Column(String, nullable=False)
    reference_raw_sha1 = Column(String, nullable=False)

    __table_args__ = (UniqueConstraint('record_sha1', 'revision_id', name='uix_record_revision'),)

    @staticmethod
    def upsert(session: Session, **kwargs):
        stmt = insert(CitationHistory).values(**kwargs)
        set_values = {k: v for k, v in kwargs.items() if v is not None}
        stmt = stmt.on_conflict_do_update(
            index_elements=['record_sha1', 'revision_id'],
            set_=set_values
        )
        session.execute(stmt)

# "RevisionBundle" represents a compressed file containing the contents of a Revision.
# Each bundle has an incrementing ID and a file_path to its location on disk.
class RevisionBundle(Base):
    __tablename__ = 'revision_bundles'
    id = Column(Integer, primary_key=True, nullable=False)
    file_path = Column(String, nullable=False, unique=True)

    __table_args__ = (
        UniqueConstraint('file_path', name='uix_revision_bundle_file_path'),
    )

    @staticmethod
    def upsert(session: Session, **kwargs):
        stmt = insert(RevisionBundle).values(**kwargs)
        set_values = {k: v for k, v in kwargs.items() if v is not None}
        stmt = stmt.on_conflict_do_update(
            index_elements=['file_path'],
            set_=set_values
        )
        session.execute(stmt)

# "Revisions" table maps revision IDs to page IDs and timestamps.
class Revision(Base):
    __tablename__ = 'revisions'
    revision_id = Column(BigInteger, primary_key=True)
    page_id = Column(Integer, nullable=False)
    parent_revision_id = Column(BigInteger)
    revision_timestamp = Column(String, nullable=False)
    found_in_bundle = Column(Integer, ForeignKey('revision_bundles.id'))
    offset_begin = Column(Integer)
    length = Column(Integer)

    bundle = relationship("RevisionBundle", foreign_keys=[found_in_bundle])

    __table_args__ = (
        UniqueConstraint('revision_id', name='uix_revision_id'),
    )

    @staticmethod
    def upsert(session: Session, **kwargs):
        stmt = insert(Revision).values(**kwargs)
        set_values = {k: v for k, v in kwargs.items() if v is not None}
        stmt = stmt.on_conflict_do_update(
            index_elements=['revision_id'],
            set_=set_values
        )
        session.execute(stmt)

# "Normalized Citations" are Citations that have been run through a normalization function. This
# alphabetizes template parameters, makes newlines and whitespace consistent, removes ref names, etc.
# This allows for the identification of citations that are identical in content/meaning but not formatting.
class NormalizedCitation(Base):
    __tablename__ = 'normalized_citations'
    record_sha1 = Column(String, nullable=False, unique=True, primary_key=True)
    reference_normalized_sha1 = Column(String, nullable=False)
    reference_normalized = Column(Text, nullable=False)
    appears_on_article = Column(Integer, ForeignKey('documents.id'), nullable=False)
    wiki_article_document = relationship("Document", foreign_keys=[appears_on_article])

    __table_args__ = (
        UniqueConstraint('record_sha1', name='uix_record_sha1'),
        UniqueConstraint('record_sha1', 'reference_normalized_sha1', name='uix_record_normalized'),
    )

    @staticmethod
    def upsert(session: Session, **kwargs):
        stmt = insert(NormalizedCitation).values(**kwargs)
        set_values = {k: v for k, v in kwargs.items() if v is not None}
        stmt = stmt.on_conflict_do_update(
            index_elements=['record_sha1', 'reference_normalized_sha1'],
            set_=set_values
        )
        session.execute(stmt)

# "NormalizedCitationWebResource" maps which WebResources appear in which NormalizedCitations.
# A WebResource is identified by its own auto-incrementing web_resources.id. A NormalizedCitation is
# identified by its reference_normalized_sha1. This table de-duplicates URL appearances across
# identical normalized citations.
class NormalizedCitationWebResource(Base):
    __tablename__ = 'normalized_citation_web_resources'
    reference_normalized_sha1 = Column(String, nullable=False)
    web_resource_id = Column(BigInteger, ForeignKey('web_resources.id'), nullable=False)

    resource = relationship("WebResource", foreign_keys=[web_resource_id])

    __table_args__ = (
        UniqueConstraint('reference_normalized_sha1', 'web_resource_id', name='uix_normcit_webresource'),
        PrimaryKeyConstraint('reference_normalized_sha1', 'web_resource_id', name='pk_normcit_webresource'),
    )

    @staticmethod
    def upsert(session: Session, **kwargs):
        stmt = insert(NormalizedCitationWebResource).values(**kwargs)
        set_values = {k: v for k, v in kwargs.items() if v is not None}
        stmt = stmt.on_conflict_do_update(
            index_elements=['reference_normalized_sha1', 'web_resource_id'],
            set_=set_values
        )
        session.execute(stmt)

    @staticmethod
    def bulk_upsert(session: Session, rows):
        if not rows:
            return
        # Sort by conflict key to ensure consistent lock ordering and prevent deadlocks
        rows = sorted(rows, key=lambda r: (r.get('reference_normalized_sha1', ''), r.get('web_resource_id', 0)))
        stmt = insert(NormalizedCitationWebResource).values(rows).on_conflict_do_nothing()
        session.execute(stmt)

# "WikiTemplate" tracks wiki template names per domain_id (e.g., en.wikipedia.org) and ties
# them back to a Concept ID. Template names are stored in normalized form: first letter
# capitalized, with spaces instead of underscores.
class WikiTemplate(Base):
    __tablename__ = 'wiki_templates'
    id = Column(Integer, primary_key=True, nullable=False)
    domain = Column(Integer, ForeignKey('domains.id'), nullable=False)
    name = Column(String, nullable=False)  # normalized template name
    wikidata_id = Column(Integer, unique=True)
    librarybase_id = Column(Integer, unique=True)

    domain_row = relationship("Domain", foreign_keys=[domain])

    __table_args__ = (
        Index('uix_template_domain_name', 'domain', func.md5(name), unique=True),
    )

    @staticmethod
    def normalize_name(raw: str) -> str:
        if not raw:
            return raw
        norm = raw.replace('_', ' ').strip()
        if len(norm) == 0:
            return norm
        return norm[0].upper() + norm[1:]

    @staticmethod
    def upsert(session: Session, **kwargs):
        # Ensure normalized storage of the name
        if 'name' in kwargs and kwargs['name']:
            kwargs['name'] = WikiTemplate.normalize_name(kwargs['name'])
        stmt = insert(WikiTemplate).values(**kwargs)
        set_values = {k: v for k, v in kwargs.items() if v is not None}
        stmt = stmt.on_conflict_do_update(
            index_elements=[WikiTemplate.domain, func.md5(WikiTemplate.name)],
            set_=set_values
        )
        session.execute(stmt)

    @staticmethod
    def bulk_upsert(session: Session, rows):
        if not rows:
            return
        # Normalize names and deduplicate by unique key (domain, name)
        merged = {}
        for r in rows:
            r2 = dict(r)
            if 'name' in r2 and r2['name']:
                r2['name'] = WikiTemplate.normalize_name(r2['name'])
            key = (r2.get('domain'), r2.get('name'))
            if key[0] is None or key[1] is None:
                continue
            if key in merged:
                cur = merged[key]
                for k, v in r2.items():
                    if v is not None:
                        cur[k] = v
            else:
                merged[key] = r2
        if not merged:
            return
        # Sort by conflict key to ensure consistent lock ordering and prevent deadlocks
        stmt = insert(WikiTemplate).values(sorted(merged.values(), key=lambda r: (r.get('domain', 0), r.get('name', '')))).on_conflict_do_update(
            index_elements=[WikiTemplate.domain, func.md5(WikiTemplate.name)],
            set_={
                'wikidata_id': insert(WikiTemplate).excluded.wikidata_id,
                'librarybase_id': insert(WikiTemplate).excluded.librarybase_id,
            }
        )
        session.execute(stmt)

# "TemplateData" stores key/value parameters for each template invocation found within a
# normalized citation. Each row is per-parameter, disambiguated by the template concept ID,
# the normalized citation hash, the template's starting offset within the normalized citation,
# and the parameter key.
class TemplateData(Base):
    __tablename__ = 'template_data'
    wiki_template_id = Column(Integer, ForeignKey('wiki_templates.id'), nullable=False)
    reference_normalized_sha1 = Column(String, nullable=False)
    offset_start = Column(Integer, nullable=False)
    parameter_key = Column(String, nullable=False)
    parameter_key_md5 = Column(CHAR(32), nullable=False)
    parameter_value = Column(Text, nullable=True)

    template = relationship("WikiTemplate", foreign_keys=[wiki_template_id])

    __table_args__ = (
        PrimaryKeyConstraint('wiki_template_id', 'reference_normalized_sha1', 'offset_start', 'parameter_key_md5', name='pk_template_param'),
    )

    @staticmethod
    def _compute_key_md5(kwargs):
        if 'parameter_key' in kwargs and kwargs['parameter_key'] is not None:
            kwargs['parameter_key_md5'] = hashlib.md5(kwargs['parameter_key'].encode()).hexdigest()

    @staticmethod
    def upsert(session: Session, **kwargs):
        TemplateData._compute_key_md5(kwargs)
        stmt = insert(TemplateData).values(**kwargs)
        set_values = {k: v for k, v in kwargs.items() if v is not None}
        stmt = stmt.on_conflict_do_update(
            index_elements=['wiki_template_id', 'reference_normalized_sha1', 'offset_start', 'parameter_key_md5'],
            set_=set_values
        )
        session.execute(stmt)

    @staticmethod
    def bulk_upsert(session: Session, rows):
        if not rows:
            return
        # Compute MD5 for parameter_key and deduplicate by composite unique key
        merged = {}
        for r in rows:
            r2 = dict(r)
            TemplateData._compute_key_md5(r2)
            key = (r2.get('wiki_template_id'), r2.get('reference_normalized_sha1'), r2.get('offset_start'), r2.get('parameter_key_md5'))
            if None in key:
                continue
            if key in merged:
                cur = merged[key]
                for k, v in r2.items():
                    if v is not None:
                        cur[k] = v
            else:
                merged[key] = r2
        if not merged:
            return
        # Sort by conflict key to ensure consistent lock ordering and prevent deadlocks
        stmt = insert(TemplateData).values(sorted(merged.values(), key=lambda r: (r.get('wiki_template_id', 0), r.get('reference_normalized_sha1', ''), r.get('offset_start', 0), r.get('parameter_key_md5', '')))).on_conflict_do_update(
            index_elements=['wiki_template_id', 'reference_normalized_sha1', 'offset_start', 'parameter_key_md5'],
            set_={'parameter_value': insert(TemplateData).excluded.parameter_value}
        )
        session.execute(stmt)
