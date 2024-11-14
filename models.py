from sqlalchemy import Column, Integer, String, ForeignKey, Text, UniqueConstraint
from sqlalchemy.orm import relationship, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import insert

Base = declarative_base()

# "Concepts" include anything: publications, documents, web pages, domain names, anything that
# can potentially be assigned an identifier. Data about these concepts are stored in, and
# retrieved from, different data sources, including Wikibases. The Concepts table reconciles
# these different Wikibase identifiers together and assigns a singular identifier. This is not
# intended to be a persistent identifier, but for internal data management.
class Concept(Base):
    __tablename__              =  'concepts'
    id                         =  Column(Integer, primary_key=True, nullable=False)
    label                      =  Column(String)
    wikidata_id                =  Column(Integer, unique=True)
    librarybase_id             =  Column(Integer, unique=True)
    internet_domains_id        =  Column(Integer, unique=True)
    open_misc_id               =  Column(Integer, unique=True)

    @staticmethod
    def upsert(session: Session, **kwargs):
        stmt = insert(Concept).values(**kwargs)
        stmt = stmt.on_conflict_do_update(
            index_elements=['id'],
            set_=kwargs
        )
        result = session.execute(stmt)
        session.commit()
        return result.inserted_primary_key[0] if result.inserted_primary_key else kwargs.get('id')

# "Documents" are the basic publication type. It is roughly analogous to a FRBR "edition".
# An individual Wikipedia article is a Document. The sources it cites are Documents.
# A Document can be made available online as one or more Web Resources. For example, an
# online news article would be an individual Document, the original publication of that
# article at a given URL would be a Web Resource, and a web archive of that article would
# be a second Web Resource of the same underlying Document. Documents can be part of other
# Documents (like chapters in a book). Documents can also appear in bigger works called
# Containers (periodicals, etc).
class Document(Base):
    __tablename__              =  'documents'
    id                         =  Column(Integer, ForeignKey('concepts.id'), primary_key=True, nullable=False)
    numeric_page_id            =  Column(Integer)
    language_code              =  Column(String)
    has_container              =  Column(Integer, ForeignKey('concepts.id'))
    part_of_larger_work        =  Column(Integer, ForeignKey('concepts.id'))
    document_concept           =  relationship("Concept", foreign_keys=[id])
    container_concept          =  relationship("Concept", foreign_keys=[has_container])
    part_of_concept            =  relationship("Concept", foreign_keys=[part_of_larger_work])

    @staticmethod
    def upsert(session: Session, **kwargs):
        if 'id' not in kwargs:
            kwargs['id'] = Concept.upsert(session)
        stmt = insert(Document).values(**kwargs)
        stmt = stmt.on_conflict_do_update(
            index_elements=['id'],
            set_=kwargs
        )
        session.execute(stmt)
        session.commit()

# "Web Resources" are individual web pages. Ideally, a Web Resource corresponds to a Document,
# but in the initial step of building the database, a Web Resource may not necessarily be
# correlated with a Document. Web archives are Web Resources of other Web Resources.
class WebResource(Base):
    __tablename__              =  'web_resources'
    id                         =  Column(Integer, ForeignKey('concepts.id'), primary_key=True, nullable=False)
    url                        =  Column(String, nullable=False, unique=True)
    instance_of_document       =  Column(Integer, ForeignKey('concepts.id'))
    availability_status        =  Column(Integer)
    is_archive_of              =  Column(Integer, ForeignKey('concepts.id'))
    domain                     =  Column(Integer, ForeignKey('concepts.id'))
    resource_concept           =  relationship("Concept", foreign_keys=[id])
    document_concept           =  relationship("Concept", foreign_keys=[instance_of_document])
    original_resource_concept  =  relationship("Concept", foreign_keys=[is_archive_of])
    domain_concept             =  relationship("Concept", foreign_keys=[domain])

    @staticmethod
    def upsert(session: Session, **kwargs):
        if 'id' not in kwargs:
            kwargs['id'] = Concept.upsert(session)
        stmt = insert(WebResource).values(**kwargs)
        stmt = stmt.on_conflict_do_update(
            index_elements=['url'],
            set_=kwargs
        )
        session.execute(stmt)
        session.commit()

# "Domains" are domain names, like example.com, archive.org, or fremont.k12.ca.us. Web Resources
# have exactly one Domain.
class Domain(Base):
    __tablename__              =  'domains'
    id                         =  Column(Integer, ForeignKey('concepts.id'), primary_key=True, nullable=False)
    value                      =  Column(String, nullable=False)
    top_level_domain           =  Column(String)
    parent_domain              =  Column(Integer, ForeignKey('concepts.id'))
    domain_concept             =  relationship("Concept", foreign_keys=[id])

    @staticmethod
    def upsert(session: Session, **kwargs):
        if 'id' not in kwargs:
            kwargs['id'] = Concept.upsert(session)
        stmt = insert(Domain).values(**kwargs)
        stmt = stmt.on_conflict_do_update(
            index_elements=['id'],
            set_=kwargs
        )
        session.execute(stmt)
        session.commit()

# "Containers" are periodicals, journals, etc. Containers contain multiple Documents.
class Container(Base):
    __tablename__              =  'containers'
    id                         =  Column(Integer, ForeignKey('concepts.id'), primary_key=True, nullable=False)
    label                      =  Column(String)
    container_concept          =  relationship("Concept", foreign_keys=[id])

    @staticmethod
    def upsert(session: Session, **kwargs):
        if 'id' not in kwargs:
            kwargs['id'] = Concept.upsert(session)
        stmt = insert(Container).values(**kwargs)
        stmt = stmt.on_conflict_do_update(
            index_elements=['id'],
            set_=kwargs
        )
        session.execute(stmt)
        session.commit()

# "Citations" appear on Wikipedia articles and other documents. Citations can have one or more
# Referenced Documents. This table stores the "raw" reference, which is the text of the reference
# exactly as it appears in wikitext. It also tracks the earliest and latest revisions the citation
# appears in, while each individual revision containing the reference is stored as Citation History.
class Citation(Base):
    __tablename__              =  'citations'
    record_sha1                =  Column(String, primary_key=True, nullable=False)
    reference_raw_sha1         =  Column(String)
    reference_raw              =  Column(Text, nullable=False)
    reference_normalized_sha1  =  Column(String, nullable=False)
    reference_name             =  Column(String)
    wiki_article_id            =  Column(Integer, ForeignKey('concepts.id'), nullable=False)
    wiki_article               =  relationship("Concept", foreign_keys=[wiki_article_id])

    @staticmethod
    def upsert(session: Session, **kwargs):
        stmt = insert(Citation).values(**kwargs)
        stmt = stmt.on_conflict_do_update(
            index_elements=['record_sha1'],
            set_={
                'reference_raw': kwargs.get('reference_raw', Citation.reference_raw),
                'reference_normalized_sha1': kwargs.get('reference_normalized_sha1', Citation.reference_normalized_sha1),
            }
        )
        session.execute(stmt)
        session.commit()

# "Citation History" tracks the individual article revisions in which a given Citation appears.
class CitationHistory(Base):
    __tablename__              =  'citation_history'
    record_sha1                =  Column(String, nullable=False)
    reference_normalized_sha1  =  Column(String, nullable=False)
    reference_raw_sha1         =  Column(String, nullable=False)
    revision_id                =  Column(Integer, nullable=False)
    revision_timestamp         =  Column(String, nullable=False)
    __table_args__             =  (UniqueConstraint('record_sha1', 'revision_id', name='uix_record_revision'),)

    @staticmethod
    def upsert(session: Session, **kwargs):
        stmt = insert(CitationHistory).values(**kwargs)
        stmt = stmt.on_conflict_do_update(
            index_elements=['record_sha1', 'revision_id'],
            set_=kwargs
        )
        session.execute(stmt)
        session.commit()

# "Normalized Citations" are Citations that have been run through a normalization function. This
# alphabetizes template parameters, makes newlines and whitespace consistent, removes ref names, etc.
# This allows for the identification of citations that are identical in content/meaning but not formatting.
class NormalizedCitation(Base):
    __tablename__              =  'normalized_citations'
    record_sha1                =  Column(String, nullable=False)
    reference_normalized_sha1  =  Column(String, nullable=False)
    reference_normalized       =  Column(Text, nullable=False)
    appears_on_article         =  Column(Integer, ForeignKey('concepts.id'), nullable=False)
    wiki_article_concept       =  relationship("Concept", foreign_keys=[appears_on_article])
    __table_args__             =  (UniqueConstraint('record_sha1', 'reference_normalized_sha1', name='uix_record_normalized'),)

    @staticmethod
    def upsert(session: Session, **kwargs):
        stmt = insert(NormalizedCitation).values(**kwargs)
        stmt = stmt.on_conflict_do_update(
            index_elements=['record_sha1', 'reference_normalized_sha1'],
            set_=kwargs
        )
        session.execute(stmt)
        session.commit()

# "Referenced Documents" match up an individual Sub-Reference Citations have one or more Sub-References,
# a Sub-Reference being an individual cited document. Since citations can be to multiple documents, we need
# to separately identify individual documents within a broader reference.
class ReferencedDocument(Base):
    __tablename__              =  'referenced_documents'
    record_sha1                =  Column(Integer, nullable=False)
    reference_normalized_sha1  =  Column(String, nullable=False)
    subreference_normalized    =  Column(Text, nullable=False)
    referenced_document        =  Column(Integer, ForeignKey('concepts.id'))
    document_concept           =  relationship("Concept", foreign_keys=[referenced_document])

    __table_args__ = (UniqueConstraint('record_sha1', 'subreference_normalized', name='uix_record_subreference'),)

    @staticmethod
    def upsert(session: Session, **kwargs):
        stmt = insert(ReferencedDocument).values(**kwargs)
        stmt = stmt.on_conflict_do_update(
            index_elements=['record_sha1', 'subreference_normalized'],
            set_=kwargs
        )
        session.execute(stmt)
        session.commit()
