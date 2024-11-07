from sqlalchemy import Column, Integer, String, ForeignKey, Text
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# "Concepts" include anything: publications, documents, web pages, domain names, anything that
# can potentially be assigned an identifier. Data about these concepts is stored in, and
# retrieved from, different data sources, including Wikibases. The Concepts table reconciles
# these different Wikibase identifiers together and assigns a singular identifier. This is not
# intended to be a persistent identifier, but for internal data management.
class Concept(Base):
    __tablename__              =  'concepts'
    id                         =  Column(Integer, primary_key=True, nullable=False)
    label                      =  Column(String)
    wikidata_id                =  Column(Integer)
    librarybase_id             =  Column(Integer)
    internet_domains_id        =  Column(Integer)
    open_misc_id               =  Column(Integer)

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
    id                         =  Column(Integer, primary_key=True)
    concept_id                 =  Column(Integer, ForeignKey('concepts.id'), nullable=False)
    page_id                    =  Column(Integer)
    language_code              =  Column(String)
    has_container              =  Column(Integer, ForeignKey('concepts.id'))
    part_of                    =  Column(Integer, ForeignKey('concepts.id'))
    concept                    =  relationship("Concept", foreign_keys=[concept_id])
    container                  =  relationship("Concept", foreign_keys=[has_container])
    part_of_concept            =  relationship("Concept", foreign_keys=[part_of])

# "Web Resources" are individual web pages. Ideally, a Web Resource corresponds to a Document,
# but in the initial step of building the database, a Web Resource may not necessarily be
# correlated with a Document. Web archives are Web Resources of other Web Resources.
class WebResource(Base):
    __tablename__              =  'web_resources'
    id                         =  Column(Integer, primary_key=True)
    resource_concept_id        =  Column(Integer, ForeignKey('concepts.id'), nullable=False)
    url                        =  Column(String, nullable=False)
    document_concept_id        =  Column(Integer, ForeignKey('concepts.id'))
    availability_status        =  Column(Integer)
    is_archive_of              =  Column(Integer, ForeignKey('concepts.id'))
    domain                     =  Column(Integer, ForeignKey('concepts.id'))
    resource_concept           =  relationship("Concept", foreign_keys=[resource_concept_id])
    document_concept           =  relationship("Concept", foreign_keys=[document_concept_id])
    archive_of                 =  relationship("Concept", foreign_keys=[is_archive_of])
    domain_concept             =  relationship("Concept", foreign_keys=[domain])

# "Domains" are domain names, like example.com, archive.org, or fremont.k12.ca.us. Web Resources
# have exactly one Domain.
class Domain(Base):
    __tablename__              =  'domains'
    id                         =  Column(Integer, primary_key=True)
    domain_concept_id          =  Column(Integer, ForeignKey('concepts.id'), nullable=False)
    value                      =  Column(String, nullable=False)
    top_level_domain           =  Column(String)
    domain_concept             =  relationship("Concept", foreign_keys=[domain_concept_id])

# "Containers" are periodicals, journals, etc. Containers contain multiple Documents.
class Container(Base):
    __tablename__              =  'containers'
    id                         =  Column(Integer, primary_key=True)
    container_concept_id       =  Column(Integer, ForeignKey('concepts.id'), nullable=False)
    label                      =  Column(String)
    container_concept          =  relationship("Concept", foreign_keys=[container_concept_id])

# "Citations" appear on Wikipedia articles and other documents. Citations can have one or more
# Referenced Documents. This table stores the "raw" reference, which is the text of the reference
# exactly as it appears in wikitext. It also tracks the earliest and latest revisions the citation
# appears in, while each individual revision containing the reference is stored as Citation History.
class Citation(Base):
    __tablename__              =  'citations'
    id                         =  Column(Integer, primary_key=True)
    wiki_article_id            =  Column(Integer, ForeignKey('concepts.id'), nullable=False)
    reference_raw              =  Column(Text, nullable=False)
    reference_normalized_sha1  =  Column(String, nullable=False)
    reference_raw_sha1         =  Column(String, nullable=False)
    latest_revision            =  Column(Integer, nullable=False)
    first_revision             =  Column(Integer, nullable=False)
    wiki_article               =  relationship("Concept", foreign_keys=[wiki_article_id])


# "Citation History" tracks the individual article revisions in which a given Citation appears.
class CitationHistory(Base):
    __tablename__              =  'citation_history'
    id                         =  Column(Integer, primary_key=True)
    reference_normalized_sha1  =  Column(String, ForeignKey('normalized_citations.reference_normalized_sha1'), nullable=False)
    record_raw_sha1            =  Column(String, ForeignKey('citations.reference_raw_sha1'), nullable=False)
    revision_id                =  Column(Integer, nullable=False)
    revision_timestamp         =  Column(String, nullable=False)

# "Normalized Citations" are Citations that have been run through a normalization function. This
# alphabetizes template parameters, makes newlines and whitespace consistent, removes ref names, etc.
# This allows for the identification of citations that are identical in content/meaning but not formatting.
class NormalizedCitation(Base):
    __tablename__              =  'normalized_citations'
    id                         =  Column(Integer, primary_key=True)
    wiki_article_concept_id    =  Column(Integer, ForeignKey('concepts.id'), nullable=False)
    reference_normalized_sha1  =  Column(String, nullable=False, unique=True)
    reference_normalized       =  Column(Text, nullable=False)
    wiki_article_concept       =  relationship("Concept", foreign_keys=[wiki_article_concept_id])

# "Referenced Documents" match up an individual Sub-Reference Citations have one or more Sub-References,
# a Sub-Reference being an individual cited document. Since citations can be to multiple documents, we need
# to separately identify individual documents within a broader reference.
class ReferencedDocument(Base):
    __tablename__              =  'referenced_documents'
    id                         =  Column(Integer, primary_key=True)
    reference_normalized_sha1  =  Column(String, ForeignKey('normalized_citations.reference_normalized_sha1'), nullable=False)
    subreference_normalized    =  Column(Text, nullable=False)
    referenced_document_id     =  Column(Integer, ForeignKey('concepts.id'))
    referenced_document        =  relationship("Concept", foreign_keys=[referenced_document_id])
