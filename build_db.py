import bz2
import os
import sys
import credentials
import mwparserfromhell
import weakref
import multiprocessing
from functools import lru_cache
from xml.etree import ElementTree
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import sessionmaker
from models import *
from refs_extractor.article import extract_references
from syntax import normalize_wikitext, get_sha1

# Set up database connection pooling
DB = f"postgresql://{credentials.dbuser}:{credentials.dbpass}@{credentials.dbhost}:{credentials.dbport}/{credentials.dbname}"
Engine = create_engine(DB, pool_size=10, max_overflow=20)
CreateSession = sessionmaker(bind=Engine)

# Wrapper class for weak references
class PageMetadata:
    """Stores page metadata in a class (allows weak references)."""
    def __init__(self, container_id, domain_id, document_id):
        self.container_id = container_id
        self.domain_id = domain_id
        self.document_id = document_id

# Weak reference dictionary for seen titles
seen_titles = weakref.WeakValueDictionary()

# Constants
BATCH_SIZE = 1000  # Number of revisions processed before committing
N_PROCESSES = max(2, multiprocessing.cpu_count() - 1)

def get_filenames(relative_path):
    absolute_path = os.path.abspath(relative_path)
    for dirpath, _, filenames in os.walk(absolute_path):
        for filename in filenames:
            yield os.path.join(dirpath, filename)

def get_revisions_from_xml(filename):
    with bz2.open(filename, 'rt') as file:
        context = ElementTree.iterparse(file, events=('start', 'end'))
        page_data = {}
        for event, elem in context:
            ns = "{http://www.mediawiki.org/xml/export-0.11/}"
            if event == 'end' and elem.tag == f"{ns}page":
                page_data = {
                    "page_title": elem.find(f"{ns}title").text,
                    "page_ns": elem.find(f"{ns}ns").text,
                    "page_id": elem.find(f"{ns}id").text
                }
                for revision in elem.findall(f"{ns}revision"):
                    yield {
                        **page_data,
                        "revision_id": revision.find(f"{ns}id").text,
                        "revision_timestamp": revision.find(f"{ns}timestamp").text,
                        "revision_text": revision.find(f"{ns}text").text
                    }
                elem.clear()  # Free memory

@lru_cache(maxsize=1000)
def get_or_create_container_id_by_domain(session, domain):
    result = session.execute(select(Container.id).where(Container.label == domain)).scalar_one_or_none()
    if result:
        return result

    new_concept_id = Concept.upsert(session, label=domain)
    new_container = Container(id=new_concept_id, label=domain)
    session.add(new_container)
    session.commit()
    return new_container.id

def get_ref_name(wikitext):
    parsed = mwparserfromhell.parse(wikitext)
    for tag in parsed.filter_tags(matches=lambda t: t.tag == "ref" and t.has("name")):
        return tag.get("name").value.strip() if tag.get("name").value else None

def process_revisions(revisions, domain="en.wikipedia.org"):
    session = CreateSession()
    citations, citation_histories, normalized_citations = [], [], []
    seen_citations, seen_normalized = set(), set()

    for data in revisions:
        if int(data["page_ns"]) != 0:
            continue  # Skip non-article pages

        language_code = domain.split('.')[0]
        page_title = data["page_title"]
        page_id = data["page_id"]
        revision_id = data["revision_id"]
        revision_timestamp = data["revision_timestamp"].replace("T", " ").replace("Z", "")

        if page_title not in seen_titles:
            container_id = get_or_create_container_id_by_domain(session, domain)
            domain_id = Domain.upsert(session, value=domain, for_container=container_id)
            document_id = Document.upsert(session, numeric_page_id=page_id, language_code=language_code, has_container=container_id)
            WebResource.upsert(session, url=f"https://{domain}/wiki/{page_title.replace(' ', '_')}", domain=domain_id, instance_of_document=document_id)
            seen_titles[page_title] = PageMetadata(container_id, domain_id, document_id)
        else:
            page_metadata = seen_titles[page_title]
            container_id, domain_id, document_id = page_metadata.container_id, page_metadata.domain_id, page_metadata.document_id

        references = extract_references(data["revision_text"])

        for reference_raw in references:
            if not reference_raw.strip():
                continue

            reference_normalized = normalize_wikitext(reference_raw)
            record_sha1 = get_sha1(domain, page_id, reference_normalized)
            reference_normalized_sha1 = get_sha1(reference_normalized)
            reference_raw_sha1 = get_sha1(reference_raw)
            reference_name = get_ref_name(reference_raw)

            citation_key = (record_sha1, reference_raw_sha1)
            if citation_key not in seen_citations:
                seen_citations.add(citation_key)
                citations.append({
                    "record_sha1": record_sha1,
                    "reference_raw_sha1": reference_raw_sha1,
                    "reference_raw": reference_raw,
                    "reference_normalized_sha1": reference_normalized_sha1,
                    "reference_name": reference_name,
                    "wiki_article_id": page_id
                })

            if record_sha1 not in seen_normalized:
                seen_normalized.add(record_sha1)
                normalized_citations.append({
                    "record_sha1": record_sha1,
                    "reference_normalized_sha1": reference_normalized_sha1,
                    "reference_normalized": reference_normalized,
                    "appears_on_article": document_id
                })

            citation_histories.append({
                "record_sha1": record_sha1,
                "reference_raw_sha1": reference_raw_sha1,
                "reference_normalized_sha1": reference_normalized_sha1,
                "revision_id": revision_id,
                "revision_timestamp": revision_timestamp
            })

    # Perform bulk inserts with ON CONFLICT DO UPDATE
    if citations:
        stmt_citations = insert(Citation).values(citations).on_conflict_do_update(
            index_elements=['record_sha1', 'reference_raw_sha1'],
            set_={"reference_raw": insert(Citation).excluded.reference_raw}
        )
        session.execute(stmt_citations)

    if citation_histories:
        stmt_histories = insert(CitationHistory).values(citation_histories).on_conflict_do_nothing()
        session.execute(stmt_histories)

    if normalized_citations:
        stmt_normalized = insert(NormalizedCitation).values(normalized_citations).on_conflict_do_update(
            index_elements=["record_sha1"],
            set_={"reference_normalized": insert(NormalizedCitation).excluded.reference_normalized}
        )
        session.execute(stmt_normalized)

    session.commit()
    session.close()

def process_file(filename):
    batch = []
    for revision in get_revisions_from_xml(filename):
        batch.append(revision)
        if len(batch) >= BATCH_SIZE:
            process_revisions(batch)
            batch.clear()

    if batch:
        process_revisions(batch)

if __name__ == '__main__':
    if len(sys.argv) > 1:
        filenames = [sys.argv[1]]
    else:
        filenames = list(get_filenames("./sources/"))

    with multiprocessing.Pool(processes=N_PROCESSES) as pool:
        pool.map(process_file, filenames)
