import bz2
import os
import sys
import credentials
import mwparserfromhell
from syntax import normalize_wikitext, get_sha1
from refs_extractor.article import extract_references
from time import sleep
from xml.etree import ElementTree
from sqlalchemy import create_engine, select, insert
from sqlalchemy.orm import sessionmaker
from models import *

DB = f"postgresql://{credentials.dbuser}:{credentials.dbpass}@{credentials.dbhost}:{credentials.dbport}/{credentials.dbname}"
Engine = create_engine(DB)
CreateSession = sessionmaker(bind=Engine)
Session = CreateSession()
Base.metadata.create_all(Engine)

seen_titles = {}

def get_filenames(relative_path):
    print("Getting filenames")
    absolute_path = os.path.abspath(relative_path)
    print(absolute_path)
    for dirpath, _, filenames in os.walk(absolute_path):
        for filename in filenames:
            full_file_path = os.path.join(dirpath, filename)
            yield full_file_path

def get_revisions_from_xml(filename):
    with bz2.open(filename, 'rt') as file:
        context = ElementTree.iterparse(file, events=('start', 'end'))
        page_data = {}
        for event, elem in context:
            ns = "{http://www.mediawiki.org/xml/export-0.11/}"
            # Parse page-level information
            if event == 'end' and elem.tag == f"{ns}page":
                page_data = {
                    "page_title": elem.find(f"{ns}title").text,
                    "page_ns": elem.find(f"{ns}ns").text,
                    "page_id": elem.find(f"{ns}id").text
                }
                # Iterate over revisions within the page
                for revision in elem.findall(f"{ns}revision"):
                    revision_data = {
                        "revision_id": revision.find(f"{ns}id").text,
                        "revision_timestamp": revision.find(f"{ns}timestamp").text,
                        "revision_text": revision.find(f"{ns}text").text
                    }
                    yield {**page_data, **revision_data}
                # Clear memory
                elem.clear()

def get_ref_name(wikitext):
    parsed = mwparserfromhell.parse(wikitext)
    for tag in parsed.filter_tags():
        if tag.tag == "ref":
            if tag.has("name"):
                if tag.get("name").value is not None:
                    return tag.get("name").value.strip()

def get_or_create_container_id_by_domain(session, domain):
    result = session.execute(
        select(Container.id).where(Container.label == domain)
    ).scalar_one_or_none()

    if result is not None:
        return result

    new_concept_id = Concept.upsert(session, label=domain)
    new_container = Container(id=new_concept_id, label=domain)
    session.add(new_container)
    session.commit()
    return new_container.id

def process_revision(data, domain="en.wikipedia.org", soft=False):
    if int(data["page_ns"]) != 0:
        return

    citations = []
    citation_histories = []
    normalized_citations = []
    seen_citations = set()
    seen_normalized = set()

    language_code = domain.split('.')[0]
    page_title = data["page_title"]
    page_title_underscores = page_title.replace(" ", "_")
    page_id = data["page_id"]
    revision_id = data["revision_id"]
    revision_timestamp = data["revision_timestamp"].replace("T", " ").replace("Z", "")

    if page_title not in seen_titles:
        container_id = get_or_create_container_id_by_domain(Session, domain)
        domain_id = Domain.upsert(Session, value=domain, for_container=container_id)
        document_id = Document.upsert(Session, numeric_page_id=page_id, language_code=language_code, has_container=container_id)
        WebResource.upsert(Session, url=f"https://{domain}/wiki/{page_title_underscores}", domain=domain_id, instance_of_document=document_id)
        seen_titles[page_title] = {
            "container_id": container_id,
            "domain_id": domain_id,
            "document_id": document_id}
    else:
        container_id = seen_titles[page_title]["container_id"]
        domain_id = seen_titles[page_title]["domain_id"]
        document_id = seen_titles[page_title]["document_id"]

    references = extract_references(data["revision_text"])
    #print(f"{page_title}\t({domain}:{page_id})\t{revision_id}\t{revision_timestamp}")

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

    if citations:
        stmt_citations = insert(Citation).values(citations).on_conflict_do_update(
            index_elements=['record_sha1', 'reference_raw_sha1'],
            set_={
                "reference_raw": insert(Citation).excluded.reference_raw,
                "reference_normalized_sha1": insert(Citation).excluded.reference_normalized_sha1,
                "reference_name": insert(Citation).excluded.reference_name,
                "wiki_article_id": insert(Citation).excluded.wiki_article_id
            }
        )
        Session.execute(stmt_citations)

    if citation_histories:
        stmt_histories = insert(CitationHistory).values(citation_histories).on_conflict_do_nothing()
        Session.execute(stmt_histories)

    if normalized_citations:
        stmt_normalized = insert(NormalizedCitation).values(normalized_citations).on_conflict_do_update(
            index_elements=["record_sha1"],
            set_={
                "reference_normalized": insert(NormalizedCitation).excluded.reference_normalized,
                "appears_on_article": insert(NormalizedCitation).excluded.appears_on_article
            }
        )
        Session.execute(stmt_normalized)

    Session.commit()

if __name__ == '__main__':
    if len(sys.argv) > 1:
        fnames = [sys.argv[1]]
    else:
        fnames = get_filenames("./sources/")

    for fname in fnames:
        #print(fname)
        for rev in get_revisions_from_xml(fname):
            process_revision(rev)
