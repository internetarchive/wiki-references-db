import io
import bz2
import os
import sys
from dotenv import load_dotenv
import weakref
import multiprocessing
from functools import lru_cache
from xml.etree import ElementTree
import zstandard as zstd
from sqlalchemy import create_engine, insert, select as sa_select, update
from sqlalchemy.orm import sessionmaker
from models import *
from refs_extractor.article import extract_references
from refs_extractor.syntax import normalize_wikitext, get_sha1

# Set up database connection pooling
load_dotenv()
DB = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
Engine = create_engine(DB, pool_size=10, max_overflow=20)
CreateSession = sessionmaker(bind=Engine)

# Wrapper class for weak references
class PageMetadata:
    """Stores page metadata in a class (allows weak references)."""
    def __init__(self, container_id, domain_id, document_id):
        self.container_id = container_id
        self.domain_id = domain_id
        self.document_id = document_id

# Weak reference dictionary for seen page IDs
seen_pages = weakref.WeakValueDictionary()

# Constants
BATCH_SIZE = 1000  # Number of revisions processed before committing
N_PROCESSES = max(2, multiprocessing.cpu_count() - 1)

def get_filenames(relative_path):
    absolute_path = os.path.abspath(relative_path)
    for dirpath, _, filenames in os.walk(absolute_path):
        for filename in filenames:
            yield os.path.join(dirpath, filename)

def get_revisions_from_mwrev_zst(filename):
    """Stream and parse revisions from a .mwrev.zst file.

    Format:
      - Lines starting with '#' contain metadata for a new revision.
      - Lines starting with a single space ' ' belong to the revision text.
    Required metadata keys:
      page_id, rev_id, parent_rev_id (optional/empty), timestamp
    """
    dctx = zstd.ZstdDecompressor()
    with open(filename, 'rb') as fh:
        with dctx.stream_reader(fh) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')
            current = None
            text_lines = []
            for raw_line in text_stream:
                if not raw_line:
                    continue
                if raw_line.startswith('#'):
                    # Flush previous
                    if current is not None:
                        current['revision_text'] = "\n".join(text_lines)
                        yield current
                    # Start new revision
                    meta_line = raw_line[1:].strip()
                    parts = [p for p in meta_line.split() if '=' in p]
                    meta = {}
                    for p in parts:
                        k, v = p.split('=', 1)
                        meta[k.strip()] = v.strip() if v is not None else ''

                    page_id = int(meta.get('page_id')) if meta.get('page_id') else None
                    # namespace id (ns) may be missing on some lines; store as None when absent
                    namespace_id = int(meta.get('ns')) if meta.get('ns') else None
                    rev_id = int(meta.get('rev_id')) if meta.get('rev_id') else None
                    parent_rev_id = meta.get('parent_rev_id')
                    parent_rev_id = int(parent_rev_id) if parent_rev_id else None
                    timestamp = (meta.get('timestamp') or '').replace('T', ' ').replace('Z', '')

                    current = {
                        'page_id': page_id,
                        'namespace_id': namespace_id,
                        'revision_id': rev_id,
                        'parent_revision_id': parent_rev_id,
                        'revision_timestamp': timestamp,
                        'revision_text': ''
                    }
                    text_lines = []
                elif raw_line.startswith(' '):
                    text_lines.append(raw_line[1:].rstrip('\n'))
                else:
                    # Ignore any other lines (shouldn't normally occur)
                    continue
            # Flush last
            if current is not None:
                current['revision_text'] = "\n".join(text_lines)
                yield current

@lru_cache(maxsize=1000)
def get_or_create_container_id_by_domain_label(domain: str) -> int:
    s = CreateSession()
    try:
        # If a container row already exists for this label, return its id
        result = s.execute(sa_select(Container.id).where(Container.label == domain)).scalar_one_or_none()
        if result is not None:
            return result
        inserted_id = Container.upsert(s, label=domain)
        return inserted_id
    finally:
        s.close()

def process_revisions(revisions, domain="en.wikipedia.org"):
    session = CreateSession()
    citations, citation_histories, normalized_citations = [], [], []
    revisions_rows = []
    seen_revision_ids = set()
    seen_citations, seen_normalized = set(), set()

    for data in revisions:
        
        language_code = domain.split('.')[0]
        page_id = data["page_id"]
        namespace_id = data.get("namespace_id")
        revision_id = data["revision_id"]
        revision_timestamp = data["revision_timestamp"].replace("T", " ").replace("Z", "")

        if page_id not in seen_pages:
            container_id = get_or_create_container_id_by_domain_label(domain)
            domain_id = Domain.upsert(session, value=domain, for_container=container_id)

            # Ensure the primary WebResource for this page exists (curid URL) and has numeric identifiers
            cur_url = f"https://{domain}/index.php?curid={page_id}"
            WebResource.upsert(
                session,
                url=cur_url,
                domain_id=domain_id,
                numeric_page_id=page_id,
                numeric_namespace_id=namespace_id,
            )
            wr_row = session.execute(
                sa_select(WebResource.id, WebResource.instance_of_document).where(WebResource.url == cur_url)
            ).first()
            if wr_row is None:
                # Extremely unlikely due to upsert
                document_id = Document.upsert(session, language_code=language_code, has_container=container_id)
                WebResource.upsert(session, url=cur_url, domain_id=domain_id, instance_of_document=document_id, numeric_page_id=page_id, numeric_namespace_id=namespace_id)
            else:
                wr_id, instance_doc_id = wr_row
                if instance_doc_id is None:
                    document_id = Document.upsert(session, language_code=language_code, has_container=container_id)
                    # Attach the new document to the web resource
                    session.execute(
                        update(WebResource)
                        .where(WebResource.id == wr_id)
                        .values(instance_of_document=document_id)
                    )
                    session.commit()
                else:
                    document_id = instance_doc_id

            seen_pages[page_id] = PageMetadata(container_id, domain_id, document_id)
        else:
            page_metadata = seen_pages[page_id]
            container_id, domain_id, document_id = page_metadata.container_id, page_metadata.domain_id, page_metadata.document_id

        references = extract_references(data["revision_text"], include_offsets=True)

        for ref in references:
            reference_raw = ref.get('raw_reference')
            offset_start = ref.get('offset_start')
            offset_end = ref.get('offset_end')
            reference_type = ref.get('reference_type', 0)
            if not reference_raw or not reference_raw.strip():
                continue

            reference_normalized = normalize_wikitext(reference_raw)
            record_sha1 = get_sha1(domain, page_id, reference_normalized)
            reference_normalized_sha1 = get_sha1(reference_normalized)
            reference_raw_sha1 = get_sha1(reference_raw)
            # Prefer name provided by the extractor; fallback remains None
            reference_name = ref.get('reference_name')

            citation_key = (record_sha1, reference_raw_sha1)
            if citation_key not in seen_citations:
                seen_citations.add(citation_key)
                citations.append({
                    "record_sha1": record_sha1,
                    "reference_raw_sha1": reference_raw_sha1,
                    "offset_start": offset_start,
                    "offset_end": offset_end,
                    "reference_type": reference_type,
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
            })

            if revision_id not in seen_revision_ids:
                seen_revision_ids.add(revision_id)
                revisions_rows.append({
                    "revision_id": revision_id,
                    "page_id": page_id,
                    "parent_revision_id": data.get("parent_revision_id"),
                    "revision_timestamp": revision_timestamp
                })

            urls = ref.get('urls') or []
            for url in urls:
                if not url:
                    continue
                try:
                    from urllib.parse import urlparse
                    netloc = urlparse(url).netloc
                except Exception:
                    netloc = None
                url_domain_id = None
                if netloc:
                    url_domain_id = Domain.upsert(session, value=netloc, for_container=container_id)
                WebResource.upsert(session, url=url, domain_id=url_domain_id)
                wr_id = session.execute(sa_select(WebResource.id).where(WebResource.url == url)).scalar_one()
                NormalizedCitationWebResource.upsert(
                    session,
                    reference_normalized_sha1=reference_normalized_sha1,
                    web_resource_id=wr_id
                )

            templates = ref.get('templates') or []
            if templates:
                def find_nth(haystack: str, needle: str, n: int) -> int:
                    start = -1
                    for _ in range(n):
                        start = haystack.find(needle, start + 1)
                        if start == -1:
                            break
                    return start

                for idx, tpl in enumerate(templates, start=1):
                    tpl_name_raw = (tpl or {}).get('template_name') or ''
                    tpl_full_text = (tpl or {}).get('full_text') or ''
                    params = (tpl or {}).get('parameters') or []

                    if not tpl_name_raw:
                        continue

                    normalized_tpl_name = WikiTemplate.normalize_name(tpl_name_raw)
                    wiki_template_id = WikiTemplate.upsert(session, domain=domain_id, name=normalized_tpl_name)

                    # Compute offset of the template in the normalized citation text.
                    # Prefer searching by beginning marker with normalized name, as the full_text
                    # may not match after normalization (params are reordered, whitespace unified).
                    marker = "{{" + normalized_tpl_name
                    # Attempt to match the same occurrence index as in the extractor list order
                    tpl_offset = find_nth(reference_normalized, marker, idx)
                    if tpl_offset is None or tpl_offset < 0:
                        # Fallback: try full_text directly; if not found, fall back to citation offset_start
                        tpl_offset = reference_normalized.find(tpl_full_text)
                        if tpl_offset < 0:
                            tpl_offset = offset_start if isinstance(offset_start, int) else 0

                    # Upsert one TemplateData per parameter
                    for p in params:
                        key = (p or {}).get('key')
                        val = (p or {}).get('value')
                        if not key:
                            continue
                        TemplateData.upsert(
                            session,
                            wiki_template_id=wiki_template_id,
                            reference_normalized_sha1=reference_normalized_sha1,
                            offset_start=tpl_offset,
                            parameter_key=key,
                            parameter_value=val
                        )

    if citations:
        stmt_citations = insert(Citation).values(citations).on_conflict_do_update(
            index_elements=['record_sha1', 'reference_raw_sha1'],
            set_={
                "offset_start": insert(Citation).excluded.offset_start,
                "offset_end": insert(Citation).excluded.offset_end,
                "reference_type": insert(Citation).excluded.reference_type,
                "reference_name": insert(Citation).excluded.reference_name
            }
        )
        session.execute(stmt_citations)

    if revisions_rows:
        stmt_revisions = insert(Revision).values(revisions_rows).on_conflict_do_update(
            index_elements=['revision_id'],
            set_={
                'page_id': insert(Revision).excluded.page_id,
                'parent_revision_id': insert(Revision).excluded.parent_revision_id,
                'revision_timestamp': insert(Revision).excluded.revision_timestamp,
            }
        )
        session.execute(stmt_revisions)

    if citation_histories:
        stmt_histories = insert(CitationHistory).values(citation_histories).on_conflict_do_nothing()
        session.execute(stmt_histories)

    if normalized_citations:
        stmt_normalized = insert(NormalizedCitation).values(normalized_citations).on_conflict_do_update(
            index_elements=["record_sha1"],
            set_={
                "reference_normalized": insert(NormalizedCitation).excluded.reference_normalized,
                "reference_normalized_sha1": insert(NormalizedCitation).excluded.reference_normalized_sha1,
                "appears_on_article": insert(NormalizedCitation).excluded.appears_on_article,
            }
        )
        session.execute(stmt_normalized)

    session.commit()
    session.close()

def process_file(filename):
    if not filename.endswith('.mwrev.zst'):
        return
    batch = []
    for revision in get_revisions_from_mwrev_zst(filename):
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
