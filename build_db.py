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
from sqlalchemy import create_engine, insert, select as sa_select
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
Engine = create_engine(
    DB,
    pool_size=10,
    max_overflow=20,
    executemany_mode='values',
    executemany_batch_page_size=1000,
    executemany_values_page_size=10000,
)
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
def get_container_id_cached(session, domain_label: str) -> int:
    # Upsert once and fetch id; no commit here, caller manages transaction
    Container.upsert(session, label=domain_label)
    return session.execute(sa_select(Container.id).where(Container.label == domain_label)).scalar_one()

def process_revisions(revisions, domain="en.wikipedia.org"):
    session = CreateSession()
    citations, citation_histories, normalized_citations = [], [], []
    revisions_rows = []
    seen_revision_ids = set()
    seen_citations, seen_normalized = set(), set()

    # Resolve container and main domain ids once per batch
    container_id = get_container_id_cached(session, domain)

    domains_needed = set([domain])
    # Accumulators for later bulk upserts
    cur_urls_info = []  # dicts with url, page_id, namespace_id, document_id, domain_label
    external_url_items = []  # (url, netloc)
    all_urls = set()
    ncwr_pairs = []  # (reference_normalized_sha1, url)
    templates_needed = set()  # (domain_label, normalized_name)
    templ_params_pending = []  # (domain_label, normalized_name, reference_normalized_sha1, offset_start, key, value)

    for data in revisions:

        language_code = domain.split('.')[0]
        page_id = data["page_id"]
        namespace_id = data.get("namespace_id")
        revision_id = data["revision_id"]
        revision_timestamp = data["revision_timestamp"].replace("T", " ").replace("Z", "")

        if page_id not in seen_pages:
            # Create a Document for this page
            document_id = Document.upsert(session, language_code=language_code, has_container=container_id)
            # Defer creating curid WebResource until domains are resolved; record needed info
            cur_url = f"https://{domain}/index.php?curid={page_id}"
            cur_urls_info.append({
                'url': cur_url,
                'numeric_page_id': page_id,
                'numeric_namespace_id': namespace_id,
                'instance_of_document': document_id,
                'domain_label': domain,
            })
            seen_pages[page_id] = PageMetadata(container_id, None, document_id)
        else:
            page_metadata = seen_pages[page_id]
            document_id = page_metadata.document_id

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
                if netloc:
                    domains_needed.add(netloc)
                external_url_items.append((url, netloc))
                all_urls.add(url)
                ncwr_pairs.append((reference_normalized_sha1, url))

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
                    templates_needed.add((domain, normalized_tpl_name))

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

                    # Collect one TemplateData per parameter (defer id resolution)
                    for p in params:
                        key = (p or {}).get('key')
                        val = (p or {}).get('value')
                        if not key:
                            continue
                        templ_params_pending.append((domain, normalized_tpl_name, reference_normalized_sha1, tpl_offset, key, val))

    # Domains: bulk upsert all needed and map ids
    if domains_needed:
        Domain.bulk_upsert(session, [{'value': d, 'for_container': container_id} for d in domains_needed])
        rows = session.execute(sa_select(Domain.value, Domain.id).where(Domain.value.in_(list(domains_needed)))).all()
        domain_id_by_value = {v: i for v, i in rows}
    else:
        domain_id_by_value = {}

    # Build WebResources rows (curid + external URLs) and bulk upsert
    wr_rows = []
    for info in cur_urls_info:
        wr_rows.append({
            'url': info['url'],
            'domain_id': domain_id_by_value.get(info['domain_label']),
            'numeric_page_id': info['numeric_page_id'],
            'numeric_namespace_id': info['numeric_namespace_id'],
            'instance_of_document': info['instance_of_document'],
        })
        all_urls.add(info['url'])

    for url, netloc in external_url_items:
        wr_rows.append({
            'url': url,
            'domain_id': domain_id_by_value.get(netloc),
        })

    if wr_rows:
        WebResource.bulk_upsert(session, wr_rows)

    # Map URL -> web_resource_id in one query
    url_to_id = {}
    if all_urls:
        rows = session.execute(sa_select(WebResource.url, WebResource.id).where(WebResource.url.in_(list(all_urls)))).all()
        url_to_id = {u: i for u, i in rows}

    # Build NCWR rows in bulk
    ncwr_rows = []
    for rsha1, url in ncwr_pairs:
        wr_id = url_to_id.get(url)
        if wr_id is not None:
            ncwr_rows.append({'reference_normalized_sha1': rsha1, 'web_resource_id': wr_id})

    if ncwr_rows:
        NormalizedCitationWebResource.bulk_upsert(session, ncwr_rows)

    # Templates: bulk upsert and then bulk upsert params
    wiki_template_rows = []
    for dom_label, name in templates_needed:
        dom_id = domain_id_by_value.get(dom_label)
        if dom_id is not None:
            wiki_template_rows.append({'domain': dom_id, 'name': name})

    if wiki_template_rows:
        WikiTemplate.bulk_upsert(session, wiki_template_rows)

    # Map (domain_id, name) -> wiki_template_id
    template_key_to_id = {}
    if wiki_template_rows:
        from sqlalchemy import tuple_
        keys = [(row['domain'], row['name']) for row in wiki_template_rows]
        rows = session.execute(
            sa_select(WikiTemplate.domain, WikiTemplate.name, WikiTemplate.id)
            .where(tuple_(WikiTemplate.domain, WikiTemplate.name).in_(keys))
        ).all()
        template_key_to_id = { (d, n): i for d, n, i in rows }

    # Build TemplateData rows
    template_data_rows = []
    for dom_label, name, rsha1, off, key, val in templ_params_pending:
        dom_id = domain_id_by_value.get(dom_label)
        if dom_id is None:
            continue
        tpl_id = template_key_to_id.get((dom_id, name))
        if tpl_id is None:
            continue
        template_data_rows.append({
            'wiki_template_id': tpl_id,
            'reference_normalized_sha1': rsha1,
            'offset_start': off,
            'parameter_key': key,
            'parameter_value': val,
        })

    if template_data_rows:
        TemplateData.bulk_upsert(session, template_data_rows)

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
