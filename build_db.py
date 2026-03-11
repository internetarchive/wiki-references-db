import io
import bz2
import os
import sys
import time
import signal
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import weakref
import multiprocessing
from multiprocessing import Queue, Event, Process
from collections import OrderedDict
from xml.etree import ElementTree
import zstandard as zstd
from sqlalchemy import create_engine, insert, select as sa_select, text
from sqlalchemy.exc import DBAPIError, OperationalError
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
_POOL_RECYCLE_S = int(os.getenv('DB_POOL_RECYCLE', '1800'))
_RETRY_ATTEMPTS = int(os.getenv('DB_RETRY_ATTEMPTS', '5'))
_RETRY_BASE_DELAY_S = float(os.getenv('DB_RETRY_BASE_DELAY', '0.5'))


class LRUSet:
    """A bounded, process-local LRU set.

    Used to avoid re-sending known-duplicate unique keys to the database across batches.
    Eviction is safe because inserts use ON CONFLICT semantics.
    """

    def __init__(self, maxsize: int):
        self.maxsize = max(0, int(maxsize))
        self._d = OrderedDict()

    def add(self, key) -> bool:
        """Return True if `key` was already present, else add it and return False."""
        if self.maxsize <= 0:
            return False
        if key in self._d:
            self._d.move_to_end(key)
            return True
        self._d[key] = None
        if len(self._d) > self.maxsize:
            self._d.popitem(last=False)
        return False


class LRUMap:
    """A bounded, process-local LRU map.

    Similar to :class:`LRUSet`, but stores values.
    """

    def __init__(self, maxsize: int):
        self.maxsize = max(0, int(maxsize))
        self._d = OrderedDict()

    def get(self, key, default=None):
        if self.maxsize <= 0:
            return default
        if key in self._d:
            self._d.move_to_end(key)
            return self._d[key]
        return default

    def set(self, key, value) -> None:
        if self.maxsize <= 0:
            return
        if key in self._d:
            self._d.move_to_end(key)
        self._d[key] = value
        if len(self._d) > self.maxsize:
            self._d.popitem(last=False)


# Cross-batch, per-process LRU de-dupe caches (safe with ON CONFLICT inserts)
_LRU_KEYS_MAX = int(os.getenv('LRU_KEYS_MAX', '200000'))
_lru_citation_keys = LRUSet(_LRU_KEYS_MAX)
_lru_normalized_citation_keys = LRUSet(_LRU_KEYS_MAX)
_lru_ncwr_keys = LRUSet(_LRU_KEYS_MAX)
_lru_wiki_template_keys = LRUSet(_LRU_KEYS_MAX)
_lru_template_data_keys = LRUSet(_LRU_KEYS_MAX)

# Container label -> id cache (bounded, process-local)
_LRU_CONTAINER_MAX = int(os.getenv('LRU_CONTAINER_MAX', '1000'))
_lru_container_id_by_label = LRUMap(_LRU_CONTAINER_MAX)


def _is_retryable_db_disconnect(exc: BaseException) -> bool:
    """Return True for transient disconnects like 'SSL SYSCALL error: Socket is not connected'."""
    if isinstance(exc, (OperationalError, DBAPIError)):
        if getattr(exc, 'connection_invalidated', False):
            return True
        msg = str(getattr(exc, 'orig', exc))
        retryable_substrings = (
            'SSL SYSCALL error: Socket is not connected',
            'could not receive data from server',
            'server closed the connection unexpectedly',
            'connection not open',
            'terminating connection due to administrator command',
        )
        return any(s in msg for s in retryable_substrings)
    return False


Engine = create_engine(
    DB,
    # Constrain each worker to a single connection to avoid connection storms
    pool_size=1,
    max_overflow=0,
    pool_pre_ping=True,
    pool_recycle=_POOL_RECYCLE_S,
    # Prevent SQLAlchemy from printing huge bound-parameter payloads when a statement fails.
    # (Bulk inserts can include very large parameter lists.)
    hide_parameters=True,
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

# Defaults (overridable via CLI)
BATCH_SIZE = 1000           # revisions per batch from parsers
PARSE_PROCS = max(1, multiprocessing.cpu_count() - 1)
WRITE_PROCS = 1             # writers should be small to protect DB
QUEUE_MAX_BATCHES = 32      # backpressure capacity (batches)
METRICS_INTERVAL = 120.0    # seconds
TUNE_DB = False             # enable per-transaction load-time tuning


@dataclass
class Metrics:
    # producer side
    revisions_read: int = 0
    batches_enqueued: int = 0
    # consumer side
    batches_dequeued: int = 0
    revisions_committed: int = 0
    per_table_rows: Dict[str, int] = field(default_factory=dict)
    # housekeeping
    start_ts: float = field(default_factory=time.time)
    last_print_ts: float = field(default_factory=time.time)
    last_revisions_read: int = 0
    last_revisions_committed: int = 0

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

def get_container_id_cached(session, domain_label: str) -> int:
    cached = _lru_container_id_by_label.get(domain_label)
    if cached is not None:
        return cached

    # Upsert once and fetch id; no commit here, caller manages transaction
    Container.upsert(session, label=domain_label)
    container_id = session.execute(
        sa_select(Container.id).where(Container.label == domain_label)
    ).scalar_one()
    _lru_container_id_by_label.set(domain_label, container_id)
    return container_id

def process_revisions(revisions, domain="en.wikipedia.org", tune_db: bool = False):
    session = CreateSession()
    try:
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
                cur_url = f"https://{domain}/w/index.php?curid={page_id}"
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
                length = ref.get('length')
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
                if citation_key not in seen_citations and not _lru_citation_keys.add(citation_key):
                    seen_citations.add(citation_key)
                    citations.append({
                        "record_sha1": record_sha1,
                        "reference_raw_sha1": reference_raw_sha1,
                        "offset_start": offset_start,
                        "length": length,
                        "reference_type": reference_type,
                        "reference_normalized_sha1": reference_normalized_sha1,
                        "reference_name": reference_name,
                        "wiki_article_id": page_id
                    })

                if record_sha1 not in seen_normalized and not _lru_normalized_citation_keys.add(record_sha1):
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
                    # NormalizedCitationWebResource unique key becomes (reference_normalized_sha1, web_resource_id)
                    # but we only know web_resource_id after URL resolution; keep as (sha1, url) for now.
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
                        tpl_key = (domain, normalized_tpl_name)
                        if not _lru_wiki_template_keys.add(tpl_key):
                            templates_needed.add(tpl_key)

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
        # Only the primary wiki domain should be tied to this container; external domains default to NULL.
        if domains_needed:
            Domain.bulk_upsert(
                session,
                [
                    {
                        'value': d,
                        'for_container': (container_id if d == domain else None),
                    }
                    for d in domains_needed
                ]
            )
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
                ncwr_key = (rsha1, wr_id)
                if not _lru_ncwr_keys.add(ncwr_key):
                    ncwr_rows.append({'reference_normalized_sha1': rsha1, 'web_resource_id': wr_id})

        if ncwr_rows:
            NormalizedCitationWebResource.bulk_upsert(session, ncwr_rows)

        # Templates: bulk upsert and then bulk upsert params
        wiki_template_rows = []
        for dom_label, name in templates_needed:
            dom_id = domain_id_by_value.get(dom_label)
            if dom_id is not None:
                # WikiTemplate unique key is (domain_id, name)
                tpl_key = (dom_id, name)
                if not _lru_wiki_template_keys.add(tpl_key):
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
            template_key_to_id = {(d, n): i for d, n, i in rows}

        # Build TemplateData rows
        template_data_rows = []
        for dom_label, name, rsha1, off, key, val in templ_params_pending:
            dom_id = domain_id_by_value.get(dom_label)
            if dom_id is None:
                continue
            tpl_id = template_key_to_id.get((dom_id, name))
            if tpl_id is None:
                continue
            # TemplateData unique key is (wiki_template_id, reference_normalized_sha1, offset_start, parameter_key)
            td_key = (tpl_id, rsha1, off, key)
            if not _lru_template_data_keys.add(td_key):
                template_data_rows.append({
                    'wiki_template_id': tpl_id,
                    'reference_normalized_sha1': rsha1,
                    'offset_start': off,
                    'parameter_key': key,
                    'parameter_value': val,
                })

        if template_data_rows:
            TemplateData.bulk_upsert(session, template_data_rows)

        if tune_db:
            # Per-transaction DB tuning to improve bulk ingest latency
            # Safe knobs for bulk load workers; they apply only within this transaction
            session.execute(text("SET LOCAL synchronous_commit=off"))
            session.execute(text("SET LOCAL lock_timeout='5s'"))
            session.execute(text("SET LOCAL statement_timeout='0'"))
            session.execute(text("SET LOCAL application_name='wikirefs-writer'"))

        if citations:
            stmt_citations = insert(Citation).values(citations).on_conflict_do_update(
                index_elements=['record_sha1', 'reference_raw_sha1'],
                set_={
                    "offset_start": insert(Citation).excluded.offset_start,
                    "length": insert(Citation).excluded.length,
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

        # Commit transaction
        session.commit()

        # Return metrics for monitoring
        return {
            'revisions_committed': len(revisions_rows),
            'per_table_rows': {
                'citations': len(citations),
                'normalized_citations': len(normalized_citations),
                'citation_histories': len(citation_histories),
                'revisions': len(revisions_rows),
                'web_resources': len(wr_rows) if 'wr_rows' in locals() else 0,
                'ncwr': len(ncwr_rows) if 'ncwr_rows' in locals() else 0,
                'wiki_templates': len(wiki_template_rows) if 'wiki_template_rows' in locals() else 0,
                'template_data': len(template_data_rows) if 'template_data_rows' in locals() else 0,
                'domains': len(domains_needed) if 'domains_needed' in locals() else 0,
            }
        }
    except Exception:
        try:
            session.rollback()
        except Exception:
            pass
        raise
    finally:
        session.close()

def parser_worker(filelist: List[str], batch_size: int, out_q: Queue, stop_event: Event, metrics_q: Queue):
    m = {'revisions_read': 0, 'batches_enqueued': 0}
    try:
        for filename in filelist:
            if stop_event.is_set():
                break
            if not filename.endswith('.mwrev.zst'):
                continue
            batch = []
            for revision in get_revisions_from_mwrev_zst(filename):
                if stop_event.is_set():
                    break
                batch.append(revision)
                m['revisions_read'] += 1
                if len(batch) >= batch_size:
                    out_q.put(batch)
                    m['batches_enqueued'] += 1
                    batch = []
            if batch:
                out_q.put(batch)
                m['batches_enqueued'] += 1
    finally:
        # signal completion for this parser
        metrics_q.put(('parser_done', m))


def writer_worker(domain: str, in_q: Queue, stop_event: Event, tune_db: bool, metrics_q: Queue):
    # Consume until stop_event and queue drained with sentinel handling
    while True:
        if stop_event.is_set() and in_q.empty():
            break
        try:
            batch = in_q.get(timeout=0.5)
        except Exception:
            continue
        attempt = 0
        while True:
            attempt += 1
            try:
                result = process_revisions(batch, domain=domain, tune_db=tune_db)
                metrics_q.put(('writer_batch', result))
                break
            except Exception as exc:
                if not _is_retryable_db_disconnect(exc) or attempt >= _RETRY_ATTEMPTS:
                    raise
                # Drop any pooled/stale connections before retrying.
                try:
                    Engine.dispose()
                except Exception:
                    pass
                time.sleep(_RETRY_BASE_DELAY_S * (2 ** (attempt - 1)))


def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


def run_pipeline(files: List[str], domain: str, parse_procs: int, write_procs: int, batch_size: int, queue_max: int, tune_db: bool, metrics_interval: float):
    # Bounded queue enforces backpressure from writers to parsers
    q: Queue = multiprocessing.Queue(maxsize=queue_max)
    metrics_q: Queue = multiprocessing.Queue()
    stop_event = multiprocessing.Event()

    # Assign files to parsers (simple round-robin chunks)
    files_per_parser = max(1, (len(files) + parse_procs - 1) // parse_procs)
    parser_files = list(chunk(files, files_per_parser))

    parsers: List[Process] = []
    for pf in parser_files[:parse_procs]:
        p = multiprocessing.Process(target=parser_worker, args=(pf, batch_size, q, stop_event, metrics_q), daemon=True)
        p.start()
        parsers.append(p)

    writers: List[Process] = []
    for _ in range(write_procs):
        w = multiprocessing.Process(target=writer_worker, args=(domain, q, stop_event, tune_db, metrics_q), daemon=True)
        w.start()
        writers.append(w)

    # Metrics aggregator/loop
    total_parsers = len(parsers)
    parsers_done = 0
    agg = Metrics()

    def _format_duration(seconds: float) -> str:
        total = int(max(0, seconds))
        days, rem = divmod(total, 24 * 3600)
        hours, rem = divmod(rem, 3600)
        minutes, secs = divmod(rem, 60)

        parts = []
        if days:
            parts.append(f"{days}d")
        if hours or parts:
            parts.append(f"{hours}h")
        if minutes or parts:
            parts.append(f"{minutes}m")
        parts.append(f"{secs}s")
        return "".join(parts)

    def print_metrics(prefix: str = "[metrics]"):
        now = time.time()
        elapsed = now - agg.start_ts
        dt = max(1e-6, now - agg.last_print_ts)
        agg.last_revisions_read = agg.revisions_read
        agg.last_revisions_committed = agg.revisions_committed
        try:
            qsize = q.qsize()
        except NotImplementedError:
            qsize = -1
        per_table = ", ".join(f"{k}={v}" for k, v in sorted(agg.per_table_rows.items()))
        ts = time.strftime('%Y-%m-%d %H:%M:%S')
        print(
            f"{ts} {prefix} elapsed={_format_duration(elapsed)} | queue={qsize}/{queue_max} | "
            f"parsers_done={parsers_done}/{total_parsers} | tables: {per_table}",
            flush=True,
        )
        agg.last_print_ts = now

    try:
        last_print = time.time()
        while True:
            # Drain metrics queue with a small timeout
            try:
                kind, payload = metrics_q.get(timeout=0.5)
            except Exception:
                kind = None
                payload = None

            if kind == 'parser_done':
                parsers_done += 1
                agg.revisions_read += payload.get('revisions_read', 0)
                agg.batches_enqueued += payload.get('batches_enqueued', 0)
            elif kind == 'writer_batch':
                agg.batches_dequeued += 1
                agg.revisions_committed += int(payload.get('revisions_committed') or 0)
                for k, v in (payload.get('per_table_rows') or {}).items():
                    agg.per_table_rows[k] = agg.per_table_rows.get(k, 0) + int(v or 0)

            # Periodic printing
            now = time.time()
            if now - last_print >= metrics_interval:
                print_metrics()
                last_print = now

            # Exit condition: all parsers finished AND queue drained
            if parsers_done >= total_parsers:
                try:
                    empty = q.empty()
                except NotImplementedError:
                    # On some platforms q.empty is unreliable; rely on writers idle time after stop
                    empty = False
                if empty:
                    break

        # Signal writers to finish after queue drain
        stop_event.set()

    finally:
        # Join all children
        for p in parsers:
            p.join(timeout=5)
        for w in writers:
            w.join(timeout=10)
        print_metrics(prefix='[final]')

def parse_args(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description='Parse and ingest wiki references with staged pipeline')
    ap.add_argument('path', nargs='?', default='./sources/', help='File or directory containing .mwrev.zst')
    ap.add_argument('--domain', default='en.wikipedia.org', help='Wiki domain for curid URLs (default: en.wikipedia.org)')
    ap.add_argument('--batch-size', type=int, default=BATCH_SIZE, help=f'Revisions per parsed batch (default: {BATCH_SIZE})')
    ap.add_argument('--parse-procs', type=int, default=PARSE_PROCS, help=f'Parser processes (default: {PARSE_PROCS})')
    ap.add_argument('--write-procs', type=int, default=WRITE_PROCS, help=f'Writer processes (default: {WRITE_PROCS})')
    ap.add_argument('--queue-max', type=int, default=QUEUE_MAX_BATCHES, help=f'Max queued batches (default: {QUEUE_MAX_BATCHES})')
    ap.add_argument('--metrics-interval', type=float, default=METRICS_INTERVAL, help=f'Seconds between metrics prints (default: {METRICS_INTERVAL})')
    ap.add_argument('--tune-db', action='store_true', default=False, help='Enable per-transaction DB load-time tuning')
    return ap.parse_args(argv)


if __name__ == '__main__':
    args = parse_args()
    # Build file list
    if os.path.isdir(args.path):
        filenames = [os.path.join(args.path, f) for f in os.listdir(args.path) if f.endswith('.mwrev.zst')]
        filenames.sort()
    else:
        filenames = [args.path]

    # Important: keep writer count small to protect Postgres. Prefer scaling parsers.
    run_pipeline(
        files=filenames,
        domain=args.domain,
        parse_procs=max(1, int(args.parse_procs)),
        write_procs=max(1, int(args.write_procs)),
        batch_size=max(1, int(args.batch_size)),
        queue_max=max(1, int(args.queue_max)),
        tune_db=bool(args.tune_db),
        metrics_interval=float(args.metrics_interval),
    )
