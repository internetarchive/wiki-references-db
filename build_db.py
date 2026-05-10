import io
import bz2
import json
import os
import sys
import time
import signal
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import multiprocessing
from multiprocessing import Queue, Event, Process
from xml.etree import ElementTree
import zstandard as zstd
from sqlalchemy.exc import DBAPIError, OperationalError
from refs_extractor.article import extract_references
from refs_extractor.syntax import normalize_wikitext, get_sha1


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
            'remaining connection slots are reserved',
        )
        return any(s in msg for s in retryable_substrings)
    return False

# ---------------------------------------------------------------------------
# StagingWriter — writes JSONL compressed with zstandard to per-source files
# ---------------------------------------------------------------------------
class StagingWriter:
    """Writes rows as JSONL compressed with zstandard (.jsonl.zst) to a staging directory.

    Each source file gets its own set of output files (keyed by source stem + table name).
    """

    def __init__(self, staging_dir: str):
        self._staging_dir = staging_dir
        os.makedirs(staging_dir, exist_ok=True)
        self._compressors: Dict[str, Any] = {}  # (source_stem, table_name) -> (cctx, fh, writer)

    def _get_writer(self, table_name: str, source_stem: str):
        key = (source_stem, table_name)
        if key not in self._compressors:
            path = os.path.join(self._staging_dir, f"{source_stem}-{table_name}.jsonl.zst")
            cctx = zstd.ZstdCompressor(level=3)
            fh = open(path, 'wb')
            writer = cctx.stream_writer(fh)
            self._compressors[key] = (cctx, fh, writer)
        return self._compressors[key][2]

    def write_rows(self, table_name: str, rows: list, source_stem: str = 'unknown'):
        if not rows:
            return
        w = self._get_writer(table_name, source_stem)
        for row in rows:
            line = json.dumps(row, default=str) + '\n'
            w.write(line.encode('utf-8'))

    def close(self):
        for key, (cctx, fh, writer) in self._compressors.items():
            writer.close()
            fh.close()
        self._compressors.clear()


load_dotenv()

# Defaults (overridable via CLI)
BATCH_SIZE = 1000           # revisions per batch from parsers
PARSE_PROCS = max(1, multiprocessing.cpu_count() - 1)
WRITE_PROCS = 1             # staging writers
QUEUE_MAX_BATCHES = 32      # backpressure capacity (batches)
METRICS_INTERVAL = 120.0    # seconds


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

def _normalize_template_name(raw: str) -> str:
    """Normalize a wiki template name: underscores to spaces, capitalize first letter."""
    if not raw:
        return raw
    norm = raw.replace('_', ' ').strip()
    if len(norm) == 0:
        return norm
    return norm[0].upper() + norm[1:]


def process_revisions(revisions, staging: StagingWriter, domain="en.wikipedia.org", source_stem: str = 'unknown'):
    """Derive rows from revisions and write them to staging JSONL.zst files.

    No database connection is used. No in-memory deduplication is performed.
    """
    citations, citation_histories, normalized_citations = [], [], []
    revisions_rows = []

    domains_rows = []  # {'value': ..., 'for_container': ...}
    containers_rows = []  # {'label': ...}
    documents_rows = []  # {'language_code': ..., 'has_container_label': ..., 'page_id': ...}
    web_resources_rows = []
    ncwr_rows = []  # (reference_normalized_sha1, url)
    wiki_template_rows = []  # {'domain_label': ..., 'name': ...}
    template_data_rows = []

    # Emit container for this domain
    containers_rows.append({'label': domain})
    domains_rows.append({'value': domain, 'for_container_label': domain})

    for data in revisions:

        language_code = domain.split('.')[0]
        page_id = data["page_id"]
        namespace_id = data.get("namespace_id")
        revision_id = data["revision_id"]
        revision_timestamp = data["revision_timestamp"].replace("T", " ").replace("Z", "")

        cur_url = f"https://{domain}/w/index.php?curid={page_id}"
        documents_rows.append({
            'language_code': language_code,
            'has_container_label': domain,
            'page_id': page_id,
        })
        web_resources_rows.append({
            'url': cur_url,
            'domain_label': domain,
            'numeric_page_id': page_id,
            'numeric_namespace_id': namespace_id,
            'page_id': page_id,
        })

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
            reference_name = ref.get('reference_name')

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

            normalized_citations.append({
                "record_sha1": record_sha1,
                "reference_normalized_sha1": reference_normalized_sha1,
                "reference_normalized": reference_normalized,
                "appears_on_page_id": page_id,
                "appears_on_domain": domain,
            })

            citation_histories.append({
                "record_sha1": record_sha1,
                "reference_raw_sha1": reference_raw_sha1,
                "reference_normalized_sha1": reference_normalized_sha1,
                "revision_id": revision_id,
            })

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
                    domains_rows.append({'value': netloc, 'for_container_label': None})
                web_resources_rows.append({
                    'url': url,
                    'domain_label': netloc,
                })
                ncwr_rows.append({
                    'reference_normalized_sha1': reference_normalized_sha1,
                    'url': url,
                })

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
                    normalized_tpl_name = _normalize_template_name(tpl_name_raw)
                    wiki_template_rows.append({
                        'domain_label': domain,
                        'name': normalized_tpl_name,
                    })

                    marker = "{{" + normalized_tpl_name
                    tpl_offset = find_nth(reference_normalized, marker, idx)
                    if tpl_offset is None or tpl_offset < 0:
                        tpl_offset = reference_normalized.find(tpl_full_text)
                        if tpl_offset < 0:
                            tpl_offset = offset_start if isinstance(offset_start, int) else 0

                    for p in params:
                        key = (p or {}).get('key')
                        val = (p or {}).get('value')
                        if not key:
                            continue
                        template_data_rows.append({
                            'domain_label': domain,
                            'template_name': normalized_tpl_name,
                            'reference_normalized_sha1': reference_normalized_sha1,
                            'offset_start': tpl_offset,
                            'parameter_key': key,
                            'parameter_value': val,
                        })

    # Write all accumulated rows to staging files
    staging.write_rows('containers', containers_rows, source_stem=source_stem)
    staging.write_rows('domains', domains_rows, source_stem=source_stem)
    staging.write_rows('documents', documents_rows, source_stem=source_stem)
    staging.write_rows('web_resources', web_resources_rows, source_stem=source_stem)
    staging.write_rows('citations', citations, source_stem=source_stem)
    staging.write_rows('normalized_citations', normalized_citations, source_stem=source_stem)
    staging.write_rows('citation_histories', citation_histories, source_stem=source_stem)
    staging.write_rows('revisions', revisions_rows, source_stem=source_stem)
    staging.write_rows('ncwr', ncwr_rows, source_stem=source_stem)
    staging.write_rows('wiki_templates', wiki_template_rows, source_stem=source_stem)
    staging.write_rows('template_data', template_data_rows, source_stem=source_stem)

    return {
        'revisions_committed': len(revisions_rows),
        'per_table_rows': {
            'citations': len(citations),
            'normalized_citations': len(normalized_citations),
            'citation_histories': len(citation_histories),
            'revisions': len(revisions_rows),
            'web_resources': len(web_resources_rows),
            'ncwr': len(ncwr_rows),
            'wiki_templates': len(wiki_template_rows),
            'template_data': len(template_data_rows),
            'domains': len(domains_rows),
        }
    }

def parser_worker(filelist: List[str], batch_size: int, out_q: Queue, stop_event: Event, metrics_q: Queue):
    m = {'revisions_read': 0, 'batches_enqueued': 0}
    try:
        for filename in filelist:
            if stop_event.is_set():
                break
            if not filename.endswith('.mwrev.zst'):
                continue
            # Derive source stem: strip .mwrev.zst (or any extension) from basename
            source_stem = os.path.basename(filename)
            if source_stem.endswith('.mwrev.zst'):
                source_stem = source_stem[:-len('.mwrev.zst')]
            batch = []
            for revision in get_revisions_from_mwrev_zst(filename):
                if stop_event.is_set():
                    break
                batch.append(revision)
                m['revisions_read'] += 1
                if len(batch) >= batch_size:
                    out_q.put((source_stem, batch))
                    m['batches_enqueued'] += 1
                    batch = []
            if batch:
                out_q.put((source_stem, batch))
                m['batches_enqueued'] += 1
    finally:
        # signal completion for this parser
        metrics_q.put(('parser_done', m))


def staging_worker(domain: str, staging_dir: str, in_q: Queue, stop_event: Event, metrics_q: Queue):
    """Consume batches from the queue and write derived rows to staging files."""
    staging = StagingWriter(staging_dir)
    try:
        while True:
            if stop_event.is_set() and in_q.empty():
                break
            try:
                source_stem, batch = in_q.get(timeout=0.5)
            except Exception:
                continue
            result = process_revisions(batch, staging=staging, domain=domain, source_stem=source_stem)
            metrics_q.put(('writer_batch', result))
    finally:
        staging.close()


def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


def run_pipeline(files: List[str], domain: str, parse_procs: int, write_procs: int, batch_size: int, queue_max: int, staging_dir: str, metrics_interval: float, log_prefix: str = ""):
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
        w = multiprocessing.Process(target=staging_worker, args=(domain, staging_dir, q, stop_event, metrics_q), daemon=True)
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
        lp = f"{log_prefix} " if log_prefix else ""
        print(
            f"{ts} {lp}{prefix} elapsed={_format_duration(elapsed)} | queue={qsize}/{queue_max} | "
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
    ap = argparse.ArgumentParser(description='Parse wiki revisions and stage derived rows as JSONL.zst files')
    ap.add_argument('path', nargs='?', default='./sources/', help='File or directory containing .mwrev.zst')
    ap.add_argument('-o', '--staging-dir', default=os.environ.get('STAGING_DIR', './staging'),
                    help='Directory to write staged JSONL.zst files (default: STAGING_DIR env or ./staging)')
    ap.add_argument('--domain', default='en.wikipedia.org', help='Wiki domain for curid URLs (default: en.wikipedia.org)')
    ap.add_argument('--batch-size', type=int, default=BATCH_SIZE, help=f'Revisions per parsed batch (default: {BATCH_SIZE})')
    ap.add_argument('--parse-procs', type=int, default=PARSE_PROCS, help=f'Parser processes (default: {PARSE_PROCS})')
    ap.add_argument('--write-procs', type=int, default=WRITE_PROCS, help=f'Staging writer processes (default: {WRITE_PROCS})')
    ap.add_argument('--queue-max', type=int, default=QUEUE_MAX_BATCHES, help=f'Max queued batches (default: {QUEUE_MAX_BATCHES})')
    ap.add_argument('--metrics-interval', type=float, default=METRICS_INTERVAL, help=f'Seconds between metrics prints (default: {METRICS_INTERVAL})')
    ap.add_argument('--log-prefix', default='', help='Prefix for log lines (e.g. [1/939])')
    return ap.parse_args(argv)


if __name__ == '__main__':
    args = parse_args()
    # Build file list
    if os.path.isdir(args.path):
        filenames = [os.path.join(args.path, f) for f in os.listdir(args.path) if f.endswith('.mwrev.zst')]
        filenames.sort()
    else:
        filenames = [args.path]

    run_pipeline(
        files=filenames,
        domain=args.domain,
        parse_procs=max(1, int(args.parse_procs)),
        write_procs=max(1, int(args.write_procs)),
        batch_size=max(1, int(args.batch_size)),
        queue_max=max(1, int(args.queue_max)),
        staging_dir=args.staging_dir,
        metrics_interval=float(args.metrics_interval),
        log_prefix=args.log_prefix,
    )
