import io
import os
import sys
import argparse
from typing import Dict, Any, List
from dotenv import load_dotenv
import zstandard as zstd
import pyarrow as pa
import pyarrow.parquet as pq
from refs_extractor.article import extract_references
from refs_extractor.syntax import normalize_wikitext, get_sha1

load_dotenv()

# ---------------------------------------------------------------------------
# Parquet schemas for each staging table
# ---------------------------------------------------------------------------
SCHEMAS = {
    'containers': pa.schema([
        ('label', pa.string()),
    ]),
    'domains': pa.schema([
        ('value', pa.string()),
        ('for_container_label', pa.string()),
    ]),
    'documents': pa.schema([
        ('language_code', pa.string()),
        ('has_container_label', pa.string()),
        ('page_id', pa.int32()),
    ]),
    'web_resources': pa.schema([
        ('url', pa.string()),
        ('domain_label', pa.string()),
        ('numeric_page_id', pa.int32()),
        ('numeric_namespace_id', pa.int32()),
        ('page_id', pa.int32()),
    ]),
    'citation_instances': pa.schema([
        ('page_id', pa.int32()),
        ('raw_sha1', pa.string()),
        ('normalized_sha1', pa.string()),
        ('reference_type', pa.int16()),
        ('reference_name', pa.string()),
    ]),
    'normalized_citations': pa.schema([
        ('normalized_sha1', pa.string()),
        ('reference_normalized', pa.string()),
        ('appears_on_page_id', pa.int32()),
        ('appears_on_domain', pa.string()),
    ]),
    'citation_histories': pa.schema([
        ('page_id', pa.int32()),
        ('raw_sha1', pa.string()),
        ('revision_id', pa.int64()),
    ]),
    'revisions': pa.schema([
        ('revision_id', pa.int64()),
        ('page_id', pa.int32()),
        ('parent_revision_id', pa.int64()),
        ('revision_timestamp', pa.string()),
    ]),
    'ncwr': pa.schema([
        ('normalized_sha1', pa.string()),
        ('url', pa.string()),
    ]),
    'wiki_templates': pa.schema([
        ('domain_label', pa.string()),
        ('name', pa.string()),
    ]),
    'template_data': pa.schema([
        ('domain_label', pa.string()),
        ('template_name', pa.string()),
        ('normalized_sha1', pa.string()),
        ('offset_start', pa.int32()),
        ('parameter_key', pa.string()),
        ('parameter_value', pa.string()),
    ]),
}

# ---------------------------------------------------------------------------
# ParquetStagingWriter — writes rows as Parquet with ZSTD compression
# ---------------------------------------------------------------------------
ROW_GROUP_SIZE = 10_000
MAX_ROWS_PER_FILE = 1_000_000


class ParquetStagingWriter:
    """Writes rows as Parquet files with ZSTD compression to a staging directory.

    Each source file gets its own set of output files (keyed by source stem + table name).
    Row groups are buffered in memory and flushed at ROW_GROUP_SIZE rows.
    Files are rotated at MAX_ROWS_PER_FILE rows.
    """

    def __init__(self, staging_dir: str, worker_id: str = '00'):
        self._staging_dir = staging_dir
        self._worker_id = worker_id
        os.makedirs(staging_dir, exist_ok=True)
        self._writers: Dict[str, Any] = {}  # (source_stem, table_name) -> _TableWriter

    def write_rows(self, table_name: str, rows: list, source_stem: str = 'unknown'):
        if not rows:
            return
        key = (source_stem, table_name)
        if key not in self._writers:
            self._writers[key] = _TableWriter(
                staging_dir=self._staging_dir,
                worker_id=self._worker_id,
                source_stem=source_stem,
                table_name=table_name,
                schema=SCHEMAS[table_name],
            )
        self._writers[key].write_rows(rows)

    def close(self):
        for w in self._writers.values():
            w.close()
        self._writers.clear()


class _TableWriter:
    """Manages Parquet file writing for a single (source_stem, table_name) pair."""

    def __init__(self, staging_dir: str, worker_id: str, source_stem: str,
                 table_name: str, schema: pa.Schema):
        self._staging_dir = staging_dir
        self._worker_id = worker_id
        self._source_stem = source_stem
        self._table_name = table_name
        self._schema = schema
        self._buffer: List[dict] = []
        self._file_index = 0
        self._row_count = 0
        self._writer = None
        self._fh = None
        self._open_writer()

    def _file_path(self):
        return os.path.join(
            self._staging_dir,
            f"{self._source_stem}-{self._table_name}-{self._file_index:04d}.parquet"
        )

    def _open_writer(self):
        path = self._file_path()
        self._writer = pq.ParquetWriter(path, self._schema, compression='zstd')

    def _flush_buffer(self):
        if not self._buffer:
            return
        # Ensure all rows have all schema fields (fill missing with None)
        field_names = [f.name for f in self._schema]
        cleaned = []
        for row in self._buffer:
            cleaned.append({f: row.get(f) for f in field_names})
        table = pa.Table.from_pylist(cleaned, schema=self._schema)
        self._writer.write_table(table)
        self._row_count += len(self._buffer)
        self._buffer.clear()
        if self._row_count >= MAX_ROWS_PER_FILE:
            self._writer.close()
            self._file_index += 1
            self._row_count = 0
            self._open_writer()

    def write_rows(self, rows: list):
        for row in rows:
            self._buffer.append(row)
            if len(self._buffer) >= ROW_GROUP_SIZE:
                self._flush_buffer()

    def close(self):
        if self._buffer:
            self._flush_buffer()
        if self._writer:
            self._writer.close()
            self._writer = None


# ---------------------------------------------------------------------------
# Legacy StagingWriter — kept for backward compatibility with JSONL+zstd
# ---------------------------------------------------------------------------
class StagingWriter:
    """Writes rows as JSONL compressed with zstandard (.jsonl.zst) to a staging directory.

    Each source file gets its own set of output files (keyed by source stem + table name).
    DEPRECATED: Use ParquetStagingWriter instead.
    """

    def __init__(self, staging_dir: str):
        import json
        self._json = json
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
            line = self._json.dumps(row, default=str) + '\n'
            w.write(line.encode('utf-8'))

    def close(self):
        for key, (cctx, fh, writer) in self._compressors.items():
            writer.close()
            fh.close()
        self._compressors.clear()


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


def process_revisions(revisions, staging, domain="en.wikipedia.org", source_stem: str = 'unknown'):
    """Derive rows from revisions and write them to staging files.

    No database connection is used. No in-memory deduplication is performed.
    Works with both ParquetStagingWriter and legacy StagingWriter.
    """
    citation_instances, citation_histories, normalized_citations = [], [], []
    revisions_rows = []

    domains_rows = []
    containers_rows = []
    documents_rows = []
    web_resources_rows = []
    ncwr_rows = []
    wiki_template_rows = []
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
            normalized_sha1 = get_sha1(reference_normalized)
            raw_sha1 = get_sha1(reference_raw)
            reference_name = ref.get('reference_name')

            citation_instances.append({
                "page_id": page_id,
                "raw_sha1": raw_sha1,
                "normalized_sha1": normalized_sha1,
                "reference_type": reference_type,
                "reference_name": reference_name,
            })

            normalized_citations.append({
                "normalized_sha1": normalized_sha1,
                "reference_normalized": reference_normalized,
                "appears_on_page_id": page_id,
                "appears_on_domain": domain,
            })

            citation_histories.append({
                "page_id": page_id,
                "raw_sha1": raw_sha1,
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
                    'normalized_sha1': normalized_sha1,
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
                            'normalized_sha1': normalized_sha1,
                            'offset_start': tpl_offset,
                            'parameter_key': key,
                            'parameter_value': val,
                        })

    # Write all accumulated rows to staging files
    staging.write_rows('containers', containers_rows, source_stem=source_stem)
    staging.write_rows('domains', domains_rows, source_stem=source_stem)
    staging.write_rows('documents', documents_rows, source_stem=source_stem)
    staging.write_rows('web_resources', web_resources_rows, source_stem=source_stem)
    staging.write_rows('citation_instances', citation_instances, source_stem=source_stem)
    staging.write_rows('normalized_citations', normalized_citations, source_stem=source_stem)
    staging.write_rows('citation_histories', citation_histories, source_stem=source_stem)
    staging.write_rows('revisions', revisions_rows, source_stem=source_stem)
    staging.write_rows('ncwr', ncwr_rows, source_stem=source_stem)
    staging.write_rows('wiki_templates', wiki_template_rows, source_stem=source_stem)
    staging.write_rows('template_data', template_data_rows, source_stem=source_stem)

    return {
        'revisions_committed': len(revisions_rows),
        'per_table_rows': {
            'citation_instances': len(citation_instances),
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


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description='Parse a single .mwrev.zst file and stage derived rows as Parquet files')
    ap.add_argument('file', help='Single .mwrev.zst file to process')
    ap.add_argument('-o', '--staging-dir', required=True,
                    help='Directory to write staged Parquet files')
    ap.add_argument('--domain', default='en.wikipedia.org',
                    help='Wiki domain for curid URLs (default: en.wikipedia.org)')
    ap.add_argument('--batch-size', type=int, default=1000,
                    help='Revisions per processing batch (default: 1000)')
    ap.add_argument('--format', choices=['parquet', 'jsonl'], default='parquet',
                    help='Output format (default: parquet)')
    ap.add_argument('--worker-id', default='00',
                    help='Worker ID for parallel runs (default: 00)')
    return ap.parse_args(argv)


if __name__ == '__main__':
    args = parse_args()

    source_stem = os.path.basename(args.file)
    if source_stem.endswith('.mwrev.zst'):
        source_stem = source_stem[:-len('.mwrev.zst')]

    if args.format == 'parquet':
        staging = ParquetStagingWriter(args.staging_dir, worker_id=args.worker_id)
    else:
        staging = StagingWriter(args.staging_dir)

    batch = []
    for revision in get_revisions_from_mwrev_zst(args.file):
        batch.append(revision)
        if len(batch) >= args.batch_size:
            process_revisions(batch, staging, domain=args.domain, source_stem=source_stem)
            batch = []
    if batch:
        process_revisions(batch, staging, domain=args.domain, source_stem=source_stem)

    staging.close()
