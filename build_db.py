import io
import json
import os
import sys
import argparse
from typing import Dict, Any
from dotenv import load_dotenv
import zstandard as zstd
from refs_extractor.article import extract_references
from refs_extractor.syntax import normalize_wikitext, get_sha1

load_dotenv()

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


def process_revisions(revisions, staging: StagingWriter, domain="en.wikipedia.org", source_stem: str = 'unknown'):
    """Derive rows from revisions and write them to staging JSONL.zst files.

    No database connection is used. No in-memory deduplication is performed.
    """
    citations, citation_histories, normalized_citations = [], [], []
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


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description='Parse a single .mwrev.zst file and stage derived rows as JSONL.zst files')
    ap.add_argument('file', help='Single .mwrev.zst file to process')
    ap.add_argument('-o', '--staging-dir', required=True,
                    help='Directory to write staged JSONL.zst files')
    ap.add_argument('--domain', default='en.wikipedia.org',
                    help='Wiki domain for curid URLs (default: en.wikipedia.org)')
    ap.add_argument('--batch-size', type=int, default=1000,
                    help='Revisions per processing batch (default: 1000)')
    return ap.parse_args(argv)


if __name__ == '__main__':
    args = parse_args()

    source_stem = os.path.basename(args.file)
    if source_stem.endswith('.mwrev.zst'):
        source_stem = source_stem[:-len('.mwrev.zst')]

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
