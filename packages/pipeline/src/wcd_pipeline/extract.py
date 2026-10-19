"""Stage 1: extract references from one ``.mwrev.zst`` bundle.

Two kinds of output:

* **staging tables** (``wcd_schema.staging``) under ``--staging-dir``: the
  per-page / per-text rows that Stage 3 deduplicates and loads into Postgres
  (citation_instances with their deterministic ids, normalized_citations with
  ``citation_type``, cited web_resources, templates);
* **Parquet-tier datasets** (``wcd_schema.parquet``) under ``--parquet-dir``,
  written in their final partitioned layout and never loaded anywhere:
  ``citation_history`` (instance × revision), ``named_ref_uses`` (one row per
  self-closing ``<ref name="X" />``) and ``revision_signals`` (seven flags per
  revision).

``--domain`` is required: the bundle format carries no domain, and the
instance id, the partition path and every staged row depend on it. A re-run
of a bundle replaces the files it wrote before.
"""
from __future__ import annotations

import argparse
import io
import os
import time
from typing import Any, Dict, List
from urllib.parse import urlparse

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd
from citation_normalizer import get_sha1, normalize_wikitext
from refs_extractor.article import extract_all
from wcd_schema.ids import instance_id
from wcd_schema.staging import MAX_ROWS_PER_FILE, ROW_GROUP_SIZE, SCHEMAS
from wcd_semantic.signals import extract_flags

from . import tier
from .config import load_dotenv  # noqa: F401  (loads .env on import)


def log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} [extract] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Staging writer
# ---------------------------------------------------------------------------

class ParquetStagingWriter:
    """Writes staging rows as Parquet files (ZSTD) into a staging directory;
    one file series per (source_stem, table)."""

    def __init__(self, staging_dir: str, worker_id: str = '00'):
        self._staging_dir = staging_dir
        self._worker_id = worker_id
        os.makedirs(staging_dir, exist_ok=True)
        self._writers: Dict[Any, _TableWriter] = {}

    def write_rows(self, table_name: str, rows: list, source_stem: str = 'unknown'):
        if not rows:
            return
        key = (source_stem, table_name)
        if key not in self._writers:
            self._writers[key] = _TableWriter(self._staging_dir, source_stem, table_name, SCHEMAS[table_name])
        self._writers[key].write_rows(rows)

    def close(self):
        for w in self._writers.values():
            w.close()
        self._writers.clear()


class _TableWriter:
    def __init__(self, staging_dir: str, source_stem: str, table_name: str, schema: pa.Schema):
        self._staging_dir = staging_dir
        self._source_stem = source_stem
        self._table_name = table_name
        self._schema = schema
        self._names = [f.name for f in schema]
        self._buffer: List[dict] = []
        self._file_index = 0
        self._row_count = 0
        self._writer = None
        self._open_writer()

    def _file_path(self):
        return os.path.join(self._staging_dir, f"{self._source_stem}-{self._table_name}-{self._file_index:04d}.parquet")

    def _open_writer(self):
        self._writer = pq.ParquetWriter(self._file_path(), self._schema, compression='zstd')

    def _flush_buffer(self):
        if not self._buffer:
            return
        table = pa.Table.from_pylist([{n: row.get(n) for n in self._names} for row in self._buffer], schema=self._schema)
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
# Bundle reader
# ---------------------------------------------------------------------------

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
                    if current is not None:
                        current['revision_text'] = "\n".join(text_lines)
                        yield current
                    meta_line = raw_line[1:].strip()
                    meta = {}
                    for p in meta_line.split():
                        if '=' in p:
                            k, v = p.split('=', 1)
                            meta[k.strip()] = v.strip()
                    parent_rev_id = meta.get('parent_rev_id')
                    current = {
                        'page_id': int(meta['page_id']) if meta.get('page_id') else None,
                        'namespace_id': int(meta['ns']) if meta.get('ns') else None,
                        'revision_id': int(meta['rev_id']) if meta.get('rev_id') else None,
                        'parent_revision_id': int(parent_rev_id) if parent_rev_id else None,
                        'revision_timestamp': (meta.get('timestamp') or '').replace('T', ' ').replace('Z', ''),
                        'revision_text': '',
                    }
                    text_lines = []
                elif raw_line.startswith(' '):
                    text_lines.append(raw_line[1:].rstrip('\n'))
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


def _find_nth(haystack: str, needle: str, n: int) -> int:
    start = -1
    for _ in range(n):
        start = haystack.find(needle, start + 1)
        if start == -1:
            break
    return start


# ---------------------------------------------------------------------------
# Per-batch processing
# ---------------------------------------------------------------------------

class TierOutputs:
    """The three per-revision datasets of one shard."""

    def __init__(self, staging_dir: str, parquet_base: str, domain: str, stem: str):
        self.parquet_base, self.domain, self.stem = parquet_base, domain, stem
        self.history = tier.FlatWriter(staging_dir, "citation_history", stem)
        self.reuses = tier.FlatWriter(staging_dir, "named_ref_uses", stem)
        for ds in ("citation_history", "named_ref_uses", "revision_signals"):
            tier.remove_shard_files(parquet_base, ds, domain, stem)
        self.signals = tier.DomainFileWriter(parquet_base, "revision_signals", domain, stem)

    def finish(self) -> dict:
        """Close the flat files and re-partition them into the tier."""
        for w in (self.history, self.reuses, self.signals):
            w.close()
        con = duckdb.connect()
        counts = {
            "citation_history": tier.repartition(con, self.history.glob, self.parquet_base, "citation_history",
                                                 self.domain, self.stem),
            "named_ref_uses": tier.repartition(con, self.reuses.glob, self.parquet_base, "named_ref_uses",
                                               self.domain, self.stem, order_by="page_id, revision_id, offset_start"),
            "revision_signals": self.signals.rows,
        }
        con.close()
        self.history.remove()
        self.reuses.remove()
        return counts


def process_revisions(revisions, staging, outputs: TierOutputs, domain: str, source_stem: str = 'unknown'):
    """Derive rows from a batch of revisions: staging rows go to ``staging``,
    per-revision rows to ``outputs``. No deduplication is performed here."""
    citation_instances, normalized_citations = [], []
    history_rows, reuse_rows, signal_rows = [], [], []
    domains_rows, containers_rows, web_resources_rows = [], [], []
    ncwr_rows, wiki_template_rows, template_data_rows = [], [], []

    containers_rows.append({'label': domain})
    domains_rows.append({'value': domain, 'for_container_label': domain})

    for data in revisions:
        page_id = data["page_id"]
        revision_id = data["revision_id"]
        text = data["revision_text"]

        flags = extract_flags(text)
        signal_rows.append({"revision_id": revision_id, "page_id": page_id, **flags})

        extracted = extract_all(text, domain=domain)

        for reuse in extracted["reuses"]:
            reuse_rows.append({
                "page_id": page_id,
                "revision_id": revision_id,
                "ref_name": reuse["ref_name"],
                "offset_start": reuse["offset_start"],
            })

        for ref in extracted["references"]:
            reference_raw = ref.get('raw_reference')
            if not reference_raw or not reference_raw.strip():
                continue
            offset_start = ref.get('offset_start')
            reference_normalized = normalize_wikitext(reference_raw)
            normalized_sha1 = get_sha1(reference_normalized)
            raw_sha1 = get_sha1(reference_raw)
            inst_id = instance_id(domain, page_id, raw_sha1)

            citation_instances.append({
                "citation_instance_id": inst_id,
                "domain": domain,
                "page_id": page_id,
                "raw_sha1": raw_sha1,
                "normalized_sha1": normalized_sha1,
                "reference_type": ref.get('reference_type', 0),
                "reference_name": ref.get('reference_name'),
            })
            normalized_citations.append({
                "normalized_sha1": normalized_sha1,
                "reference_normalized": reference_normalized,
                "appears_on_page_id": page_id,
                "appears_on_domain": domain,
                "citation_type": ref.get("citation_type"),
            })
            history_rows.append({
                "citation_instance_id": inst_id,
                "revision_id": revision_id,
                "page_id": page_id,
            })

            for url in ref.get('urls') or []:
                if not url:
                    continue
                try:
                    netloc = urlparse(url).netloc
                except Exception:
                    netloc = None
                if netloc:
                    domains_rows.append({'value': netloc, 'for_container_label': None})
                web_resources_rows.append({'url': url, 'domain_label': netloc})
                ncwr_rows.append({'normalized_sha1': normalized_sha1, 'url': url})

            for idx, tpl in enumerate(ref.get('templates') or [], start=1):
                tpl_name_raw = (tpl or {}).get('template_name') or ''
                if not tpl_name_raw:
                    continue
                tpl_full_text = (tpl or {}).get('full_text') or ''
                normalized_tpl_name = _normalize_template_name(tpl_name_raw)
                wiki_template_rows.append({'domain_label': domain, 'name': normalized_tpl_name})

                tpl_offset = _find_nth(reference_normalized, "{{" + normalized_tpl_name, idx)
                if tpl_offset < 0:
                    tpl_offset = reference_normalized.find(tpl_full_text)
                    if tpl_offset < 0:
                        tpl_offset = offset_start if isinstance(offset_start, int) else 0
                for p in (tpl or {}).get('parameters') or []:
                    key = (p or {}).get('key')
                    if not key:
                        continue
                    template_data_rows.append({
                        'domain_label': domain,
                        'template_name': normalized_tpl_name,
                        'normalized_sha1': normalized_sha1,
                        'offset_start': tpl_offset,
                        'parameter_key': key,
                        'parameter_value': (p or {}).get('value'),
                    })

    staging.write_rows('containers', containers_rows, source_stem=source_stem)
    staging.write_rows('domains', domains_rows, source_stem=source_stem)
    staging.write_rows('web_resources', web_resources_rows, source_stem=source_stem)
    staging.write_rows('citation_instances', citation_instances, source_stem=source_stem)
    staging.write_rows('normalized_citations', normalized_citations, source_stem=source_stem)
    staging.write_rows('ncwr', ncwr_rows, source_stem=source_stem)
    staging.write_rows('wiki_templates', wiki_template_rows, source_stem=source_stem)
    staging.write_rows('template_data', template_data_rows, source_stem=source_stem)
    outputs.history.write(history_rows)
    outputs.reuses.write(reuse_rows)
    outputs.signals.write(signal_rows)

    return {
        'revisions': len(revisions),
        'per_table_rows': {
            'citation_instances': len(citation_instances),
            'normalized_citations': len(normalized_citations),
            'citation_history': len(history_rows),
            'named_ref_uses': len(reuse_rows),
            'web_resources': len(web_resources_rows),
            'ncwr': len(ncwr_rows),
            'wiki_templates': len(wiki_template_rows),
            'template_data': len(template_data_rows),
            'domains': len(domains_rows),
        },
    }


def source_stem_of(path: str) -> str:
    stem = os.path.basename(path)
    return stem[:-len('.mwrev.zst')] if stem.endswith('.mwrev.zst') else stem


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description='Stage 1: extract one .mwrev.zst bundle into staging Parquet and the Parquet tier')
    ap.add_argument('file', help='Single .mwrev.zst file to process')
    ap.add_argument('-o', '--staging-dir', required=True, help='Directory to write staged Parquet files')
    ap.add_argument('--domain', required=True, help='Site domain of the bundle (e.g. en.wikipedia.org)')
    ap.add_argument('--parquet-dir', default=os.environ.get('PARQUET_DIR'),
                    help='Parquet tier base directory for citation_history / named_ref_uses / revision_signals (default: PARQUET_DIR)')
    ap.add_argument('--batch-size', type=int, default=1000, help='Revisions per processing batch (default: 1000)')
    ap.add_argument('--worker-id', default='00', help='Worker ID for parallel runs (default: 00)')
    args = ap.parse_args(argv)
    if not args.parquet_dir:
        ap.error('--parquet-dir (or PARQUET_DIR) is required')
    return args


def main(argv=None):
    args = parse_args(argv)
    stem = source_stem_of(args.file)
    staging = ParquetStagingWriter(args.staging_dir, worker_id=args.worker_id)
    outputs = TierOutputs(args.staging_dir, args.parquet_dir, args.domain, stem)

    n_rev = 0
    batch = []
    for revision in get_revisions_from_mwrev_zst(args.file):
        batch.append(revision)
        if len(batch) >= args.batch_size:
            process_revisions(batch, staging, outputs, domain=args.domain, source_stem=stem)
            n_rev += len(batch)
            batch = []
    if batch:
        process_revisions(batch, staging, outputs, domain=args.domain, source_stem=stem)
        n_rev += len(batch)

    staging.close()
    counts = outputs.finish()
    log(f"{stem}: {n_rev} revisions; tier rows {counts}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
