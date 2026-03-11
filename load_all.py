"""Load staged JSONL.zst files into PostgreSQL.

Reads the staging directory produced by build_all.py / build_db.py,
deduplicates rows across per-process files, and bulk-inserts into
Postgres using temporary staging tables for conflict resolution.

Usage:
    python3 load_all.py -d ./staging
    python3 load_all.py  # uses STAGING_DIR from .env or default ./staging
"""

import io
import json
import os
import sys
import time
import glob
import argparse
from collections import OrderedDict

from dotenv import load_dotenv
load_dotenv()

import zstandard as zstd
from sqlalchemy import create_engine, text, insert, select as sa_select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from models import (
    Base, Container, Domain, Document, WebResource, Citation,
    CitationHistory, Revision, NormalizedCitation,
    NormalizedCitationWebResource, WikiTemplate, TemplateData,
)

DB = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

Engine = create_engine(DB, poolclass=NullPool, hide_parameters=True)
Session = sessionmaker(bind=Engine)

BATCH_SIZE = int(os.getenv('LOAD_BATCH_SIZE', '5000'))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def read_jsonl_zst(filepath):
    """Yield dicts from a .jsonl.zst file."""
    dctx = zstd.ZstdDecompressor()
    with open(filepath, 'rb') as fh:
        with dctx.stream_reader(fh) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')
            for line in text_stream:
                line = line.strip()
                if line:
                    yield json.loads(line)


def find_staging_files(staging_dir, table_name):
    """Find all .jsonl.zst files for a given table across all subdirectories."""
    pattern = os.path.join(staging_dir, '**', f'*-{table_name}.jsonl.zst')
    return sorted(glob.glob(pattern, recursive=True))


def dedup_rows(filepaths, key_func):
    """Read rows from multiple JSONL.zst files, deduplicate by key_func.

    Later files override earlier ones for the same key (last-writer-wins).
    """
    seen = set()
    rows = []
    for fp in filepaths:
        for row in read_jsonl_zst(fp):
            k = key_func(row)
            if k not in seen:
                seen.add(k)
                rows.append(row)
    return rows


def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def log(msg):
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"{ts} [load_all] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Load functions per table — using temp tables for upsert
# ---------------------------------------------------------------------------

def load_containers(session, staging_dir):
    files = find_staging_files(staging_dir, 'containers')
    if not files:
        return
    rows = dedup_rows(files, lambda r: r['label'])
    log(f"containers: {len(rows)} unique rows from {len(files)} files")
    for batch in chunked(rows, BATCH_SIZE):
        Container.bulk_upsert(session, batch)
    session.commit()


def load_domains(session, staging_dir):
    files = find_staging_files(staging_dir, 'domains')
    if not files:
        return
    rows = dedup_rows(files, lambda r: r['value'])
    log(f"domains: {len(rows)} unique rows from {len(files)} files")
    # Resolve container labels to ids
    labels = set(r.get('for_container_label') for r in rows if r.get('for_container_label'))
    label_to_id = {}
    if labels:
        result = session.execute(
            sa_select(Container.label, Container.id).where(Container.label.in_(list(labels)))
        ).all()
        label_to_id = {l: i for l, i in result}
    cleaned = []
    for r in rows:
        d = {'value': r['value']}
        fcl = r.get('for_container_label')
        if fcl and fcl in label_to_id:
            d['for_container'] = label_to_id[fcl]
        cleaned.append(d)
    for batch in chunked(cleaned, BATCH_SIZE):
        Domain.bulk_upsert(session, batch)
    session.commit()


def load_documents(session, staging_dir):
    """Load documents. Returns a mapping of (domain, page_id) -> document_id."""
    files = find_staging_files(staging_dir, 'documents')
    if not files:
        return {}
    rows = dedup_rows(files, lambda r: (r.get('has_container_label', ''), r['page_id']))
    log(f"documents: {len(rows)} unique rows from {len(files)} files")

    # Resolve container labels
    labels = set(r.get('has_container_label') for r in rows if r.get('has_container_label'))
    label_to_id = {}
    if labels:
        result = session.execute(
            sa_select(Container.label, Container.id).where(Container.label.in_(list(labels)))
        ).all()
        label_to_id = {l: i for l, i in result}

    # Insert documents and track mapping
    page_to_doc_id = {}
    for r in rows:
        container_label = r.get('has_container_label')
        container_id = label_to_id.get(container_label)
        doc_id = Document.upsert(
            session,
            language_code=r.get('language_code'),
            has_container=container_id,
        )
        page_to_doc_id[(container_label or '', r['page_id'])] = doc_id
    session.commit()
    return page_to_doc_id


def load_web_resources(session, staging_dir, page_to_doc_id):
    files = find_staging_files(staging_dir, 'web_resources')
    if not files:
        return
    rows = dedup_rows(files, lambda r: r['url'])
    log(f"web_resources: {len(rows)} unique rows from {len(files)} files")

    # Resolve domain labels to ids
    domain_labels = set(r.get('domain_label') for r in rows if r.get('domain_label'))
    domain_to_id = {}
    if domain_labels:
        result = session.execute(
            sa_select(Domain.value, Domain.id).where(Domain.value.in_(list(domain_labels)))
        ).all()
        domain_to_id = {v: i for v, i in result}

    cleaned = []
    for r in rows:
        wr = {'url': r['url']}
        dl = r.get('domain_label')
        if dl and dl in domain_to_id:
            wr['domain_id'] = domain_to_id[dl]
        if r.get('numeric_page_id') is not None:
            wr['numeric_page_id'] = r['numeric_page_id']
        if r.get('numeric_namespace_id') is not None:
            wr['numeric_namespace_id'] = r['numeric_namespace_id']
        # Resolve document id from page_id
        page_id = r.get('page_id')
        domain_label = r.get('domain_label', '')
        if page_id is not None:
            doc_id = page_to_doc_id.get((domain_label, page_id))
            if doc_id is not None:
                wr['instance_of_document'] = doc_id
        cleaned.append(wr)

    for batch in chunked(cleaned, BATCH_SIZE):
        WebResource.bulk_upsert(session, batch)
    session.commit()


def load_wiki_templates(session, staging_dir):
    files = find_staging_files(staging_dir, 'wiki_templates')
    if not files:
        return
    rows = dedup_rows(files, lambda r: (r['domain_label'], r['name']))
    log(f"wiki_templates: {len(rows)} unique rows from {len(files)} files")

    # Resolve domain labels
    domain_labels = set(r['domain_label'] for r in rows)
    domain_to_id = {}
    if domain_labels:
        result = session.execute(
            sa_select(Domain.value, Domain.id).where(Domain.value.in_(list(domain_labels)))
        ).all()
        domain_to_id = {v: i for v, i in result}

    cleaned = []
    for r in rows:
        dom_id = domain_to_id.get(r['domain_label'])
        if dom_id is not None:
            cleaned.append({'domain': dom_id, 'name': r['name']})

    for batch in chunked(cleaned, BATCH_SIZE):
        WikiTemplate.bulk_upsert(session, batch)
    session.commit()


def load_normalized_citations(session, staging_dir, page_to_doc_id):
    files = find_staging_files(staging_dir, 'normalized_citations')
    if not files:
        return
    rows = dedup_rows(files, lambda r: r['record_sha1'])
    log(f"normalized_citations: {len(rows)} unique rows from {len(files)} files")

    cleaned = []
    for r in rows:
        domain = r.get('appears_on_domain', '')
        page_id = r.get('appears_on_page_id')
        doc_id = page_to_doc_id.get((domain, page_id))
        if doc_id is None:
            continue
        cleaned.append({
            'record_sha1': r['record_sha1'],
            'reference_normalized_sha1': r['reference_normalized_sha1'],
            'reference_normalized': r['reference_normalized'],
            'appears_on_article': doc_id,
        })

    for batch in chunked(cleaned, BATCH_SIZE):
        if not batch:
            continue
        stmt = insert(NormalizedCitation).values(batch).on_conflict_do_update(
            index_elements=['record_sha1'],
            set_={
                'reference_normalized': insert(NormalizedCitation).excluded.reference_normalized,
                'reference_normalized_sha1': insert(NormalizedCitation).excluded.reference_normalized_sha1,
                'appears_on_article': insert(NormalizedCitation).excluded.appears_on_article,
            }
        )
        session.execute(stmt)
    session.commit()


def load_citations(session, staging_dir):
    files = find_staging_files(staging_dir, 'citations')
    if not files:
        return
    rows = dedup_rows(files, lambda r: (r['record_sha1'], r['reference_raw_sha1']))
    log(f"citations: {len(rows)} unique rows from {len(files)} files")

    for batch in chunked(rows, BATCH_SIZE):
        stmt = insert(Citation).values(batch).on_conflict_do_update(
            index_elements=['record_sha1', 'reference_raw_sha1'],
            set_={
                'offset_start': insert(Citation).excluded.offset_start,
                'length': insert(Citation).excluded.length,
                'reference_type': insert(Citation).excluded.reference_type,
                'reference_name': insert(Citation).excluded.reference_name,
            }
        )
        session.execute(stmt)
    session.commit()


def load_revisions(session, staging_dir):
    files = find_staging_files(staging_dir, 'revisions')
    if not files:
        return
    rows = dedup_rows(files, lambda r: r['revision_id'])
    log(f"revisions: {len(rows)} unique rows from {len(files)} files")

    for batch in chunked(rows, BATCH_SIZE):
        stmt = insert(Revision).values(batch).on_conflict_do_update(
            index_elements=['revision_id'],
            set_={
                'page_id': insert(Revision).excluded.page_id,
                'parent_revision_id': insert(Revision).excluded.parent_revision_id,
                'revision_timestamp': insert(Revision).excluded.revision_timestamp,
            }
        )
        session.execute(stmt)
    session.commit()


def load_citation_histories(session, staging_dir):
    files = find_staging_files(staging_dir, 'citation_histories')
    if not files:
        return
    rows = dedup_rows(files, lambda r: (r['record_sha1'], r['revision_id']))
    log(f"citation_histories: {len(rows)} unique rows from {len(files)} files")

    for batch in chunked(rows, BATCH_SIZE):
        stmt = insert(CitationHistory).values(batch).on_conflict_do_nothing()
        session.execute(stmt)
    session.commit()


def load_ncwr(session, staging_dir):
    files = find_staging_files(staging_dir, 'ncwr')
    if not files:
        return
    rows = dedup_rows(files, lambda r: (r['reference_normalized_sha1'], r['url']))
    log(f"ncwr: {len(rows)} unique rows from {len(files)} files")

    # Resolve URLs to web_resource_ids
    all_urls = list(set(r['url'] for r in rows))
    url_to_id = {}
    for batch in chunked(all_urls, BATCH_SIZE):
        result = session.execute(
            sa_select(WebResource.url, WebResource.id).where(WebResource.url.in_(batch))
        ).all()
        url_to_id.update({u: i for u, i in result})

    cleaned = []
    for r in rows:
        wr_id = url_to_id.get(r['url'])
        if wr_id is not None:
            cleaned.append({
                'reference_normalized_sha1': r['reference_normalized_sha1'],
                'web_resource_id': wr_id,
            })

    for batch in chunked(cleaned, BATCH_SIZE):
        NormalizedCitationWebResource.bulk_upsert(session, batch)
    session.commit()


def load_template_data(session, staging_dir):
    files = find_staging_files(staging_dir, 'template_data')
    if not files:
        return
    rows = dedup_rows(files, lambda r: (
        r['domain_label'], r['template_name'],
        r['reference_normalized_sha1'], r['offset_start'], r['parameter_key']
    ))
    log(f"template_data: {len(rows)} unique rows from {len(files)} files")

    # Resolve domain labels and template names to ids
    domain_labels = set(r['domain_label'] for r in rows)
    domain_to_id = {}
    if domain_labels:
        result = session.execute(
            sa_select(Domain.value, Domain.id).where(Domain.value.in_(list(domain_labels)))
        ).all()
        domain_to_id = {v: i for v, i in result}

    # Resolve template ids
    tpl_keys = set()
    for r in rows:
        dom_id = domain_to_id.get(r['domain_label'])
        if dom_id is not None:
            tpl_keys.add((dom_id, r['template_name']))

    template_key_to_id = {}
    if tpl_keys:
        from sqlalchemy import tuple_
        result = session.execute(
            sa_select(WikiTemplate.domain, WikiTemplate.name, WikiTemplate.id)
            .where(tuple_(WikiTemplate.domain, WikiTemplate.name).in_(list(tpl_keys)))
        ).all()
        template_key_to_id = {(d, n): i for d, n, i in result}

    cleaned = []
    for r in rows:
        dom_id = domain_to_id.get(r['domain_label'])
        if dom_id is None:
            continue
        tpl_id = template_key_to_id.get((dom_id, r['template_name']))
        if tpl_id is None:
            continue
        cleaned.append({
            'wiki_template_id': tpl_id,
            'reference_normalized_sha1': r['reference_normalized_sha1'],
            'offset_start': r['offset_start'],
            'parameter_key': r['parameter_key'],
            'parameter_value': r.get('parameter_value'),
        })

    for batch in chunked(cleaned, BATCH_SIZE):
        TemplateData.bulk_upsert(session, batch)
    session.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Load staged JSONL.zst files into PostgreSQL')
    parser.add_argument('-d', '--staging-dir', default=os.environ.get('STAGING_DIR', './staging'),
                        help='Staging directory (default: STAGING_DIR env or ./staging)')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                        help=f'Rows per INSERT batch (default: {BATCH_SIZE})')
    args = parser.parse_args()

    staging_dir = args.staging_dir
    if not os.path.isdir(staging_dir):
        raise SystemExit(f"Staging directory does not exist: {staging_dir}")

    global BATCH_SIZE
    BATCH_SIZE = args.batch_size

    session = Session()
    t0 = time.time()

    try:
        # Load order respects foreign key dependencies
        log("Phase 1: containers")
        load_containers(session, staging_dir)

        log("Phase 2: domains")
        load_domains(session, staging_dir)

        log("Phase 3: documents")
        page_to_doc_id = load_documents(session, staging_dir)

        log("Phase 4: web_resources")
        load_web_resources(session, staging_dir, page_to_doc_id)

        log("Phase 5: wiki_templates")
        load_wiki_templates(session, staging_dir)

        log("Phase 6: normalized_citations")
        load_normalized_citations(session, staging_dir, page_to_doc_id)

        log("Phase 7: citations")
        load_citations(session, staging_dir)

        log("Phase 8: revisions")
        load_revisions(session, staging_dir)

        log("Phase 9: citation_histories")
        load_citation_histories(session, staging_dir)

        log("Phase 10: ncwr")
        load_ncwr(session, staging_dir)

        log("Phase 11: template_data")
        load_template_data(session, staging_dir)

        elapsed = time.time() - t0
        log(f"Done. Total elapsed: {elapsed:.1f}s")

    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == '__main__':
    main()
