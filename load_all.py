"""Load pre-deduplicated staged Parquet files into PostgreSQL.

Reads the deduped/ subdirectory produced by dedup_parquet.py and
bulk-inserts into Postgres. ON CONFLICT upserts are kept as a
safety net for residual duplicates or re-runs.

Pipeline:
    build_db.py (Parquet)  →  dedup_parquet.py  →  load_all.py

Usage:
    python3 load_all.py -d ./staging
    python3 load_all.py  # uses STAGING_DIR from .env or default ./staging
    python3 load_all.py --tables containers domains documents
    python3 load_all.py --tables citation_histories  # load only citation_histories
"""

import hashlib
import itertools
import os
import sys
import time
import glob
import argparse
from collections import OrderedDict

from dotenv import load_dotenv
load_dotenv()

import duckdb
from sqlalchemy import create_engine, text, select as sa_select, func as sa_func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from models import (
    Base, Container, Domain, Document, WebResource, CitationInstance,
    CitationHistory, Revision, NormalizedCitation,
    NormalizedCitationWebResource, WikiTemplate, TemplateData,
)

_required_db_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASS']
_missing = [v for v in _required_db_vars if not os.getenv(v)]
if _missing:
    raise RuntimeError(
        f"Missing required environment variable(s): {', '.join(_missing)}. "
        f"Check your .env file (see example.env)."
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

def log(msg):
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"{ts} [load_all] {msg}", flush=True)


def find_deduped_parquet(staging_dir, table_name):
    """Find the deduped Parquet file for a given table."""
    deduped_dir = os.path.join(staging_dir, 'deduped')
    path = os.path.join(deduped_dir, f'{table_name}.parquet')
    if os.path.exists(path):
        return path
    return None


def read_parquet_batches(filepath, batch_size=BATCH_SIZE):
    """Yield batches of dicts from a Parquet file using DuckDB for efficiency."""
    if not filepath or not os.path.exists(filepath):
        return
    con = duckdb.connect()
    result = con.execute(f"SELECT * FROM '{filepath}'")
    columns = [desc[0] for desc in result.description]
    while True:
        chunk = result.fetchmany(batch_size)
        if not chunk:
            break
        batch = []
        for row in chunk:
            d = {}
            for i, col in enumerate(columns):
                val = row[i]
                # Convert DuckDB NoneType properly
                if val is not None:
                    d[col] = val
            batch.append(d)
        yield batch
    con.close()


def chunked_iterable(iterable, n):
    """Yield successive n-sized chunks from an iterable."""
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, n))
        if not chunk:
            break
        yield chunk


# ---------------------------------------------------------------------------
# Load functions per table
# ---------------------------------------------------------------------------

def load_containers(session, staging_dir):
    filepath = find_deduped_parquet(staging_dir, 'containers')
    if not filepath:
        return
    log(f"containers: loading from {filepath}")
    count = 0
    for batch in read_parquet_batches(filepath):
        Container.bulk_upsert(session, batch)
        count += len(batch)
    log(f"containers: {count} rows loaded")
    session.commit()


def load_domains(session, staging_dir):
    filepath = find_deduped_parquet(staging_dir, 'domains')
    if not filepath:
        return
    log(f"domains: loading from {filepath}")

    count = 0
    for batch in read_parquet_batches(filepath):
        # Resolve container labels to ids for this batch
        labels = set(r.get('for_container_label') for r in batch if r.get('for_container_label'))
        label_to_id = {}
        if labels:
            result = session.execute(
                sa_select(Container.label, Container.id).where(Container.label.in_(list(labels)))
            ).all()
            label_to_id = {l: i for l, i in result}

        cleaned = []
        for r in batch:
            d = {'value': r['value']}
            fcl = r.get('for_container_label')
            if fcl and fcl in label_to_id:
                d['for_container'] = label_to_id[fcl]
            cleaned.append(d)

        Domain.bulk_upsert(session, cleaned)
        count += len(cleaned)
    log(f"domains: {count} rows loaded")
    session.commit()


def load_documents(session, staging_dir):
    """Load documents. Returns a mapping of (domain, page_id) -> document_id."""
    filepath = find_deduped_parquet(staging_dir, 'documents')
    if not filepath:
        return {}
    log(f"documents: loading from {filepath}")

    page_to_doc_id = {}
    count = 0
    for batch in read_parquet_batches(filepath):
        # Resolve container labels
        labels = set(r.get('has_container_label') for r in batch if r.get('has_container_label'))
        label_to_id = {}
        if labels:
            result = session.execute(
                sa_select(Container.label, Container.id).where(Container.label.in_(list(labels)))
            ).all()
            label_to_id = {l: i for l, i in result}

        for r in batch:
            container_label = r.get('has_container_label')
            container_id = label_to_id.get(container_label)
            doc_id = Document.upsert(
                session,
                language_code=r.get('language_code'),
                has_container=container_id,
            )
            page_to_doc_id[(container_label or '', r['page_id'])] = doc_id
            count += 1

    log(f"documents: {count} rows loaded")
    session.commit()
    return page_to_doc_id


def load_web_resources(session, staging_dir, page_to_doc_id):
    filepath = find_deduped_parquet(staging_dir, 'web_resources')
    if not filepath:
        return
    log(f"web_resources: loading from {filepath}")

    # Defer foreign key constraint checks until commit for faster inserts
    session.execute(text("SET CONSTRAINTS ALL DEFERRED"))

    count = 0
    for batch in read_parquet_batches(filepath):
        # Resolve domain labels to ids
        domain_labels = set(r.get('domain_label') for r in batch if r.get('domain_label'))
        domain_to_id = {}
        if domain_labels:
            result = session.execute(
                sa_select(Domain.value, Domain.id).where(Domain.value.in_(list(domain_labels)))
            ).all()
            domain_to_id = {v: i for v, i in result}

        cleaned = []
        for r in batch:
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

        WebResource.bulk_upsert(session, cleaned)
        count += len(cleaned)

    log(f"web_resources: {count} rows loaded")
    session.commit()


def load_wiki_templates(session, staging_dir):
    filepath = find_deduped_parquet(staging_dir, 'wiki_templates')
    if not filepath:
        return
    log(f"wiki_templates: loading from {filepath}")

    count = 0
    for batch in read_parquet_batches(filepath):
        # Resolve domain labels
        domain_labels = set(r['domain_label'] for r in batch)
        domain_to_id = {}
        if domain_labels:
            result = session.execute(
                sa_select(Domain.value, Domain.id).where(Domain.value.in_(list(domain_labels)))
            ).all()
            domain_to_id = {v: i for v, i in result}

        cleaned = []
        for r in batch:
            dom_id = domain_to_id.get(r['domain_label'])
            if dom_id is not None:
                cleaned.append({'domain': dom_id, 'name': r['name']})

        WikiTemplate.bulk_upsert(session, cleaned)
        count += len(cleaned)
    log(f"wiki_templates: {count} rows loaded")
    session.commit()


def load_normalized_citations(session, staging_dir, page_to_doc_id):
    filepath = find_deduped_parquet(staging_dir, 'normalized_citations')
    if not filepath:
        return
    log(f"normalized_citations: loading from {filepath}")

    count = 0
    for batch in read_parquet_batches(filepath):
        cleaned = []
        for r in batch:
            domain = r.get('appears_on_domain', '')
            page_id = r.get('appears_on_page_id')
            doc_id = page_to_doc_id.get((domain, page_id))
            if doc_id is None:
                continue
            cleaned.append({
                'normalized_sha1': r['normalized_sha1'],
                'reference_normalized': r['reference_normalized'],
                'appears_on_article': doc_id,
            })

        if cleaned:
            stmt = insert(NormalizedCitation).values(cleaned).on_conflict_do_update(
                index_elements=['normalized_sha1'],
                set_={
                    'reference_normalized': insert(NormalizedCitation).excluded.reference_normalized,
                    'appears_on_article': insert(NormalizedCitation).excluded.appears_on_article,
                }
            )
            session.execute(stmt)
            count += len(cleaned)
    log(f"normalized_citations: {count} rows loaded")
    session.commit()


def load_citation_instances(session, staging_dir):
    """Load citation instances. Resolves normalized_sha1 -> normalized_id via DB lookup."""
    filepath = find_deduped_parquet(staging_dir, 'citation_instances')
    if not filepath:
        return
    log(f"citation_instances: loading from {filepath}")

    count = 0
    for batch in read_parquet_batches(filepath):
        # Resolve normalized_sha1 -> normalized_id
        sha1s = list(set(r['normalized_sha1'] for r in batch if r.get('normalized_sha1')))
        sha1_to_id = {}
        if sha1s:
            # Query in chunks to avoid overly large IN clauses
            for chunk in chunked_iterable(sha1s, 1000):
                result = session.execute(
                    sa_select(NormalizedCitation.normalized_sha1, NormalizedCitation.id)
                    .where(NormalizedCitation.normalized_sha1.in_(chunk))
                ).all()
                sha1_to_id.update({s: i for s, i in result})

        cleaned = []
        for r in batch:
            norm_id = sha1_to_id.get(r.get('normalized_sha1'))
            if norm_id is None:
                continue
            cleaned.append({
                'page_id': r['page_id'],
                'raw_sha1': r['raw_sha1'],
                'normalized_id': norm_id,
                'reference_type': r.get('reference_type', 0),
                'reference_name': r.get('reference_name'),
            })

        if cleaned:
            CitationInstance.bulk_upsert(session, cleaned)
            count += len(cleaned)
    log(f"citation_instances: {count} rows loaded")
    session.commit()


def load_revisions(session, staging_dir):
    filepath = find_deduped_parquet(staging_dir, 'revisions')
    if not filepath:
        return
    log(f"revisions: loading from {filepath}")

    count = 0
    for batch in read_parquet_batches(filepath):
        # Ensure every row has parent_revision_id (even if None) so
        # SQLAlchemy multi-row INSERT sees consistent columns.
        for row in batch:
            row.setdefault('parent_revision_id', None)
        stmt = insert(Revision).values(batch)
        stmt = stmt.on_conflict_do_update(
            index_elements=['revision_id'],
            set_={
                'page_id': stmt.excluded.page_id,
                'parent_revision_id': stmt.excluded.parent_revision_id,
                'revision_timestamp': stmt.excluded.revision_timestamp,
            }
        )
        session.execute(stmt)
        count += len(batch)
    log(f"revisions: {count} rows loaded")
    session.commit()


def load_citation_histories(session, staging_dir):
    """Load citation histories. Resolves (page_id, raw_sha1) -> citation_instance_id."""
    filepath = find_deduped_parquet(staging_dir, 'citation_histories')
    if not filepath:
        return
    log(f"citation_histories: loading from {filepath}")

    count = 0
    skipped = 0
    for batch in read_parquet_batches(filepath):
        # Resolve (page_id, raw_sha1) -> citation_instance_id
        keys = list(set((r['page_id'], r['raw_sha1']) for r in batch))
        key_to_id = {}
        if keys:
            from sqlalchemy import tuple_
            for chunk in chunked_iterable(keys, 1000):
                result = session.execute(
                    sa_select(CitationInstance.page_id, CitationInstance.raw_sha1, CitationInstance.id)
                    .where(tuple_(CitationInstance.page_id, CitationInstance.raw_sha1).in_(chunk))
                ).all()
                key_to_id.update({(p, s): i for p, s, i in result})

        cleaned = []
        for r in batch:
            ci_id = key_to_id.get((r['page_id'], r['raw_sha1']))
            if ci_id is None:
                skipped += 1
                continue
            cleaned.append({
                'citation_instance_id': ci_id,
                'revision_id': r['revision_id'],
            })

        if cleaned:
            stmt = insert(CitationHistory).values(cleaned).on_conflict_do_nothing()
            session.execute(stmt)
            count += len(cleaned)

    if skipped:
        log(f"citation_histories: warning: {skipped} rows skipped (no matching citation_instance)")
    log(f"citation_histories: {count} rows loaded")
    session.commit()


def load_ncwr(session, staging_dir):
    """Load normalized_citation_web_resources. Resolves normalized_sha1 -> normalized_id and url -> web_resource_id."""
    filepath = find_deduped_parquet(staging_dir, 'ncwr')
    if not filepath:
        return
    log(f"ncwr: loading from {filepath}")

    count = 0
    for batch in read_parquet_batches(filepath):
        # Resolve URLs to web_resource_ids
        urls = list(set(r['url'] for r in batch))
        url_to_id = {}
        for chunk in chunked_iterable(urls, 1000):
            result = session.execute(
                sa_select(WebResource.url, WebResource.id).where(WebResource.url.in_(chunk))
            ).all()
            url_to_id.update({u: i for u, i in result})

        # Resolve normalized_sha1 -> normalized_id
        sha1s = list(set(r['normalized_sha1'] for r in batch))
        sha1_to_id = {}
        for chunk in chunked_iterable(sha1s, 1000):
            result = session.execute(
                sa_select(NormalizedCitation.normalized_sha1, NormalizedCitation.id)
                .where(NormalizedCitation.normalized_sha1.in_(chunk))
            ).all()
            sha1_to_id.update({s: i for s, i in result})

        cleaned = []
        for r in batch:
            wr_id = url_to_id.get(r['url'])
            norm_id = sha1_to_id.get(r['normalized_sha1'])
            if wr_id is not None and norm_id is not None:
                cleaned.append({
                    'normalized_id': norm_id,
                    'web_resource_id': wr_id,
                })
        if cleaned:
            NormalizedCitationWebResource.bulk_upsert(session, cleaned)
            count += len(cleaned)
    log(f"ncwr: {count} rows loaded")
    session.commit()


def load_template_data(session, staging_dir):
    filepath = find_deduped_parquet(staging_dir, 'template_data')
    if not filepath:
        return
    log(f"template_data: loading from {filepath}")

    count = 0
    for batch in read_parquet_batches(filepath):
        # Resolve domain labels and template names to ids
        domain_labels = set(r['domain_label'] for r in batch)
        domain_to_id = {}
        if domain_labels:
            result = session.execute(
                sa_select(Domain.value, Domain.id).where(Domain.value.in_(list(domain_labels)))
            ).all()
            domain_to_id = {v: i for v, i in result}

        # Resolve template ids
        tpl_keys = set()
        for r in batch:
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

        # Resolve normalized_sha1 -> normalized_id
        sha1s = list(set(r['normalized_sha1'] for r in batch))
        sha1_to_id = {}
        for chunk in chunked_iterable(sha1s, 1000):
            result = session.execute(
                sa_select(NormalizedCitation.normalized_sha1, NormalizedCitation.id)
                .where(NormalizedCitation.normalized_sha1.in_(chunk))
            ).all()
            sha1_to_id.update({s: i for s, i in result})

        cleaned = []
        for r in batch:
            dom_id = domain_to_id.get(r['domain_label'])
            if dom_id is None:
                continue
            tpl_id = template_key_to_id.get((dom_id, r['template_name']))
            if tpl_id is None:
                continue
            norm_id = sha1_to_id.get(r['normalized_sha1'])
            if norm_id is None:
                continue
            cleaned.append({
                'wiki_template_id': tpl_id,
                'normalized_id': norm_id,
                'offset_start': r['offset_start'],
                'parameter_key': r['parameter_key'],
                'parameter_value': r.get('parameter_value'),
            })

        if cleaned:
            TemplateData.bulk_upsert(session, cleaned)
            count += len(cleaned)
    log(f"template_data: {count} rows loaded")
    session.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global BATCH_SIZE
    parser = argparse.ArgumentParser(description='Load staged Parquet files into PostgreSQL')
    parser.add_argument('-d', '--staging-dir', default=os.environ.get('STAGING_DIR', './staging'),
                        help='Staging directory (default: STAGING_DIR env or ./staging)')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                        help=f'Rows per INSERT batch (default: {BATCH_SIZE})')

    all_phases = OrderedDict([
        ('containers',          ('Phase 1:  containers',          lambda s, d, ctx: load_containers(s, d))),
        ('domains',             ('Phase 2:  domains',             lambda s, d, ctx: load_domains(s, d))),
        ('documents',           ('Phase 3:  documents',           lambda s, d, ctx: ctx.update(page_to_doc_id=load_documents(s, d)))),
        ('web_resources',       ('Phase 4:  web_resources',       lambda s, d, ctx: load_web_resources(s, d, ctx.get('page_to_doc_id', {})))),
        ('wiki_templates',      ('Phase 5:  wiki_templates',      lambda s, d, ctx: load_wiki_templates(s, d))),
        ('normalized_citations',('Phase 6:  normalized_citations', lambda s, d, ctx: load_normalized_citations(s, d, ctx.get('page_to_doc_id', {})))),
        ('citation_instances',  ('Phase 7:  citation_instances',  lambda s, d, ctx: load_citation_instances(s, d))),
        ('revisions',           ('Phase 8:  revisions',           lambda s, d, ctx: load_revisions(s, d))),
        ('citation_histories',  ('Phase 9:  citation_histories',  lambda s, d, ctx: load_citation_histories(s, d))),
        ('ncwr',                ('Phase 10: ncwr',                lambda s, d, ctx: load_ncwr(s, d))),
        ('template_data',       ('Phase 11: template_data',       lambda s, d, ctx: load_template_data(s, d))),
    ])

    parser.add_argument('--tables', nargs='+', metavar='TABLE',
                        choices=list(all_phases.keys()),
                        help='Load only the specified table(s). '
                             f'Choices: {" ".join(all_phases.keys())}')
    args = parser.parse_args()

    staging_dir = args.staging_dir
    if not os.path.isdir(staging_dir):
        raise SystemExit(f"Staging directory does not exist: {staging_dir}")

    BATCH_SIZE = args.batch_size

    session = Session()
    t0 = time.time()

    phases_to_run = OrderedDict(
        (k, all_phases[k]) for k in (args.tables if args.tables else all_phases)
    )

    try:
        ctx = {}  # shared context (e.g. page_to_doc_id) between phases
        for name, (label, loader) in phases_to_run.items():
            log(label)
            loader(session, staging_dir, ctx)

        elapsed = time.time() - t0
        log(f"Done. Total elapsed: {elapsed:.1f}s")

    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == '__main__':
    main()
