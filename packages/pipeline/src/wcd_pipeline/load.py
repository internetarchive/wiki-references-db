"""Stage 3 load: deduplicated staging Parquet → Postgres.

Reads the deduped/ subdirectory produced by ``wcd dedup`` and bulk-inserts
into the citation tables. ON CONFLICT upserts are kept as a safety net for
residual duplicates or re-runs.

What this stage no longer writes (rebuild model, docs/design/proposed_data_model.md):

- ``documents`` — not used for page identity; populated in the citation-graph phase
- ``revisions`` and article ``web_resources`` — RevisionChest writes them
  (Stage 0), or ``wcd stage0-load`` from its Parquet metadata
- ``citation_history`` — Parquet only (``citation_history/`` under the
  Parquet base), never a Postgres table

Page identity is ``(domain_id, page_id)``; ``citation_instances.id`` is the
deterministic ``wcd_schema.ids.instance_id(domain, page_id, raw_sha1)``.

Usage:
    wcd load -d ./staging
    wcd load --tables containers domains
"""

import hashlib
import itertools
import os
import sys
import time
import glob
import argparse
from collections import OrderedDict


import duckdb
from sqlalchemy import create_engine, text, select as sa_select, func as sa_func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from wcd_schema.ids import instance_id
from wcd_schema.postgres import (
    CitationInstance, Container, Domain, NormalizedCitation,
    NormalizedCitationWebResource, TemplateData, WebResource, WikiTemplate,
)

from .config import database_url

def make_session():
    """A session on the configured database (connection details are read when
    the loader runs, not when the module is imported)."""
    engine = create_engine(database_url(), poolclass=NullPool, hide_parameters=True)
    return sessionmaker(bind=engine)()


BATCH_SIZE = int(os.getenv('LOAD_BATCH_SIZE', '5000'))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def log(msg):
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"{ts} [load] {msg}", flush=True)


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
        session.commit()
        count += len(batch)
    log(f"containers: {count} rows loaded")


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

        if cleaned:
            Domain.bulk_upsert(session, cleaned)
            session.commit()
        count += len(cleaned)
    log(f"domains: {count} rows loaded")


def load_web_resources(session, staging_dir):
    """Cited URLs. Article curid rows (those carrying ``numeric_page_id``) are
    skipped: RevisionChest owns them (Stage 0)."""
    filepath = find_deduped_parquet(staging_dir, 'web_resources')
    if not filepath:
        return
    log(f"web_resources: loading from {filepath}")

    count = 0
    skipped_articles = 0
    for batch in read_parquet_batches(filepath):
        domain_to_id = _domain_ids(session, (r.get('domain_label') for r in batch))
        cleaned = []
        for r in batch:
            if r.get('numeric_page_id') is not None or r.get('page_id') is not None:
                skipped_articles += 1
                continue
            wr = {'url': r['url']}
            dl = r.get('domain_label')
            if dl and dl in domain_to_id:
                wr['domain_id'] = domain_to_id[dl]
            cleaned.append(wr)
        if cleaned:
            WebResource.bulk_upsert(session, cleaned)
            session.commit()
        count += len(cleaned)
    if skipped_articles:
        log(f"web_resources: {skipped_articles} article curid rows skipped (RevisionChest writes those)")
    log(f"web_resources: {count} rows loaded")


def _domain_ids(session, labels):
    labels = {l for l in labels if l}
    if not labels:
        return {}
    result = session.execute(sa_select(Domain.value, Domain.id).where(Domain.value.in_(list(labels)))).all()
    return {v: i for v, i in result}


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

        if cleaned:
            WikiTemplate.bulk_upsert(session, cleaned)
            session.commit()
        count += len(cleaned)
    log(f"wiki_templates: {count} rows loaded")


def load_normalized_citations(session, staging_dir):
    filepath = find_deduped_parquet(staging_dir, 'normalized_citations')
    if not filepath:
        return
    log(f"normalized_citations: loading from {filepath}")

    count = 0
    skipped = 0
    for batch in read_parquet_batches(filepath):
        domain_to_id = _domain_ids(session, (r.get('appears_on_domain') for r in batch))
        cleaned = []
        for r in batch:
            domain_id = domain_to_id.get(r.get('appears_on_domain'))
            page_id = r.get('appears_on_page_id')
            if domain_id is None or page_id is None:
                skipped += 1
                continue
            cleaned.append({
                'normalized_sha1': r['normalized_sha1'],
                'reference_normalized': r['reference_normalized'],
                'appears_on_domain_id': domain_id,
                'appears_on_page_id': page_id,
                'citation_type': r.get('citation_type'),   # None until Stage 1 emits it
            })
        if cleaned:
            NormalizedCitation.bulk_upsert(session, cleaned)
            session.commit()
            count += len(cleaned)
    if skipped:
        log(f"normalized_citations: warning: {skipped} rows skipped (unknown domain or missing page id)")
    log(f"normalized_citations: {count} rows loaded")


def load_citation_instances(session, staging_dir):
    """Resolves ``normalized_sha1`` → ``normalized_id`` (and the page's domain,
    from the normalized citation it belongs to, until Stage 1 stages the
    domain itself) and assigns the deterministic instance id."""
    filepath = find_deduped_parquet(staging_dir, 'citation_instances')
    if not filepath:
        return
    log(f"citation_instances: loading from {filepath}")

    domain_values = {i: v for v, i in session.execute(sa_select(Domain.value, Domain.id)).all()}
    count = 0
    skipped = 0
    for batch in read_parquet_batches(filepath):
        sha1s = list({r['normalized_sha1'] for r in batch if r.get('normalized_sha1')})
        sha1_to_norm = {}
        for chunk in chunked_iterable(sha1s, 1000):
            result = session.execute(
                sa_select(NormalizedCitation.normalized_sha1, NormalizedCitation.id, NormalizedCitation.appears_on_domain_id)
                .where(NormalizedCitation.normalized_sha1.in_(chunk))
            ).all()
            sha1_to_norm.update({s: (i, d) for s, i, d in result})

        cleaned = []
        for r in batch:
            norm = sha1_to_norm.get(r.get('normalized_sha1'))
            if norm is None:
                skipped += 1
                continue
            norm_id, domain_id = norm
            domain = r.get('domain') or domain_values.get(domain_id)
            if domain is None:
                skipped += 1
                continue
            if r.get('domain') and r['domain'] != domain_values.get(domain_id):
                # staged domain wins once Stage 1 provides it
                domain_id = next((i for i, v in domain_values.items() if v == r['domain']), domain_id)
            cleaned.append({
                'id': instance_id(domain, r['page_id'], r['raw_sha1']),
                'domain_id': domain_id,
                'page_id': r['page_id'],
                'raw_sha1': r['raw_sha1'],
                'normalized_id': norm_id,
                'reference_type': r.get('reference_type', 0),
                'reference_name': r.get('reference_name'),
            })

        if cleaned:
            CitationInstance.bulk_upsert(session, cleaned)
            session.commit()
            count += len(cleaned)
    if skipped:
        log(f"citation_instances: warning: {skipped} rows skipped (no matching normalized citation)")
    log(f"citation_instances: {count} rows loaded")


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
            session.commit()
            count += len(cleaned)
    log(f"ncwr: {count} rows loaded")


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
            session.commit()
            count += len(cleaned)
    log(f"template_data: {count} rows loaded")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None):
    global BATCH_SIZE
    parser = argparse.ArgumentParser(description='Load staged Parquet files into PostgreSQL')
    parser.add_argument('-d', '--staging-dir', default=os.environ.get('STAGING_DIR', './staging'),
                        help='Staging directory (default: STAGING_DIR env or ./staging)')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                        help=f'Rows per INSERT batch (default: {BATCH_SIZE})')

    all_phases = OrderedDict([
        ('containers',           ('Phase 1: containers',            lambda s, d, ctx: load_containers(s, d))),
        ('domains',              ('Phase 2: domains',               lambda s, d, ctx: load_domains(s, d))),
        ('web_resources',        ('Phase 3: web_resources (cited)', lambda s, d, ctx: load_web_resources(s, d))),
        ('wiki_templates',       ('Phase 4: wiki_templates',        lambda s, d, ctx: load_wiki_templates(s, d))),
        ('normalized_citations', ('Phase 5: normalized_citations',  lambda s, d, ctx: load_normalized_citations(s, d))),
        ('citation_instances',   ('Phase 6: citation_instances',    lambda s, d, ctx: load_citation_instances(s, d))),
        ('ncwr',                 ('Phase 7: ncwr',                  lambda s, d, ctx: load_ncwr(s, d))),
        ('template_data',        ('Phase 8: template_data',         lambda s, d, ctx: load_template_data(s, d))),
    ])

    parser.add_argument('--tables', nargs='+', metavar='TABLE',
                        choices=list(all_phases.keys()),
                        help='Load only the specified table(s). '
                             f'Choices: {" ".join(all_phases.keys())}')
    args = parser.parse_args(argv)

    staging_dir = args.staging_dir
    if not os.path.isdir(staging_dir):
        raise SystemExit(f"Staging directory does not exist: {staging_dir}")

    BATCH_SIZE = args.batch_size

    session = make_session()
    t0 = time.time()

    phases_to_run = OrderedDict(
        (k, all_phases[k]) for k in (args.tables if args.tables else all_phases)
    )

    try:
        ctx = {}  # shared context between phases
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
