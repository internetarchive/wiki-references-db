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
- template parameters — Parquet only: ``template_params`` (by page) and
  ``template_param_index`` (by template/key/value), written by the
  ``template_data`` phase with ``--parquet-dir``; Postgres keeps only
  ``template_value_counts`` for the report's heavy values

Page identity is ``(domain_id, page_id)``; ``citation_instances.id`` is the
deterministic ``wcd_schema.ids.instance_id(domain, page_id, raw_sha1)``.

Usage:
    wcd load -d ./staging --parquet-dir ./parquet
    wcd load --tables containers domains
"""

import io
import itertools
import os
import time
import argparse
from collections import OrderedDict


import duckdb
import pyarrow as pa
import pyarrow.csv as pa_csv
from sqlalchemy import create_engine, select as sa_select, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from wcd_schema.ids import instance_id
from wcd_schema.postgres import (
    CitationInstance, Container, Domain, NormalizedCitation,
    NormalizedCitationWebResource, WebResource, WikiTemplate,
)
from wcd_schema import parquet as layout
from wcd_schema.ids import PAGE_BUCKETS, template_search_bucket_sql

from .config import database_url

def make_session():
    """A session on the configured database (connection details are read when
    the loader runs, not when the module is imported)."""
    engine = create_engine(database_url(), poolclass=NullPool, hide_parameters=True)
    return sessionmaker(bind=engine)()


BATCH_SIZE = int(os.getenv('LOAD_BATCH_SIZE', '5000'))
# Rows per COPY chunk for the set-based loaders (one transaction per chunk).
COPY_ROWS = int(os.getenv('LOAD_COPY_ROWS', '1000000'))


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


def copy_parquet_in_chunks(session, source_sql, temp_table, temp_ddl, columns, insert_sql,
                           duck_tables=None, chunk_rows=None):
    """Set-based load: run ``source_sql`` in DuckDB (it reads the staged
    Parquet, and may join the small Arrow tables in ``duck_tables``), stream its
    rows into a temporary table with ``COPY … CSV``, ``chunk_rows`` at a time,
    and after each chunk run ``insert_sql`` (an ``INSERT … SELECT`` from the
    temp table) and commit. Returns (rows staged, rows written).

    This replaces per-batch Python id lookups and multi-row ``INSERT … VALUES``
    statements, which ran at ~10k rows/s; the rows never become Python
    objects. NULLs travel as unquoted empty CSV fields and every non-NULL value
    is quoted, so an empty string stays an empty string.

    Keep ``insert_sql`` to single-column joins on columns typed as in
    ``temp_ddl``: Postgres multiplies the selectivities of a multi-column join
    key as if they were independent, estimates a chunk of a million rows at a
    few dozen, and picks a nested loop. Resolve small lookups (domains,
    templates) in DuckDB instead, through ``duck_tables``.
    """
    chunk_rows = chunk_rows or COPY_ROWS
    dbapi = session.connection().connection.dbapi_connection
    cur = dbapi.cursor()
    cur.execute(f"CREATE TEMP TABLE IF NOT EXISTS {temp_table} ({temp_ddl})")
    col_list = ", ".join(columns)
    options = pa_csv.WriteOptions(include_header=False, quoting_style="all_valid")
    con = duckdb.connect()
    staged = written = 0
    try:
        for name, table in (duck_tables or {}).items():
            con.register(name, table)
        reader = con.execute(source_sql).fetch_record_batch(chunk_rows)
        for batch in reader:
            if batch.num_rows == 0:
                continue
            buf = io.BytesIO()
            pa_csv.write_csv(batch, buf, write_options=options)
            buf.seek(0)
            cur.execute(f"TRUNCATE {temp_table}")
            cur.copy_expert(f"COPY {temp_table} ({col_list}) FROM STDIN WITH (FORMAT csv)", buf)
            # a freshly filled temp table has no statistics, and autovacuum
            # never analyzes temp tables
            cur.execute(f"ANALYZE {temp_table}")
            cur.execute(insert_sql)
            written += cur.rowcount
            staged += batch.num_rows
            dbapi.commit()
    except Exception:
        dbapi.rollback()
        raise
    finally:
        con.close()
        cur.close()
    return staged, written


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
    """Cited URLs (article curid rows are written at Stage 0)."""
    filepath = find_deduped_parquet(staging_dir, 'web_resources')
    if not filepath:
        return
    log(f"web_resources: loading from {filepath}")

    count = 0
    for batch in read_parquet_batches(filepath):
        domain_to_id = _domain_ids(session, (r.get('domain_label') for r in batch))
        cleaned = []
        for r in batch:
            wr = {'url': r['url']}
            dl = r.get('domain_label')
            if dl and dl in domain_to_id:
                wr['domain_id'] = domain_to_id[dl]
            cleaned.append(wr)
        if cleaned:
            WebResource.bulk_upsert(session, cleaned)
            session.commit()
        count += len(cleaned)
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
            if domain_id is None or page_id is None or not r.get('citation_type'):
                skipped += 1
                continue
            cleaned.append({
                'normalized_sha1': r['normalized_sha1'],
                'reference_normalized': r['reference_normalized'],
                'appears_on_domain_id': domain_id,
                'appears_on_page_id': page_id,
                'citation_type': r['citation_type'],
            })
        if cleaned:
            NormalizedCitation.bulk_upsert(session, cleaned)
            session.commit()
            count += len(cleaned)
    if skipped:
        log(f"normalized_citations: warning: {skipped} rows skipped (unknown domain, missing page id or citation_type)")
    log(f"normalized_citations: {count} rows loaded")


def load_citation_instances(session, staging_dir):
    """Resolves ``normalized_sha1`` → ``normalized_id`` and the staged domain
    → ``domain_id``. The id is the staged ``citation_instance_id`` (which
    Stage 1 computed and wrote into ``citation_history``); it is re-derived
    here and must agree."""
    filepath = find_deduped_parquet(staging_dir, 'citation_instances')
    if not filepath:
        return
    log(f"citation_instances: loading from {filepath}")

    count = 0
    skipped = 0
    for batch in read_parquet_batches(filepath):
        sha1s = list({r['normalized_sha1'] for r in batch if r.get('normalized_sha1')})
        sha1_to_norm = {}
        for chunk in chunked_iterable(sha1s, 1000):
            result = session.execute(
                sa_select(NormalizedCitation.normalized_sha1, NormalizedCitation.id)
                .where(NormalizedCitation.normalized_sha1.in_(chunk))
            ).all()
            sha1_to_norm.update(dict(result))
        domain_to_id = _domain_ids(session, (r.get('domain') for r in batch))

        cleaned = []
        for r in batch:
            norm_id = sha1_to_norm.get(r.get('normalized_sha1'))
            domain_id = domain_to_id.get(r.get('domain'))
            if norm_id is None or domain_id is None:
                skipped += 1
                continue
            expected = instance_id(r['domain'], r['page_id'], r['raw_sha1'])
            staged = r.get('citation_instance_id')
            if staged is not None and staged != expected:
                raise RuntimeError(
                    f"citation_instance_id mismatch for ({r['domain']}, {r['page_id']}, {r['raw_sha1']}): "
                    f"staged {staged}, computed {expected}")
            cleaned.append({
                'id': expected,
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
        log(f"citation_instances: warning: {skipped} rows skipped (no matching normalized citation or unknown domain)")
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


# ---------------------------------------------------------------------------
# Template parameters → Parquet (template_params, template_param_index) and
# the heavy-value counts → Postgres (template_value_counts)
# ---------------------------------------------------------------------------

# A (template, key, value) with at least this many citations gets its report
# total precomputed in template_value_counts.
TEMPLATE_COUNT_MIN = int(os.getenv('TEMPLATE_COUNT_MIN', '1000'))
# Rows per partition of the sha1-keyed join behind template_params; each
# partition is one hash join, so memory follows this, not the table size.
TEMPLATE_ROWS_PER_PARTITION = int(os.getenv('LOAD_ROWS_PER_PARTITION', '20000000'))


def _duck(temp_dir=None):
    con = duckdb.connect()
    mem = os.environ.get('LOAD_MEMORY_LIMIT')
    if mem:
        con.execute(f"SET memory_limit = '{mem}'")
    if temp_dir:
        con.execute(f"SET temp_directory = '{temp_dir}'")
    con.execute("SET preserve_insertion_order = false")
    try:
        con.execute(f"SET partitioned_write_max_open_files = {PAGE_BUCKETS * 4}")
    except duckdb.Error:
        pass
    return con


def _q(value: str) -> str:
    return value.replace("'", "''")


def _partitions(root, first, second):
    """``[(domain, n, dir)]`` for ``root/domain=*/<second>=*`` directories."""
    import glob as _glob
    out = []
    for d in sorted(_glob.glob(os.path.join(root, f'{first}=*', f'{second}=*'))):
        domain = os.path.basename(os.path.dirname(d)).split('=', 1)[1]
        out.append((domain, int(os.path.basename(d).split('=', 1)[1]), d))
    return out


def _install(work_dataset, parquet_base, name):
    """Move each domain of a dataset built under ``work_dataset`` into the tier,
    replacing that domain's partitions (the step owns them, as stage0-load owns
    revisions/)."""
    import shutil
    for d in sorted(os.listdir(work_dataset)):
        target = os.path.join(layout.dataset_dir(parquet_base, name), d)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        if os.path.exists(target):
            shutil.rmtree(target)
        os.replace(os.path.join(work_dataset, d), target)


def write_template_parquet(session, staging_dir, parquet_base):
    """Write ``template_param_index`` and ``template_params`` from the deduped
    ``template_data`` (and ``citation_instances``, for the pages), and the
    heavy-value totals to ``template_value_counts``.

    Both datasets are built in passes with bounded memory: the rows are first
    split into Parquet partitions on disk, then each partition is sorted (and,
    for template_params, joined) on its own. The finished domains replace the
    domains in the tier at the end."""
    import math
    import shutil

    td_path = find_deduped_parquet(staging_dir, 'template_data')
    ci_path = find_deduped_parquet(staging_dir, 'citation_instances')
    if not td_path:
        return
    work = os.path.join(staging_dir, '.template-work')
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    con = _duck(temp_dir=os.path.join(work, 'duckdb-tmp'))
    td = f"read_parquet('{_q(td_path)}')"
    td_where = ("domain_label IS NOT NULL AND template_name IS NOT NULL AND parameter_key IS NOT NULL "
                "AND normalized_sha1 IS NOT NULL")
    try:
        # -- template_param_index: split by search bucket, then one sorted, distinct file per bucket
        t = time.time()
        bucket = template_search_bucket_sql('template_name', 'parameter_key', 'parameter_value')
        con.execute(f"""COPY (SELECT domain_label AS domain, {bucket} AS search_bucket, template_name, parameter_key,
                                     parameter_value, normalized_sha1 FROM {td} WHERE {td_where})
                        TO '{_q(os.path.join(work, 'index-split'))}' (FORMAT PARQUET, PARTITION_BY (domain, search_bucket))""")
        counts = []   # (domain, template_name, key, value, n)
        out_root = os.path.join(work, 'template_param_index')
        n_index = 0
        index_parts = _partitions(os.path.join(work, 'index-split'), 'domain', 'search_bucket')
        domains = sorted({p[0] for p in index_parts})
        for domain, b, d in index_parts:
            dest = os.path.join(out_root, f'domain={domain}', f'search_bucket={b}')
            os.makedirs(dest)
            src = f"read_parquet('{_q(d)}/*.parquet', hive_partitioning=false)"
            out_file = os.path.join(dest, 'data.parquet')
            con.execute(f"""COPY (SELECT DISTINCT template_name, parameter_key, parameter_value, normalized_sha1 FROM {src}
                                  ORDER BY template_name, parameter_key, parameter_value, normalized_sha1)
                            TO '{_q(out_file)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)""")
            for name, key, value, n in con.execute(
                    f"""SELECT template_name, parameter_key, parameter_value, count(*) FROM read_parquet('{_q(out_file)}')
                        WHERE parameter_value IS NOT NULL GROUP BY ALL HAVING count(*) >= {TEMPLATE_COUNT_MIN}""").fetchall():
                counts.append((domain, name, key, value, n))
            n_index += con.execute(f"SELECT count(*) FROM read_parquet('{_q(out_file)}')").fetchone()[0]
        shutil.rmtree(os.path.join(work, 'index-split'))
        log(f"template_param_index: {n_index} rows in {time.time() - t:.0f}s")

        # -- template_params: rows keyed by citation, joined to the pages that cite it, one file per page bucket
        t = time.time()
        pages_root = os.path.join(work, 'pages-split')
        if ci_path:
            n_rows = con.execute(f"SELECT coalesce(sum(num_rows), 0) FROM parquet_file_metadata('{_q(td_path)}')").fetchone()[0]
            parts = max(1, math.ceil(n_rows / TEMPLATE_ROWS_PER_PARTITION))
            ci = f"read_parquet('{_q(ci_path)}')"
            key = f"CAST(hash(normalized_sha1) % {parts} AS INTEGER)"
            con.execute(f"""COPY (SELECT {key} AS k, domain_label AS domain, normalized_sha1, template_name, offset_start,
                                         parameter_key, parameter_value FROM {td} WHERE {td_where})
                            TO '{_q(os.path.join(work, 'td-split'))}' (FORMAT PARQUET, PARTITION_BY (k))""")
            con.execute(f"""COPY (SELECT DISTINCT {key} AS k, domain, page_id, normalized_sha1 FROM {ci}
                                  WHERE domain IS NOT NULL AND page_id IS NOT NULL AND normalized_sha1 IS NOT NULL)
                            TO '{_q(os.path.join(work, 'ci-split'))}' (FORMAT PARQUET, PARTITION_BY (k))""")
            for k in range(parts):
                tdk, cik = os.path.join(work, 'td-split', f'k={k}'), os.path.join(work, 'ci-split', f'k={k}')
                if not (os.path.isdir(tdk) and os.path.isdir(cik)):
                    continue
                con.execute(f"""COPY (SELECT c.domain, CAST(c.page_id % {PAGE_BUCKETS} AS INTEGER) AS page_bucket, c.page_id,
                                             t.normalized_sha1, t.template_name, t.offset_start, t.parameter_key, t.parameter_value
                                      FROM read_parquet('{_q(tdk)}/*.parquet', hive_partitioning=false) t
                                      JOIN read_parquet('{_q(cik)}/*.parquet', hive_partitioning=false) c
                                        ON c.domain = t.domain AND c.normalized_sha1 = t.normalized_sha1)
                                TO '{_q(pages_root)}' (FORMAT PARQUET, PARTITION_BY (domain, page_bucket),
                                                       OVERWRITE_OR_IGNORE true, FILENAME_PATTERN 'k{k}_{{i}}')""")
            shutil.rmtree(os.path.join(work, 'td-split'))
            shutil.rmtree(os.path.join(work, 'ci-split'))
        params_root = os.path.join(work, 'template_params')
        n_params = 0
        if os.path.isdir(pages_root):
            for domain, b, d in _partitions(pages_root, 'domain', 'page_bucket'):
                dest = os.path.join(params_root, f'domain={domain}', f'page_bucket={b}')
                os.makedirs(dest)
                out_file = os.path.join(dest, 'data.parquet')
                con.execute(f"""COPY (SELECT page_id, normalized_sha1, template_name, offset_start, parameter_key, parameter_value
                                      FROM read_parquet('{_q(d)}/*.parquet', hive_partitioning=false)
                                      ORDER BY page_id, normalized_sha1, offset_start, parameter_key)
                                TO '{_q(out_file)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)""")
                n_params += con.execute(f"SELECT count(*) FROM read_parquet('{_q(out_file)}')").fetchone()[0]
            shutil.rmtree(pages_root)
        log(f"template_params: {n_params} rows in {time.time() - t:.0f}s")

        for name in ('template_param_index', 'template_params'):
            if os.path.isdir(os.path.join(work, name)):
                _install(os.path.join(work, name), parquet_base, name)
    finally:
        con.close()
        shutil.rmtree(work, ignore_errors=True)
    _write_template_counts(session, domains, counts)


def _write_template_counts(session, domains, counts):
    """Replace ``template_value_counts`` for ``domains`` (those this run wrote
    template parameters for) with the heavy values found; other domains keep theirs."""
    ids = {(d, n): i for d, n, i in session.execute(text(
        "SELECT d.value, w.name, w.id FROM wiki_templates w JOIN domains d ON d.id = w.domain")).all()}
    rows = [{"w": ids[(d, n)], "k": k, "v": v, "n": c} for d, n, k, v, c in counts if (d, n) in ids]
    session.execute(text(
        "DELETE FROM template_value_counts WHERE wiki_template_id IN "
        "(SELECT w.id FROM wiki_templates w JOIN domains d ON d.id = w.domain WHERE d.value = ANY(:d))"), {"d": domains})
    if rows:
        session.execute(text(
            "INSERT INTO template_value_counts (wiki_template_id, parameter_key, parameter_value_md5, parameter_value, n_citations) "
            "VALUES (:w, :k, md5(:v), :v, :n)"), rows)
    session.commit()
    skipped = len(counts) - len(rows)
    log(f"template_value_counts: {len(rows)} heavy values (>= {TEMPLATE_COUNT_MIN} citations)"
        + (f"; {skipped} skipped (template not in wiki_templates)" if skipped else ""))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None):
    global BATCH_SIZE
    parser = argparse.ArgumentParser(description='Load staged Parquet files into PostgreSQL')
    parser.add_argument('-d', '--staging-dir', default=os.environ.get('STAGING_DIR', './staging'),
                        help='Staging directory (default: STAGING_DIR env or ./staging)')
    parser.add_argument('--parquet-dir', default=os.environ.get('PARQUET_DIR'),
                        help='Parquet tier base for template_params / template_param_index (default: PARQUET_DIR); '
                             'required for the template_data phase')
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
        ('template_data',        ('Phase 8: template parameters → Parquet',
                                  lambda s, d, ctx: write_template_parquet(s, d, ctx['parquet_dir']))),
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

    if 'template_data' in phases_to_run and not args.parquet_dir:
        raise SystemExit("--parquet-dir (or PARQUET_DIR) is required for the template_data phase")

    try:
        ctx = {'parquet_dir': args.parquet_dir}  # shared context between phases
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
