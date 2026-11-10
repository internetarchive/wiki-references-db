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
import shutil
import time
import argparse
from collections import OrderedDict


import duckdb
import pyarrow.csv as pa_csv
from sqlalchemy import create_engine, select as sa_select, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from wcd_schema.ids import instance_id_sql
from wcd_schema.postgres import (
    Base, Container, Domain, WikiTemplate,
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
# Connections for the citation tables (copy_parquet_parallel); --jobs overrides.
LOAD_JOBS = int(os.getenv('LOAD_JOBS', '8'))
# Rows per row group (= per unit) of the key-sorted copies the citation tables load from
SORTED_GROUP = 500_000
# Keep the key-sorted copies after a successful load (for repeated loads of the same staging)
KEEP_SORTED = os.getenv('LOAD_KEEP_SORTED', '') not in ('', '0', 'false')
# Drop the foreign keys of citation_instances / normalized_citation_web_resources when the table
# is empty at the start of its phase, and add them back (NOT VALID, then VALIDATE) after it;
# --defer-fks / --no-defer-fks override
DEFER_FKS = os.getenv('LOAD_DEFER_FKS', '1') not in ('', '0', 'false')


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


_CSV_OPTIONS = pa_csv.WriteOptions(include_header=False, quoting_style="all_valid")


def _copy_and_insert(cur, rows, temp_table, temp_ddl, columns, insert_sql, index_lookups=False):
    """``COPY`` the Arrow table or batch ``rows`` into the temporary table
    ``temp_table`` (created if missing, emptied first) and run ``insert_sql``;
    returns the rows it wrote. The caller commits.

    NULLs travel as unquoted empty CSV fields and every non-NULL value is
    quoted, so an empty string stays an empty string.

    ``index_lookups=True`` turns hash and merge joins off for ``insert_sql``:
    use it when the rows join a table far larger than themselves (half a
    million staged rows against hundreds of millions of
    ``normalized_citations``), where a hash join would scan the whole table for
    each unit, and the planner's costs for that and for the index probes are
    close.
    """
    buf = io.BytesIO()
    pa_csv.write_csv(rows, buf, write_options=_CSV_OPTIONS)
    buf.seek(0)
    cur.execute(f"CREATE TEMP TABLE IF NOT EXISTS {temp_table} ({temp_ddl})")
    cur.execute(f"TRUNCATE {temp_table}")
    cur.copy_expert(f"COPY {temp_table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv)", buf)
    # a freshly filled temp table has no statistics, and autovacuum never
    # analyzes temp tables
    cur.execute(f"ANALYZE {temp_table}")
    if index_lookups:
        # SET LOCAL lasts until the caller's commit
        cur.execute("SET LOCAL enable_hashjoin = off")
        cur.execute("SET LOCAL enable_mergejoin = off")
    cur.execute(insert_sql)
    return cur.rowcount


def copy_parquet_in_chunks(session, source_sql, temp_table, temp_ddl, columns, insert_sql,
                           duck_tables=None, chunk_rows=None):
    """Set-based load: run ``source_sql`` in DuckDB (it reads the staged
    Parquet, and may join the small Arrow tables in ``duck_tables``), stream its
    rows into a temporary table with ``COPY … CSV``, ``chunk_rows`` at a time,
    and after each chunk run ``insert_sql`` (an ``INSERT … SELECT`` from the
    temp table) and commit. Returns (rows staged, rows written).

    This replaces per-batch Python id lookups and multi-row ``INSERT … VALUES``
    statements, which ran at ~10k rows/s; the rows never become Python
    objects. ``copy_parquet_parallel`` does the same on several connections.

    Keep ``insert_sql`` to single-column joins on columns typed as in
    ``temp_ddl``: Postgres multiplies the selectivities of a multi-column join
    key as if they were independent, estimates a chunk of a million rows at a
    few dozen, and picks a nested loop. Resolve small lookups (domains,
    templates) in DuckDB instead, through ``duck_tables``.
    """
    chunk_rows = chunk_rows or COPY_ROWS
    dbapi = session.connection().connection.dbapi_connection
    cur = dbapi.cursor()
    con = duckdb.connect()
    staged = written = 0
    try:
        for name, table in (duck_tables or {}).items():
            con.register(name, table)
        reader = con.execute(source_sql).fetch_record_batch(chunk_rows)
        for batch in reader:
            if batch.num_rows == 0:
                continue
            written += _copy_and_insert(cur, batch, temp_table, temp_ddl, columns, insert_sql)
            staged += batch.num_rows
            dbapi.commit()
    except Exception:
        dbapi.rollback()
        raise
    finally:
        con.close()
        cur.close()
    return staged, written


def _sorted_copy(filepath, sort_by, work_dir):
    """A copy of ``filepath`` ordered by the SQL expression ``sort_by``
    (DuckDB, under ``LOAD_MEMORY_LIMIT``), in row groups of ``SORTED_GROUP``
    rows, at ``work_dir/<name>.<fingerprint>.sorted.parquet``; returns
    (path, reused).

    The fingerprint covers the source file (path, size, mtime), ``sort_by``
    and ``SORTED_GROUP``, so a copy left by an interrupted load is reused (the
    sort is the slow part: 43 min for enwiki's normalized_citations) and a
    stale one is replaced. The copy is written under a temporary name and
    renamed when complete. Spill directories of earlier, killed sorts are
    removed first (loads do not run concurrently on one staging directory)."""
    import hashlib
    import tempfile

    os.makedirs(work_dir, exist_ok=True)
    st = os.stat(filepath)
    fp = hashlib.sha1(f"{os.path.abspath(filepath)}|{st.st_size}|{st.st_mtime_ns}|{sort_by}|{SORTED_GROUP}"
                      .encode()).hexdigest()[:12]
    stem = os.path.basename(filepath).removesuffix('.parquet')
    out = os.path.join(work_dir, f"{stem}.{fp}.sorted.parquet")
    if os.path.exists(out):
        return out, True
    for name in os.listdir(work_dir):
        stale = os.path.join(work_dir, name)
        if name.startswith('spill-') or (name.startswith(f"{stem}.") and '.sorted.parquet' in name):
            shutil.rmtree(stale, ignore_errors=True) if os.path.isdir(stale) else os.remove(stale)
    spill = tempfile.mkdtemp(prefix='spill-', dir=work_dir)
    partial = out + '.partial'
    con = _duck(temp_dir=spill)
    try:
        con.execute(f"""COPY (SELECT * FROM read_parquet('{_q(filepath)}') ORDER BY {sort_by})
                        TO '{_q(partial)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {SORTED_GROUP})""")
        os.replace(partial, out)
    finally:
        con.close()
        shutil.rmtree(spill, ignore_errors=True)
        if os.path.exists(partial):
            os.remove(partial)
    return out, False


def copy_parquet_parallel(engine, filepath, source_sql, temp_table, temp_ddl, columns, insert_sql,
                          jobs=None, index_lookups=False, progress=None, sort_by=None, work_dir=None):
    """``copy_parquet_in_chunks`` on ``jobs`` connections: each row group of
    the Parquet file ``filepath`` is one unit, read with pyarrow, put through
    ``source_sql`` in DuckDB (which reads it as the relation ``src``), copied
    into a temporary table and inserted with ``insert_sql``, then committed.
    Returns (rows staged, rows written).

    One backend was the bottleneck at ~32k rows/s on enwiki: each row's index
    entries and foreign-key checks run in that backend while the machine idles.
    Units may run in any order and at the same time, so ``insert_sql`` must not
    depend on another unit's rows, and two units must not write the same
    conflict key (the deduped tables have one row per key). A unit commits on
    its own, so a load that fails part way leaves the committed units in
    place, and a re-run upserts them again. After a failure, units not yet
    started are cancelled.

    ``progress``, if given, is called as ``progress(units_done, units, staged,
    written)`` in the calling thread.

    ``sort_by`` (an SQL expression over the file's columns) first writes a
    copy of the file in that order under ``work_dir`` (``_sorted_copy``; it is
    deleted after a successful load unless ``LOAD_KEEP_SORTED`` is set, and
    reused by the next load if one fails), and the units are its row groups,
    started in key order. Use the key of the table's largest random-keyed
    index (a SHA-1, an md5): the units in flight then cover adjacent key
    ranges, so that index is written in a few narrow regions instead of all
    over. Unsorted, enwiki's ``normalized_citations`` fell from ~114k to ~14k
    rows/s as its ``normalized_sha1`` index (18 GB) outgrew the cache and
    every insert became a random read.
    """
    jobs = jobs or LOAD_JOBS
    sorted_file = None
    if sort_by:
        if not work_dir:
            raise ValueError("sort_by needs a work_dir")
        t0 = time.time()
        sorted_file, reused = _sorted_copy(filepath, sort_by, work_dir)
        filepath = sorted_file
        log(f"{temp_table}: " + (f"reusing the copy sorted by {sort_by}" if reused
                                 else f"sorted by {sort_by} in {time.time() - t0:.0f}s"))
    result = _run_units(engine, filepath, source_sql, temp_table, temp_ddl, columns, insert_sql,
                        jobs, index_lookups, progress)
    if sorted_file and not KEEP_SORTED:
        os.remove(sorted_file)
    return result


def _run_units(engine, filepath, source_sql, temp_table, temp_ddl, columns, insert_sql,
               jobs, index_lookups, progress):
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import pyarrow.parquet as pq

    n_units = pq.ParquetFile(filepath).metadata.num_row_groups

    def unit(i):
        table = pq.ParquetFile(filepath).read_row_group(i)
        con = duckdb.connect()
        try:
            con.register('src', table)
            result = con.execute(source_sql)
            # to_arrow_table() replaces fetch_arrow_table() in newer DuckDB
            rows = (getattr(result, 'to_arrow_table', None) or result.fetch_arrow_table)()
        finally:
            con.close()
        if rows.num_rows == 0:
            return 0, 0
        raw = engine.raw_connection()
        try:
            with raw.cursor() as cur:
                n = _copy_and_insert(cur, rows, temp_table, temp_ddl, columns, insert_sql, index_lookups)
            raw.commit()
        finally:
            raw.close()
        return rows.num_rows, n

    staged = written = done = 0
    with ThreadPoolExecutor(max_workers=jobs) as pool:
        futures = [pool.submit(unit, i) for i in range(n_units)]
        try:
            for f in as_completed(futures):
                s, w = f.result()
                staged += s
                written += w
                done += 1
                if progress:
                    progress(done, n_units, staged, written)
        except BaseException:
            for f in futures:
                f.cancel()
            raise
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


def _analyze(session, *tables, vacuum=False):
    """Refresh planner statistics on lookup tables an earlier phase just
    filled (autovacuum may not have analyzed them yet).

    ``vacuum=True`` runs ``VACUUM (ANALYZE)`` instead, for tables the next
    phase probes row by row: a freshly loaded table otherwise gets an
    insert-triggered, throttled autovacuum that reads the whole heap (68 GB
    for enwiki's normalized_citations) while that phase competes for the
    same disk. Run here it is unthrottled, once, and later runs skip the
    pages it marked all-visible."""
    session.commit()
    if not vacuum:
        for t in tables:
            session.execute(text(f"ANALYZE {t}"))
        session.commit()
        return
    t0 = time.time()
    with session.get_bind().connect().execution_options(isolation_level="AUTOCOMMIT") as con:
        for t in tables:
            con.execute(text(f"VACUUM (ANALYZE) {t}"))
    log(f"vacuumed {', '.join(tables)} in {time.time() - t0:.0f}s")


def _foreign_keys(table):
    """The foreign keys the schema declares on ``table``, as (constraint name,
    column, referenced table, referenced column); names are Postgres' defaults,
    which is what ``init-db`` creates."""
    t = Base.metadata.tables[table]
    return sorted((f"{table}_{fk.parent.name}_fkey", fk.parent.name, fk.column.table.name, fk.column.name)
                  for fk in t.foreign_keys)


def _present_foreign_keys(session, table):
    """{constraint name: validated} for the foreign keys ``table`` has now."""
    return dict(session.execute(text(
        "SELECT conname, convalidated FROM pg_constraint WHERE conrelid = CAST(:t AS regclass) AND contype = 'f'"
    ), {"t": table}).all())


def _drop_foreign_keys_if_empty(session, table):
    """Drop ``table``'s foreign keys if it has no rows (a load into an empty
    table). Each row's check otherwise locks the referenced row (``FOR KEY
    SHARE``), which writes into the referenced heap: on enwiki that turned
    normalized_citations' 68 GB heap into random writes while
    citation_instances loaded. ``_restore_foreign_keys`` adds them back."""
    if session.execute(text(f"SELECT EXISTS (SELECT 1 FROM {table})")).scalar_one():
        session.commit()
        return
    present = _present_foreign_keys(session, table)
    for name, *_ in _foreign_keys(table):
        if name in present:
            session.execute(text(f"ALTER TABLE {table} DROP CONSTRAINT {name}"))
    session.commit()
    log(f"{table}: foreign keys dropped for the load: {', '.join(sorted(present)) or '(none)'}")


def _restore_foreign_keys(session, table):
    """Add back any of ``table``'s declared foreign keys that are missing
    (``NOT VALID``, which takes no row locks) and validate every one that is
    not yet validated: one set-based check per key instead of one lookup per
    row. Runs after every load phase, so a load interrupted after dropping
    them restores them on its re-run (gate 3 checks that they are there)."""
    present = _present_foreign_keys(session, table)
    for name, col, ref_table, ref_col in _foreign_keys(table):
        if name not in present:
            session.execute(text(f"ALTER TABLE {table} ADD CONSTRAINT {name} FOREIGN KEY ({col}) "
                                 f"REFERENCES {ref_table} ({ref_col}) NOT VALID"))
            session.commit()
            present[name] = False
        if not present[name]:
            t0 = time.time()
            session.execute(text(f"ALTER TABLE {table} VALIDATE CONSTRAINT {name}"))
            session.commit()
            log(f"{table}: validated {name} in {time.time() - t0:.0f}s")


def _copy_load(session, name, filepath, target=None, defer_fks=False, **kwargs):
    """``copy_parquet_parallel`` with progress lines (every 40 units) and a
    summary; ``source_sql`` reads one row group as ``src``. ``target`` is the
    table written: its foreign keys are restored and validated afterwards in
    any case, and with ``defer_fks`` (and ``DEFER_FKS``) dropped first if it is
    empty.

    Deferral is for tables whose keys reference large tables: on enwiki
    citation_instances went from <6k to ~82k rows/s with it (plus
    ``INCLUDE (id)`` on normalized_citations' normalized_sha1), and validating
    took 9.4 min. normalized_citations, whose only key references a domain,
    loaded no faster without it and its validation cost 7.5 min (plan §22)."""
    if target and defer_fks and DEFER_FKS:
        _drop_foreign_keys_if_empty(session, target)
    session.commit()  # earlier phases' rows are visible to the other connections
    t0 = time.time()

    def progress(done, units, staged, written):
        if done % 40 == 0 or done == units:
            dt = time.time() - t0
            log(f"{name}: {done}/{units} units, {staged:,} staged, {written:,} written, "
                f"{staged / dt:,.0f} rows/s")

    # sorted copies go under <staging>/.load-sort (filepath is <staging>/deduped/<table>.parquet)
    kwargs.setdefault('work_dir', os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(filepath))),
                                               '.load-sort'))
    staged, written = copy_parquet_parallel(session.get_bind(), filepath, progress=progress, **kwargs)
    dt = time.time() - t0
    log(f"{name}: {staged:,} rows staged, {written:,} written in {dt:.1f}s "
        f"({staged / dt if dt else 0:,.0f} rows/s, {LOAD_JOBS} jobs)")
    if target:
        _restore_foreign_keys(session, target)
    return staged, written


def load_web_resources(session, staging_dir):
    """Cited URLs (article curid rows are written at Stage 0). A URL already
    present (an article row, or a re-run) keeps its other columns; a resolved
    domain replaces the stored one."""
    filepath = find_deduped_parquet(staging_dir, 'web_resources')
    if not filepath:
        return
    log(f"web_resources: loading from {filepath}")
    _analyze(session, 'domains')
    _copy_load(
        session, 'web_resources', filepath, target='web_resources',
        # md5 of the UTF-8 bytes as lowercase hex, as WebResource.compute_url_hash
        source_sql="SELECT url, md5(url) AS url_hash, domain_label FROM src WHERE url IS NOT NULL",
        temp_table='_wr_stage', temp_ddl='url text, url_hash char(32), domain_label text',
        columns=['url', 'url_hash', 'domain_label'],
        insert_sql="""
            INSERT INTO web_resources (url, url_hash, domain_id)
            SELECT s.url, s.url_hash, d.id
            FROM _wr_stage s LEFT JOIN domains d ON d.value = s.domain_label
            ON CONFLICT (url_hash) DO UPDATE SET
                url = EXCLUDED.url,
                domain_id = COALESCE(EXCLUDED.domain_id, web_resources.domain_id)""",
        sort_by='md5(url)')  # url_hash order


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
    _analyze(session, 'domains')
    staged, written = _copy_load(
        session, 'normalized_citations', filepath, target='normalized_citations',
        source_sql="SELECT normalized_sha1, reference_normalized, appears_on_domain, appears_on_page_id, "
                   "citation_type FROM src WHERE normalized_sha1 IS NOT NULL",
        temp_table='_nc_stage',
        temp_ddl='normalized_sha1 char(40), reference_normalized text, appears_on_domain text, '
                 'appears_on_page_id integer, citation_type text',
        columns=['normalized_sha1', 'reference_normalized', 'appears_on_domain', 'appears_on_page_id',
                 'citation_type'],
        # rows with an unknown domain, no page id or no citation_type are skipped
        insert_sql="""
            INSERT INTO normalized_citations
                (normalized_sha1, reference_normalized, appears_on_domain_id, appears_on_page_id, citation_type)
            SELECT s.normalized_sha1, s.reference_normalized, d.id, s.appears_on_page_id, s.citation_type
            FROM _nc_stage s JOIN domains d ON d.value = s.appears_on_domain
            WHERE s.appears_on_page_id IS NOT NULL AND s.citation_type <> ''
            ON CONFLICT (normalized_sha1) DO UPDATE SET
                reference_normalized = EXCLUDED.reference_normalized,
                appears_on_domain_id = EXCLUDED.appears_on_domain_id,
                appears_on_page_id = EXCLUDED.appears_on_page_id,
                citation_type = EXCLUDED.citation_type""",
        sort_by='normalized_sha1')
    if staged != written:
        log(f"normalized_citations: warning: {staged - written} rows skipped "
            f"(unknown domain, missing page id or citation_type)")


def load_citation_instances(session, staging_dir):
    """Resolves ``normalized_sha1`` → ``normalized_id`` and the staged domain
    → ``domain_id``. The id is the staged ``citation_instance_id`` (which
    Stage 1 computed and wrote into ``citation_history``); it is re-derived
    here and must agree."""
    filepath = find_deduped_parquet(staging_dir, 'citation_instances')
    if not filepath:
        return
    log(f"citation_instances: loading from {filepath}")
    _analyze(session, 'domains', 'normalized_citations', vacuum=True)
    computed = instance_id_sql('domain', 'page_id', 'raw_sha1')
    staged, written = _copy_load(
        session, 'citation_instances', filepath, target='citation_instances', defer_fks=True,
        source_sql=f"""
            SELECT CASE WHEN citation_instance_id IS NOT NULL AND citation_instance_id <> computed
                        THEN error('citation_instance_id mismatch for (' || domain || ', ' || page_id || ', '
                                   || raw_sha1 || '): staged ' || citation_instance_id || ', computed ' || computed)
                        ELSE computed END AS id,
                   domain, page_id, raw_sha1, normalized_sha1,
                   COALESCE(reference_type, 0) AS reference_type, reference_name
            FROM (SELECT *, {computed} AS computed FROM src)""",
        temp_table='_ci_stage',
        temp_ddl='id bigint, domain text, page_id integer, raw_sha1 char(40), normalized_sha1 char(40), '
                 'reference_type smallint, reference_name text',
        columns=['id', 'domain', 'page_id', 'raw_sha1', 'normalized_sha1', 'reference_type', 'reference_name'],
        # rows without a matching normalized citation or domain are skipped
        insert_sql="""
            INSERT INTO citation_instances
                (id, domain_id, page_id, raw_sha1, normalized_id, reference_type, reference_name)
            SELECT s.id, d.id, s.page_id, s.raw_sha1, n.id, s.reference_type, s.reference_name
            FROM _ci_stage s
            JOIN domains d ON d.value = s.domain
            JOIN normalized_citations n ON n.normalized_sha1 = s.normalized_sha1
            ON CONFLICT (id) DO UPDATE SET
                normalized_id = EXCLUDED.normalized_id,
                reference_type = EXCLUDED.reference_type,
                reference_name = EXCLUDED.reference_name""",
        index_lookups=True,
        # the order of uix_ci_domain_page_raw, the largest index; the 8-byte
        # primary key and the normalized_sha1 lookups stay random but small
        sort_by='domain, page_id, raw_sha1')
    if staged != written:
        log(f"citation_instances: warning: {staged - written} rows skipped "
            f"(no matching normalized citation or unknown domain)")


def load_ncwr(session, staging_dir):
    """Load normalized_citation_web_resources. Resolves normalized_sha1 -> normalized_id and url -> web_resource_id."""
    filepath = find_deduped_parquet(staging_dir, 'ncwr')
    if not filepath:
        return
    log(f"ncwr: loading from {filepath}")
    _analyze(session, 'normalized_citations', 'web_resources', vacuum=True)
    staged, written = _copy_load(
        session, 'ncwr', filepath, target='normalized_citation_web_resources', defer_fks=True,
        # URLs resolve through url_hash, the indexed unique key (there is no index on url)
        source_sql="SELECT normalized_sha1, md5(url) AS url_hash FROM src "
                   "WHERE normalized_sha1 IS NOT NULL AND url IS NOT NULL",
        temp_table='_ncwr_stage', temp_ddl='normalized_sha1 char(40), url_hash char(32)',
        columns=['normalized_sha1', 'url_hash'],
        insert_sql="""
            INSERT INTO normalized_citation_web_resources (normalized_id, web_resource_id)
            SELECT n.id, w.id
            FROM _ncwr_stage s
            JOIN normalized_citations n ON n.normalized_sha1 = s.normalized_sha1
            JOIN web_resources w ON w.url_hash = s.url_hash
            ON CONFLICT DO NOTHING""",
        index_lookups=True,
        # normalized_citations were inserted in normalized_sha1 order, so their
        # ids, and this table's primary key, grow with it
        sort_by='normalized_sha1, url')
    if staged != written:
        log(f"ncwr: {staged - written} rows not written (unresolved citation or URL, or already present)")


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
    global BATCH_SIZE, LOAD_JOBS, DEFER_FKS
    parser = argparse.ArgumentParser(description='Load staged Parquet files into PostgreSQL')
    parser.add_argument('-d', '--staging-dir', default=os.environ.get('STAGING_DIR', './staging'),
                        help='Staging directory (default: STAGING_DIR env or ./staging)')
    parser.add_argument('--parquet-dir', default=os.environ.get('PARQUET_DIR'),
                        help='Parquet tier base for template_params / template_param_index (default: PARQUET_DIR); '
                             'required for the template_data phase')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                        help=f'Rows per INSERT batch (default: {BATCH_SIZE})')
    parser.add_argument('--jobs', type=int, default=LOAD_JOBS,
                        help='Connections loading web_resources, normalized_citations, citation_instances '
                             f'and ncwr in parallel, one Parquet row group per unit (default: LOAD_JOBS or {LOAD_JOBS})')
    parser.add_argument('--defer-fks', action=argparse.BooleanOptionalAction, default=DEFER_FKS,
                        help='Drop the foreign keys of citation_instances / normalized_citation_web_resources '
                             'when the table is empty at the start of its phase, and add and validate them '
                             'after it (default: on; LOAD_DEFER_FKS=0 turns it off)')

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
    if args.jobs < 1:
        raise SystemExit("--jobs must be at least 1")
    LOAD_JOBS = args.jobs
    DEFER_FKS = args.defer_fks
    log(f"jobs {LOAD_JOBS}, defer foreign keys {DEFER_FKS}")

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
