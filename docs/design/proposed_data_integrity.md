# Proposed Data Integrity & Error Handling Strategy

*Revised 2026-09-10 per `claude/decisions_2026-09-10.md`.*

Stage numbering: 0 RevisionChest; 1–5 (`proposed_extractor.md`); 6a–6d, 7a–7h (`proposed_matching.md`); 8 (`proposed_page_revision_stats.md`).

---

## 1. Design Principles

1. **Idempotency.** Every stage is safe to re-run; a re-run fully replaces its output.
2. **Stage isolation.** Each stage reads only its declared inputs and writes only its declared outputs.
3. **Fail-fast.** A failed stage halts the pipeline before downstream consumption.
4. **Bucket granularity.** `(domain, page_bucket)` — `page_bucket = page_id % 512` — is the unit of parallelism, checkpointing and re-run for every page-partitioned step.

---

## 2. Idempotency by Stage

### Parquet-output stages (1, 3-export, 4.6, 5-export, 6a–6d, 7c, 7g, 8)

| Stage | Output | Overwrite unit |
|---|---|---|
| 1 | `citation_history/`, `named_ref_uses/`, `revision_signals/` (per shard files inside bucket dirs) | shard (`build_all.py` DONE/STARTED markers, existing) |
| 3 | `revisions/` | domain |
| 5 | `ref_name_links/` | domain |
| 4.6, 7c | `citation_instances_denorm/` | domain (`COPY … PARTITION_BY … OVERWRITE_OR_IGNORE`) |
| 6a–6c | `matching_intermediate/{sources,citation_source_map,template_sources,template_citation_source_map,merged_sources,merged_citation_source_map}/` | bucket |
| 6d | `matching_intermediate/global_source_map/`, `global_sources_stage.parquet` | global |
| 7g | `global_source_scores/` | global |
| 8 | `page_revision_stats/` | bucket |

Stage 1 is the one exception to "delete and rewrite": each shard writes its own files into the bucket directories (`domain=…/page_bucket=…/{shard}-{n}.parquet`), so shard re-runs delete only that shard's files (as `build_all.py` already does for incomplete shards).

### Postgres-output stages (3-load, 4, 5, 7a, 7b, 7e, 7f)

Single transaction per step (or per load batch), `DELETE`/`TRUNCATE` scope + `INSERT`/`UPDATE`; rollback on failure. 7f's two `UPDATE`s in one transaction.

### Cross-engine stages (7d, 7h: DuckDB → Postgres)

DuckDB's Postgres ATTACH auto-commits each statement. Strategy: **batch-and-checkpoint per bucket.**

1. Enumerate buckets from the `citation_instances_denorm` partition layout.
2. Per bucket: `DELETE FROM pg.<table> WHERE domain_id = :d AND page_id % 512 = :b`, then one `INSERT … SELECT` reading only that bucket's Parquet partitions.
3. Record completed buckets in `materialization_checkpoint_<stage>.json`; on retry skip completed buckets, re-run the incomplete one from its `DELETE`.
4. Delete the checkpoint on success.

```python
def materialize_by_bucket(stage, domain, domain_id, pg_dsn, parquet_base, build_sql):
    completed = load_checkpoint(stage)
    con = duckdb.connect()
    con.execute(f"ATTACH '{pg_dsn}' AS pg (TYPE POSTGRES)")
    for bucket in list_buckets(parquet_base, "citation_instances_denorm", domain):
        key = f"{domain}/{bucket}"
        if key in completed:
            continue
        bind_bucket_views(con, parquet_base, domain, bucket,
                          ("citation_instances_denorm", "citation_history", "revisions", "named_ref_uses", "ref_name_links"))
        con.execute(f"DELETE FROM pg.{TARGET[stage]} WHERE domain_id = {domain_id} AND page_id % 512 = {bucket}")
        con.execute(build_sql(domain_id))
        completed.add(key); save_checkpoint(stage, completed)
    clear_checkpoint(stage)
```

Between a bucket's `DELETE` and `INSERT` its pages have no rows; acceptable for a batch rebuild (the API is not served from a database mid-build).

---

## 3. Retry Strategy

| Failure | Behaviour | Recovery |
|---|---|---|
| Parquet read error | stage crashes | re-run (idempotent) |
| ATTACH connection lost | `duckdb.IOException` | retry 3× (5s/15s/45s) |
| Postgres failure mid-transaction (3, 4, 5, 7a, 7b, 7e, 7f) | rolled back | re-run step |
| Postgres failure mid-bucket (7d, 7h) | checkpoint keeps completed buckets | re-run; resumes |
| `COPY` interrupted (Parquet stages) | partial directory | re-run stage/bucket |

Permanent failures (schema mismatch, constraint violation, disk full, **instance-id collision**) are not retried; the stage logs full context and exits non-zero.

`retry_transient` decorator as before (`duckdb.IOException`, `psycopg2.OperationalError`, `ConnectionError`, `TimeoutError`).

---

## 4. Inter-Stage Validation Gates

| After | Check | Blocks |
|---|---|---|
| Stage 0 | `revisions` has rows for the domain; `page_metadata` has a row for every `(domain_id, page_id)` in `revisions`; `revision_timestamp` is `TIMESTAMP` | 1 |
| Stage 3 dedup | **Instance-id collision gate**: `COUNT(DISTINCT id) = COUNT(DISTINCT (domain, page_id, raw_sha1))` over staged `citation_instances`; abort on mismatch (switch id to UUID before rebuilding) | load |
| Stage 3 load | `citation_instances`, `normalized_citations` non-empty; no NULL `citation_type`; every `citation_history` `citation_instance_id` (sampled per bucket) exists in `citation_instances`; `revisions` Parquet row count = `pg.revisions` count for the domain | 4 |
| Stage 4 | every `normalized_citations` row has ≥1 part; every `bare_url` / `external_link` part has `extracted_url`; every `template` part has `template_name`; `part_sha1 = sha1(content)` on a sample | 4.6 |
| Stage 4.6 | denorm row count = `citation_instances ⋈ parts` count for the domain | 5, 6 |
| Stage 5 | no overlapping revision ranges per `(domain_id, page_id, ref_name)`; every `defining_instance_id` exists | 6a |
| Stage 6d | all six intermediate datasets exist; `merged_citation_source_map` row count > 0; every `merged_source_id` in `global_source_map` exists in `merged_sources` | 7 |
| 7a | `global_sources` > 0 | 7b |
| 7b | `citation_source_resolution` count ≥ `merged_citation_source_map` count | 7c |
| 7c | resolved denorm: `COUNT(global_source_id IS NOT NULL)` = `citation_source_resolution` count | 7d |
| 7d | no `(domain_id, page_id)` in `citation_source_resolution` missing from `page_source_summary` | 7e |
| 7e | no NULL `n_pages` on `global_sources` referenced by `page_source_summary` | 7f |
| 7f | no NULL `reliability_score` / `verdict` (the UVA scorer never yields NULL) | 7g |
| 7g | `global_source_scores` count = `global_sources` count | 7h, 8 |

### Referential integrity (after Stage 7)

```sql
SELECT COUNT(*) FROM citation_source_resolution csr
  LEFT JOIN global_sources gs USING (global_source_id) WHERE gs.global_source_id IS NULL;   -- 0
SELECT COUNT(*) FROM page_source_summary pss
  LEFT JOIN global_sources gs USING (global_source_id) WHERE gs.global_source_id IS NULL;   -- 0
SELECT COUNT(*) FROM citation_source_resolution csr
  LEFT JOIN normalized_citation_parts p USING (normalized_id, part_index) WHERE p.normalized_id IS NULL; -- 0
SELECT COUNT(*) FROM ref_name_links r
  LEFT JOIN citation_instances ci ON ci.id = r.defining_instance_id WHERE ci.id IS NULL;    -- 0
```

---

## 5. Partial Parquet Write Recovery

Delete the affected output unit (bucket directory, or domain directory for 4.6/7c) and re-run. Never resume a partial `COPY`.

---

## 6. DuckDB ↔ Postgres Connection Management

- One ATTACH per stage; `READ_ONLY` unless the stage writes (7d, 7h).
- DSN from `DATABASE_URL`.
- `SET statement_timeout = '30min'` on the Postgres side for 7a/7b/7e/7f.
- Every Parquet view in a bucketed stage is bound to a single partition path (no globs over the whole dataset inside the loop).

---

## 7. Logging and Observability

Structured JSON lines to stderr: stage, event (start/progress/complete/failure), bucket, row counts, elapsed, truncated query text on failure. Unchanged from the previous version of this document.

---

## 8. Summary of New Code

| Component | Status |
|---|---|
| `retry_transient` | new, ~20 lines |
| `materialize_by_bucket` (7d, 7h) | new, ~40 lines |
| Validation gate functions (§4) | new, ~100 lines |
| Instance-id collision gate in `dedup_parquet.py` | new, ~10 lines |
| Structured logging helper | new, ~10 lines |
| Stage-level DELETE+INSERT | existing pattern (UVA, `load_all.py`) |