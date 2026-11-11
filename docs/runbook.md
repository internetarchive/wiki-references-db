# Runbook — building and serving the Wikipedia Citations Database

How a build runs end to end, what each step owns, how to re-run part of it,
and how the API is served and refreshed. The stage numbers are the design's
(`docs/design/proposed_extractor.md`, `proposed_matching.md`,
`proposed_data_integrity.md`); every command is `uv run wcd <command>` from the
repository root with `.env` filled in (`example.env`).

## Environment

| Variable | Used by | Meaning |
|---|---|---|
| `DATABASE_URL` (or `DB_*`) | every Postgres step, the API | the serving database |
| `PARQUET_DIR` | Stages 1, 3–8, the API | the Parquet tier: `<dataset>/domain=<d>/page_bucket=<n>/` |
| `STAGING_DIR` | Stages 0, 1, 3 | per-shard staging Parquet, consumed by `dedup`/`load`, disposable afterwards; Stage 0's default `--work-dir` |
| `STAGE0_MEMORY_LIMIT` | Stage 0 | DuckDB memory limit for `stage0-load` (default for `--memory-limit`) |
| `DEDUP_ROWS_PER_PARTITION` | Stage 3 | staged rows per key partition in `dedup` (memory follows it) |
| `LOAD_JOBS`, `LOAD_DEFER_FKS`, `LOAD_KEEP_SORTED` | Stage 3 | connections for the citation tables in `load` (`--jobs`; one Parquet row group per unit); deferred foreign keys for `citation_instances` / `ncwr` (`--defer-fks`, default on); keep the key-sorted copies after a successful load |
| `LOAD_MEMORY_LIMIT`, `LOAD_ROWS_PER_PARTITION`, `TEMPLATE_COUNT_MIN` | Stage 3 | DuckDB memory limit and join partition size for the template Parquet written by `load`; the citation count from which a template value's report total is precomputed |
| `PARTS_JOBS`, `PARTS_UNIT_IDS` | Stage 4 | worker processes for `parts` (`--jobs`) and normalized-citation ids per unit |
| `DUCKDB_MEMORY_LIMIT`, `DUCKDB_THREADS`, `DUCKDB_TEMP_DIRECTORY` | Stages 4.6–8, gates | per DuckDB connection, so per worker in `match` / `revision-stats`; unset, DuckDB takes 80 % of RAM and every core per connection |
| `DENORM_WORK_MEM`, `DENORM_MAX_LINE_SIZE`, `DENORM_GROUPS` | Stage 4.6 / 7c | Postgres `work_mem` for the parallel export (256MB: 1GB took enwiki past RAM); longest CSV row DuckDB accepts (64 MB); bucket groups of the repartition (16) |
| `MATCH_PAGE_SLICES` | Stage 6 | page slices a bucket is matched in (8; memory follows the slice) |
| `REVISION_BUNDLES_DIR` | Stage 1 | the `.mwrev.zst` bundles, one directory per domain |
| `API_*`, `WIKIPEDIA_API_*` | the API | CORS, default domain, pool size, User-Agent for title lookups |

Postgres 16 and Python ≥ 3.10 with `uv sync`; Rust ≥ 1.85 only to build
RevisionChest (`cargo build --release` in `rust/revisionchest`; CI also
publishes the Linux binary as a workflow artifact).

**Postgres settings for a build.** The defaults are sized for a laptop. On the
28-core, 62 GB build VM these were used (in a `conf.d` file):

```
shared_buffers = 16GB              # restart
wal_buffers = 256MB                # restart; the important one for Stage 0
effective_cache_size = 40GB
maintenance_work_mem = 4GB         # index builds, REINDEX
max_wal_size = 32GB
checkpoint_timeout = 30min
max_parallel_workers_per_gather = 8
max_parallel_maintenance_workers = 8
```

`wal_buffers` matters most: with the 16 MB default, the parallel Stage-0
revision load spent its time waiting on `WALInsert`/`WALWrite` (163k rows/s
on 16 connections against 211k with 256 MB).

RevisionChest `build` holds one dump file per job: about 300 MB RSS each
(the 128 MiB zstd window of its bundle encoder included), and about 1.5 cores,
since 7z decoding runs on its own thread. On enwiki-20261001 the 7z set
(240.7 GB, 980 files) gave 90.2 GB of namespace-0 bundles with the 128 MiB
window (201.7 GB before it) and 49.6 GB of metadata in one Parquet file, in
2 h 08 min with `build -d -j 18` on 28 cores (45 core-hours, 3.8 GB peak
RSS); see the plan, §17.

## Full build, one domain

```
# 0  dumps → bundles + metadata (RevisionChest)
RevisionChest build -d /dumps/enwiki -o $BUNDLES/en.wikipedia.org \
    --parquet $META/en.wikipedia.org/enwiki-YYYYMMDD.parquet --domain en.wikipedia.org
#    (--postgres writes revisions / page_metadata straight into the schema as well)

# schema
uv run wcd init-db --no-indexes

# 0  metadata → Postgres + revisions Parquet; gate 0  (see "Stage 0 load" below)
uv run wcd stage0-load --domain en.wikipedia.org --metadata $META/en.wikipedia.org \
    --jobs 16 --work-dir $STAGING

# 1  bundles → staging + citation_history / named_ref_uses / revision_signals
uv run wcd extract-all -d $BUNDLES/en.wikipedia.org -o $STAGING --domain en.wikipedia.org --jobs 8

# 3  dedup (instance-id collision gate) → load → gate 3 → indexes
uv run wcd dedup -d $STAGING
uv run wcd load -d $STAGING --parquet-dir $PARQUET_DIR --jobs 16
uv run wcd gate 3 --domain en.wikipedia.org
# secondary indexes of the loaded tables only; each later table's after its own stage
uv run wcd init-db --add-indexes --table web_resources citation_instances revisions

# 4  parts (gate 4), 4.6 denorm export; 5 ref_name_links (gate 5)
export DUCKDB_MEMORY_LIMIT=16GB DUCKDB_TEMP_DIRECTORY=$STAGING/.duck-spill   # Stages 4.6–8 (see below)
uv run wcd parts --domain en.wikipedia.org --jobs 24
uv run wcd init-db --add-indexes --table normalized_citation_parts
uv run wcd denorm-export --domain en.wikipedia.org
uv run wcd ref-name-links --domain en.wikipedia.org
uv run wcd init-db --add-indexes --table ref_name_links

# 6  matching per bucket + cross-page (gate 6); per worker: DuckDB 3 GB, 2 threads
DUCKDB_MEMORY_LIMIT=3GB DUCKDB_THREADS=2 uv run wcd match --domain en.wikipedia.org --jobs 9

# 7  materialization 7a–7h (gate 7);  8  revision stats (gate 8)
uv run wcd materialize --domain en.wikipedia.org
uv run wcd revision-stats --domain en.wikipedia.org --jobs 8
```

`scripts/run_pipeline.sh <bundles> <metadata> [domain] [jobs]` runs exactly
this sequence with per-step timings and a memory sampler (it is how the
rehearsal shard is run). Each command ends with its gate and exits non-zero on
failure; a failed gate means *stop and look*, not retry. `wcd gate <0|3|4|5|6|7|8> --domain D`
re-runs one on demand. The ci fixture shard runs this whole sequence in
`packages/pipeline/tests/test_fixture_ci.py`; it is the rehearsal for any
change to the order above.

### Stage 0 load: parallelism, memory, disk

`wcd stage0-load` loads pages, then revisions, then writes the `revisions/`
Parquet dataset and runs gate 0:

* **Pages** (`page_metadata` and the article `web_resources` rows) are
  `COPY`'d in 100k-row batches and upserted with set-based SQL, one commit per
  batch.
* **Revisions** are sorted by `rev_id` once by DuckDB into a Parquet file in
  `--work-dir` (default `STAGING_DIR`, else the current directory). Each 1M-row
  row group is a unit with its own `rev_id` range, loaded by one of `--jobs`
  connections (default 8) and committed on its own. **A failed load is resumed
  by re-running the command**: finished units are skipped by `ON CONFLICT DO
  NOTHING`. The WAL is the shared limit: on a 20M-revision test slice with
  the default `wal_buffers`, 16 jobs were barely faster than 8 (163k against
  152k rows/s); the enwiki load used 16 with 256 MB. More was not measured.
* After a load into an **empty** `revisions` table, the primary key is
  rebuilt with `REINDEX` (parallel inserts leave it half full: 31 GB against
  19 GB). This blocks writes to `revisions` while it runs; incremental loads
  skip it.
* The `revisions/` dataset is deduplicated by `rev_id` only when the metadata
  has duplicates (overlapping `sync` files); an exact count decides, in seconds.

**Memory.** DuckDB's sorts take up to 80 % of the RAM it can see, on top of
Postgres' `shared_buffers`, with no swap to fall back on. Set
`--memory-limit` (or `STAGE0_MEMORY_LIMIT`), which applies to each of the
loader's DuckDB connections. The process runs ~5–7 GB above it (Python,
Arrow, allocations outside DuckDB's buffer manager): on enwiki the Parquet
step peaked at 27.2 GB RSS with `--memory-limit 20GB` (476 s, against 395 s
with ~29 GB). Leave the rest of RAM to Postgres and the OS cache.

Alternatively, or as a hard backstop, run the load in a memory-capped scope;
DuckDB reads the cgroup limit and sizes itself to 80 % of it:

```
sudo systemd-run --scope --uid=$USER --gid=$(id -g) -p MemoryMax=36G \
    --setenv=PATH=$PATH --setenv=HOME=$HOME --working-directory=$PWD -- \
    bash -c 'set -a; . ./.env; set +a; exec uv run wcd stage0-load …'
```

With `MemoryMax=36G` DuckDB limits itself to 28.7 GiB; the enwiki load peaked
at 37 GB RSS (DuckDB plus Python and Arrow).

**Disk.** `--work-dir` holds the sorted file (~19 bytes per revision) and
DuckDB's spill for both the revision sort and the Parquet step, each in its
own temporary directory removed afterwards: 45–50 GB at the peak for enwiki
(more with a lower memory limit). `revisions` itself is ~100 bytes per
revision (enwiki: 70 GB heap and 19 GB primary key).

**Time, enwiki-20261001** (19.3M pages, 895.6M namespace-0 revisions,
`--jobs 16`, 28 cores, settings above): 1 h 31 min end to end — pages 14 min,
revision sort 6 min, inserts 48 min, key rebuild 11 min, Parquet 8 min,
gate 2 min. The single-connection loader before it took 8 h 47 min.

### Stage 1 memory and parallelism

**Size and time, enwiki-20261001** (namespace 0, 21.6 TB of revision text,
980 bundles; measured, plan §19): 45 h 17 min with `extract-all -j 28` on 28
cores, 1,186 core-hours; 78.5 GB of staging and 33.3 GB of Parquet tier
(27.8 billion `citation_history` rows). Workers use up to ~2 GB RSS on the
heaviest pages; all 28 together peaked at 39.5 GB, so give the run ~40 GB
beyond Postgres (a `systemd-run --scope -p MemoryMax=…` cap confines an
out-of-memory kill to one worker, whose shard the next run redoes). Staging holds each entity row once per page
per shard (repeats within a page are skipped at extraction), so `dedup` reads
little.

An extraction worker's memory is bounded by the batch it holds in flight:
`--batch-size` revisions *or* `--batch-bytes` of revision text (`BATCH_BYTES`,
default 32 MiB), whichever fills first. The byte cap is what matters on pages
with long histories — a 1000-revision batch of a 150 KB article is 150 MB of
text that expands roughly tenfold into extracted references, which is how a
worker reached 3.3 GB and was OOM-killed on the rehearsal shard; with the cap
the same shard runs at ~0.6 GB per worker. Budget about 20× `BATCH_BYTES` per
worker, plus up to 128 MiB for the bundle decoder's window (RevisionChest
writes bundles with a 128 MiB long-distance window; reading an enwiki bundle
measured ~145 MB RSS against ~40 MB for the older 2 MiB-window bundles), plus
the DuckDB re-partition at the end of each shard.

Stage 1 parallelises across bundles, one worker per bundle, so a single
large bundle (one dump file, or a fixture built from one export) runs on one
core; `scripts/split_bundle.py bundle.mwrev.zst -n 8 -o pieces/` cuts it on
page boundaries first. A worker that crashes or is killed leaves
`STARTED.txt` + `FAILED.txt` and no `DONE.txt`; `extract-all` reports it,
exits non-zero, and the next run clears and redoes that shard only.

### Stage 3 load: parallelism, order, disk

`wcd load` loads `web_resources`, `normalized_citations`, `citation_instances`
and `ncwr` with COPY and `INSERT … SELECT` on `--jobs` connections
(`LOAD_JOBS`). Each unit is one row group of a copy of the deduped file sorted
by the table's random key, written to `$STAGING_DIR/.load-sort/` and deleted
after a successful load. Budget disk for the largest sorted copy plus its spill
(~150 GB for enwiki's `normalized_citations`). A failed or killed load keeps
its copy and the next load reuses it (the copy is named after its source's
size and mtime, so a re-deduped file is sorted again); delete
`$STAGING_DIR/.load-sort/` to reclaim the space. Units commit on their own, so
a re-run upserts the committed ones again.

`citation_instances` and `ncwr` load into empty tables without their foreign
keys (`--defer-fks`, default on), which are then added `NOT VALID` and
validated in one pass each. Checked row by row, each check locks a row of the
68 GB `normalized_citations` heap, and the load stalls. The keys are restored
after every load, also after an interrupted one, and gate 3 fails while any is
missing or unvalidated. `normalized_citations` keeps its key; deferring it
saved nothing.

Measured on enwiki-20261001 with 16 jobs (plan §22), phase by phase; `wcd load`
end to end ≈ 6 h 25 min (sum of the phases, plus ~5 min of VACUUM before
`citation_instances`):

| phase | rows | time |
|---|---|---|
| `domains` | 7.5M | 6–8 min |
| `web_resources` (cited) | 108M | 20 min (2 min sort) |
| `normalized_citations` | 285M | 1 h 42 min (43–48 min sort) |
| `citation_instances` | 338M | 1 h 28 min (10 min sort, 9.4 min validation) |
| `ncwr` | 290M | 49 min (11 min sort, 13.4 min validation) |
| template Parquet + `template_value_counts` | 2.5B → 1.37B / 6.44B | 1 h 51 min |

The disk was the limit throughout (WAL, then checkpoints); give Postgres the
runbook's `max_wal_size` or more, and watch `pg_stat_bgwriter.checkpoints_req`.

## What each step owns, and the unit of re-run

| Step | Writes | Replaces | Re-run unit |
|---|---|---|---|
| RevisionChest `build` | bundles, metadata file | — (new files) | dump file |
| `stage0-load` | `domains`, `revision_bundles`, `revisions`, `page_metadata`, article `web_resources`; `revisions/` Parquet | Postgres rows upserted / `ON CONFLICT DO NOTHING`, committed per page batch and per `rev_id` unit (an interrupted load resumes on re-run); the domain's `revisions/` partitions rebuilt from **everything under `--metadata`** | domain |
| `extract` (`extract-all`) | staging files of the shard; `citation_history/`, `named_ref_uses/`, `revision_signals/` files named after the shard | that shard's files only (`<shard>-<i>.parquet` in every bucket) | shard (one bundle) |
| `dedup` | `$STAGING/deduped/*.parquet` | per table (`.done` markers; delete `deduped/` to redo) | staging set |
| `load` | `containers`, `domains`, `web_resources` (cited), `wiki_templates`, `normalized_citations`, `citation_instances`, `normalized_citation_web_resources`, `template_value_counts`; Parquet `template_params/`, `template_param_index/` | upserts — idempotent, the citation tables committed per Parquet row group (an interrupted load resumes on re-run); the template datasets' domain partitions and that domain's `template_value_counts` replaced | staging set |
| `parts` | `normalized_citation_parts` | DELETE + INSERT per batch; `--resume` skips citations that already have parts | domain (or all) |
| `denorm-export` | `citation_instances_denorm/` | the domain's partitions | domain |
| `ref-name-links` | `ref_name_links` table and Parquet | the domain | domain |
| `match` | `matching_intermediate/*` | bucket directories with a `_SUCCESS` marker are skipped unless `--force`; `--buckets 3,17` selects | bucket (6a–6c), domain (6d) |
| `materialize` | `global_sources`, `citation_source_resolution`, resolved `citation_instances_denorm/`, `page_source_summary`, counts, scores, `global_source_scores/`, `page_summary_cache` | the domain, step by step; 7d/7h checkpoint per bucket under `$PARQUET_DIR/_checkpoints/` and resume; `--steps a,b` selects; `--force` ignores checkpoints | domain |
| `revision-stats` | `page_revision_stats/` | bucket directories | bucket |

Rules of thumb:

* **A new or re-extracted bundle**: re-run `extract` for that shard (it
  replaces its own files), then `dedup` (after deleting `$STAGING/deduped/`),
  `load`, and everything from `parts` on for the domain. Stages 4–8 are
  derived data; rebuilding them for the domain is the simple, safe path.
* **A matcher change** (anything in `wcd_semantic`): `match --force` for the
  domain, then `materialize` and `revision-stats`. Parts are untouched.
* **A normalizer change** (`reftag` / `template` / `content`): `parts`, then
  `denorm-export`, `match --force`, `materialize`, `revision-stats`.
* **Crash mid-step**: just re-run the command. `materialize` resumes 7d/7h at
  the first unfinished bucket; `match` skips finished buckets; `parts --resume`
  skips finished citations; everything else replaces its output whole.
* **Several domains** run one after another with the same commands; the
  per-domain outputs are independent except `global_sources` (upserted) — see
  the cross-domain note in the plan (§10).

## Incremental updates (RevisionChest `sync`)

`RevisionChest sync` appends recent changes to daily bundles and writes a
metadata file per run into the `--parquet` directory. Treat each day's bundle
as a new shard: `stage0-load` on the metadata *directory* (every revision
once, overlapping batches included), `extract` on the new bundle, then the
domain re-run from `dedup` onward. Incremental matching (re-running only the
buckets a new bundle touched) is designed but not built; a full domain re-run
of Stages 4–8 is the current refresh.

## Serving

```
uv run wcd-api --host 0.0.0.0 --port 8000      # /api/v2, /api/v1 aliases, the dashboard at /
```

The service reads Postgres through a small pool and the Parquet tier one
bucket partition per request; it needs read access to `$PARQUET_DIR` and no
DuckDB extension. `/healthz` is the liveness check; `/openapi.json` the
contract.

**Refreshing under a running service.** Postgres steps are transactional per
batch, so the API sees each table's new rows as they commit (a page can be
briefly inconsistent between 7d and 7h; the summary cache is written last).
Parquet datasets are replaced partition by partition; a request that reads a
partition while DuckDB's `COPY` rewrites it can see a partial directory. For
a clean swap, build into a fresh `PARQUET_DIR`, then point the service at it
and restart (the service holds no DuckDB state between requests, so a restart
is all that is needed — nothing to re-initialise).

**Serving during a full rebuild.** Build into a second database and a second
Parquet directory, run the gates, then switch `DATABASE_URL` / `PARQUET_DIR`
and restart the service. The v1 explorer (`legacy/`) stays on its own
database until the cutover.

## Checks before announcing a build

1. Every gate passed (they print `Stage N gate: pass`).
2. `SELECT count(*) FROM global_sources WHERE verdict IS NULL` is 0 (7f ran).
3. `page_summary_cache` has one row per page in `revisions` for the domain
   (the Stage-7 gate checks this).
4. Spot-check three pages in the dashboard: summary, timeline, top sources,
   "Show related pages" on a multi-page source.
