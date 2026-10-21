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
| `STAGING_DIR` | Stages 1, 3 | per-shard staging Parquet, consumed by `dedup`/`load`, disposable afterwards |
| `REVISION_BUNDLES_DIR` | Stage 1 | the `.mwrev.zst` bundles, one directory per domain |
| `API_*`, `WIKIPEDIA_API_*` | the API | CORS, default domain, pool size, User-Agent for title lookups |

Postgres 16 and Python ≥ 3.10 with `uv sync`; Rust ≥ 1.85 only to build
RevisionChest (`cargo build --release` in `rust/revisionchest`; CI also
publishes the Linux binary as a workflow artifact).

## Full build, one domain

```
# 0  dumps → bundles + metadata (RevisionChest)
RevisionChest build -d /dumps/enwiki -o $BUNDLES/en.wikipedia.org \
    --parquet $META/en.wikipedia.org/enwiki-YYYYMMDD.parquet --domain en.wikipedia.org
#    (--postgres writes revisions / page_metadata straight into the schema as well)

# schema
uv run wcd init-db --no-indexes

# 0  metadata → Postgres + revisions Parquet; gate 0
uv run wcd stage0-load --domain en.wikipedia.org --metadata $META/en.wikipedia.org

# 1  bundles → staging + citation_history / named_ref_uses / revision_signals
uv run wcd extract-all -d $BUNDLES/en.wikipedia.org -o $STAGING --domain en.wikipedia.org --jobs 8

# 3  dedup (instance-id collision gate) → load → gate 3 → indexes
uv run wcd dedup -d $STAGING
uv run wcd load -d $STAGING
uv run wcd gate 3 --domain en.wikipedia.org
uv run wcd init-db --add-indexes

# 4  parts (gate 4), 4.6 denorm export; 5 ref_name_links (gate 5)
uv run wcd parts --domain en.wikipedia.org
uv run wcd denorm-export --domain en.wikipedia.org
uv run wcd ref-name-links --domain en.wikipedia.org

# 6  matching per bucket + cross-page (gate 6)
uv run wcd match --domain en.wikipedia.org --jobs 8

# 7  materialization 7a–7h (gate 7);  8  revision stats (gate 8)
uv run wcd materialize --domain en.wikipedia.org
uv run wcd revision-stats --domain en.wikipedia.org --jobs 8
```

Each command ends with its gate and exits non-zero on failure; a failed gate
means *stop and look*, not retry. `wcd gate <0|3|4|5|6|7|8> --domain D`
re-runs one on demand. The ci fixture shard runs this whole sequence in
`packages/pipeline/tests/test_fixture_ci.py`; it is the rehearsal for any
change to the order above.

## What each step owns, and the unit of re-run

| Step | Writes | Replaces | Re-run unit |
|---|---|---|---|
| RevisionChest `build` | bundles, metadata file | — (new files) | dump file |
| `stage0-load` | `domains`, `revision_bundles`, `revisions`, `page_metadata`, article `web_resources`; `revisions/` Parquet | Postgres rows upserted / `ON CONFLICT DO NOTHING`; the domain's `revisions/` partitions rebuilt from **everything under `--metadata`** | domain |
| `extract` (`extract-all`) | staging files of the shard; `citation_history/`, `named_ref_uses/`, `revision_signals/` files named after the shard | that shard's files only (`<shard>-<i>.parquet` in every bucket) | shard (one bundle) |
| `dedup` | `$STAGING/deduped/*.parquet` | per table (`.done` markers; delete `deduped/` to redo) | staging set |
| `load` | `containers`, `domains`, `web_resources` (cited), `wiki_templates`, `normalized_citations`, `citation_instances`, `normalized_citation_web_resources`, `template_data` | upserts — idempotent | staging set |
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
