# Wikipedia Citations Database

Monorepo for the third-generation Wikipedia Citations Database (WCD): a
database of the reference strings that appear on Wikipedia articles across
their full revision history, the semantic layer that resolves those strings
to sources, and the API and dashboard that serve them. Development is
supported by the Internet Archive.

This repository was `wiki-references-db`; it now also hosts the pieces that
used to be separate checkouts. The design for the merged system is in
[`docs/design/`](docs/design/) (start with `executive_summary.md` and
`repo_structure_and_implementation_plan_2026_10_08.md`).

## Layout

```
packages/
  normalizer/   git submodule → internetarchive/wiki-references-normalizer
                `citation_normalizer`: stateless canonicalisation of citation wikitext.
                Reusable on its own.
  extractor/    git submodule → internetarchive/wiki-references-extractor
                `refs_extractor`: finds references (ref tags, {{sfn}}, endnotes,
                external links) in wikitext; per-wiki config in wikis.yaml.
  schema/       `wcd_schema`: Postgres models and staging Parquet schemas —
                the single definition of every table and dataset.
  semantic/     `wcd_semantic`: citation classification, bibliographic-field
                and identifier extraction, per-page / cross-page source
                matching, reliability scoring. Pure functions, vendored from
                the UVA Wikipedia Citations Database (see its NOTICE).
  pipeline/     `wcd_pipeline` and the `wcd` CLI: the build stages.
rust/
  revisionchest/  git submodule → internetarchive/RevisionChest, a Cargo workspace:
                  `mwdump` (streaming XML dump reader), `mwrev` (the .mwrev.zst
                  bundle format), `revisionchest-meta` (Parquet / Postgres
                  metadata sinks) and the `RevisionChest` binary (build, sync).
                  Stage 0: XML dumps → bundles + revisions / page_metadata.
legacy/         the v1 Flask API and explorer, frozen; served until v2 cutover.
fixtures/       committed test inputs and golden outputs (see fixtures/README.md).
docs/design/    the plan: proposed_*.md, decisions, executive summary.
docs/uva/       the UVA project's documentation, kept as the reference for packages/semantic.
scripts/        one-off tooling (fixture page selection and history export, extractor sample).
```

`apps/api` (`wcd_api`) is the `/api/v2` FastAPI service over the built data
(`apps/api/README.md`, `docs/design/proposed_api.md`). Planned, not yet
present: `apps/frontend` (the dashboard, Phase 5).

## Setup

Requires Python ≥ 3.10 and [uv](https://docs.astral.sh/uv/). Rust ≥ 1.85
only if you build RevisionChest (`cargo build --release` inside
`rust/revisionchest`).

```
git clone --recurse-submodules https://github.com/internetarchive/wiki-references-db
cd wiki-references-db
uv sync                 # installs every workspace package (editable) + pytest
cp example.env .env     # fill in DATABASE_URL (or DB_*), STAGING_DIR, …
uv run pytest           # tests across the workspace
```

## Pipeline

```
Stage 0  RevisionChest build      XML dump  → .mwrev.zst bundles + page/revision metadata
                                              (--parquet file, and/or --postgres straight into the schema)
Stage 0  wcd stage0-load          metadata  → Postgres revisions/page_metadata + revisions Parquet tier, gate
Stage 1  wcd extract-all          bundles   → staging Parquet (instances, normalized text, URLs, templates)
                                            + Parquet tier: citation_history, named_ref_uses, revision_signals
Stage 3  wcd dedup                staging   → deduped Parquet (+ instance-id collision gate)
Stage 3  wcd load                 deduped   → Postgres;  wcd gate 3
Stage 4  wcd parts                normalized_citations → normalized_citation_parts (UVA normalizers); gate
Stage 4.6 wcd denorm-export       Postgres  → citation_instances_denorm Parquet (unresolved)
Stage 5  wcd ref-name-links       history × revisions × named instances → ref_name_links (Postgres + Parquet); gate
Stage 6  wcd match                 denorm + history → matching_intermediate/* (6a-6c per bucket, 6d per domain); gate
Stage 7  wcd materialize           global_sources, citation_source_resolution, resolved denorm, page_source_summary,
                                   counts, reliability scores, global_source_scores, page_summary_cache (7a-7h); gate
Stage 8  wcd revision-stats        revisions × history × resolved denorm → page_revision_stats Parquet; gate
```

```
uv run wcd init-db --no-indexes
RevisionChest build -d /path/to/dumps -o /path/to/bundles --parquet /path/to/meta/enwiki.parquet
uv run wcd stage0-load --domain en.wikipedia.org --metadata /path/to/meta --parquet-dir ./parquet
uv run wcd extract-all -d /path/to/bundles -o ./staging --domain en.wikipedia.org --parquet-dir ./parquet --jobs 4
uv run wcd dedup -d ./staging
uv run wcd load -d ./staging
uv run wcd gate 3 --domain en.wikipedia.org --parquet-dir ./parquet
uv run wcd init-db --add-indexes
uv run wcd parts --domain en.wikipedia.org
uv run wcd denorm-export --domain en.wikipedia.org --parquet-dir ./parquet
uv run wcd ref-name-links --domain en.wikipedia.org --parquet-dir ./parquet
uv run wcd match --domain en.wikipedia.org --parquet-dir ./parquet --jobs 8
uv run wcd materialize --domain en.wikipedia.org --parquet-dir ./parquet
uv run wcd revision-stats --domain en.wikipedia.org --parquet-dir ./parquet --jobs 8
```

One bundle directory per domain: the bundle format carries no domain, and
`--domain` decides the instance ids, the Parquet partitions and every staged
row. `wcd stage0-load` is idempotent (`ON CONFLICT DO NOTHING` on `revisions`,
upserts elsewhere) and rebuilds the domain's `revisions` Parquet from
everything under `--metadata` — point it at the directory holding all of the
domain's metadata files, overlapping `sync` batches included. Each stage ends
with its gate from `docs/design/proposed_data_integrity.md`; `wcd gate <0|3|4|5|6|7|8>`
re-runs one. `wcd match` runs the vendored UVA matchers (`wcd_semantic`)
unchanged, one process per page bucket, and writes only Parquet
(`matching_intermediate/<name>/domain=/page_bucket=/`, plus the per-domain
`global_source_map` / `global_sources_stage`); a bucket with a `_SUCCESS`
marker is skipped unless `--force`. `wcd materialize` runs 7a–7h in order
(`--steps` selects a subset); DuckDB computes each bucket from the Parquet
tier and psycopg2 writes Postgres, with per-bucket checkpoints for 7d and 7h
under `<parquet>/_checkpoints/`.

Two tiers hold the data: Postgres for the serving tables, and a Parquet
directory (`PARQUET_DIR`) laid out `<dataset>/domain=<d>/page_bucket=<n>/`
with `page_bucket = page_id % 512`. Stage 1 writes its per-revision datasets
straight into that layout (one `<shard>-<i>.parquet` per bucket per bundle;
re-running a bundle replaces its files), so `citation_history` never passes
through dedup or Postgres. `wcd_schema.parquet` defines the datasets and
schemas, `wcd_schema.duckdb.register_views` opens them (and, ATTACHed, the
Postgres tables) in one DuckDB connection, and `wcd_schema.ids` holds the
deterministic identifiers (`citation_instances.id` is the first 8 bytes of
`SHA1("{domain}|{page_id}|{raw_sha1}")`, so every engine can compute it).

`wcd <command> --help` lists each stage's options. Environment variables are
documented in `example.env`.

`wcd fixture-dump` filters full-history XML dumps down to a page list, to
produce the input for the fixture bundle (`fixtures/README.md`).

The end-to-end Postgres test (`packages/pipeline/tests/test_postgres_integration.py`)
runs when `WCD_TEST_DATABASE_URL` points at a scratch database (its `public`
schema is dropped); add `REVISIONCHEST_BIN=…/RevisionChest` to also check the
committed Stage-0 metadata fixture against the binary.

## Data model (current)

* Articles are identified by domain and page id. Reference strings are
  normalized (`citation_normalizer`) before hashing; `normalized_sha1`
  identifies a reference's content, `raw_sha1` the exact text on a page.
* `revisions` (every revision, written at Stage 0) and `page_metadata` carry
  the page's domain; `normalized_citations` holds one row per distinct
  normalized reference with its `citation_type` (`ref_tag`, `template`,
  `bare_url`, `endnote`, `shorthand`, `external_link`); `citation_instances`
  one per (domain, page, raw text) with a deterministic id;
  `citation_history` (Parquet only) records which revisions each instance
  appears in; `named_ref_uses` (Parquet) each self-closing `<ref name=…/>`
  reuse; `template_data` stores template parameters for search;
  `web_resources` the cited URLs.
* `normalized_citation_parts` (Stage 4) holds the UVA normalizers' output per
  semantic part of a citation; `ref_name_links` (Stage 5) maps a page's
  `<ref name>` to its defining instance per revision range; `global_sources`
  (one row per real-world source, with reliability score and verdict),
  `citation_source_resolution` (part → source per page), `page_source_summary`
  (per page and source: history range, counts, reuse) and `page_summary_cache`
  (the pre-computed page summary) are written by Stage 7. The Parquet tier
  adds `citation_instances_denorm`, `global_source_scores` and
  `page_revision_stats` for the DuckDB-served endpoints.

## Working with the submodules

Each submodule is its own repository with its own history. Make changes
inside it, commit there, then commit the updated pointer here:

```
cd packages/extractor && git checkout -b my-change && … && git commit
cd ../.. && git add packages/extractor && git commit -m "extractor: …"
```

`git submodule update --init --recursive` after pulling.
