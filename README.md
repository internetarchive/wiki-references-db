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
scripts/        one-off tooling (fixture page selection).
```

Planned, not yet present: `apps/api` (FastAPI v2), `apps/frontend` (the
dashboard), and pipeline Stages 4–8 wiring `wcd_semantic` to the data. See
the implementation plan.

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
Stage 1  wcd extract-all          bundles   → staging Parquet (references, normalized text, history)
Stage 3  wcd dedup                staging   → deduped Parquet
Stage 3  wcd load                 deduped   → Postgres
```

```
uv run wcd init-db --no-indexes
RevisionChest build -d /path/to/dumps -o /path/to/bundles --parquet /path/to/meta.parquet
uv run wcd stage0-load --domain en.wikipedia.org --metadata /path/to/meta.parquet --parquet-dir ./parquet
uv run wcd extract-all -d /path/to/bundles -o ./staging --jobs 4
uv run wcd dedup -d ./staging
uv run wcd load -d ./staging
uv run wcd init-db --add-indexes
```

`wcd stage0-load` is idempotent (`ON CONFLICT DO NOTHING` on `revisions`,
upserts elsewhere) and ends with the Stage-0 gate: every revision's page has a
`page_metadata` row, `revision_timestamp` is a real `TIMESTAMP`, and the
`revisions` Parquet partition for the domain has the same row count as
Postgres. If RevisionChest already wrote Postgres (`--postgres`), the command
only adds the Parquet copy.

Two tiers hold the data: Postgres for the serving tables, and a Parquet
directory (`PARQUET_DIR`) laid out `<dataset>/domain=<d>/page_bucket=<n>/`
with `page_bucket = page_id % 512`. `wcd_schema.parquet` defines the datasets
and schemas, `wcd_schema.duckdb.register_views` opens them (and, ATTACHed,
the Postgres tables) in one DuckDB connection, and `wcd_schema.ids` holds the
deterministic identifiers (`citation_instances.id` is the first 8 bytes of
`SHA1("{domain}|{page_id}|{raw_sha1}")`, so every engine can compute it).

`wcd <command> --help` lists each stage's options; they are unchanged from
the former `build_all.py` / `build_db.py` / `dedup_parquet.py` /
`load_all.py` / `init_db.py` / `purge.py` scripts. Environment variables are
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
  normalized reference; `citation_instances` one per (domain, page, raw text)
  with a deterministic id; `citation_history` (Parquet only) records which
  revisions each instance appears in; `template_data` stores template
  parameters for search; `web_resources` the cited URLs.
* The semantic layer (`normalized_citation_parts`, `global_sources`,
  `citation_source_resolution`, `page_source_summary`, `page_summary_cache`,
  `ref_name_links`) is defined in `wcd_schema.postgres` per
  `docs/design/proposed_data_model.md`; Stages 4–8 fill it.

## Working with the submodules

Each submodule is its own repository with its own history. Make changes
inside it, commit there, then commit the updated pointer here:

```
cd packages/extractor && git checkout -b my-change && … && git commit
cd ../.. && git add packages/extractor && git commit -m "extractor: …"
```

`git submodule update --init --recursive` after pulling.
