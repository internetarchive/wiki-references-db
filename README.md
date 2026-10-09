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
  pipeline/     `wcd_pipeline` and the `wcd` CLI: the build stages.
rust/
  revisionchest/  git submodule → internetarchive/RevisionChest
                  Stage 0: XML dumps → .mwrev.zst revision bundles + metadata.
upstream/
  uva/          git submodule → scatter-llc/UVAWikipediaCitationsDatabase
                Reference checkout of the UVA pipeline while its normalizers,
                matchers and scorer are ported into packages/semantic (removed after).
legacy/         the v1 Flask API and explorer, frozen; served until v2 cutover.
fixtures/       committed test inputs and golden outputs (see fixtures/README.md).
docs/design/    the plan: proposed_*.md, decisions, executive summary.
scripts/        one-off tooling (fixture page selection).
```

Planned, not yet present: `packages/semantic` (ported UVA logic), `apps/api`
(FastAPI v2), `apps/frontend` (the dashboard). See the implementation plan.

## Setup

Requires Python ≥ 3.10 and [uv](https://docs.astral.sh/uv/). Rust/cargo only
if you build RevisionChest.

```
git clone --recurse-submodules https://github.com/internetarchive/wiki-references-db
cd wiki-references-db
uv sync                 # installs every workspace package (editable) + pytest
cp example.env .env     # fill in DATABASE_URL (or DB_*), STAGING_DIR, …
uv run pytest           # tests across the workspace
```

## Pipeline

```
Stage 0  RevisionChest            XML dump  → .mwrev.zst bundles + page/revision metadata
Stage 1  wcd extract-all          bundles   → staging Parquet (references, normalized text, history)
Stage 3  wcd dedup                staging   → deduped Parquet
Stage 3  wcd load                 deduped   → Postgres
```

```
uv run wcd init-db --no-indexes
uv run wcd extract-all -d /path/to/bundles -o ./staging --jobs 4
uv run wcd dedup -d ./staging
uv run wcd load -d ./staging
uv run wcd init-db --add-indexes
```

`wcd <command> --help` lists each stage's options; they are unchanged from
the former `build_all.py` / `build_db.py` / `dedup_parquet.py` /
`load_all.py` / `init_db.py` / `purge.py` scripts. Environment variables are
documented in `example.env`.

`wcd fixture-dump` filters full-history XML dumps down to a page list, to
produce the input for the fixture bundle (`fixtures/README.md`).

## Data model (current)

* Articles are identified by domain and page id. Reference strings are
  normalized (`citation_normalizer`) before hashing; `normalized_sha1`
  identifies a reference's content, `raw_sha1` the exact text on a page.
* `normalized_citations` holds one row per distinct normalized reference;
  `citation_instances` one per (page, raw text); `citation_history` records
  which revisions each instance appears in; `template_data` stores template
  parameters for search; `web_resources` the cited URLs.
* The semantic layer (citation parts, source matching, reliability scores,
  per-page summaries) is specified in `docs/design/proposed_data_model.md`
  and is being built on top of this.

## Working with the submodules

Each submodule is its own repository with its own history. Make changes
inside it, commit there, then commit the updated pointer here:

```
cd packages/extractor && git checkout -b my-change && … && git commit
cd ../.. && git add packages/extractor && git commit -m "extractor: …"
```

`git submodule update --init --recursive` after pulling.
