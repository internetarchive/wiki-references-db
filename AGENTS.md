# AGENTS.md — Wikipedia Citations Database (v3 monorepo)

Instructions for coding agents working in this repository. Humans should start
with `README.md`; this file is the short version plus the rules that are easy
to break without noticing.

The WCD extracts every reference string from Wikipedia articles across their
full revision history, resolves those strings to real-world sources, and serves
the result through `/api/v2` and a dashboard. Work happens on the **`v3`** branch.

## Map

| Path | What | Edit here? |
|---|---|---|
| `packages/schema/` (`wcd_schema`) | Postgres models, Parquet dataset schemas, staging schemas, deterministic ids, DuckDB view helpers | Yes. **This package defines every table and dataset.** |
| `packages/pipeline/` (`wcd_pipeline`, the `wcd` CLI) | Build stages 0–8 and their gates | Yes |
| `packages/semantic/` (`wcd_semantic`) | Vendored UVA classifiers, normalizers, matchers, scorer | Only under the vendoring rules in `packages/semantic/NOTICE` |
| `apps/api/` (`wcd_api`, `wcd-api`) | FastAPI `/api/v2` (and `/api/v1` aliases) | Yes |
| `apps/frontend/` | Dashboard: plain HTML/JS/CSS, no build step, served by the API at `/` | Yes |
| `packages/extractor/`, `packages/normalizer/`, `rust/revisionchest/` | **Git submodules** (internetarchive repos) | Commit inside the submodule first; see skill `wcd-submodules` |
| `legacy/` | Frozen v1 Flask API and explorer | **No.** Deleted at v2 cutover |
| `fixtures/` | Committed test inputs and goldens | Only through the regeneration commands in `fixtures/README.md` |
| `docs/design/` | The design (`proposed_*.md`, decisions, implementation plan) | Update when behaviour changes |
| `docs/runbook.md` | Build order, what each step owns, re-run units, serving | Update when a step's outputs or re-run behaviour change |
| `docs/uva/` | Upstream UVA docs, reference for `wcd_semantic` | No |
| `scripts/` | Fixture page selection, history export, extractor sample | Yes |

## Commands

```
uv sync                                   # every workspace member, editable, plus pytest
uv run pytest -q                          # fast tier: ~5 s, no database (DB tests skip)
WCD_TEST_DATABASE_URL=postgresql://…/scratch uv run pytest -q   # + Postgres tier (drops `public`!)
uv run wcd --help                         # stage commands; `wcd <cmd> --help` for options
uv run wcd-api --port 8000                # API + dashboard
(cd rust/revisionchest && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all -- --check)
```

Test tiers, goldens and the scratch database are in skill `wcd-testing`.

**Sandboxes and linked machines.** The checked-in `.venv` belongs to the
owner's Mac. If you run from a different OS (a Linux VM or container), do not
let `uv` rebuild it: set `UV_PROJECT_ENVIRONMENT` to a path outside the repo
and pass `--frozen`, e.g.
`UV_PROJECT_ENVIRONMENT=$HOME/venv-wcd uv run --frozen pytest -q`.

## Invariants — do not break these

1. **One schema definition.** Tables live in `wcd_schema.postgres`, Parquet
   datasets in `wcd_schema.parquet`, staging files in `wcd_schema.staging`.
   Never define a table or column anywhere else. RevisionChest carries a copy
   of the Stage-0 DDL (`rust/revisionchest/crates/revisionchest-meta/tests/schema.sql`);
   `packages/schema/tests/test_revisionchest_ddl_parity.py` keeps it in step,
   so a Stage-0 table change means changing both.
2. **Deterministic ids.** `citation_instances.id` is the first 8 bytes of
   `SHA1("{domain}|{page_id}|{raw_sha1}")` as a signed BIGINT;
   `page_bucket = page_id % 512`. Use `wcd_schema.ids` (and
   `instance_id_sql` in DuckDB/SQL); never re-derive these by hand.
3. **Page identity is `(domain, page_id)`.** Every page-keyed table and
   partition carries the domain. The word is `domain`, never `wiki`.
4. **Parquet layout is `<dataset>/domain=<d>/page_bucket=<n>/`.** Build paths
   with `wcd_schema.parquet` (`partition_dir`, `partition_glob`, …) and write
   with `wcd_pipeline.tier`; read with `wcd_schema.duckdb.register_views` /
   `bind_bucket_views`.
5. **Each step owns its outputs and is safely re-runnable.** The ownership
   table in `docs/runbook.md` is a contract: a step replaces only what it
   owns (shard files, a domain's partitions, a bucket with `_SUCCESS`, 7d/7h
   checkpoints). Keep new code idempotent in the same way.
6. **Gates mean stop.** Every stage ends with its gate from
   `docs/design/proposed_data_integrity.md`. A failing gate is a bug to find,
   never a threshold to loosen to get a green run.
7. **`wcd_semantic` is vendored verbatim** from UVA at the commit in
   `packages/semantic/NOTICE`. Additions are marked `ADDED (wcd)` and tested in
   `tests/test_additions.py`; any change to vendored logic is a documented
   deviation in `NOTICE`, with goldens regenerated deliberately.
8. **Self-closing `<ref name=…/>` is a reuse event** (`named_ref_uses`, one
   row per occurrence), not a citation instance. `citation_type` is exactly
   `ref_tag | template | bare_url | endnote | shorthand | external_link`.
9. **Data never enters git.** `data/`, `output/`, `staging/`, `sources/`,
   `*.zst` are ignored (except under `fixtures/`). `.env` is never committed;
   every variable is documented in `example.env` (and in the runbook's table).
10. **Tests don't touch the network.** The API's title resolver is pluggable
    for that reason. Postgres tests run only against `WCD_TEST_DATABASE_URL`,
    which they wipe; never point it at a real database.

## Conventions

* Python ≥ 3.10, one uv workspace (`pyproject.toml` at the root). Add a
  dependency to the member's `pyproject.toml`, then `uv lock`.
* A stage is a module in `wcd_pipeline` with `main(argv) -> int` and its own
  argparse, registered in `wcd_pipeline/cli.py` `COMMANDS` (imported lazily so
  `wcd extract` needs no database config).
* Postgres via SQLAlchemy/psycopg2; bulk computation in DuckDB over the
  Parquet tier. The API holds no DuckDB state between requests.
* Commits: subject `area: what` (`normalizer: …`, `fixtures: …`) or
  `Phase N: …` for plan phases; the body is prose saying what changed and how
  it was checked. Design/doc records go in their own `docs: …` commit.
* After a significant piece of work, record it in
  `docs/design/repo_structure_and_implementation_plan_2026_10_08.md` as a new
  dated section (what landed, gate/verification, decisions taken without
  asking, what remains open), matching §§5–13.

## Where to look

* What a table/endpoint/stage should do: `docs/design/proposed_data_model.md`,
  `proposed_extractor.md`, `proposed_matching.md`, `proposed_api.md`,
  `proposed_data_integrity.md` (gates), `proposed_frontend.md`.
* Why it is that way: `docs/design/decisions_2026_09_01.md`,
  `decisions_2026_09_10.md`; history and open items in the implementation plan.
* How to run, re-run or serve a build: `docs/runbook.md`.
* Fixtures and goldens: `fixtures/README.md`.

## Skills

Task playbooks live in `.claude/skills/` (plain Markdown; any agent can read them):

* `wcd-testing` — test tiers, scratch Postgres, the three golden sets and when to regenerate them.
* `wcd-pipeline-change` — checklist for changing a stage, the schema, or the matchers.
* `wcd-submodules` — editing the extractor, normalizer or RevisionChest.
* `wcd-local-serve` — building the ci fixture database and running the API and dashboard against it.
