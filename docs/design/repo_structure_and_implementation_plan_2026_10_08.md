# Monorepo Structure and Implementation Plan

*2026-10-08 · Review of the `notes/` plan against the `wrdb-v3` working tree. Companion to `claude/codebase_gap_assessment.md` (2026-09-09) and `claude/decisions_2026-09-10.md`. This document does not re-litigate data-model or pipeline-semantics findings already settled there; it is about where the code lives, how the pieces depend on each other, and in what order the plan should be executed.*

> **Status (2026-10-08, later the same day).** The §4 decisions were taken and Phase R was executed; see §5 at the end for what was decided, what landed, and what is still open. §2 and §3 are left as written; where the executed layout differs from §2 (directory names, which pieces are submodules), §5 is authoritative.

Tree examined: `wrdb-v3` root (wiki-references-db `v2` branch @ `1b047a4`), `refs_extractor` (wiki-references-extractor `v3` @ `1f3efb0`), `refs_normalizer` (wiki-references-normalizer `main` @ `1692198`), `UVAWikipediaCitationsDatabase` (`main` @ `29f3ae3`), `RevisionChest` (`main` @ `c6cabb3`). Note the extractor pin has moved from the `v2` commit the gap assessment examined to the `v3` branch head; nothing in the findings below changes because of that.

---

## 1. What the plan assumes about the repository, and what the tree actually is

The nine `proposed_*.md` documents are a design for a system; they are silent on how that system is packaged. They name new modules (`matching/pipeline_runner.py`, `materialize_parquet.py`, a "Stage 4 driver", a "Stage 5 link builder", `retry_transient`, `materialize_by_bucket`) and they say UVA code is "ported as-is" and "reused wherever it exists", but they never say where any of it lives, how it imports the UVA code, or how the four git repositories become one. The tree as it stands cannot support the plan without a restructuring pass first, for the following reasons.

### 1.1 The dependency direction between the three IA repositories is already inverted

`refs_extractor/article.py` (the extractor submodule) does `from wiki_config import …` — it imports a module that lives in the *parent* repository, and `wiki_config.py` in turn reads `wikis.yaml` next to itself. The extractor also imports `citation_normalizer`, which is installed into the environment from `./refs_normalizer` through a `file:` requirement in the root `requirements.txt`. So the nominal layering (db → extractor → normalizer) is really a cycle (extractor ↔ db), and the extractor cannot be installed or tested on its own. `refs_extractor/syntax.py` is now an 8-line re-export shim over `citation_normalizer`, and the root `tests/test_syntax.py` is a near-duplicate of `refs_normalizer/tests/test_syntax.py` that goes through the shim. The plan's Stage 1 changes (candidate kind, self-closing reuse events, `citation_type` via `wikis.yaml`) all land in the extractor *and* in `build_db.py` at the same time, which is exactly the kind of change a submodule boundary makes painful.

### 1.2 The root is a flat script directory, not a package

Every stage is a top-level script (`build_all.py`, `build_db.py`, `dedup_parquet.py`, `load_all.py`, `init_db.py`, `purge.py`) plus the Flask app (`app.py`, `api_v1.py`, `explorer.py`, `templates/`). `build_all.py` launches workers with `subprocess.Popen(["python3", "build_db.py", …])`, so it only works with the repo root as the working directory; `tests/conftest.py` inserts the repo root on `sys.path` for the same reason. There is no `pyproject.toml` at the root. The plan adds roughly a dozen more stage entry points (4, 4.6, 5, 6a–6d, 7a–7h, 8) on top of this; as flat scripts they would have no shared place for the integrity gates, the retry decorator, the DuckDB view registration, or the `(domain, page_bucket)` conventions that every stage must agree on.

### 1.3 The staging contract is defined twice

`build_db.py`'s `SCHEMAS` dict (pyarrow) defines what Stage 1 writes; `models.py` (SQLAlchemy) defines what Stage 3 loads into; `dedup_parquet.py` and `load_all.py` each hard-code the column lists again. The plan adds columns to `citation_instances`/`normalized_citations`, three new Tier-2 datasets written by Stage 1, a `normalized_citation_parts` table, five serving tables, and seven DuckDB views whose column sets must match the Parquet on disk. Without a single schema module, those will drift the same way `revisions.revision_timestamp` already has (string in `models.py`, `TIMESTAMP` in the notes, two formats in a database fed by both writers).

### 1.4 UVA cannot be consumed as a submodule; "port as-is" means vendoring the pure half

The two normalizers, the ingester, the matching runner and the history module each begin with `sys.path.insert(0, <src>)` followed by `from pipeline_log import …` at module scope (the four matcher modules are importable on their own but are only ever driven through the runner); the scripts are run as `python src/<pkg>/<module>.py`. Importing `UVAWikipediaCitationsDatabase.src.citation_normalize.reftag_normalizer` from the monorepo is not possible without reproducing those hacks. The dependency file pins `torch`, `transformers`, `accelerate`, CUDA wheels and Jupyter, for the optional BERT passes the decisions log puts off the critical path. Four modules carry four identical copies of `UnionFind`. The repo also contains ~5,000 lines of `legacy/` extractors and ingesters, Rivanna-specific Slurm scripts, and notebooks.

The good news, which the plan relies on but does not state: each UVA module is already split into a pure-logic half and a DuckDB I/O half. `classify(content)` and `extract_template(...)` in the normalizers, `build_source_map(df, page_id)`, `build_merged_sources(...)`, `hard_identifier_pass`, `author_year_fuzzy_pass`, `build_global_sources` and `_compute_score` in the matchers all take DataFrames or strings and return DataFrames or dicts; `load_*`, `write_to_db`, `write_normalized`, `run_page` and `main` are the DuckDB wrappers the plan replaces anyway (bucketed Parquet in, Parquet out). So the port is a *copy* of the pure functions into a package of our own plus new adapters — a vendoring operation with a recorded upstream commit, not a submodule reference. The plan should say this explicitly because it determines what "as-is" means for review: the adapters are new code; the logic is a verbatim copy that can be diff-tested against upstream.

### 1.5 Two of the four pieces are candidates for a submodule; two are not

RevisionChest is a separate language, a separate build (`cargo`), owned by the Internet Archive, and the plan asks it for three small changes (TIMESTAMP output, drop the title-keyed `documents` insert, write `page_metadata`). It is consumed as a binary at a pinned version. That is the textbook submodule case. The extractor and normalizer are the opposite: pure Python, edited in lockstep with the pipeline, and already entangled with the parent. UVA is a source of code to copy, not a dependency.

The current `.gitmodules` also carries two stale entries (`iari`, `wiki-references-extractor`) with no directory in the tree, and `git status` shows the `RevisionChest` and `UVAWikipediaCitationsDatabase` additions staged but uncommitted, with `notes/` untracked.

### 1.6 Three configuration conventions, two web frameworks, one undecided UI

wiki-references-db reads `DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASS`, `STAGING_DIR`, `REVISION_BUNDLES_DIR`; the extractor reads `CONTACT_EMAIL`/`SECONDARY_USER_AGENT` from its own `example.env`; UVA reads `DB_PATH` and `ANALYTICS_*`. The notes already use `DATABASE_URL`. The v1 API (`api_v1.py`) is Flask + SQLAlchemy; UVA's analytics service (`main.py`) is FastAPI; the notes say "Serving (FastAPI)" and keep UVA's response models, so v2 is a FastAPI port and v1 is not carried forward as code (the API doc already says v1 is served from the old database until cutover). What the plan does not say is what happens to the Flask **explorer** (`explorer.py`, 719 lines, six Jinja templates): it covers template-parameter reports and citation/web-resource reports that the UVA dashboard does not, and `proposed_frontend.md` only ever talks about the UVA frontend. That is a product decision the restructure needs.

### 1.7 There is no regression net, and the plan's gates assume one

The gap assessment already said UVA has no tests and that F3 (testing strategy) should precede Phase 2. The repository side of that: the one fixture that exists (`testcases/easter_island.py`) is checked into both the extractor and the normalizer, and the root `tests/` tests code that lives in other repositories. There is no sample `.mwrev.zst`, no golden Parquet, and nothing that exercises `dedup_parquet.py` or `load_all.py`. Every phase gate in the executive summary ("one-shard end-to-end benchmark", "collision gate", "no NULL verdict") needs a small fixed input to run against; the memory notes record that you have already decided the test-article subset should carry full revision history and serve as matching-quality evaluation, scale rehearsal, and API fixture at once. That subset is the first artifact the monorepo should contain.

---

## 2. Recommended repository structure

One repository, one Python workspace (`uv` workspaces; `hatch` or plain `pip -e` on each member also works), one Rust workspace, five Python packages with a strict dependency order, two apps, fixtures and docs at the top level. The layout maps one-to-one onto the plan's stage numbering so each `proposed_*.md` has an obvious home.

```
wcd/                                  # monorepo root
├── pyproject.toml                    # uv workspace: members = ["packages/*", "apps/*"]; shared dev deps (pytest, ruff)
├── uv.lock
├── Cargo.toml                        # Rust workspace: members = ["rust/revisionchest"]
├── .gitmodules                       # exactly one entry: rust/revisionchest
├── README.md                         # one page: what the pipeline is, where each stage lives, how to run it
│
├── packages/
│   ├── wcd-normalizer/               # ← refs_normalizer, moved verbatim (already a proper package: citation_normalizer)
│   │   ├── pyproject.toml            #   dependencies: mwparserfromhell
│   │   ├── src/citation_normalizer/
│   │   └── tests/
│   │
│   ├── wcd-extractor/                # ← refs_extractor + wikis.yaml + wiki_config.py (fixes the inverted import)
│   │   ├── pyproject.toml            #   dependencies: wcd-normalizer, mwparserfromhell, requests
│   │   ├── src/wcd_extractor/
│   │   │   ├── article.py            #   extract_references() — Stage 1 scanner; gains candidate kind + reuse events
│   │   │   ├── wikilist.py, wikiapi.py, cli.py
│   │   │   ├── config.py             #   was wiki_config.py; loads wikis.yaml (gains shorthand_templates per domain)
│   │   │   └── wikis.yaml
│   │   └── tests/                    #   ← root tests/test_article_extract_references.py, test_refs_extractor_cli.py,
│   │                                 #     test_wikiapi_env.py, test_wikis.py; easter_island fixture lives here once
│   │
│   ├── wcd-semantic/                 # ← UVA pure-logic halves, vendored (upstream commit recorded in NOTICE)
│   │   ├── pyproject.toml            #   dependencies: pandas, rapidfuzz, mwparserfromhell  (NO torch; bert passes behind an extra)
│   │   ├── src/wcd_semantic/
│   │   │   ├── reftag.py             #   classify(), normalize_content(), identifier extraction  (reftag_normalizer.py minus DuckDB)
│   │   │   ├── template.py           #   extract_template(), _classify_subtype()  (+ arXiv/eprint, {{Cite Q}} QID)
│   │   │   ├── footnote_filter.py    #   UVA _is_citation, _is_multi_block, iter_citations (from wcd_extract) — Stage 4 inputs
│   │   │   ├── unionfind.py          #   the one copy
│   │   │   ├── match_reftag.py       #   build_source_map(), fuzzy_cluster(), merge_fuzzy()  (6a)
│   │   │   ├── match_template.py     #   build_source_map()  (6b)
│   │   │   ├── merge_types.py        #   build_merged_sources()  (6c)
│   │   │   ├── match_global.py       #   hard_identifier_pass(), author_year_fuzzy_pass(), build_global_sources()  (6d)
│   │   │   ├── scoring.py            #   compute_score()  (7f)
│   │   │   └── signals.py            #   _FLAG_RES regexes for revision_signals (Stage 1)
│   │   └── tests/                    #   golden tests: upstream outputs on fixture strings == ported outputs
│   │
│   ├── wcd-schema/                   # single source of truth for every table, dataset and view
│   │   ├── pyproject.toml            #   dependencies: pyarrow, sqlalchemy, duckdb
│   │   └── src/wcd_schema/
│   │       ├── postgres.py           #   SQLAlchemy models (← models.py, revised per proposed_data_model.md)
│   │       ├── staging.py            #   pyarrow schemas for Stage-1 staging (← build_db.SCHEMAS)
│   │       ├── parquet.py            #   Tier-2 dataset schemas + layout: dataset_path(name, domain, bucket), page_bucket(), partition columns
│   │       ├── duckdb.py             #   register_views(con, parquet_base, pg_dsn): the seven Parquet views + pg.* views (proposed_cache.md §3)
│   │       ├── ids.py                #   instance_id(domain, page_id, raw_sha1), global_source_id(members), source_id(page_id, root)
│   │       └── ddl/                  #   generated SQL for review / psql-based bulk paths
│   │
│   └── wcd-pipeline/                 # Stages 1–8, one CLI, the integrity layer
│       ├── pyproject.toml            #   dependencies: wcd-extractor, wcd-semantic, wcd-schema, duckdb, psycopg, zstandard, pyyaml
│       └── src/wcd_pipeline/
│           ├── cli.py                #   `wcd <stage> …`; every stage is a subcommand with --domain, --parquet-dir, --buckets
│           ├── config.py             #   DATABASE_URL, PARQUET_DIR, STAGING_DIR, BUNDLES_DIR, CONTACT_EMAIL — one place
│           ├── stages.py             #   registry: name, inputs, outputs, gate(); the orchestrator walks it
│           ├── integrity/            #   proposed_data_integrity.md: gates.py, retry.py, checkpoint.py (materialize_by_bucket), log.py
│           ├── s0_revisionchest.py   #   invoke the binary; load --parquet metadata → revisions, page_metadata; revisions Parquet
│           ├── s1_extract.py         #   ← build_db.py (worker) + build_all.py (launcher, now a function, not a subprocess by filename)
│           ├── s2_normalize.py       #   thin: calls citation_normalizer (unchanged)
│           ├── s3_dedup.py           #   ← dedup_parquet.py (+ domain in keys, collision gate)
│           ├── s3_load.py            #   ← load_all.py (+ new columns; no revisions/documents writes)
│           ├── s4_semantic.py        #   NEW driver: content extraction → bullet split → wcd_semantic → normalized_citation_parts; 4.6 denorm export
│           ├── s5_ref_name_links.py  #   NEW: revision-range builder
│           ├── s6_matching.py        #   NEW adapters: per-bucket DuckDB input query → wcd_semantic.match_* → bucketed Parquet; process pool
│           ├── s7_materialize.py     #   7a–7h
│           ├── s8_stats.py           #   page_revision_stats
│           └── export.py             #   export_citation_instances_denorm(domain, resolved) and other COPY … TO helpers
│
├── apps/
│   ├── api/                          # FastAPI v2  (← UVA analytics_service/main.py response models + v1 endpoint ports)
│   │   ├── pyproject.toml            #   dependencies: wcd-schema, fastapi, uvicorn, duckdb, psycopg
│   │   └── src/wcd_api/
│   │       ├── main.py, settings.py
│   │       ├── duckdb_pool.py        #   proposed_cache.md §3
│   │       ├── routers/              #   article.py, pages.py (Group 2), revisions.py (Group 3), templates.py, web_resources.py, sources.py
│   │       └── queries/              #   the page_summary.py rewrite, split by endpoint
│   └── frontend/                     # ← UVA src/analytics_service/frontend (index.html, app.js, styles.css), rebranded per proposed_frontend.md
│
├── rust/
│   └── revisionchest/                # git submodule → internetarchive/RevisionChest (pinned); the three small changes go upstream
│
├── fixtures/
│   ├── shards/                       # the test-article subset: one or two small .mwrev.zst with full history
│   ├── golden/                       # expected Stage-1 staging Parquet, Stage-4 parts, 6d global sources for those shards
│   └── README.md                     # how the subset was chosen and how to regenerate golden files
│
├── docs/
│   ├── design/                       # ← notes/*.md (proposed_* as the living spec; decisions/ as the log)
│   ├── uva/                          # ← UVA docs/, normalizers.md, citation_matching.md, citation_history.md (reference for the vendored code)
│   └── runbook.md                    # full rebuild, per-stage re-run, serving during rebuild (open item F2 lands here)
│
├── legacy/                           # frozen, not installed: api_v1.py, explorer.py, templates/, openapi.yaml, app.py
│                                     # (deleted at v1 cutover; or omitted entirely and served from the old repo — see §4)
└── scripts/                          # ops only: psql bulk-load helpers, Slurm wrappers if Rivanna is still used
```

### Why these boundaries

**Dependency order is enforced by packaging, not convention.** `wcd-normalizer ← wcd-extractor ← wcd-pipeline`, `wcd-semantic ← wcd-pipeline`, `wcd-schema ← wcd-pipeline, wcd-api`. The extractor and the semantic package never import the pipeline or each other; neither touches a database. This fixes the inverted `wiki_config` import by moving `wikis.yaml` into the extractor, which is the only consumer of `reference_sections`, and the first consumer of `citation_templates` (Stage 4's part-type classification imports the same function from `wcd_extractor.config`).

**`wcd-semantic` has no I/O and no torch.** That is what makes the UVA logic unit-testable and what makes the Phase 2 adapters (bucketed Parquet in, per-page `build_source_map` unchanged, Parquet out) a clean layer. The BERT/`author_key_enriched` passes stay in the vendored code behind their existing table-exists checks; the dependencies become an optional extra (`wcd-semantic[bert]`) that nothing installs by default.

**`wcd-schema` is the plan's missing module.** The seven Parquet views, the `(domain, page_bucket)` layout, the deterministic id functions, the Postgres DDL and the staging schemas are each referenced from at least four stages and the API. One package, imported by everyone, replaces the current three-way duplication and gives the integrity gates something to validate against.

**One CLI with a stage registry.** `wcd extract --domain en.wikipedia.org -d bundles/ -o staging/`, `wcd semantic --domain …`, `wcd match --buckets 17,18`, `wcd materialize --from 7d`, `wcd gate 3-load`. Each stage declares its inputs and outputs (dataset names from `wcd_schema.parquet`), so the orchestrator can run the gate from `proposed_data_integrity.md` §4 after each stage, and `build_all.py`'s subprocess-by-filename launcher becomes `python -m wcd_pipeline.s1_extract` with a real entry point. This is also where the memory-leak/shard-checkpointing work recorded in the earlier pipeline run belongs.

**RevisionChest stays a submodule; everything else is in-tree.** Subtree-merge the extractor and normalizer so their history is preserved (`git subtree add --prefix packages/wcd-extractor … v3`), then develop them in place. If the Internet Archive wants `wiki-references-extractor` / `-normalizer` to continue as standalone repositories, `git subtree push` publishes the package directories back; if not, archive them with a pointer. UVA is copied, not subtree'd: the vendored files are the pure halves only, and `NOTICE` records `scatter-llc/UVAWikipediaCitationsDatabase @ 29f3ae3` plus the per-file provenance. Keep the UVA submodule checked out at that pin only until the golden tests in `wcd-semantic/tests` pass, then remove it.

**`legacy/` or nothing.** The plan says v1 is served from the old database until v2 is live; the simplest reading is that the Flask app does not move into the monorepo at all and keeps running from the existing `wiki-references-db` deployment. Moving it to `legacy/` only buys a single checkout for both services. Either way, do not spend effort making v1 import from the new packages.

---

## 3. Implementation plan

The notes' Phases 0–5 are sound; what they lack is a Phase before Phase 0, and an explicit statement that the testing strategy (F3) is produced by that phase rather than deferred. The sequence below keeps the notes' numbering for the pipeline phases and adds two preparatory ones. Each phase ends with a gate that can be run from the repository.

### Phase R — Restructure without behaviour change (≈1 week)

Create the workspace skeleton and move code into it with no semantic edits:

1. Subtree-add the normalizer and extractor into `packages/`; move `wikis.yaml` and `wiki_config.py` into the extractor; delete the `refs_extractor/syntax.py` shim and the duplicated `easter_island` fixture and `test_syntax.py`. Clean `.gitmodules` to the one RevisionChest entry at `rust/revisionchest`.
2. Move `models.py` and `build_db.SCHEMAS` into `wcd-schema` untouched. Move `build_db.py`, `build_all.py`, `dedup_parquet.py`, `load_all.py`, `init_db.py`, `purge.py` into `wcd-pipeline` as `s1_extract`, `s3_dedup`, `s3_load`, with a `wcd` CLI whose subcommands reproduce the current flags. Replace the `subprocess.Popen(["python3","build_db.py"…])` launcher with an entry-point invocation. Centralise env handling in `config.py` (accept the old variable names; add `DATABASE_URL`).
3. Park `app.py`, `api_v1.py`, `explorer.py`, `templates/`, `openapi.yaml` in `legacy/` (or leave them in the old repo).
4. Move `notes/` to `docs/design/` and commit it — it is currently untracked.
5. Commit the test-article subset: one small `.mwrev.zst` (full history, a few hundred pages across the taxonomy: templated refs, prose refs, named-ref reuse, bullet-block refs, `{{sfn}}`, reference-section endnotes) plus a RevisionChest metadata Parquet for it. Record the golden Stage-1 staging output produced by the *unmodified* `build_db.py`.
6. CI: `uv sync`, `pytest` across the workspace, `cargo build` for the submodule.

**Gate R:** `wcd extract` → `wcd dedup` → `wcd load` on the fixture shard from the new layout produces staging Parquet identical (DuckDB `EXCEPT` both ways) to the golden output from the old layout, and the existing extractor/normalizer tests pass from their new locations. This is the baseline every later phase is measured against.

### Phase S — Vendor the UVA logic with golden tests (≈1–2 weeks)

1. Copy the pure functions listed in §2 into `wcd-semantic`; delete the DuckDB wrappers, the `pipeline_log` imports and the `sys.path` lines; collapse `UnionFind` to one class; make `rapidfuzz`/`pandas` the only hard dependencies.
2. Build the golden corpus from the fixture shard: run upstream `reftag_normalizer.classify` / `template_normalizer.extract_template` (from the still-checked-out UVA submodule) over the unique inner contents of the fixture's references and record the outputs; run upstream `build_source_map` / `build_merged_sources` / `hard_identifier_pass` / `author_year_fuzzy_pass` / `_compute_score` on the same DataFrames and record those. Store under `fixtures/golden/semantic/`.
3. Add the three identifier extractions the decisions log asks for (arXiv/`eprint`, `{{Cite Q}}` QID) and the `_FLAG_RES` regexes; these are additions with their own tests, not covered by the golden comparison.
4. Move UVA's markdown (`normalizers.md`, `citation_matching.md`, `citation_history.md`, `docs/`) to `docs/uva/`.

**Gate S:** every ported function reproduces the upstream output on the golden corpus; `wcd-semantic` installs without torch; the UVA submodule is removed from the tree.

This phase closes open item F3 for the heuristic code: the golden files are the matching-quality regression detector, and the fixture shard is the input to the Phase 2 benchmark. It can run in parallel with Phase 0, since it touches no schema.

### Phase 0 — Schema and RevisionChest (as in the notes)

Lands in `wcd-schema` (new Postgres DDL, Tier-2 dataset schemas, `register_views`, id functions) and `rust/revisionchest` (TIMESTAMP output, `page_metadata`, drop the title-keyed `documents` insert — submitted upstream, pin bumped). `s0_revisionchest.py` loads the `--parquet` metadata into `revisions`/`page_metadata` and writes the re-partitioned `revisions` Parquet. Gate: the Stage-0 checks in `proposed_data_integrity.md` §4 on the fixture shard.

### Phase 1 — Stages 1–5

`wcd-extractor` gains the candidate kind on each result and the reuse-event output for self-closing refs (the `seen_texts` per-revision dedup in `extract_references` has to be bypassed for them, as the gap assessment noted); `s1_extract` gains `citation_type`, deterministic instance ids via `wcd_schema.ids`, and the three Tier-2 writers; `s3_dedup` gains the domain key and the collision gate; `s3_load` drops `revisions`/`documents` writes; `s4_semantic` and `s5_ref_name_links` are new. Gate: Stage-3/4/5 checks from the integrity doc, plus the Stage-1 golden comparison updated for the new columns (the diff against the Gate-R golden should be exactly the planned additions and the removed self-closing rows — anything else is a regression).

### Phase 2 — Stage 6

`s6_matching.py` is the adapter: per-bucket DuckDB input query over `citation_instances_denorm` ⋈ `citation_history` → `wcd_semantic.match_*` per page → `COPY … TO` the bucket directory; process pool over buckets. The notes' benchmark gate (one shard end-to-end) now has a concrete input (the fixture shard, then one real shard) and a concrete reference (UVA's own output on the same shard, via the golden files).

### Phase 3 — Stages 7–8

`s7_materialize.py` (7a–7h, with `integrity.checkpoint.materialize_by_bucket` for 7d/7h) and `s8_stats.py`, both reading only `wcd_schema.parquet` paths and `wcd_schema.duckdb.register_views`. Gate: the 7a–7g and referential-integrity checks.

### Phase 4 — V2 API

`apps/api`: port UVA `main.py` response models into `routers/`, write `queries/` against the materialized tables, add the v1 endpoint ports and `GET /sources/{id}`. FastAPI generates the OpenAPI document, so `openapi.yaml` is not maintained by hand any more. The fixture database (Phases 0–3 run on the fixture shard) is the API's test fixture.

### Phase 5 — Frontend

`apps/frontend`: the mechanical changes in `proposed_frontend.md` against the Phase 4 API.

### Write the runbook as you go

Open item F2 (refresh cadence, serving during rebuild) becomes `docs/runbook.md`, written incrementally: Phase R documents the stage CLI, Phase 1 the re-run units (shard for Stage 1, domain for 3/5, bucket for 6–8), Phase 3 the full-rebuild order and the swap of Parquet directories under the running API (the DuckDB init pass has to be re-run after a swap, per `proposed_cache.md` §3).

---

## 4. Decisions needed before Phase R

These are repository questions the notes do not settle and that change the layout:

1. **Where the monorepo lives and what happens to the four upstream repositories.** The extractor, normalizer, db and RevisionChest are under `internetarchive/`; the UVA fork is under `scatter-llc/`. The structure above assumes the monorepo is the canonical home for the Python code and that the IA repositories either become subtree-published mirrors or are archived. If IA needs the extractor to remain an independently installable library, the only extra cost is keeping `packages/wcd-extractor` free of imports from the other packages (which the layout already guarantees) and running `git subtree push` on release.
2. **The explorer.** Keep it (then it moves into `apps/` and is ported to the v2 API in Phase 4), drop it in favour of the UVA dashboard (then `proposed_frontend.md` should say the template-report and citation-report views are out of scope), or fold its two report pages into the dashboard as a later item.
3. **Whether v1 code enters the monorepo at all.** Recommendation: it does not; it keeps running from the existing deployment until v2 cutover, and the monorepo starts clean.
4. **The fixture shard.** Which pages, and whether it is built by running RevisionChest over a filtered XML dump or by splitting an existing `.mwrev.zst` (UVA's `notebooks/split_mwrev.ipynb` does the latter). It needs to exist before anything else in Phase R can be gated.
5. **Rust scope.** `rust/` is laid out as a workspace so that the wikitext-scanner rewrite you have considered has a home next to RevisionChest if it happens; nothing in this plan depends on it.

---

## 5. Decisions and Phase R outcome (2026-10-08)

### Decisions

1. **Home.** The monorepo is `internetarchive/wiki-references-db`, branch `v3` — the `wrdb-v3` checkout itself, history preserved. `internetarchive` is the organisation for every repository; the `scatter-llc` UVA fork is the exception.
2. **Submodules, not subtrees.** Anything that is a module in its own right stays its own repository and is consumed as a submodule. The normalizer is explicitly meant to be usable by other projects, not just the extractor. So: `packages/normalizer` → `wiki-references-normalizer`, `packages/extractor` → `wiki-references-extractor`, `rust/revisionchest` → `RevisionChest`, and `upstream/uva` → the UVA fork, kept only until Phase S has ported its logic. Everything else (`wcd_schema`, `wcd_pipeline`, the future `wcd_semantic`, `apps/`) is in-tree.
3. **Explorer.** Replaced by the UVA dashboard; not carried forward.
4. **v1.** Frozen in `legacy/` with its own `requirements.txt` and a copy of the v1 models; served until v2 cutover, then deleted.
5. **Naming.** `wcd-*` distribution names, `wcd_*` import names, `wcd` CLI. Existing import names (`citation_normalizer`, `refs_extractor`) unchanged.
6. **Rust scope** as proposed: `rust/` is a workspace home for RevisionChest and, if it happens, the wikitext-scanner rewrite.
7. **Fixture sampling.** A sampling strategy for the test-article subset was agreed in an earlier conversation that is not accessible from this one; `scripts/select_fixture_pages.py` is a stub with the input/output contract and awaits those criteria. `wcd fixture-dump` (the XML-dump filter that feeds RevisionChest) is implemented and tested.

### What landed (commit `7cf66f9` on `v3`, plus `10f7384` in the extractor and `088a460` in the normalizer — all local, not pushed)

```
wiki-references-db (v3)
├── pyproject.toml, uv.lock         uv workspace: members packages/*, apps/*
├── README.md, example.env          rewritten for the layout; DATABASE_URL added (DB_* still accepted)
├── .github/workflows/ci.yml        uv sync + pytest; cargo build of RevisionChest
├── packages/
│   ├── normalizer/   (submodule)   unchanged API; egg-info untracked
│   ├── extractor/    (submodule)   src/refs_extractor/ + pyproject; wikis.yaml, wiki_config.py, wikis.py moved in;
│   │                               syntax.py shim removed; tests moved in; console script refs-extractor
│   ├── schema/       wcd_schema    postgres.py (← models.py), staging.py (← build_db.SCHEMAS)
│   └── pipeline/     wcd_pipeline  extract (← build_db), extract_all (← build_all), dedup (← dedup_parquet),
│                                   load (← load_all), init_db, purge, config, cli (`wcd`), fixture_dump
│       └── tests/                  synthetic_bundle.py, test_stage1_golden.py, test_fixture_dump.py
├── rust/revisionchest/ (submodule) unchanged
├── upstream/uva/       (submodule) unchanged; reference for Phase S
├── legacy/                         app.py, api_v1.py, explorer.py, templates/, openapi.yaml, models.py (frozen copy), requirements.txt
├── fixtures/                       README; golden/synthetic/ (11 Parquet tables from the unmodified pipeline)
├── docs/design/                    the former notes/ (now tracked)
└── scripts/select_fixture_pages.py stub
```

**Gate R passed.** The unmodified `build_db.py` + `dedup_parquet.py` were run on the synthetic bundle before any move (12 citation instances, 28 history rows, 33 template-data rows across two pages and five revisions); after the restructure `wcd extract` + `wcd dedup` reproduce all eleven tables exactly (DuckDB `EXCEPT` in both directions, zero rows). That comparison is now the permanent test `test_stage1_golden.py`. All 39 tests pass (`uv run pytest`): 15 normalizer, 11 extractor, 13 pipeline.

Behaviour-preserving edits that were unavoidable: `build_db` imports `citation_normalizer` directly instead of through the removed shim and takes `SCHEMAS` from `wcd_schema.staging`; `load_all` no longer validates database variables at import time (it calls `wcd_pipeline.config.database_url()` instead, so `wcd extract` works without a database configured); `build_all` launches `python -m wcd_pipeline.extract` rather than `build_db.py` by filename; every stage exposes `main(argv)`.

### Still open after Phase R

- **Sampling criteria** for `select_fixture_pages.py`, then the fixture shard itself (`fixtures/shards/`) built via `wcd fixture-dump` → RevisionChest. Until it exists, the golden tests run on the synthetic bundle only.
- **Push.** Three local commits (monorepo `v3`; extractor `v3`; normalizer `v3`) are unpushed. The extractor's `v3` restructure changes its layout, so its README's standalone-install instructions should be checked against a fresh clone after pushing.
- **Python version.** The workspace says `>=3.10` because that is what ran the gate; CI uses 3.12. Pin higher once the UVA port (which was developed on 3.12) lands if anything needs it.
- **Phase S** is next: `packages/semantic` with the vendored UVA pure functions and golden tests generated from `upstream/uva`; then `upstream/uva` is removed.

## 6. Phase S outcome (2026-10-08)

Three commits on `v3`: `cbd6891` (the package), `03cf2f2` (identifier additions), `21511ef` (UVA submodule removed). Still local, not pushed.

**`packages/semantic` (`wcd_semantic`)** holds the logic half of the UVA modules, sliced out by line range from `scatter-llc/UVAWikipediaCitationsDatabase @ 29f3ae3` with the DuckDB loaders/writers, `sys.path` hacks and `pipeline_log` imports left behind: `content` (template-vs-ref_tag parsing, embedded templates, bullet-block split, footnote filter), `reftag` and `template` (the two normalizers), `signals` (the seven revision flags), one `unionfind`, `match_reftag` (6a), `match_template` (6b), `merge_types` (6c), `match_global` (6d), `scoring` (7f). Each matcher gained a database-free entry point (`match_page`, `merge_page`, `match_global`) reproducing upstream `run_page`/`main`. Dependencies are pandas, numpy, rapidfuzz and mwparserfromhell; torch/transformers are an uninstalled extra. `NOTICE` records provenance per file.

**Gate S passed.** A fixed corpus (52 ref contents across every ref-tag subtype and five template subtypes, four revision texts, a three-page citation corpus, nine scorer rows) is run through one assembly — which mirrors upstream's `load_*` SQL in pandas — against both the upstream modules (`gen_golden.py` → `fixtures/golden/semantic/expected.json`) and `wcd_semantic`. 77 comparisons are equal, deterministic across hash seeds. The corpus exercises the anchor_key / ref_name / fuzzy per-page signals, isbn / url / doi / author_year cross-type merges, and doi / isbn / author_year_fuzzy cross-page matches, and a coverage test keeps it that way.

**Additions** (decisions item 7), marked `ADDED (wcd)` in the source and tested in `test_additions.py` while the golden comparison strips their output keys: arXiv (`|arxiv=`, `|eprint=` on `{{cite arXiv}}`, arxiv.org URLs and `arXiv:` text in prose; version suffix dropped) and Wikidata QID (`{{Cite Q}}` first positional) extraction; PMC / arXiv / QID as 0.95 hard-identifier signals in all four matchers, excluded from the cross-page fuzzy pass and carried on every output; the 10-point scorer bonus for those match types.

**Two things learned about the upstream code while building the golden corpus**, both worth remembering in Phase 2:

1. `reftag._ID_URL_PARAMS` was a Python *set*, so which URL a `{{cite …}}` inside a `<ref>` yielded depended on the hash seed. Fixed in the vendored copy (ordered tuple, original URL first) and applied to upstream during golden generation — the one deviation from upstream logic.
2. `build_merged_sources` leaves `title` NULL on ref-tag-only merged sources (it takes `title` from the template side; the ref-tag side's italicised title arrives as `ref_title` and is not copied). Since `author_year_fuzzy_pass` requires `title`, **prose-only sources never match across pages**, whatever their author/year/title. That is upstream behaviour and the golden preserves it; whether to carry `ref_title` into `title` for ref-only sources is a matching-quality decision for Phase 2, not a port question.

**Open after Phase S:** the sampling criteria for `scripts/select_fixture_pages.py` (unchanged); Phase 0 (schema and RevisionChest) and Phase 1 (Stages 1–5) are next, and Stage 4's driver is where `wcd_semantic.content` + `reftag`/`template` get wired to `normalized_citations`.

## 7. Phase 0, part 1 — RevisionChest as reusable crates (2026-10-19)

Decisions taken first: the dump reader lives in RevisionChest (as `mwdump`; triplespace can adopt it later), the SQLite sink is dropped (Parquet + Postgres remain), and the two defects found in review are fixed in the split rather than deferred.

**Landed** as `98bb8fb` + `e42cc49` on RevisionChest's `v3` branch (submodule pointer bumped in monorepo commit `1bba007`; all still local). The repository is now a Cargo workspace:

| Crate | Contents |
|---|---|
| `mwdump` | Streaming MediaWiki XML dump reader: one event per `<siteinfo>`, page start, revision, page end. A revision carries page id/ns, ids, timestamp, contributor (id, name, IP, hidden), minor, comment, model/format, sha1 and verbatim text. `.xml` / `.bz2` / `.gz` / `.7z`; 7z is decompressed on a worker thread through a bounded channel instead of being read into memory whole. A truncated dump is an error. |
| `mwrev` | The `.mwrev.zst` format: `RevisionHeader`, `BundleWriter` (create, or append a frame with offsets continued via a `.len` sidecar), `BundleReader` over all frames, `read_at`. Offsets are defined once — decompressed byte offsets from the start of the bundle — which is what `build` always produced and what `sync` now produces too. |
| `revisionchest-meta` | `MetaEvent`/`MetaSink` behind a bounded channel; `ParquetSink` (same columns as before, written in row groups so memory is bounded); `PostgresSink` (feature) writing the Stage-0 contract: `domains`, `revision_bundles`, `revisions (domain_id, revision_timestamp TIMESTAMP, …)`, `page_metadata (domain_id, page_id, page_title, page_namespace)`, article `web_resources` — and nothing to `documents`. |
| `revisionchest` | The binary. Same command line except `--db` is gone and `--postgres` is explicit (`DATABASE=postgres` still works); `sync` gained `--overlap`/`--max-retries` from the unmerged `dev` branch, which is thereby folded in. |

**Gate.** On a fixture dump (4 pages, 9 revisions; redirect, hidden contributor and hidden text, CRLF, empty text, namespaces 0/1/118) in `.xml`, `.bz2` and `.7z`, with and without `--namespace=0`, the new binary's bundles are byte-identical to 0.1's — compressed bytes included — and the Parquet metadata is identical except that a plain-`.xml` dump's `file_path` now names the bundle actually written (0.1 recorded `fixture.xml.mwrev.zst` for a file it had called `fixture.mwrev.zst`). The Postgres sink was run twice against a scratch Postgres 16 with the Stage-0 DDL (`crates/revisionchest-meta/tests/schema.sql`): offsets match the Parquet run, timestamps land as `TIMESTAMP`, re-runs are idempotent; that test is in the crate behind `REVISIONCHEST_TEST_DATABASE_URL`. `sync` could not be exercised against a live wiki from either machine available to this session (wikipedia.org is not reachable from the cloud workspace, nor through the proxy on the linked machine), so its append path is covered by an offline test that feeds captured-shape API JSON and checks every recorded offset reads back its own revision across batches, days and zstd frames.

**Still to do in Phase 0:** `wcd_schema.postgres` has to be brought to the same contract (`revisions.domain_id`, `TIMESTAMP`, `page_metadata`, `citation_instances.id` as deterministic BIGINT, `normalized_citation_parts`, the serving tables), `s0_revisionchest.py` to load the `--parquet` metadata when Stage 0 ran without Postgres and to write the re-partitioned `revisions` Parquet, and the Stage-0 gate from the integrity doc. A real run of `sync` against a wiki from a machine with network access is also outstanding.

## 8. Phase 0, part 2 — schema, Parquet tier, `wcd stage0-load` (2026-10-19)

Landed as `2e6a7ce` on `v3` (with `7fa2268`, a `pandas < 3` pin for `wcd_semantic`, and RevisionChest `0f11d22` for a one-line DDL fix). Phase 0 is complete apart from a live `sync` run.

**`wcd_schema.postgres`** now *is* the data model in `proposed_data_model.md`, not the v1 schema with a semantic layer bolted on. Every page-scoped table carries `domain_id`; `revisions` holds every revision (`revision_timestamp TIMESTAMP NOT NULL`, bundle offsets, no sequence on `revision_id`) and `page_metadata` the title/namespace, both written at Stage 0; `citation_instances.id` is the deterministic BIGINT with `UNIQUE (domain_id, page_id, raw_sha1)`; `citation_history` is gone from Postgres (Parquet only); `normalized_citations.citation_type` is nullable until Stage 3 fills it, then becomes NOT NULL in Phase 1. The Stage 4–8 tables — `normalized_citation_parts` (with the arXiv / QID / LibraryBase columns from the Phase S additions), `ref_name_links`, `global_sources`, `citation_source_resolution`, `page_source_summary`, `page_summary_cache` — exist with their partial indexes, so later phases only write rows. `STAGE_TABLES` maps each stage to the tables it owns.

**Three small modules** carry the cross-engine contract: `wcd_schema.ids` (the instance id in Python and as a DuckDB expression proven equal across signed/unsigned boundary cases, UVA's `global:`/page/template source ids, `part_sha1`); `wcd_schema.parquet` (one `pyarrow` schema per dataset, `PAGE_PARTITIONED` / `DOMAIN_PARTITIONED` / `GLOBAL`, the `<dataset>/domain=<d>/page_bucket=<page_id % 512>/` paths and globs); `wcd_schema.duckdb` (`register_views` over the Parquet tier plus `ATTACH`ed Postgres, `bind_bucket_views` so a bucketed stage reads one partition — a missing partition becomes an empty typed view rather than an error).

**`wcd stage0-load`** (`wcd_pipeline.stage0`) takes RevisionChest's `--parquet` metadata and a domain: upserts the domain, bundles, `page_metadata` and the article `web_resources`, `COPY`s revisions through a temp table into `revisions` with `ON CONFLICT DO NOTHING`, writes the `revisions` Parquet dataset partitioned by bucket via DuckDB, and ends with the Stage-0 gate (domain present, every revision's page has metadata, `revision_timestamp` is a real `TIMESTAMP`, Parquet row count equals Postgres for the domain). Re-running it changes nothing; running it after RevisionChest's own `--postgres` sink only adds the Parquet copy. **`wcd load`** was adapted to the new model: `normalized_citations` get `(appears_on_domain_id, appears_on_page_id)`, instances get the deterministic id with the staged `domain` column winning when present (Phase 1 makes the extractor write it), and article `web_resources` are no longer invented from `documents`. One environment detail: SQLAlchemy 2.1 resolves a bare `postgresql://` URL to psycopg 3, which the `COPY` paths cannot use, so `database_url()` pins `+psycopg2`.

**Gate.** `test_postgres_integration.py` runs the whole thing against a scratch database (`WCD_TEST_DATABASE_URL`): `init-db` from the models → `stage0-load` on the committed metadata fixture (`fixtures/golden/stage0/fixture.parquet`, regenerated from the mwdump fixture and compared when `REVISIONCHEST_BIN` is set) → row-by-row check of pages, revisions, parent chain, timestamps and bundle offsets, Parquet partitions and types, gate pass, idempotency, gate failure on a wrong domain or missing Parquet → `extract`/`dedup`/`load` of the synthetic bundle with every instance id recomputed in Python and a second `load` leaving every count unchanged. RevisionChest's `--postgres` sink was also run against the model-created schema (not only its own `schema.sql` copy), and a parity test keeps that copy's tables, columns and nullability equal to the models — it caught one drift (`web_resources.url`). CI provides Postgres 16 to both jobs and passes the Rust job's binary to the Python job, so the integration test runs on every push. 183 tests pass with the database, 177 without.

**Phase 1 next (Stages 1–5):** the extractor writes `citation_history` / `named_ref_uses` / `revision_signals` straight into the partitioned layout and emits the candidate kind (`citation_type`) and self-closing reuse events; staging gains the `domain` column; `citation_type` becomes NOT NULL with the Stage-3 gate; Stage 4 (parts, via `wcd_semantic.content`/`reftag`/`template`) and Stage 5 (`ref_name_links`) get drivers. Design question to settle before the extractor change: whether Stage 1 should also bypass `dedup` for `citation_history` entirely, since that table no longer passes through Postgres.

## 9. Phase 1 — Stages 1–5 (2026-10-19)

Decisions taken first: Stage 1 writes `citation_history` / `named_ref_uses` / `revision_signals` straight into the partitioned layout (dedup never touches them again), and the phase runs through Stage 5 before review. Landed as `bf9ede6` (Stages 1–3) and `bf85803` (Stages 4–5) on `v3`, with extractor commit `613cf3e`.

**Extractor** (`wiki-references-extractor`). `extract_all(wikitext, domain)` returns references and *reuse events*; each reference carries `candidate_kind` (which scanner found it) and `citation_type` — `ref_tag` / `template` / `bare_url` / `endnote` / `shorthand` / `external_link` — decided from the kind, the domain's `citation_templates` and a bare-URL test on the `<ref>` content. Self-closing `<ref name="X" />` tags are no longer references: every occurrence is a reuse event with its offset, so repeats in one revision survive (the old raw-text dedup collapsed them). The standalone-shorthand scanner reads `shorthand_templates` per domain from `wikis.yaml` (en: the `{{sfn}}`/`{{harv*}}` family) instead of matching `{{sfn}}` only.

**Stage 1** (`wcd extract`, now `--domain` and `--parquet-dir` required; `extract-all` forwards both — the old "everything is en.wikipedia.org" gap is closed). Staging rows carry the deterministic `citation_instance_id`, the domain and `citation_type`; `documents`, article `web_resources` and `revisions` rows are gone (Stage 0 owns them). The three per-revision datasets go to the tier as `<dataset>/domain=/page_bucket=/<shard>-<i>.parquet`: a shard's rows are collected in flat files while the bundle streams and re-partitioned once by DuckDB at the end — one file per bucket per shard, bounded memory, no 512 open writers — and a re-run first removes exactly that shard's files. `revision_signals` (UVA's seven flags, every revision) streams to `domain=<d>/<shard>.parquet`.

**Stage 3.** `dedup` keys instances on `(domain, page_id, raw_sha1)` and runs the instance-id collision gate (distinct ids = distinct keys, else abort); `normalized_citations` carry `citation_type`, now `NOT NULL`; `load` takes the staged id (re-deriving and checking it), the domain and the type. `wcd gate <0|3|4|5> --domain` runs the gates of the integrity doc from `wcd_pipeline.gates`; the Stage-3 gate checks non-empty tables, no NULL type, every history id of a bucket sample against `citation_instances`, and the revisions Parquet count.

**Stage 4** (`wcd parts`). Content per `citation_type` (inside the `<ref>` wrapper; after the list marker; as is), bullet-block split via `wcd_semantic.content.iter_citations`, part type `template` / `ref_tag` / `external_link`, then `extract_template` or `classify`; a part the footnote filter rejects is kept as a `noise` `ref_tag` part so every citation has ≥ 1 part (the gate requires it). DELETE + INSERT per batch, `--resume` for restarts, a content cache for repeats. **4.6** `wcd denorm-export` writes `citation_instances_denorm` (unresolved; `--resolved` is 7c) by streaming the join out of Postgres into the same flat-file + re-partition path — deliberately *not* through DuckDB's Postgres scanner: the extension cannot be installed in this workspace (extensions.duckdb.org is off the allow-list) and the export does not need it; the count must equal `citation_instances ⋈ parts`.

**Stage 5** (`wcd ref-name-links`). Per bucket, a DuckDB gaps-and-islands query over `citation_history` × `revisions` × the domain's named instances: for each `(page, ref_name)` the defining instance per revision (smallest id if a revision defines the name twice), islands of equal instance in timestamp order become ranges, a definition still present in the latest revision leaves `last_revision_id` NULL. The domain is replaced in Postgres and in the `ref_name_links` Parquet. Gate: no overlapping ranges, no dangling instance, Parquet = Postgres.

**Gate.** Golden files regenerated for the synthetic bundle, which gained a named-ref redefinition (and page-B revision ids clear of the mwdump fixture): the only changes versus the Phase R goldens are the two self-closing instances disappearing, the article `web_resources` rows moving to Stage 0, and the new columns — plus the three tier datasets, now golden too. The Postgres integration test runs the whole chain 0 → 1 → 3 → 4 → 4.6 → 5 (the mwdump fixture and the synthetic bundle share page 101 and revisions 1001–1003 on purpose; their metadata is loaded from one directory, so `stage0-load` now rebuilds the domain's `revisions` Parquet from *all* metadata, each revision once) and checks the exact `ref_name_links` ranges — `diamond`: 1001–1002 first edition, 1003–1003 second edition (page 101 has later revisions without it); `web1`: 2201–open — the parts, the gates' pass and failure paths, and idempotency of every stage. 196 tests with a database, 185 without.

**Things to carry into Phase 2:**

1. The UVA author-date classifier does not recognise `Métraux, Alfred (1940)` (non-ASCII initial); upstream behaviour, preserved. Matching quality, not a port question — same bucket as the `ref_title` item from Phase S.
2. The UVA parser recognises only `{{cite …}}` / `{{citation}}` as templates (Stage 4.3). `{{Cita web}}` on it.wikipedia is a `template` citation at Stage 1 (from `wikis.yaml`) but a `ref_tag` part at Stage 4. Per-domain tuning was declared out of scope; the hook is `content._CITE_PREFIX_RE`.
3. `{{r|name}}` is a named-ref reuse on en.wikipedia (like a self-closing tag) and is not recorded in `named_ref_uses`; `{{rp}}` is a page number, not a citation. Neither is extracted; both are in the signals regex.
4. Stages 6–8 as designed ATTACH Postgres from DuckDB. Where that runs, the Postgres extension must be installable (or pre-bundled); otherwise the pg-side streaming pattern of `materialize.py` is the alternative for 7d/7h.
5. `wcd parts` is single-process Python; at full scale it wants the same per-shard parallelism as Stage 1 (split by `normalized_id` range).

## 10. Phase 2 — Stage 6 matching (2026-10-19)

Landed as `45ef3cb` on `v3`. The matchers themselves were vendored in Phase S; this phase is the runner around them.

**`wcd match --domain D`** (`wcd_pipeline.stage6`). Per `(domain, page_bucket)`, in a process pool: one DuckDB query over the bucket's unresolved `citation_instances_denorm` and `citation_history` partitions produces the design's 6a/6b input (unique parts per page with `n_revisions`, `first_rev_id`, `ref_name`); per page, largest first, `match_reftag.match_page` (6a), `match_template.match_page` (6b) and `merge_types.merge_page` (6c) run with the input shaping of upstream's `load_*` SQL (the Phase S assembly, now in production code); the six outputs land as `matching_intermediate/<name>/domain=/page_bucket=/data.parquet` with UVA's column names (`part_sha1`, `canonical_part_sha1`), and a `_SUCCESS` marker makes the bucket skippable (`--force` rebuilds, `--buckets` selects). 6d reads the domain's `merged_sources`, runs `match_global`, and writes `global_source_map` — with the component's cross-page `match_type`/`confidence`, as the design asks — and `global_sources_stage` with the canonical member's `source_type` for 7f. Gate 6: all six datasets present, merged map non-empty, map and merged sources cover each other.

**Two small additions to the vendored code**, both marked and tested: `merge_types` now lists every member of a merged source (`rt_source_ids` / `tm_source_ids`) beside upstream's first-member columns, because `merged_citation_source_map` has to cover every part and upstream only exposed the first ref/template member; and `citation_instances_denorm` carries `author_last` / `journal` / `pages` so the template matcher's canonical rows are complete. The cross-page fields the scorer reads (`id_tier`, `year_reliable`, `norm_subtype`) come from the merged source's *canonical member*, whichever side it is on, as the design specifies — upstream read them from the ref-tag side only.

**Per domain, not global.** `global_source_map` and `global_sources_stage` are `domain=` partitioned: per-page source ids are built from `page_id`, which is unique only within a domain, so a cross-domain run would need the domain folded into those ids first (deferred in the design). 7a has to be written knowing two domains can produce the same `global_source_id` for different sources until that happens.

**Gate.** The integration test runs the chain through Stage 6 on the synthetic bundle: the two editions of the Diamond book on page 101 become one template source on ISBN (n = 2); the ref-tag parts stay singletons; `{{sfn|Routledge|1919}}` merges with the Routledge `{{cite book}}` endnote on `author_year` (0.65, `source_type = both`); every part of the bucket has exactly one map row carrying the per-page match values; the Diamond book cited on both pages collapses to one global source (ISBN, `n_pages = 2`, `n_total_citations = 3`); singletons carry no confidence; `--force` reproduces the output byte for byte. 198 tests with a database, 186 without.

**Next (Phase 3, Stages 7–8):** 7a `global_sources` from the stage file, 7b `citation_source_resolution`, 7c the resolved denorm (already `wcd denorm-export --resolved`), 7d `page_source_summary` per bucket from Parquet (the reuse attribution through `named_ref_uses` × `ref_name_links`), 7e counts, 7f the scorer (`wcd_semantic.scoring`), 7g `global_source_scores`, 7h `page_summary_cache`, Stage 8 `page_revision_stats`. 7d/7h were designed as DuckDB-writes-Postgres through ATTACH; given item 4 of §9, the pg-side pattern (DuckDB computes per bucket, psycopg2 inserts) is the likely shape.

## 11. Phase 3 — Stages 7–8 (2026-10-19)

Landed as `5b10e46` on `v3`. With it the build pipeline of the design runs end to end: `init-db → stage0-load → extract-all → dedup → load → parts → denorm-export → ref-name-links → match → materialize → revision-stats`, each step with its gate.

**`wcd materialize --domain D`** (`wcd_pipeline.stage7`) runs 7a–7h in order (`--steps` selects a subset), each replacing its output for the domain. 7a upserts `global_sources` from the 6d stage file with counts and score NULL (an upsert rather than a truncate, because the global outputs are per domain — §10); 7b builds `citation_source_resolution` from `merged_citation_source_map ⋈ global_source_map ⋈ normalized_citation_parts` on `part_sha1`, restricted to normalized citations that actually have an instance on that page (a part's content can recur under other ref names on other pages), then deletes global sources nothing references any more; 7c is `wcd denorm-export --resolved`; 7d fills `page_source_summary` per bucket from the resolved denorm × history × revisions, with self-closing reuse attributed through `named_ref_uses × ref_name_links` as designed; 7e refreshes `n_pages` / `n_total_citations`; 7f runs `wcd_semantic.scoring` over each source's own columns plus the canonical member's `id_tier` / `year_reliable` / `norm_subtype` (parts on `canonical_part_sha1`); 7g exports `global_source_scores`; 7h writes `page_summary_cache` per bucket — revision counts, the citation set at the latest revision, type and template breakdowns, and the UVA source profile, whose classifiers are now vendored verbatim in `wcd_semantic.profile`. Per-instance classification uses the instance's first part, so the buckets sum to `reference_count`. The shape decision of §10 stands: DuckDB computes each bucket from the Parquet tier, psycopg2 writes Postgres, 7d and 7h checkpoint per bucket under `<parquet>/_checkpoints/`.

**`wcd revision-stats --domain D`** (Stage 8) is the design's query per bucket over `revisions × citation_history × named_ref_uses × resolved denorm`, every revision present with zeros, `--jobs` across buckets; its gate is one row per revision.

**Gate.** The integration test now runs 0 → 8 on the synthetic bundle and checks: the ISBN global source spans both pages with `n_total_citations = 3`, score 85 and `high_confidence`; resolution rows keep the per-page match values (`isbn` 0.95 on page 101, `singleton` on page 202 — the UVA `citations_resolved` semantics the design asked for); `page_source_summary` ranges, `is_current`, and reuse counts (five self-closing reuses of `diamond` on page 101 across the redefinition, one of `web1` on page 202); the page summary cache JSON; the scores export; and every `page_revision_stats` row of page 101 (6 → 8 → 6 citations, new-source counts 5 → 3 → 0, the 2nd edition joining the existing ISBN source). The Stage-7 gate also runs the four referential-integrity queries of the integrity doc. 202 tests with a database, 188 without.

**Worth knowing:** (1) a prose `<ref>` whose only identifier is a URL gets `high_confidence` from the scorer's `id_tier > 0` rule — upstream behaviour, visible on the fixture's two bare-URL refs; (2) `page_summary_cache` rows exist for every page in `revisions`, including pages whose latest revision carries no citation (the empty profile text is UVA's).

**Next:** Phase 4 — the V2 API (`apps/api`, `proposed_api.md`) over Postgres + the DuckDB views of `wcd_schema.duckdb`, then Phase 5 the dashboard. Before either, the fixture shard (`scripts/select_fixture_pages.py` → `wcd fixture-dump` → RevisionChest) would give the whole chain its first real-data run and the API a fixture database.

## 12. Phase 4 — the V2 API (2026-10-21)

Landed as `c308206` on `v3`. Between Phases 3 and 4 the fixture shard arrived from the other side (`ac3115b`…`6bdc368`: page selection, API history export, the ci shard with goldens, `test_fixture_ci.py`, two Stage 6/7 defects found by the first real-data run, and the 7f DSN fix), so the API had a real database to be checked against from the start.

**`apps/api` (`wcd_api`, console script `wcd-api`)** is the FastAPI service of `proposed_api.md`: Postgres through SQLAlchemy for the serving tables; the Parquet tier through DuckDB, one bucket partition per request via `wcd_schema.duckdb.bind_bucket_views` — no DuckDB Postgres extension and no pool, since a DuckDB connection is an in-process object and every read is pruned to one `domain=/page_bucket=` directory. Group 1 resolves curid URLs directly and `/wiki/Title` (or `?title=`) URLs through the site API (a pluggable resolver, so tests do not touch the network; a failure there is a 503, not a 500). Group 2 keeps the UVA dashboard's field names — `PageSummaryResponse`, `TimelinePoint`, `TopSource`, `SourceUsagePage` — plus `domain`, `url`, the full identifier set, `high_confidence_count`, and the new `/sources/{id}`; the summary is one read of `page_summary_cache`, the timeline one read of the page's `page_revision_stats` partition (the Stage-8 query on the raw partitions is the fallback when the dataset has not been built). Group 3 is the v1 shape with `citation_type` and `parts[]` (each part carrying its resolution *on that page*); the citation detail gives each part its `resolutions[]` across pages instead, since a normalized text's resolution is per page. Group 4 is v1 plus domain/page and source fields. `/api/v1/…` are aliases with the domain defaulted (`document_id` is gone, as designed). Errors are `{"error", "code"}`.

**Fixture for tests.** The synthetic bundle moved into the package (`wcd_pipeline.synthetic`) with `build_database(dsn, work)`, which runs Stages 0–8 on it in a few seconds; `apps/api/tests` build that database once and exercise every endpoint, the v1 aliases, validation and the error paths (13 tests, under `WCD_TEST_DATABASE_URL` like the rest). The service was also run against the ci shard after `test_fixture_ci.py` built it here (all 45 of its checks pass in this workspace too, 10 minutes): summary, top sources, revisions and a 170-citation page answer in 10–120 ms, the 327-point timeline in 0.3 s (DuckDB start-up dominates), and the cross-page endpoints return the expected things (a 7-page ISBN source, a `{{cite book}}` shared by five pages with its per-page resolutions). 218 tests with a database, 191 without, plus the shard test.

**Decisions taken without asking, to revisit if wrong:** CORS open by default (`API_CORS_ORIGINS`); no authentication; `url` fields use the `/w/index.php?curid=` form Stage 0 writes; `other_articles` on a citation lists the *other* pages whose instances share the normalized text (a named and an unnamed `<ref>` around the same template are two normalized texts, as in v1); template parameter values are served as stored (raw, trailing whitespace included — the v1 behaviour); the random summary samples `page_summary_cache` with `TABLESAMPLE` and falls back to `ORDER BY random()` on small tables.

**Next (Phase 5):** `apps/frontend` — the UVA dashboard with the mechanical changes of `proposed_frontend.md` (API base and domain-qualified paths, URL helpers on `currentDomain`, branding, data-driven identifier badges, the "reuses" relabel and the `high_confidence_count` series); `create_app` already serves `apps/frontend/dist/` at `/` when it exists. The runbook (`docs/runbook.md`) is still to be written.

## 13. Phase 5 — the dashboard (2026-10-21)

Landed as `123971e` on `v3`, with `docs/runbook.md` alongside. This closes the implementation plan of §3: every phase from R to 5 has landed on `v3`.

**`apps/frontend`** is the UVA analytics frontend (`index.html`, `app.js`, `styles.css` at `29f3ae3`) with the changes of `proposed_frontend.md`, no more and little less: `API_BASE = /api/v2` and domain-qualified page paths; a wiki-domain field (recent domains remembered in a datalist) beside a page field that takes a page ID *or an article URL*, the latter resolved through `/api/v2/article` — the one addition beyond the design, since Group 1 existed for exactly this; shareable links as `?domain=…&page_id=…` with the `#domain/page_id` form accepted; page, revision and diff links from the loaded page's `domain`, related-page links from each page's own domain; branding without UVA or engine names; identifier badges and links as a loop over the canonical set; "named refs" → "reuses", `high_confidence_count` as a dashed second series, a tooltip line and a highlight stat; labels for the six citation types; the v2 error shape. No build step: `wcd-api` serves the directory at `/` and `/static`, and the uv workspace excludes it.

**Gate.** Headless Chromium against the ci fixture: loading by id, by article URL (through `/article`), random, a hash link, the identifier badges on a medicine page (DOI / PMID / Link), "Show related pages" on the seven-page Dermatology source and navigation to one of them, and the error status for a missing page — all as intended; the only console errors are the ECharts CDN (unreachable from this workspace, so the page's textual fallback showed) and the deliberate 404. An API test asserts the dashboard is served with the right branding and base path. 219 tests with a database.

**Runbook.** `docs/runbook.md` records the full-build order, what each step owns and its re-run unit (shard for Stage 1, domain for 3–5 and 7, bucket for 6 and 8), the rules of thumb for partial rebuilds (new bundle, matcher change, normalizer change, crash), the `sync` path, and serving / refreshing under a running API (the service holds no DuckDB state, so a Parquet swap is a restart).

**What remains open, from earlier sections:** a live `RevisionChest sync` run from a networked machine (§7); the matching-quality items (§9: non-ASCII author names, non-English cite templates at Stage 4, `{{r|name}}` reuse); cross-domain matching (§10); incremental matching; `wcd parts` parallelism at full scale (§9); and the v1 cutover itself — pointing the explorer's users at the dashboard and retiring `legacy/`.

## 14. Rehearsal shard — first run and what it found (2026-10-21)

The rehearsal tier (308 pages, 379,720 revisions, one 342 MB bundle) was run for the first time, in the cloud sandbox (2 cores, ~6 GB usable): the bundle split in two on page boundaries (`scripts/split_bundle.py`), `scripts/run_pipeline.sh` driving Stages 0–8 with timings and a memory sampler. The run did not finish there — a Stage-1 worker was OOM-killed at 3.3 GB after an hour, and the other worker's progress (46 % of its 148k revisions after 2½ hours) put the remaining time beyond what the sandbox allows — but it found two defects, both fixed in `6a2defd`:

1. **Memory was unbounded by input size.** A batch was 1000 revisions whatever their size; on the long-history pages (the ADHD article: 14k revisions of ~150 KB) that is 150 MB of text per batch, expanding roughly tenfold into extracted references and template parameters. The batch now also flushes at `--batch-bytes` of revision text (`BATCH_BYTES`, default 32 MiB), and the worker returns freed heap to the OS after each batch. Measured on the same shard: 2.2 GB after 30 minutes before, ~0.6 GB after 18 minutes with the cap (a `malloc_trim` / `MALLOC_ARENA_MAX` variant made no further difference, so the retained memory was the batch itself, not fragmentation). This is the backpressure James asked about in its simplest form — the reader stops at a byte budget and nothing downstream buffers more than a row group — and the budget is explicit: about 20× `BATCH_BYTES` per worker.
2. **A killed worker was marked done.** `extract-all` wrote `DONE.txt` on any exit; the OOM-killed shard would have been silently missing from the build. It now checks the exit code, leaves `FAILED.txt` and no `DONE.txt`, reports the shard and exits non-zero; the next run clears and redoes that shard only. Tested with a corrupt bundle.

**Throughput, for planning.** The ci shard ran at ~32 revisions/s per core; the rehearsal pages ran at ~7–8 revisions/s per core, because citation-dense articles with long histories dominate and every revision re-extracts every reference. At that rate English Wikipedia's ~1.2 billion revisions are on the order of 40,000 core-hours of Python extraction — the number the extractor comparison sample (`scripts/sample_revisions.py`) exists to improve on with a Rust extractor. The split-bundle step matters for the same reason: a dump file is one bundle, so without it a 64-core machine extracts on one.

**Next:** run the rehearsal shard to completion on the Mac with the fixes (`fixtures/README.md` has the two commands; eight pieces, eight jobs), then look at Stage 6/7 timings and memory on the long pages and at API latency on them — none of which this run reached.

## 15. Rehearsal shard — second run: Stage 1 through, Stage-4 gate caught the extractor (2026-10-21)

The second run, on the Mac (16 pieces, 8 jobs), went through Stage 1 in 8705 s with the byte cap holding memory where §14 measured it, then dedup (7 s), load (342 s), gate 3 and the indexes without incident. `parts` wrote 179,100 parts for 176,676 normalized citations in 35 s and the **Stage-4 gate failed**: 18 `external_link` citations had no `extracted_url`.

All 18 were scheme-only text that MediaWiki never renders as a link: `<nowiki>http://</nowiki>` from help-page prose (fourteen of them, as `http://</nowiki>''`), and vandalism of the shape `[http:// LIAR! LIAR! …]` and `[http://\nwww.example.com/…]`. The extractor's bare-URL regex accepted a scheme followed by nothing, and its bracketed-link scanner accepted a space or newline after the scheme and ran to the next `]` on any later line. Fixed in the extractor (`packages/extractor` `13fd907`): a URL needs at least one URL character after `://`, with MediaWiki's URL character set (no whitespace, `[]<>"`, plus the existing `|` `}`); a bracketed link's URL starts right after `[` and its `]` is on the same line, so a bracketed link that spans lines is no longer swallowed (the URL inside is still found as a bare link). The `bare_url` ref classification uses the same character set. The gate was right and nothing in Stage 4 changed; the normalizer's own regex fallback stays as it was.

Because the fix changes what Stage 1 extracts, the rehearsal's staging set still holds the 18 rows and the rest of the run has to restart from Stage 1 (`extract-all` is re-run on all pieces: delete the per-piece `DONE.txt` or the staging directory, then `dedup` fresh, `load`, `parts` onward — `fixtures/README.md`). The ci fixture goldens were regenerated, and the regeneration exposed a quieter defect the same character set fixes: the URL of a bare `<ref>http://…</ref>` had been extracted as `http://…</ref>` — 159 of the ci fixture's 2,618 cited `web_resources` ended in `</ref>` and seven cited `domains` ended in `<`, and `<a href="…">text</a>` URLs carried their `">text</a>`. All now come out as the URL alone, which also merges five `global_sources` that were the same URL with and without the tail. One multi-line `[url label]` in the ci shard was an external_link spanning lines and is now the bare URL alone; a `<ref>` holding a URL plus `{{full}}` is `ref_tag` rather than `bare_url`.

**Open after this:** the rehearsal's Stages 4–8 and the API on the long pages (unchanged from §14); whether `<nowiki>…</nowiki>` spans should join comments as ignored spans in the extractor (help pages quote `<ref>` syntax inside them; today those come out as references, which is wrong but rare — decide with the extractor comparison sample).

## 16. Stage 1 profile and the per-page reference cache (2026-10-23)

The second rehearsal run (on the Mac, on battery) spent 8,953 s of its 2 h 40 m in `extract-all`, everything after it about ten minutes. A cProfile run of one piece, `fixture.xml-3` (19 pages, 54,008 revisions, 10.1M reference rows), split `process_revisions` as: `normalize_wikitext` 71 %, `extract_all` 26 % (of which `extract_templates_from_text` 13 points), everything else — signal flags, SHA1s, bundle reading, Parquet writes — under 2 %. mwparserfromhell's C tokenizer is in use; of ~8,000 s inside `parse()` only 477 s is tokenizing, the rest is building Python node objects.

Both hot functions run once per reference per revision, and consecutive revisions repeat nearly every reference: the rehearsal's staging set has 55.45M reference rows but only 199,049 distinct (page, raw text) pairs — 99.64 % repeats, every piece above 98 %. `PageRefCache` (`395dd58`, extractor `9a06e12`) memoizes the normalized text with both SHA1s and the template list by raw reference text, cleared on each page change and at `max_entries`, so no state crosses pages or shards and dedup is untouched. Pieces `fixture.xml-14` and `fixture.xml-8` re-extracted with it are row-for-row identical to the rehearsal run in all staging tables and tier datasets; `fixture.xml-14` went from more than 172 s to 26 s. Not run: the full ci fixture tier (no Postgres in the workspace used), and a timed full rehearsal on the Mac.

**Decisions taken without asking:** the cache lives in the pipeline, with the extractor change limited to an optional `template_cache` argument (default off) so the extractor stays usable on its own; the normalizer is unchanged.

**Open after this:** a re-profile of a cached run, which should leave the rest of `extract_all` (candidate scanning, per-revision `re.sub` comment stripping and the scanners) as most of Stage 1 — the input to the revision-skip and Rust-extractor decisions. The normalizer's double parse in `normalize_template` (each parameter value parsed, then re-parsed by `Template.add`) and its `sorted()` of `Parameter` objects by string comparison now cost 0.4 % as often; worth fixing for the normalizer's other users, and `normalized_sha1` may change since nothing depends on it yet.

**Follow-up, same day: overlap checks.** A second profile of `fixture.xml-3`, with the cache, took 3,276 s under cProfile against 22,110 s before (normalization 69 s, from 15,744 s); `extract_all` was 82 % of it, and about half of that was the scanners' and final assembly's overlap tests, each a walk over every span found earlier in the revision (8.3 billion `_span_overlaps` calls). The extractor now tests against a byte mask of claimed positions (`_SpanSet`, extractor `08c7ad2`, pointer `4ea4f12`), with the exact linear comparison kept for empty and negative spans. Pieces 14, 8 and 11 re-extracted row-for-row identical to the rehearsal; wall time on top of the cache 26 → 21 s, 45 → 33 s, 100 → 72 s. What remains of Stage 1 is per-character candidate scanning in Python (`_scan_sfn_templates` above all, then the list-item and external-link scanners) — the part a Rust extractor would replace, and which a revision skip can't avoid, since it is how a change is detected.

## 17. Stage 0 at full scale: enwiki-20261001 through RevisionChest, and two reader fixes (2026-11-05)

The first full-scale Stage-0 run: every enwiki-20261001 `pages-meta-history` 7z file (980 files, 240.7 GB) through `RevisionChest build --namespace 0`, on a Linux VM with 28 cores, 62 GB RAM and a 1 TB disk. The bz2 set (1.94 TB) does not fit the disk, and at the ~14 MB/s the mirrors allowed it would also have taken about eight times longer to download; RevisionChest streams 7z, so the 7z set is the input. A 7z archive can't be read while it is still downloading, because its index is at the end of the file. Each file therefore went to RevisionChest as soon as its SHA-1 checked out, up to 12 at a time, with one metadata Parquet file per dump file (which `wcd stage0-load --metadata <dir>` already expects). Download and processing overlapped.

**Result.** All 980 files built, with no failures: **895.6M namespace-0 revisions**, 201.7 GB of bundles and 47.8 GB of metadata. End to end it took **5 h 13 min**: the download (00:52–06:00 UTC, from `dumps.wikimedia.your.org` at about 13 MB/s per connection and 14 MB/s in total) set the pace, and the last bundle was written 5½ minutes after the last download. RevisionChest used 62.7 core-hours (~3,970 revisions per core-second), and no single file went above 171 MB RSS. One file set the tail: `history10.xml-p5120968p5137514` is the largest 7z (1.55 GB), unpacks to **568 GB** of XML (a single LZMA2 block, so it can't be split) and holds only 370k namespace-0 revisions, yet took 1,819 s. On the first files, namespace 0 cut the bundle output to about 0.15× the 7z size, against 1.5× for all namespaces. By the end the ratio was 0.84×, because the early page ranges are almost entirely articles with long histories.

**Profile.** perf on a typical file (history19 p27721413p27864664: 257 MB of 7z, 27.4 GB of XML) put **~38 % of all CPU in the 7z CRC check** and ~50 % in the 7z path overall. sevenz-rust 0.6.1 hashed with the `crc` crate's byte-at-a-time table. Of the rest, zstd took ~13 %, and XML parsing, unescaping and copies ~15 %. Piping system `7z x -so` into RevisionChest instead cut that file from 181 to 114 CPU-seconds.

**What landed** (RevisionChest `v3`, local, not pushed):

1. **Namespace filtering inside the reader** (RevisionChest `5484781`, pointer `e501e5c`). `mwdump::DumpReader::with_namespaces` makes the decision at a page's `<ns>` element and skips an unwanted page to its `</page>` with quick-xml's `read_to_end_into`, so its text is never unescaped or turned into strings. `build --namespace` uses it. On history10 CPU fell from 2,377 to 1,856 s, but wall time only from 1,819 to 1,719 s: unpacking became the limit.
2. **sevenz-rust2, single-threaded** (RevisionChest `f349d69`, pointer `0626f8e`). The maintained fork checks CRCs with `crc32fast` and decodes with lzma-rust2. 0.20.2 is the newest release whose MSRV is still 1.85. Its default multi-threaded LZMA2 reader starts one worker per CPU. On Wikimedia's single-block archives those workers have nothing to do in parallel, and on the typical file the reader cost +9 % wall time, 3× RSS and ~12k futex calls per second, so it is pinned to one thread.

Measured with both changes, against the previous binary: the typical file went from 111 s wall and 188 CPU-s to **75 s and 110 CPU-s**, and history10 from 1,819 s and 2,377 CPU-s to **372 s and 513 CPU-s**. Peak RSS was unchanged (~120–150 MB). Bundles and metadata Parquet are **byte-identical** on both files. A 7z with one byte flipped still fails the build (exit 1). Scaled up from these two files, the full run would now take roughly 35–40 core-hours; this has not been re-run.

**Decisions taken without asking:** use the 7z set, not bz2 (disk and download time); fetch from the your.org mirror, with SHA-1 checked against Wikimedia's `dumpstatus.json` and dumps.wikimedia.org as the fallback; take sevenz-rust2 0.20.2 rather than 0.24 so the MSRV stays at 1.85; pin its decoder to one thread. The user chose namespace 0 only and keeping the 7z originals.

**Not done:** the Python Postgres-tier check with `REVISIONCHEST_BIN` (no Postgres on the VM). The byte-identical enwiki outputs cover the same Stage-0 output. `wcd stage0-load` for this build also waits on Postgres.

**Follow-up, same day: copies, writes and ordering** (RevisionChest `8fade08`, `8e9fabe`, `889a954`, pointer `e22f840`). The XML reader no longer copies every event with `into_owned()`: the buffer is taken out of `self` for the length of one event, and unescaped text is moved in, not copied. The bundle writer assembles each revision in a reusable buffer and gives zstd one write instead of three per line. Stdout mode goes through a 1 MiB `BufWriter` (365 s → 72 s on the typical file, output equal to the decompressed bundle). `build -d` sorts files largest first and feeds them through `par_bridge`, because rayon splits a `Vec` into per-thread ranges and would ignore the order. Measured together: typical file 75 → **51 s** wall and 110 → **85** CPU-seconds; history10 372 → **349 s** and 513 → **473**. Byte-identical, RSS unchanged. Against the start of the day, the typical file is 2.2× faster in wall time, and history10 5.2×.

**Still open:** zstd long-distance matching (a format decision, as above). `build -d` writes all metadata through one sink thread and one Parquet file, which may limit a many-core `-d` run. The per-file runs avoided this, and it has not been measured. Element names are still allocated as a `String` per XML element. history10 is now bound by LZMA decoding itself (568 GB in ~350 s, ~1.6 GB/s). Its block is a single LZMA2 stream, so unwanted pages can't be skipped before decompression, and further gains there would have to come from the decoder.

**Open before the follow-up:** the bundle writer hands zstd one line at a time (`ZSTD_compressStream` plus `Counting::write_all`, ~7 %). Stdout mode (no `-o`) is line-buffered and ~3× slower. The XML reader copies each revision's text 3–4 times. `build -d` goes in name order, so the slowest file can start last. zstd long-distance matching (`--long=27`) roughly halves bundle size (194 → 101 MB on the typical file), but every bundle reader, Rust and Python, would need a 128 MB window limit, so it is a format decision rather than a tuning change.

**Follow-up: zstd long-distance matching** (RevisionChest `fc1d711`, pointer `af53f83`; Python readers `963bd2a`). Bundle frames are now written with long-distance matching and a 128 MiB window (`mwrev::WINDOW_LOG` = 27). The note above was wrong to call this a format decision needing reader changes. 2^27 is libzstd's default *decoder* limit, so the zstd crate, python-zstandard and `zstd -d` all read the new bundles with default settings. RevisionChest 0.1 readers read them too, and old bundles read as before. A window of 2^28 is refused by default readers, and it compressed no smaller. Measured on enwiki-20261001, namespace 0:

| | 2 MiB window | 128 MiB + LDM |
|---|---|---|
| history19 bundle | 194 MB | **101 MB** |
| history19 wall / CPU | 51 s / 85 s | 62 s / 97 s |
| history10 bundle | 66 MB | **35 MB** |
| history10 wall / CPU | 349 s / 473 s | 351 s / 467 s |
| RevisionChest RSS | 125–160 MB | 265–290 MB |
| Stage-1 reader RSS (python-zstandard) | ~40 MB | ~145 MB |

Decompressed content and metadata are byte-identical, and `fixtures/golden/stage0` matches the new binary (run by hand: no Postgres here). With the zstd CLI on the same 20 GB stream, the 128 MiB window alone gives 116 MB at the default level's CPU cost (18.5 s), and LDM takes it to 101 MB for about 15 more CPU-seconds. For enwiki that is an estimated ~105 GB of bundles instead of 201.7 GB.

**Decisions taken without asking:** LDM on, not just the window: Stage 0 was download-bound and bundles are stored and read many times. Window-only is the fallback if Stage-0 CPU ever matters more than ~13 % of bundle size. `scripts/split_bundle.py` keeps writing its pieces at plain level 3, because N open long-window compressors would cost N × ~150 MB for disposable files. Both Python readers now pass `read_across_frames=True`. python-zstandard 0.25 already reads across frames in chunked reads, but the parameter is documented to default to `False`, and `sync` bundles hold a frame per batch. The committed ci fixture bundle stays as it was (2 MiB window): it doubles as the old-format compatibility case.

Stage 1's memory budget now includes ~105 MB per worker for the decoder window (runbook).

**Follow-up: full re-run with `build -d`.** enwiki-20261001 was built again from the kept 7z files with the binary at `fc1d711`, as one `RevisionChest build -d … --namespace 0 -j 18` with one metadata file, on the same 28-core VM. A sampler read the process's and the metadata thread's CPU from `/proc` every minute.

| | First run (per file, during download) | `build -d`, all fixes |
|---|---|---|
| Wall time | 5 h 13 min (download-bound) | **2 h 08 min** (no download) |
| CPU | 62.7 core-hours | **45.2 core-hours** |
| Bundles | 201.7 GB | **90.2 GB** |
| Metadata | 47.8 GB (980 files) | 49.6 GB (1 file) |
| Peak RSS | ≤171 MB per file | 3.8 GB for 18 jobs |

All 980 files built. The metadata is identical: 19,314,502 pages and 895,604,187 revisions, with equal per-type checksums over every column. Five sampled bundles, history10 among them, decompress byte-identical to the first run's. Bundles came out 55 % smaller, more than the −47 % the two test files suggested; the early page ranges gained most (history1 p3395p3995: 307 → 58 MB). The shared metadata thread averaged 45 % of a core and peaked at 54 %, so the single sink is not a bottleneck at this scale. The process averaged 21.2 of 28 cores (each job ~1.2 cores, the big files decode-bound), so `-j 22`–`24` should fill the machine; not measured. With largest-first ordering there was no tail: the last minute still had 23 cores busy.

**Open:** nothing in Stage 0 itself. `wcd stage0-load` on the new metadata waits on a Postgres instance. The first run's bundles (201.7 GB) are still on the VM next to the new ones.


## 18. Stage 0 load at full scale: enwiki into Postgres (2026-11-06)

`wcd stage0-load` for enwiki-20261001 (namespace 0: 19,314,502 pages, 895,604,187 revisions, from §17's 49.6 GB metadata file) into a local Postgres 16 on the same 28-core VM. Postgres came from the PGDG apt repository, because Debian 13 ships only 17; the settings are in the runbook. Each run happened inside a `systemd-run` scope with `MemoryMax=36G`, so DuckDB sized itself next to Postgres.

**Baseline** (`941b610`): 8 h 47 min, gate pass. Pages took 85 min: each 100k batch went through SQLAlchemy as an `INSERT … VALUES` with ~400k parameters, for `page_metadata` and again for `web_resources`. Revisions took 6 h 43 min: one connection, one transaction, ~40k rows/s, with one backend at 100 % CPU on each row's primary-key entry and two foreign-key checks. The revisions Parquet took 31 min and the gate 7 min. The Parquet dataset came out as 685,899 files of ~19 KB, because with DuckDB's default of 100 open partition files a `page_id`-ordered write keeps reopening the 512 bucket files.

**What landed** (`c62b681`, `4e3c76a`, `05318e3`, `1c47f72`):

1. Pages: `COPY` into a temp table, then set-based upserts, with `url_hash = md5(url)` computed in SQL. Titles are escaped for `COPY`.
2. Revisions: one DuckDB sort by `rev_id` into a Parquet file in `--work-dir`; each 1M-row row group is a unit with a disjoint `rev_id` range, loaded by one of `--jobs` connections and committed on its own, so an interrupted load resumes. Rows reach Postgres as pyarrow CSV, with no per-row Python. A first version took its ranges from `row_group_id` instead of the `rev_id` minimum, which left all units but one empty. A fast-tier test now checks that the ranges cover every revision exactly once.
3. One file per bucket: `partitioned_write_max_open_files` is set above the bucket count. On a slice the write was 2× faster and a bucket read 25× faster.
4. The primary key is rebuilt after a load into an empty table. Parallel range inserts split B-tree pages in the middle, giving 31 GB instead of 19 GB.
5. The revisions Parquet is deduplicated only when an exact duplicate count (13 s) finds duplicates, which only overlapping `sync` files produce. That took the step from 1,006 s to 289 s in isolation. Deduplicating with `arg_min … GROUP BY` instead ran out of memory.
6. The gate's page check is now an anti-join. Its `DISTINCT page_id` over all revisions depended on `ANALYZE`'s n_distinct estimate (681k against 19.3M real pages); with that estimate the hash aggregate ran for 7 h 56 min before I cancelled it. The anti-join takes 94 s whatever the estimate.

Measurements on a 20.3M-revision slice drove the settings. With the default `wal_buffers` of 16 MB, 16 connections reached 163k rows/s, mostly waiting on `WALInsert`/`WALWrite`; with 256 MB, 211k. Without the two foreign keys on `revisions` they reached 344k.

**Result** (`1c47f72`, `--jobs 16`): **1 h 30 min 42 s** end to end, gate pass. Pages 14 min, sort 6 min, inserts 48 min, key rebuild 11 min, Parquet 8 min, gate 2 min. Peak RSS 37 GB, work directory 45 GB at its peak; `revisions` is 89 GB (70 GB heap, 19 GB key), the same as the baseline. Every output matches the baseline exactly: order-independent checksums over all columns of `revisions` (bundle as file path), `page_metadata`, the article `web_resources` rows and the revisions Parquet. The Parquet dataset is 512 files instead of 685,899. Tests: Postgres tier and ci fixture, 284 passed, 1 skipped (the golden-file recorder).

**Decisions taken without asking:** Postgres 16 from PGDG; `wal_buffers` 256 MB and the other settings in the runbook; the foreign keys on `revisions` stay during the load (dropping and re-adding them would save roughly a third of the insert time, but it is a schema-management change, so it is open below); the `REINDEX` runs only after a load into an empty table, because it blocks writes; `--jobs` defaults to 8.

**Fixed along the way:** `apps/api` test `test_template_report_and_web_resources` expected the untrimmed template value `"Diamond "`. Extractor `f41db2d` trims values on purpose, which the user confirmed. Only the Postgres tier runs that test, which is how it went unnoticed (`941b610`).

**Open at the time:** dropping and re-adding `revisions`' foreign keys around a fresh load; a `--memory-limit` for the loader's DuckDB; whether `tier.py`'s partitioned writes (Stage 1) produce the small-file pattern of item 3. See the follow-up.

**Follow-up, same day.** *Memory limit* (`6bcd75b`): `--memory-limit` / `STAGE0_MEMORY_LIMIT` applies to every DuckDB connection the loader opens, and the Parquet step now spills under `--work-dir`. On enwiki, outside any cgroup, `--memory-limit 20GB` gave 27.2 GB peak RSS (the process runs ~5–7 GB above DuckDB's limit) and 476 s for the Parquet step. *Stage 1 small files* (checked, not changed): `tier.repartition` orders rows by bucket, so within a shard it writes about one file per bucket it touches (1.2 on average for a 20M-row synthetic shard, ~265 KB each), not the reopen pattern the revisions write had. By design every shard writes its own file into each bucket, though, so enwiki's 980 bundles would leave ~1,200 files per bucket in `citation_history` and `named_ref_uses`. Batch stages won't notice. The API reads one bucket per request, and one page lookup took 169 ms cold / 132 ms warm over 1,000 shard files against 64 / 51 ms over one file with the same rows. A per-bucket compaction after `extract-all` would remove that, but it changes Stage 1's "a re-run replaces only that shard's files" contract (runbook ownership table), so it is open. Still open as well: dropping and re-adding `revisions`' foreign keys around a fresh load.

**Foreign keys: tried at full scale, not adopted.** The 20M-revision slice suggested that the two foreign keys on `revisions` cost ~40 % of the insert rate (211k against 344k rows/s on 16 connections). So I implemented dropping them before a load into an empty table and restoring them afterwards. The definitions came from `wcd_schema`'s model, they went back `NOT VALID` in a `finally`, and they were validated after a successful load. Tests covered a crash mid-load and a bad row slipped in while they were off. On enwiki (same settings as the 1 h 31 min run) the inserts took 51 min against 48.5 with the keys in place: at full scale, with 895 units streaming, the load already ran at ~308k rows/s with the keys, so the WAL and index writes are the limit, not the per-row checks. The two `VALIDATE` passes added 9.7 min (4.5 + 5.2), for 1 h 46 min 38 s end to end. The change was not committed; the patch is kept outside the repository. Lesson: the slice understates throughput at full scale (21 units against 895), so measure a load change end to end before adopting it.

## 19. Stage 1 at enwiki scale: a stratified benchmark, and staging once per page (2026-11-07)

A full Stage 1 for enwiki is days of CPU, so it was measured on a sample and extrapolated. enwiki-20261001 namespace 0 holds **21.6 TB of revision text** (19.3M pages, 895.6M revisions), and the work is extremely skewed: the heaviest 1 % of pages hold 75 % of the text, the heaviest 0.1 % hold 39 %, and 1,256 pages have more than 1 GB of history each. The sample is 834 pages, drawn at random within strata of total page bytes (powers of ten, seeded), with far denser sampling at the heavy end. That is 219k revisions and 20.3 GB of text. Each page's history was cut out of the real bundles by decompressed byte range (from the metadata offsets) and checked against the metadata to the byte. The sample bundles then ran through `wcd extract`, 28 concurrently, as a full run would.

**Throughput.** 3.4–4.6 MB of text per CPU-second for pages above 1 MB of history, far beyond §14's 7–8 revisions per core-second before §16. Extrapolated per stratum (cost per byte, or per revision for the smallest pages): **~1,555 core-hours** for enwiki (90 % bootstrap interval 1,494–1,610). Dump-file bundles are balanced for Stage 1 (largest predicted 2.5 h, median 1.6 h), so one process per bundle on 28 workers finishes in ~56 h, close to the ideal; splitting bundles would not help. Peak RSS is 2 GB per worker, on the heaviest page. A sampling profile (py-spy) of a heavy page: extractor 45 %, writing staging rows 19 %, signal flags 7 %, normalization 5 %. cProfile had blamed 98 s on `tier.repartition`, which takes 0.08 s, so its numbers were discarded.

**Staging once per page** (`0af1bf9`). Stage 1 wrote a full set of staging rows for every occurrence of a reference in every revision, and `wcd dedup` removed the repeats. On the sample, staged rows were 324× the distinct (page, reference) pairs (`template_data` 96×). That extrapolates to ~1.2 TB of staging for enwiki, more than the VM's free disk. The user explained the history: tracking seen citations had used too much memory, so rows were persisted at once and deduplicated later. The data showed that nearly all repeats are within a page (103,719 distinct reference texts across pages against 103,821 distinct (page, reference) pairs), and that a page's distinct references are few (13.7k for the heaviest sampled page). So `PageRefCache` now also keeps the exact values of the staging rows its page has produced, clears them with the rest of the cache on a page change, and skips a repeat before building it. Memory is bounded by the largest page, not the shard, and a cap of 2M keys falls back to the old behaviour. `dedup` only uses DISTINCT and MIN, so its output cannot change.

Verified on the sample, before and after: every deduped table and all three Parquet-tier datasets are identical. CPU fell 4,768 → 3,611 s (−24 %) and staging 1,140 → 25 MB; `dedup` went 7.2 → 1.8 s and 4.1 → 1.1 GB peak. New estimate for enwiki: **~1,200 core-hours (90 % interval 1,161–1,242), ~43 h on 28 cores, ~27 GB of staging, ~17 GB of Parquet tier.**

**Decisions taken without asking:** exact row values as the per-page keys (no hashing, so no collision question; the strings are shared with the cache); a 2M-key cap.

**Open:** a full enwiki Stage 1 run to confirm the estimate. The extractor's candidate scanning (45 %) is where a Rust extractor would pay. `_TableWriter._flush_buffer` still builds Arrow tables row-wise (`from_pylist`), now on 1/45 of the rows. `citation_history` remains one row per reference per revision (33.6M rows for the sample; ~17 GB of Parquet for enwiki), and could be stored as revision ranges if a later stage needs it smaller. Stage 1's ~1,200 files per Parquet bucket (§18 follow-up) are unchanged.

**Follow-up: the full enwiki Stage 1** (`8af7a75`; `wcd extract-all -j 28` on the 28-core VM, in a systemd scope with `MemoryMax=40G`; 2026-11-07 01:37 → 2026-11-08 22:54). All 980 shards finished, none failed, in **45 h 17 min**, using **1,186 core-hours** of CPU (user + system) against the sample's estimate of 1,202 (interval 1,161–1,242). `revision_signals` has exactly one row per revision (895,604,187, none missing). `citation_history` holds 27.84 billion rows and `named_ref_uses` 6.77 billion. All 28 workers stayed busy until 42.4 h; the remaining 27 shards then drained over 2.8 h, the last one alone for 1 h. This run used the old smallest-first order (`be9ee55` changes it to largest-first); the simulation had predicted 44.1 h for smallest-first and 43.6 h for largest-first. The scope's memory peaked at 39.5 GB of its 40 GB cap, and the host never had less than 23.5 GB available.

**Sizes, corrected.** Staging came to **78.5 GB** and the Stage-1 Parquet tier to **33.3 GB** (`citation_history` 16.5 GB, `named_ref_uses` 11.9 GB, `revision_signals` 4.9 GB), against the 27 GB and 17 GB estimated above. The estimate scaled output by text bytes. After the per-page dedup, though, staging grows with the number of distinct references per page, and the per-revision datasets with references × revisions, and small pages have more of both per byte than the heavy-weighted sample. The CPU estimate, which does scale with bytes, held to within 1.3 %. The tier is ~1,150 files per bucket for `citation_history` (589,258 files) and ~940 for `named_ref_uses` (483,055), as the §18 follow-up predicted.

## 20. Stage 3 at enwiki scale: dedup in key partitions, template parameters to Parquet, and where the build stands (2026-11-09)

**dedup.** After Stage 1's per-page staging (§19), the staged rows are already nearly unique. On 400 shards, 123,581,762 `citation_instances` rows dedup to 123,576,307. A single `GROUP BY` over a table therefore holds one group per row (~330M for `citation_instances`), and DuckDB keeps `MIN(varchar)` state outside its memory limit. The first enwiki attempt (32 GB DuckDB limit) was stopped by Claude Code's memory-pressure guard. The second (20 GB limit, in a 28 GB systemd scope) was OOM-killed inside the scope within a minute. On 124M groups the process peaked at 11.1 GB under a 4 GB limit. A DISTINCT-first rewrite does not help, because there is nothing to remove. `dedup` now runs the four large tables in key partitions (`2fac6c6`): one pass splits the staged rows to Parquet by a column every row of a group shares (`page_id`, `hash(normalized_sha1)`), then the unchanged `GROUP BY` runs per partition and streams into the single `deduped/<table>.parquet`. The partition count comes from Parquet row counts (`--rows-per-partition`, default 20M). The output is identical: on the 400 shards, 5.9 GB peak against 11.1 GB, in the same time. On enwiki (8 GB DuckDB limit, 28 GB scope) it peaked at 14.9 GB RSS. Results:

| table | staged rows | deduped rows | time |
|---|---|---|---|
| citation_instances | 338,075,299 | 338,050,560 (collision gate passed) | 10.5 min |
| normalized_citations | 322,401,267 | 284,514,231 | 19.5 min |
| ncwr | 321,663,432 | 289,840,001 | 7.4 min |
| template_data | 2,841,094,631 | 2,511,487,354 | 57 min (143 partitions) |
| web_resources / domains / wiki_templates | — | 107,961,812 / 7,509,746 / 311,839 | — |

`template_data`'s first attempt ran out of disk in its split pass; the failure was clean, and the user then approved deleting the first Stage-0 run's bundles and metadata (233 GB). The deduped files total 160 GB and are 1–4× their staged inputs: hash partitioning scatters rows that compressed well side by side, and pyarrow's zstd level is lower than DuckDB's. Sorting within a partition, or a higher compression level, would recover this; not done.

**Template parameters to Parquet** (`edd0cfb`, docs `0742d59`). As a Postgres table with its keys and two secondary indexes, `template_data` would be ~750 GB (estimated from sampled row widths), three quarters of the load. It is now two Parquet datasets written by `wcd load` (`template_params` by page bucket, for the page and citation endpoints; `template_param_index` by `search_bucket` = md5(template, key, value) mod 512, for the report). There is also a small Postgres table, `template_value_counts`, holding report totals for values with ≥ 1,000 citations. Prototype latency on real enwiki partitions, server-style: report 19–36 ms, 160 ms for a 1.48M-match value before precomputation; page lookup 20 ms, 619 ms for the heaviest page (392,784 template rows across its history). The report now lists citations in `normalized_sha1` order. All tiers: 366 passed. The ci goldens were re-recorded, with only the template snapshots changed.

**Disk: the build does not fit on this VM.** The estimates below (sampled row widths for Stage 3; Stages 4–8 scaled from the ci fixture by enwiki's row counts, ±20–30 %) assume everything is kept (7z dumps, staging, deduped files):

| remaining | estimate |
|---|---|
| Stage 3 Postgres without `template_data`, all indexes | ~265 GB |
| template Parquet (both layouts) | ~60 GB |
| Stage 4 `normalized_citation_parts` (~300M rows, 9 indexes) | ~270 GB |
| Stage 7 `citation_source_resolution`, `global_sources`, `page_source_summary`, `page_summary_cache` | ~155–170 GB |
| Stage 5 `ref_name_links` | ~25 GB |
| Parquet: denorm, matching intermediates, revision stats, scores | ~60–190 GB |
| WAL and stage spill while running | +30–80 GB |

That is ~850–950 GB against 182 GB free: **~750 GB more disk (range 600–950 GB); provision 1–1.5 TB.** `normalized_citation_parts` is now the largest single item, and the next place to look for savings: it holds the UVA normalizers' output for every normalized citation, every historical version included.

**Where the build stands** (enwiki-20261001, namespace 0):

| stage | state |
|---|---|
| 0 RevisionChest + `stage0-load` | done: bundles (85 GB), metadata (47 GB), Postgres `revisions` / `page_metadata` / article `web_resources`, revisions Parquet; gate 0 passed |
| 1 `extract-all` | done: 980/980 shards; staging (raw shards ~67 GB) and the Parquet tier (`citation_history`, `named_ref_uses`, `revision_signals`, 33 GB) |
| 3 `dedup` | done: 8/8 tables in `staging/deduped/` (160 GB) |
| 3 `load`, gate 3, `init-db --add-indexes` | **next**, after the disk is added: `wcd load -d $STAGING --parquet-dir $PARQUET_DIR` |
| 4–8 | not started |

The database already has the new schema (`template_data` dropped, `template_value_counts` created) and holds only Stage-0 data (95 GB). Postgres needs the settings from the runbook. Run memory-heavy steps inside a `systemd-run --scope -p MemoryMax=…`, with the step's own DuckDB limit set, as above.

**Decisions taken without asking:** `dedup` partitioned by `page_id` / `hash(normalized_sha1)` with a 20M-row default; template rows keyed by name and `normalized_sha1` (not Postgres ids), so `load` never resolves 2.5 billion ids; md5 for the search bucket (DuckDB's `hash()` is not stable across versions); a 1,000-citation threshold for precomputed totals; report order by `normalized_sha1`.

**Open:** add the disk, then Stage 3 `load` onward; the deduped files' compression; whether `normalized_citation_parts` needs every historical normalized citation; response caching for the API (the build-id-keyed LRU of the discussion, not built); Stage 1's ~1,200 files per Parquet bucket (§18).

## 21. Stage 3 `load` at enwiki scale: COPY, parallel units, key order, and where it stops (2026-11-09/10)

The disk was grown to 2.5 TB (1.6 TB free), enough for the rest of the build by §20's estimate. This section benchmarks `wcd load` on enwiki-20261001 namespace 0. All runs used the 28 GB systemd scope, `LOAD_MEMORY_LIMIT=8GB`, and the Postgres settings in the runbook. Before each run, the tables were reset to the post-Stage-0 state: the cited `web_resources` rows were deleted (`numeric_page_id IS NULL`; all 19,314,502 Stage-0 rows have it set) and `normalized_citations` was truncated with its cascade. Logs and a per-minute row sampler are in `/home/jh/wcd-data/stage3-load/`.

**Baseline (39424d5: Python lookups plus multi-row `INSERT … VALUES`).** `domains` took 5 min 50 s for 7,509,746 rows (21.5k rows/s). `web_resources` ran at a steady 7.8k rows/s over 16 minutes, about 3.85 h for its 107,961,812 rows; the four citation tables hold ~1.02 billion rows. The `ncwr` loader looked URLs up by `web_resources.url`, which has no index (only `url_hash` does), so on enwiki it would have scanned ~127M rows per 1,000 URLs and never finished. The run was stopped during `web_resources`.

**COPY and `INSERT … SELECT` (454a3c6).** `web_resources`, `normalized_citations`, `citation_instances` and `ncwr` now go through the COPY path that §20 wrote for template data. DuckDB reads the deduped Parquet and computes `url_hash` (`md5(url)`) and the checked `citation_instance_id` (`instance_id_sql`; a mismatch raises). COPY fills a temp table, and one `INSERT … SELECT` per unit resolves the natural keys in Postgres. `ncwr` resolves URLs through `url_hash`. The skip rules, the upsert onto Stage-0 article rows, and the id check are unchanged; `test_load_sets` covers them, and the ci fixture golden run is identical. With one connection, `web_resources` took 3,653 s (29.6k rows/s), with that one backend at 100 % of a core and the machine otherwise idle.

**Parallel units (6052d06).** `copy_parquet_parallel` loads one Parquet row group per unit on `--jobs` connections (`LOAD_JOBS`, default 8), each unit committed on its own, as Stage 0 loads revisions. The deduped tables have one row per conflict key, so units never write the same key, and an interrupted load resumes on re-run. With 16 jobs, `web_resources` took 1,039 s (103.9k rows/s). `normalized_citations` started at ~114k rows/s and fell steadily to ~12k (9,822 s for 284,514,231 rows, 29.0k rows/s on average). Its `normalized_sha1` unique index (18 GB; random keys leave pages ~70 % full) outgrew the cache, so every insert became a random read: 14 of 16 backends in `IO:DataFileRead`, the disk 87 % busy at ~5.7k IOPS. `citation_instances` then committed no unit (500k rows) in 16.6 min, while an insert-triggered, throttled autovacuum read `normalized_citations`' 68 GB heap.

**Key-ordered units and a VACUUM before the lookups (0b1cbe1, 13d7532).** With `sort_by`, DuckDB first writes a copy of the deduped file in key order (in `<staging>/.load-sort`, removed afterwards; a SIGTERM skips that cleanup). The units are its row groups, started in key order, so the 16 in flight cover adjacent key ranges. Keys: `md5(url)` for `web_resources`, `normalized_sha1` for `normalized_citations`, `(domain, page_id, raw_sha1)` for `citation_instances` (the order of its largest index), and `(normalized_sha1, url)` for `ncwr`. `VACUUM (ANALYZE)` now runs on the lookup tables before `citation_instances` and `ncwr`, unthrottled and only once. Results with 16 jobs:

| phase | unsorted, 16 jobs | sorted, 16 jobs |
|---|---|---|
| `web_resources` (108.0M) | 1,039 s | 127 s sort + 1,077 s load = 1,204 s |
| `normalized_citations` (284.5M) | 9,822 s, 114k → 12k rows/s | 2,558 s sort + 3,543 s load (80k rows/s, no decline) = 6,101 s |
| VACUUM `normalized_citations` | — (throttled autovacuum during the next phase) | 387 s |
| `citation_instances` (338.1M) | no unit committed in 16.6 min | 606 s sort; no unit committed in 22 min |

Sorting gains nothing for `web_resources`, whose 9 GB `url_hash` index stays cached. It removes the collapse for `normalized_citations`, but the sort itself (43 min, mostly DuckDB writing the ordered run on about one core) eats much of the gain. With sorted units, 10–14 of 16 backends wait on `LWLock:BufferContent`. The likely cause is that every connection takes ids from the same sequence and appends to the rightmost leaf of the primary key.

**Why `citation_instances` still stalls.** An `EXPLAIN (ANALYZE, BUFFERS)` of the real INSERT on 20,000 sorted rows, in one extra connection beside the 16 loaders, took 241 s (83 rows/s):

- 44.5 s (2.2 ms per row) went on the `normalized_sha1` lookups: 52k pages read, and 45k dirty pages that backend had to write out first to get free buffers.
- Most of the rest went to the INSERT itself, mostly waiting for buffers to be written back. Each write is a synchronous random 8 kB write (this disk: ~30 ms write latency, ~240 MB/s writes, ~25k read IOPS).

The dirt comes from the foreign key on `normalized_id`. Each check takes a `FOR KEY SHARE` lock on the referenced row, which writes `xmax` into a random page of the 68 GB `normalized_citations` heap. Across 338M rows, the whole referenced heap becomes random writes. No single input order fixes this: `citation_instances` is keyed by page, the lookup by normalized citation. `ncwr` has the same pattern against `web_resources`. The run was stopped there. The database keeps `web_resources` (127,276,314 rows) and `normalized_citations` (284,514,231) fully loaded; `citation_instances` and `ncwr` are empty.

**Decisions taken without asking:** the COPY and parallel rewrite of the four loaders (output identical, re-run behaviour unchanged); index lookups forced per unit (`index_lookups`: hash and merge joins off) for `citation_instances` and `ncwr`; key-ordered units; VACUUM before the probing phases; `LOAD_JOBS` default 8 (16 on enwiki). All tiers: 370 passed, 3 skipped.

**Open (needs a decision):**
1. **Defer the foreign keys of `citation_instances` (`normalized_id`, `domain_id`) and `ncwr` when loading into empty tables:** drop, load, then `ADD CONSTRAINT … NOT VALID` plus `VALIDATE CONSTRAINT`, one set-based join without row locks. §18 measured this on `revisions` and did not adopt it. There the referenced tables were small. Here the referenced table is 68 GB and the per-row lock is the bottleneck. A crash between drop and re-add has to be handled on re-run.
2. **Resolve `normalized_sha1` to an id without a heap fetch per row:** `INCLUDE (id)` on the unique `normalized_sha1` index (index-only lookups), or resolution in DuckDB from an exported `(normalized_sha1, id)` file.
3. **The hot rightmost primary-key leaf:** have the loader assign each unit a disjoint id range (offset plus row position in the sorted file, then `setval`), as Stage 0's units have disjoint `rev_id` ranges.
4. **The sort's cost:** have `dedup` write the citation tables in key order (it already partitions by key), or sort only the key and a row index.
5. **I/O:** a larger `max_wal_size` (57 requested checkpoints during `normalized_citations`) and the disk's throughput limit.

## 22. Stage 3 `load`: deferred foreign keys, `INCLUDE (id)`, per-unit id ranges, measured; Stage 3 complete on enwiki (2026-11-10)

This section follows up §21's open items 1–3, which the user asked to try alone and in combination. All runs used 16 jobs, the 28 GB scope, and the key-sorted units of §21. The sorted copies were kept between runs (`LOAD_KEEP_SORTED=1`, `52b3f82`: copies are named after their source and reused). Times below are load time, without the sort, unless a row says otherwise. Partial runs compare the first 200 units. Logs: `/home/jh/wcd-data/stage3-load/exp-*.log`.

**normalized_citations (284.5M rows).**

| variant | first 200 units | full load | notes |
|---|---|---|---|
| sorted, sequence ids, FK kept (§21) | 1,246 s | 3,543 s | 10–14 of 16 backends on `LWLock:BufferContent` |
| per-unit id ranges | 1,212 s | — | the BufferContent waits are gone; backends now wait on `WALInsert` / `WALWrite` |
| deferred FK | 1,302 s | 3,578 s, + 449 s to validate | — |

With id ranges, each unit takes ids `max(id) + 1 + row position`, so the units append to separate ranges of the primary key. That removes the contention on its rightmost leaf, but the load is WAL-bound and throughput did not change. The FK on `appears_on_domain_id` references a single domain row, and deferring it saves nothing while its validation costs 7.5 min. Neither was adopted; the id-range code was removed and stays in `52b3f82`.

**citation_instances (338.1M rows).**

| variant | rate | notes |
|---|---|---|
| FKs kept, plain `normalized_sha1` index (§21) | < 6k rows/s | no unit committed in 22 min |
| `INCLUDE (id)`, FKs kept | ≈ 0 | no unit committed in 13 min; 541 read IOPS but 3.7k random write IOPS at 89 % busy (the FK row locks dirtying `normalized_citations`' heap) |
| FKs deferred, plain index | 42–54k rows/s | 20–38k read IOPS: the `normalized_sha1` leaf plus a heap fetch for `id`, per row |
| FKs deferred, `INCLUDE (id)` | 81.8k rows/s average (up to ~140k) | full run: 4,132 s, + 225 s and 336 s to validate the two keys |

Building the `INCLUDE (id)` index on the loaded table took 8.6 min (18 GB, the size of the old index, which inserts had left ~70 % full). Both changes are needed: the per-row FK lock is the bottleneck, and once it is gone the heap fetch for `id` is.

**ncwr (289.8M rows), keys deferred:** 650 s sort, ~1,460 s load (255k rows/s steady, 617 read IOPS: `web_resources` stays cached, so `url_hash` needs no `INCLUDE`), and 655 s + 148 s to validate: 49 min in all. Every row resolved.

**Template parameters (phase 8, first run at enwiki scale):** `template_param_index` 1,365,988,136 rows in 24 min; `template_params` 6,438,109,681 rows in 86 min (one row per template parameter per page that uses the citation); `template_value_counts` 122,757 heavy values. 1 h 51 min in all, peak RSS 11.6 GB (`LOAD_MEMORY_LIMIT=8GB`). The Parquet takes 35 GB and 25 GB.

**Adopted (`9003000`):**

- `load` defers the foreign keys of `citation_instances` and `normalized_citation_web_resources` when the table is empty at the start of its phase (`--defer-fks`, `LOAD_DEFER_FKS`, default on). Afterwards it adds every missing declared key `NOT VALID` and validates every unvalidated one. This runs after every load phase, so a load killed in between restores the keys on its re-run, and gate 3 fails while any is missing or unvalidated (`test_interrupted_deferred_load_restores_foreign_keys`).
- `normalized_citations`' natural key is now `UNIQUE INDEX normalized_citations_normalized_sha1_key (normalized_sha1) INCLUDE (id)`, which replaces the unique constraint under the same name. `ON CONFLICT (normalized_sha1)` infers it, and `init-db` always creates unique indexes. An existing database needs the swap by hand: create the new index, drop the constraint, rename.

**Stage 3 on enwiki, end to end** (phase by phase across these runs; the sum is ≈ 6 h 25 min for `load`):

| phase | rows | time |
|---|---|---|
| domains | 7.5M | 6–8 min |
| web_resources (cited) | 108.0M | 20 min |
| normalized_citations | 284.5M | 1 h 42 min (incl. 43–48 min sort) |
| citation_instances | 338.1M | 1 h 28 min (10 min sort, 69 min load, 9.4 min validation) |
| ncwr | 289.8M | 49 min |
| template Parquet | 2.5B → 1.37B + 6.44B | 1 h 51 min |

Against the §21 baseline (row-by-row upserts at ~7.8k rows/s, with ncwr never finishing), the four Postgres phases went from 36+ hours (projected) to ~4.3 h. **Gate 3 passes** (11 min; it now includes the foreign-key check), so Stage 3 is complete for enwiki-20261001.

**Decisions taken without asking:** dropping id ranges after measuring no gain; keeping `normalized_citations`' FK; making deferral the default for the two tables; no `INCLUDE` on `url_hash`; reusing a kept sorted copy, matched by source size and mtime.

**Open:** the sort is now the largest single cost (43–48 min of DuckDB writing the ordered run on about one core for `normalized_citations`). Having `dedup` write the citation tables in key order, or sorting only the key and a row index, would remove most of it. `init-db --add-indexes` (Stage 3's secondary indexes) has not been run yet. WAL and checkpoint I/O is the remaining Postgres limit (§21 item 5).

## 23. Stages 4–6 at enwiki scale: what broke and what changed (2026-11-10/11)

Run on enwiki-20261001 after Stage 3 (§22), on the 28-core, 62 GB VM, each step in a memory-capped systemd scope. All changes keep the output identical unless a bullet says otherwise; the fixture goldens and the upstream `wcd_semantic` goldens pass throughout (full suite: 374 → ~390 tests, all passing).

**Indexes after Stage 3.** `init-db --add-indexes` built every secondary index in the schema, including the Stage 4–7 tables', which would then have been loaded through 8 indexes on `normalized_citation_parts` alone. `--table` (several tables) now scopes it: the 7 indexes of `web_resources`, `citation_instances` and `revisions` took 28 min (`revisions` 2 × ~11 min); each later table's indexes are built after its stage (`normalized_citation_parts` 8 indexes in 28 + 14 min, `ref_name_links` 47 s).

**Stage 4 `parts`** (`8449043`, `4cb61c7`, `1cd5389`, `81f2850`): 295,029,274 parts (211 GB) from 284.5M citations, gate 4 passes.
- One process would have taken a day: the normalizers cost ~4,170 citations per CPU-second (sample of 58,754). `--jobs` (24) spawned processes now take 1M-id ranges; ~2 h, Python-bound (~46k citations/s).
- The parts table's foreign key is deferred like Stage 3's (`fkeys` module, shared by load, parts, ref-name-links and gates 3–5); validation 12 min.
- One citation (a 227,137-character vandalised `<ref>`) held a worker for ~85 CPU-min: the vendored `_RELIABLE_YEAR_RE` retried two name alternatives at every letter of a word. They now start only at a word start (NOTICE deviation 2; identical matches on the corpus and 20,000 random strings; 227k letters 0.56 s). A first fix that cut long letter runs before the normalizer was replaced at the user's request: it would also have cut real base64 URL parameters.
- Gate 4 found URL citations without `extracted_url`: 2,385 `ftp://` (the normalizer extracts only http/https), 127 whose only part was an embedded `{{Citation needed}}` (the splitter dropped the URL text), ~30 malformed. Stage 4 now falls back to the citation's first URL (templates and wiki quote markup removed, a host required) and, decided with the user, adds a URL part when no part carries one. Gate 4 checks per citation, and only citations whose text has a URL with a host (90 have none, e.g. `[http://'"around%2080%20…`).

**Stage 4.6 `denorm-export`** (`93e3640` … `ec5c5b1`): 348,766,774 rows (50 GB), gate passes; ~2 h.
- A server-side cursor never runs a parallel plan: the 3-way join spilled 95 GB of hash batches on one backend without a row in 30 min. The export is now `COPY (SELECT …) TO STDOUT` (8 parallel workers) into a CSV (203 GB, 45 min), read by DuckDB with explicit types into Parquet (26 min; one core, quoted newlines), then repartitioned.
- `work_mem` 1GB × 9 parallel processes took Postgres past RAM: the kernel OOM-killed a worker and the server restarted (crash recovery, no loss). `DENORM_WORK_MEM` 256MB.
- A 2.8 MB row (one URL repeated) exceeded DuckDB's 2 MB CSV line limit (`DENORM_MAX_LINE_SIZE`, 64 MB); the cleanup then deleted the export. The CSV and the converted Parquet are now kept on failure and `--reuse-export` reuses them.
- The one-statement repartition (sort + 512-way partitioned write) ran DuckDB out of memory at 16 GB: it now splits by bucket group (16) first and sorts each group alone (41 min).

**Stage 5 `ref-name-links`** (`aad31fc`, `333c258`, `dbbaaca`): 92,759,078 links from 72.8M named instances, gate 5 passes; 1 h 34 min.
- Named instances move into DuckDB as Arrow batches, bucket results go to Postgres by COPY, foreign keys deferred (was `executemany` and `execute_values`; the fixture step went 5.1 → 1.5 s).
- A B-tree entry is limited to ~2.7 KB: 14 ref names exceed it (longest 83 KB, all vandalism). Decided with the user: `ref_name_links` is keyed on `ref_name_md5`, a stored generated column. `idx_ncp_doi` / `idx_gs_doi` are hash indexes (a 3,984-byte extracted DOI).
- Gate 5 compared range bounds as revision ids and reported 703 overlapping pairs on 126 pages; none overlap in time (imported histories whose ids run backwards). It now orders ranges by (timestamp, id), per bucket in DuckDB.

**Stage 6 `match`** (`0c5bc92`, `f5f3e10`, `7583cec`, `dd6bacf`): running at the time of writing; one bucket (~14k pages, ~610k parts) takes ~21 min on one core.
- Each page's rows were found by a boolean mask over the whole bucket (O(pages × rows)); now one `groupby`.
- py-spy put 56 % of the time in `DataFrame.iterrows` inside the vendored `build_merged_sources` Pass 3, which rebuilt the template side per unmerged ref-tag source; it is now built once per page (NOTICE deviation 3). The rest is spread over per-page pandas overhead (~85 ms per page) in the vendored matchers.
- Every DuckDB connection now takes `DUCKDB_MEMORY_LIMIT` / `DUCKDB_THREADS` / `DUCKDB_TEMP_DIRECTORY` (unset, each of 14 workers would take 80 % of RAM). Materialising a bucket with 28 threads under 8 GB made DuckDB 1.5.6 segfault in `.df()`; buckets are now matched in 8 page slices, each written as it finishes (peak 9.4 → 4.3 GB per worker). Bucket 0's outputs are identical, as row multisets, to the one-piece run's.
- Full run: 9 workers (DuckDB 3 GB, 2 threads each) in a 44 GB scope beside Postgres' 16 GB of shared buffers; ~20 h for 6a–6c. 6d (cross-page, single process) and gate 6 follow.

**Decisions taken without asking:** per-table index builds; `parts` worker count; hash DOI indexes; gate 4 skipping URL citations without a host; the denorm export via CSV; Stage 6 slicing and worker sizing.

**Open:** Stage 6's per-page pandas cost (vectorising the vendored matchers is the only large lever left); 6d's memory; Stage 7 (7b moves the whole match result through Python into a table with foreign keys to the 295M-row parts table, the pattern that stalled Stage 3); Stage 8.

## 24. Stages 6d–8 at enwiki scale: the build completes (2026-11-12/13)

Continues §23 on the same VM and scopes. Every change keeps the output identical: the fixture goldens pass throughout (full suite 392 passed, 3 skipped), and each rewrite was also compared with the code it replaced, as each bullet says.

**Stage 6 `match`** (`50bf090`, `1fc859f`, `eff2d59`, `30ef2d8`): 131,713,712 merged sources → 121,902,075 global sources, gate 6 passes.
- 6a–6c, rerun with `--force` after the shared-spill-directory fix (`1fc859f`, one DuckDB temp directory per connection): 512/512 buckets in 22 h 12 min, 9 workers, peak ~4.3 GB each.
- 6d is the scaled engine of `50bf090` (integer union-find; author+year groups scored with rapidfuzz `cdist`); on the fixture and random inputs it writes exactly what the vendored `match_global` writes. On enwiki: numbering 19–22 min; hard identifiers (doi 1,260,602, isbn 3,412,601, pmid 509,616, pmc 203,262, arxiv 74,865, wikidata 13,312 merges) in 22 s; author+year 145,599,163 pairs over the threshold in 64 min, nearly all inside components already joined. 13,393,378 sources fall in 3,581,741 multi-member components.
- The component build ran DuckDB out of memory twice. First the per-root `string_agg(… ORDER BY …)` / `list()` aggregates over all 131.7M sources (ordered aggregates hold every group); now a singleton is emitted from its own row and only the 13.4M members of multi-member components are aggregated, their sha1 and page list computed in Python over a stream sorted by (root, merged_source_id). Then the one `gs` table of ~118M wide rows joined to `canon_type` on a sha1 string; now the global sources and their map are built and written per 8M-source range (`MATCH_GLOBAL_CHUNK_ROWS`), 17 files per dataset.
- The union-find result is saved (`_global_unions/`); `match --reuse-unions` skipped the hour-long pass on the third run: 32 min, peak 31.9 GB with DuckDB at 28 GB.

**Stage 7 `materialize`** (`379a27d`, `5457685`, `4c1f1c6`, `1a637ba`, `a34c62f`): first run on enwiki; 7a–7h ~10 h of run time once fixed.
- 7a: 121,902,075 global sources streamed in 25 min. 7b: 332,975,833 `citation_source_resolution` rows per bucket in DuckDB, 72 min, plus 30 min validating its three deferred foreign keys. 7c: the resolved denorm (348,766,774 rows, as Stage 4.6) in 2 h 06 min.
- 7d joined each bucket's ~256k `page_source_summary` rows to `global_sources` in Postgres: one random primary-key read per row into a 29 GB table, ~2 min per bucket (~17 h). Now each bucket's rows go to scratch Parquet (the file is its checkpoint) and one DuckDB join with `global_sources_stage` (what 7a loaded) adds the source columns before a plain COPY: 131,510,883 rows in 1 h 57 min, foreign keys validated. The step fails if fewer rows load than were computed, where the old inner join dropped them silently.
- 7e and 7f, each one UPDATE of every global source, both met the planner's estimate of ~16M / ~2M groups where there are ~121M / ~100M: hash aggregates and hash joins in 64 MB of `work_mem`, spilling for over an hour (7e) and 2 h 20 min (7f, ~800 GB read) without writing a row; both were cancelled. 7e now sorts and merge-joins `global_sources` through its primary key (7a loads it in key order): 40 min. 7f now builds the canonical part per sha1 and the scores into analyzed temp tables and applies them the same way: 57 min. Both with `STAGE7_WORK_MEM` (1GB). `test_stage7_score` (25,200 input combinations against the per-row `compute_score`) passes for both.
- 7g 11 min; 7h 19,314,502 pages, 65 min. The 12 secondary indexes of the three Stage 7 tables: 48 min (`idx_gs_doi` 16 min).
- Gate 7 (after the indexes) passes in 42 min, most of it the orphan anti-joins.

**Stage 8 `revision-stats`** (`3481bbb`): 895,604,187 rows (one per revision), gate 8 passes, 45.6 min with 6 workers (DuckDB 4 GB, 3 threads; 9.1 GB of Parquet).
- The first run went at ~11 min per bucket (~16 h): ten `count(DISTINCT …) FILTER` aggregates over the ~62M-row history ⋈ denorm join per bucket. The denorm is now reduced per instance and per (instance, source) first, and the revision counts are plain counts (`citation_history` has one row per (instance, revision); an instance has one type, both checked on a bucket). Old and new queries give the same rows on buckets 0, 30 and 257, 5–6× faster.

**Decisions taken without asking:** stopping and rewriting 7d, 7e, 7f and Stage 8 mid-run rather than letting them finish; 6d's saved unions and `--reuse-unions`; 7d's scratch files under `$PARQUET_DIR/_scratch/`; `STAGE7_WORK_MEM` 1GB; running Stage 8 beside the Stage 7 index build.

**Open:** gate 7's orphan anti-joins re-check foreign keys Postgres already validated (each a full scan of a 100M+-row table); 7e and 7f still rewrite all of `global_sources` twice (~55 GB of heap before VACUUM), which a single rewrite of the table could avoid; Stage 6a–6c's per-page pandas cost (§23); the API against the enwiki database is not yet exercised.

## 25. When was this link added? WCD against InternetArchiveBot's search (2026-11-13)

InternetArchiveBot needs the date a link was added to an article, to pick the archive snapshot closest to when it was cited. It finds the date by binary-searching the article's history through the Wikipedia API. This section times the same lookup from the enwiki build (§24) on 50 articles and runs IABot's own algorithm on the same links (SCA-349). Scripts: `scripts/benchmarks/link_added/` (README there). Report for the client: https://claude.ai/artifact/4jAKdP7PjMxmFW97An2G4r.

**The lookup.** For one page: the URLs of all its citation instances (`citation_instances` → `normalized_citation_web_resources` → `web_resources`), then each instance's revisions from `citation_history`, joined to the page's revisions in (timestamp, id) order. Each instance's revisions become runs of consecutive revisions; the runs are mapped to URLs and merged per URL. For each link in the latest revision this gives its **first addition**, the **start of its current run** (the revision that last put it back), and its number of runs. Two choices made it fast. The interval form gives the same rows as a per-(URL, revision) DISTINCT, 2–6× faster. The instance→URL map goes into DuckDB as an Arrow table; `executemany` took 4 s on India.

**Sample.** Pages with at least one reference, ten per band of revision count (1–9 up to 10,000+), spread over each band by reference count, fixed seed (`pick_sample.sql`): 1 to 30,569 revisions, 5,684 current links. 7 pages have references with no URL.

**WCD timings** (one process on the build machine, no HTTP layer or network):

| | median | 90th percentile | max |
|---|---|---|---|
| cold (OS page cache dropped; Postgres buffers kept) | 0.19 s | 0.95 s | 6.5 s (Syrian civil war, 750 links) |
| warm | 0.08 s | 0.22 s | 1.2 s |
| whole-bucket `citation_history` read, as the API does today | 0.70 s | 0.96 s | 4.1 s |

46 of 50 pages answer in under a second cold. A bucket's `citation_history` is 1,203 files, one per dump shard. Their names carry the page range, so the lookup opens only the page's file; the whole-bucket read gave the same links on all 50 pages.

**IABot's algorithm.** `APII::getTimesAdded()` and `getPageHistory()` were ported line by line (`iabot_get_times_added_port.py`, AGPL-3.0 like IABot) and run against live Wikipedia, with all of a page's links in one call, as on its first visit to a page. Without a bot account, revisions come 50 at a time; the port follows continuation up to what one bot response holds (500 revisions or 8 MiB) and counts the bot's requests. The history was cut at the dump's last revision. Articles with more than 150 links (9) were left out to limit the load on Wikipedia. On the other 34 articles (1,076 links):

* IABot took 308 s in total (median 2.1 s, worst 65 s on Federer–Nadal rivalry), with 480 bot requests and 314 MB of wikitext. These times include all network traffic, from the build machine rather than from Wikimedia Cloud.
* WCD answered the same articles in 8.4 s cold, a median of 13× faster per article, with no requests to Wikipedia.

**Agreement.** 969 links get the same revision from both. 70 more agree once `http` and `https` count as one link, IABot's definition (`any_scheme.py`). 12 IABot dates to the start of the current run. That makes 1,051 of 1,076 (97.7 %). The other 25 are IABot artefacts:

* 20 dated earlier. IABot's pattern is a substring match, so it also fires inside a longer URL that contains the link: `http://www.ctv.ca` inside `…/servlet/…`, or the original inside its own `web.archive.org/web/…/` URL (`explain_disagreements.py` shows each match).
* 5 dated later. These links were removed and re-added 3 to 18 times, and a binary search can land on any re-addition.

**WCD checked against Wikipedia.** For 45 links (the 5 above and 40 at random), every URL is in the revision WCD names. 42 are absent from the revision before it (2 had no earlier revision); the other 3 had the same link with `http://` there. Of the 5,786 URLs in the 50 articles' current wikitext, WCD returns 5,681; the other 104 are all inside HTML comments. 2,503 of the 5,684 links (44 %) have been removed and re-added at least once, mostly vandalism and its revert, so both dates matter.

**Decisions taken without asking:** emulating the bot account's request limits in the IABot port (the port itself was the user's call); leaving out the 9 largest articles; counting a link as "current" when it is in the dump's latest revision.

**Open:** an API endpoint for the lookup (SCA-388), which should also read only the page's `citation_history` file (the citations endpoint takes 10 s on Albert Einstein for the same reason); keeping WCD current, since links added after the dump are unknown to it (SCA-389); throughput under concurrent lookups.

## 26. Cutover preparation: explorer reports in the dashboard, and no build-dependent ids in the API (2026-11-13)

**Context.** citations.scatteranalytics.com serves the v1 Flask app (`legacy/`: htmx explorer + `/api/v1`) over a June database holding one revision per page. The plan is to serve this build from the build VM, with the existing Caddy proxy pointing at it.

**Explorer reports ported (branch `dashboard-reports`, 7fa63a3).** The dashboard gained a citations list (latest revision, or any revision from the timeline) linking to three report pages (`reports.html`): citation history (`/citations/{sha1}`), pages citing a citation (`/citations/{sha1}/pages`, new `GET /api/v2/citations/{sha1}/pages`) and template-parameter matches. `/explorer/…` and `/api/v1/docs` redirect (302). Found on enwiki while testing: the template report took ~60 s because `normalized_sha1 = ANY(:text[])` casts the CHAR(40) column and skips its index (now `bpchar[]`, 0.07–0.45 s); ten serving tables had never been analysed (ANALYZE, 12 s); an unscoped citation history on 122k pages took 181 s (now refused above 25 pages).

**No build-dependent ids (this commit).** The API names everything by upstream ids, content hashes, names or URLs (`proposed_api.md`, principle 5):
- templates are `(domain, name)`: `GET /api/v2/templates/report?domain=&template=…`; `wiki_template_id` is internal.
- web resources are their URL: `web_resource_id` dropped.
- citation instances are `(domain, page_id, raw_sha1)`: `citation_instance_id` dropped from v2, kept (as a string) in the v1 aliases until v1 is retired.
- v1 template ids are the v1 database's, translated by name through `API_LEGACY_TEMPLATE_IDS` both ways.
- `global_source_id` stays as the interim per-build handle, now qualified by a run id: `WCD_RUN_ID` makes 6d write `<run id>.<hash>` (both engines; bare when unset, so this build, its goldens and tests are unchanged); `run_id` is reported (`API_BASE_RUN_ID` for bare ids). Columns widened to `varchar(64)` (catalog-only on enwiki, 34 ms).
- `source_key`: the standardized identifier (`doi`, `isbn`, `pmid`, `pmc`, `arxiv`, `wikidata`, `librarybase`, `url`, in that order), else `part:<canonical_part_sha1>`; `GET /api/v2/sources?doi=|…|key=` looks sources up by it. New indexes `idx_gs_librarybase`, `idx_gs_url` (hash), `idx_gs_canonical_part`.

**Verified.** Full suite 401 (399 passed, 2 skipped) with Postgres; new tests for the v2 shapes (no row ids), template report by name, v1 template-id translation both ways, source lookup (ISBN-10/13, `key=`, `part:`, Wikidata vs LibraryBase), and both 6d engines writing identical run-qualified ids. Checked against the enwiki build after the index build.

**Decisions taken without asking:** source-key order puts intrinsic identifiers before Wikidata/LibraryBase so a key only changes toward a more standard identifier (a source that gains a Wikidata item keeps its DOI key), and `url:` before the hash; the run id is a `.`-separated prefix (the hash part still compares across runs); lookups match the source's representative identifier, not every member's; `web_resource_id` is dropped from v1 too (its values never matched the v1 database's).

**Open:** assigning standard identifiers (LibraryBase as assigner of last resort) and deciding which global sources become durable records; a lookup over member identifiers (parts) rather than the representative one; removing the v1 aliases.

## 27. `wcd setup`: from a domain to a built database, shards streamed (2026-11-13)

**What.** `wcd setup --domain D [--mirror PREFIX] [--date YYYYMMDD]` finds the wiki's dumps (siteinfo `wikiid` → newest run whose `metahistory7zdump` is done, from `dumpstatus.json`), downloads the history files (`--download-jobs`, largest first, resumable with HTTP Range, sha1-verified, `.ok` markers as `tools/fetch_one.sh`), and hands each verified file to one of `--jobs` shard workers that run `RevisionChest build` (bundle + per-shard metadata) and Stage 1 `extract`. The barrier is `stage0-load` (it rebuilds the domain's `revisions` dataset from all metadata) and `dedup`; after it the runbook's whole-wiki sequence runs step by step with `state/steps/*.done` markers. `--shards-only` stops at the barrier; `--from-step` redoes from a step; `--delete-dumps` (+ `--max-ahead`) bounds disk. The run id `<dbname>-<date>` becomes `WCD_RUN_ID` (§26). Modules: `wcd_pipeline.dumps` (discovery, download; standard library only), `wcd_pipeline.setup_wiki`; `extract_all` now shares its per-shard helpers (`prepare_shard`, `extract_command`, `mark_shard`).

**Fixed on the way.** `extract-all` cleared an interrupted shard's staging only when `STARTED.txt` existed, but nothing wrote it, so a re-run extracted into the leftovers (harmless after `dedup`'s DISTINCT, but untidy); `prepare_shard` now clears any unfinished shard and writes `STARTED.txt`.

**Verified.** Tests against a local dump site (listing, `dumpstatus.json`, Range): newest-finished run selection, resume from a `.part`, sha1 mismatch, three streamed shards through a stub RevisionChest and the real Stage 1 (including an empty shard), a failed shard and its re-run (no re-download, only that shard redone), `--delete-dumps`, and with Postgres the whole chain on two shards (16 steps, gates, run-qualified global source ids). Real run: rm.wikipedia.org (rmwiki-20261101, one 29 MB file) end to end from dumps.wikimedia.org with the real RevisionChest in 3 min 51 s (download 6 s, build 5 s, extract 72 s, whole-wiki steps 2 min 29 s; match 67 s and materialize 36 s the longest): 4,824 pages, 115,050 revisions, 20,164 citation instances, 13,176 global sources, ids `rmwiki-20261101.<hash>`. Full suite 406 passed, 2 skipped. Then af.wikipedia.org (afwiki-20261101, one 512 MB file) with `--jobs 10`: 35 min end to end — download 1 min 42 s, build 1 min 28 s (303 MB bundle), split into 10 pieces 2 min (214,527 pages, 2,456,540 revisions), Stage 1 ~6.5 min wall (10 pieces of 5–7 min; one process would have taken ~an hour), then load 1 min 33 s, parts 4 min 39 s, match 11 min 15 s, 6d 23 s, materialize 3 min 13 s; 1,108,384 citation instances, 547,436 global sources (`afwiki-20261101.<hash>`), every gate passing. The single-file case is what made pieces necessary (`b2ad8a8`).

**Decisions taken without asking:** namespace 0 by default (what the enwiki build used; `--namespace ''` for all); largest shards first; whole-wiki steps take memory settings from the environment rather than per-step defaults (they are host-specific; the runbook's values apply); a mirror without `dumpstatus.json` falls back to the primary's status file while files still come from the mirror.

**Open:** per-step resource settings for very large wikis (today: environment or a memory-capped scope); following a dump run that is still being published (files as they appear); deleting bundles is not offered (they are Stage 1's and `sync`'s input).

## 28. The article page: sources with their time on the page (2026-11-14)

**What.** The dashboard's first page is now the article lookup from the design canvas "WCD article page — revised layout": paste an article URL (or random, or domain + page ID); see the article's sources-cited-now and removed counts and its edits per year; then **every source** cited on the page as a card (title, durable key, byline, badges; a bar of its time on the page with diff links; the added / removed / restored / converted dates; every wikitext form it is cited in, with dates, uses, template parameters and links to the citation reports; other articles citing it), current and removed in their own sections, filtered and sorted; then the article metrics (references over time, largest changes, citation types, source kinds, templates). It replaces the UVA dashboard's metric cards, ECharts timeline and top-sources list. New endpoint `GET /api/v2/pages/{d}/{id}/sources` (`apps/api/src/wcd_api/queries/page_sources.py`), new report page `/sources/{id}/pages`; the frontend has no third-party script any more.

**Verified.** API tests (22, including the new endpoint's counts, intervals, removal, forms, filters, sorting and paging on the synthetic build); on enwiki, 10 sources in ~2.7 s (Albert Einstein, 1,226 sources) and ~2.0 s (United States, 4,217). The JavaScript was only syntax-checked: no browser on the build VM.

**Decisions taken without asking:** absences under 7 days are merged (vandalism and its revert); a form change is marked only for a conversion to short-form citations (every text edit makes a new normalized form, which would mark nearly every source); "Order in article" is not offered (citation positions are not stored) and Bibcode is not a badge (not extracted); forms show the normalized text, repaired for display; the report links of the old citations list moved into each form row.

**Seen on the way:** global sources from author+year fuzzy matching can gather unrelated citations (Albert Einstein's "Einstein" by Woolley lists citations of other works); that is Stage 6 matching quality, not the page.
