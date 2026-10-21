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
