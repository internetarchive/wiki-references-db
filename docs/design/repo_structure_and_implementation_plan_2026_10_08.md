# Monorepo Structure and Implementation Plan

*2026-10-08 · Review of the `notes/` plan against the `wrdb-v3` working tree. Companion to `claude/codebase_gap_assessment.md` (2026-09-09) and `claude/decisions_2026-09-10.md`. This document does not re-litigate data-model or pipeline-semantics findings already settled there; it is about where the code lives, how the pieces depend on each other, and in what order the plan should be executed.*

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
