---
name: wcd-pipeline-change
description: Checklist for changing a WCD pipeline stage, a table or Parquet dataset, ids, gates, or the vendored matchers, so schema, docs, runbook, goldens and downstream stages stay consistent.
---

# Changing the pipeline, schema or matchers

Work out first which layer the change belongs to, then follow its row.

| Changing… | Touch | Then re-run (runbook rules) |
|---|---|---|
| A Postgres table/column | `wcd_schema/postgres.py`; Stage-0 tables also the RevisionChest `schema.sql` copy (parity test) and its Rust sink | everything from the step that writes it |
| A Parquet dataset | `wcd_schema/parquet.py` (schema + layout); writers via `wcd_pipeline.tier`; readers via `wcd_schema.duckdb` | the owning step and its downstream |
| A staging file | `wcd_schema/staging.py`; `extract.py` / `dedup.py` / `load.py` | extract → dedup → load → 4–8 |
| Ids or bucketing | `wcd_schema/ids.py` only (Python and `instance_id_sql` together); `tests/test_ids.py` | full rebuild. Ask first: ids are a cross-engine contract (Rust, DuckDB, Postgres) |
| Extraction (what counts as a reference) | the `packages/extractor` submodule (see `wcd-submodules`); `wikis.yaml` for per-wiki templates | extract onward |
| Normalization of text before hashing | `packages/normalizer` submodule | full rebuild: hashes change |
| Stage-4 normalizers (`reftag`, `template`, `content`) | `wcd_semantic` under the NOTICE rules | parts → denorm-export → match `--force` → materialize → revision-stats |
| Matchers/scorer (`match_*`, `merge_types`, `scoring`) | `wcd_semantic` under the NOTICE rules | match `--force` → materialize → revision-stats |
| A gate | `wcd_pipeline/gates.py` and `docs/design/proposed_data_integrity.md` | the gate (`wcd gate N --domain D`) |
| A new stage command | new module with `main(argv) -> int` + argparse; register in `cli.py` `COMMANDS` (lazy import) | — |
| API response | `apps/api` (`models.py`, `queries/`, `routers/`); `docs/design/proposed_api.md`; the frontend if it reads the field | — |

## Rules that apply to every change

1. **Keep the step idempotent and scoped to what it owns.** Match the existing
   unit of re-run: shard files for extract, a domain's partitions, a bucket
   with a `_SUCCESS` marker for match/revision-stats, per-bucket checkpoints
   under `$PARQUET_DIR/_checkpoints/` for 7d/7h. A crash followed by a plain
   re-run must give the same result (the "crash-resume" fix in history is the
   kind of bug this prevents).
2. **Scale.** Real runs are all of English Wikipedia's history. Work per
   bucket (`page_id % 512`) and let DuckDB spill; don't scan `pg.*` tables inside
   a bucket loop (that's why there are Parquet copies of `revisions` and
   `ref_name_links`). Respect `--jobs`.
3. **Vendored logic.** In `wcd_semantic`, adapt inputs rather than edit
   upstream functions. If upstream logic must change, document the deviation
   in `NOTICE`; mark additions `ADDED (wcd)` and test them in
   `test_additions.py`.
4. **Gates.** If the change adds an invariant, add it to the stage's gate and
   to `proposed_data_integrity.md`.
5. **Config.** New environment variables go in `example.env` (with a comment)
   and the runbook's Environment table; read them through
   `wcd_pipeline.config` / `wcd_api.settings`.

## Verify

1. `uv run pytest -q`, then the Postgres tier (skill `wcd-testing`).
2. If output changes: the synthetic golden, then the full ci fixture run.
   Re-record with `UPDATE_GOLDEN=1` only after reading the diff, and say in the
   commit message why each changed count changed.
3. For an API-visible change: run the service against the ci fixture (skill
   `wcd-local-serve`) and check the affected endpoint or dashboard view.

## Record it

* Update the `proposed_*.md` doc that specifies the behaviour, and
  `docs/runbook.md` if outputs, ownership or re-run rules changed.
* Significant work gets a dated section in
  `docs/design/repo_structure_and_implementation_plan_2026_10_08.md`: what
  landed (commit), how it was verified, decisions taken without asking, what
  remains open.
* Commit code as `area: …` with a prose body; commit the doc record separately
  as `docs: …`.
