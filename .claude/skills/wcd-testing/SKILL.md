---
name: wcd-testing
description: Run the WCD test suite at the right tier (fast, Postgres, full ci fixture, Rust) and regenerate golden files correctly. Use before committing, when a test fails, or when a change alters pipeline or matcher output.
---

# Testing the WCD monorepo

## Pick the tier

| Tier | Command | Time | Needs |
|---|---|---|---|
| Fast | `uv run pytest -q` | ~5 s | nothing (≈191 pass, ≈74 skip) |
| Postgres | `WCD_TEST_DATABASE_URL=… uv run pytest -q` | ~1 min | scratch Postgres 16 |
| Full ci fixture | `WCD_TEST_DATABASE_URL=… uv run pytest packages/pipeline/tests/test_fixture_ci.py -v` | 10–14 min (Stage 1 dominates; single bundle, so `--jobs` doesn't help) | scratch Postgres 16 |
| Rust | `cargo test --workspace` in `rust/revisionchest` (+ clippy `-D warnings`, `fmt --check`) | minutes | Rust ≥ 1.85; `REVISIONCHEST_TEST_DATABASE_URL` for the Postgres sink test |

Rules of thumb:
* Any change at all: fast tier.
* Anything that touches SQL, Postgres writes, a stage, or the API: Postgres tier.
  The API tests and `test_postgres_integration.py` **skip silently** without
  `WCD_TEST_DATABASE_URL`, so a green fast run proves nothing about them.
* A change to Stages 1–8, `wcd_semantic`, or the extractor/normalizer: the full ci
  fixture run before committing.
* A change in `rust/revisionchest`: the Rust tier, all four commands, as CI does.
  Set `REVISIONCHEST_BIN=…/target/release/RevisionChest` for the Postgres tier so
  the Stage-0 golden is regenerated from the binary and compared.

## Scratch Postgres

The DB tests **drop and recreate the `public` schema** of the database they're
given. Use a throwaway database only. CI's setup, reproducible locally:

```
docker run -d --name wcd-pg -e POSTGRES_USER=wcd -e POSTGRES_PASSWORD=wcd \
  -e POSTGRES_DB=wcd_test -p 5432:5432 postgres:16
export WCD_TEST_DATABASE_URL=postgresql://wcd:wcd@127.0.0.1:5432/wcd_test
```

(Homebrew `postgresql@16` + `createdb wcd_test` works too.) Check that the
URL is not the serving database from `.env` before running.

## Running from another OS

The repo's `.venv` belongs to the owner's Mac. From a Linux VM/container:
`export UV_PROJECT_ENVIRONMENT=$HOME/venv-wcd` and use `uv sync --frozen` /
`uv run --frozen …`, so the Mac's environment and `uv.lock` are untouched.

## Golden files

Three independent golden sets. Regenerate only after an **intentional** output
change, then review the diff and explain it in the commit message.

| Golden | Guards | Regenerate |
|---|---|---|
| `fixtures/golden/synthetic/` | Stage 1 + dedup on the synthetic bundle (`wcd_pipeline.synthetic`) | `UPDATE_GOLDEN=1 uv run pytest packages/pipeline/tests/test_stage1_golden.py` |
| `fixtures/golden/ci/` | Every Postgres table and Parquet dataset, Stages 0–8, on `fixtures/shards/ci` | `UPDATE_GOLDEN=1 uv run pytest packages/pipeline/tests/test_fixture_ci.py -v` (needs DB) |
| `fixtures/golden/semantic/expected.json` | `wcd_semantic` == upstream UVA at the NOTICE commit | `packages/semantic/tests/gen_golden.py` against an upstream checkout (see its docstring). **Almost never**: it records upstream behaviour, not ours |
| `fixtures/golden/stage0/fixture.parquet` | RevisionChest metadata output | `RevisionChest build …/fixture.xml.bz2 -o /tmp/rc --parquet fixtures/golden/stage0/fixture.parquet` |

Reviewing a ci re-record: start with `git diff fixtures/golden/ci/summary.json`
(row counts, coverage). Large tables are stored as per-page digests
(`*.fp.parquet`), so a mismatch names the changed `page_id`s; inspect those
pages in the work directory (`WCD_FIXTURE_WORK=/some/dir` keeps it).

If a semantic golden test fails and you didn't mean to change upstream logic,
the change is wrong: revert it rather than regenerate. Intended deviations go
in `packages/semantic/NOTICE`; additions are marked `ADDED (wcd)` and tested in
`test_additions.py`, and `strip_added` removes their keys from the comparison.

## When a ci fixture step fails

The test runs the commands in README order, and once one step fails the later
ones skip. Fix the first failure. Rerun with `WCD_FIXTURE_WORK` set to inspect
staging/Parquet output; `<work>/timings.json` has step timings. A failed gate
is a real defect (the first real-data run found two in Stages 6/7), not a
threshold to relax.
