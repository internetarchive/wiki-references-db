---
name: wcd-local-serve
description: Build a real WCD database from the committed ci fixture shard (or the synthetic bundle) and run the /api/v2 service and dashboard against it, for checking endpoints or UI changes by hand.
---

# Serving the API and dashboard locally

Two data sources, from fast to realistic:

## A. Synthetic (seconds): 2 pages, 5 revisions, every stage

```
export WCD_TEST_DATABASE_URL=postgresql://wcd:wcd@127.0.0.1:5432/wcd_dev   # scratch: `public` is dropped
uv run python -c "from wcd_pipeline.synthetic import build_database as b; print(b('$WCD_TEST_DATABASE_URL', '/tmp/wcd-syn'))"
DATABASE_URL=$WCD_TEST_DATABASE_URL PARQUET_DIR=/tmp/wcd-syn/parquet uv run wcd-api --port 8000
```

Pages: `en.wikipedia.org` 101 ("Example Island") and 202 ("Second Page").
Good for checking the shape of a response; not for realistic content.

## B. ci fixture (10–14 min): 51 real enwiki pages, full history

```
export WCD_TEST_DATABASE_URL=postgresql://wcd:wcd@127.0.0.1:5432/wcd_fixture
export WCD_FIXTURE_WORK=$HOME/wcd-fixture-work      # kept after the run; emptied at the start of the next one
uv run pytest packages/pipeline/tests/test_fixture_ci.py -v
DATABASE_URL=$WCD_TEST_DATABASE_URL PARQUET_DIR=$WCD_FIXTURE_WORK/parquet uv run wcd-api --port 8000
```

`WCD_FIXTURE_WORK` keeps the run's output (`parquet/`, `staging/`, `timings.json`);
without it everything goes to a temp directory and the API has nothing to read.
Rebuild after a pipeline change that affects what you're looking at.

## Using it

* Dashboard: `http://127.0.0.1:8000/` (the page field takes a page ID or an
  article URL; links are `?domain=…&page_id=…`). Without network access,
  ECharts (jsDelivr) won't load and the page falls back to its textual
  timeline. That's expected.
* API: `/api/v2/…`, schema at `/openapi.json`, UI at `/docs`, liveness at `/healthz`.
* Pick fixture pages from `fixtures/pages.ci.txt` / `fixtures/pages.tsv`
  (WikiProject Medicine and Military history clusters; good for multi-page
  sources and `{{sfn}}`).
* `/api/v2/article?url=…/wiki/Title` resolves titles through the live site
  API: set `WIKIPEDIA_API_CONTACT_EMAIL` and have network access, or use curid
  URLs.
* `uv run uvicorn wcd_api.main:app --reload` gives auto-reload while editing
  the API. Frontend files are served straight from `apps/frontend/`, so a
  browser refresh picks up changes.

## Checks worth doing after a change

The runbook's pre-announcement checks, on the fixture: summary, timeline, top
sources, and "Show related pages" on a multi-page source for three pages; an
unknown page gives the `{"error","code"}` 404. For a dashboard change, also
watch the browser console. If a browser tool is available, drive it headless
as Phase 5 did.

Never point `DATABASE_URL` / `PARQUET_DIR` at the serving deployment for
experiments. Building into a fresh `PARQUET_DIR` and restarting is how a real
swap is done (runbook, "Serving").
