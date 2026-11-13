# wcd-api — the `/api/v2` service

FastAPI service over the Wikipedia Citations Database: Postgres for the
serving tables, the Parquet tier (DuckDB, one bucket partition per request)
for the revision dimension. Endpoints, response fields and the table each one
reads are specified in `docs/design/proposed_api.md`; FastAPI publishes the
OpenAPI document at `/openapi.json` (`/docs` for the UI).

```
cp example.env .env         # DATABASE_URL (or DB_*), PARQUET_DIR, API_* …
uv run wcd-api --host 0.0.0.0 --port 8000
# or: uv run uvicorn wcd_api.main:app --reload
```

| Group | Endpoints | Engine |
|---|---|---|
| 1 Article resolution | `GET /api/v2/article?url=` (curid or `/wiki/Title`; titles resolved through the site's API) | Postgres |
| 2 Page analytics | `/pages/random/summary`, `/pages/{domain}/{page_id}/summary`, `…/timeline`, `…/top-sources`, `/sources/{id}`, `/sources/{id}/pages`, `/sources?doi=…` (lookup by identifier) | Postgres; timeline reads `page_revision_stats` |
| 3 Revision & citation detail | `…/revisions`, `…/citations?revision_id=&raw=`, `/citations/{sha1}`, `/citations/{sha1}/history?domain=&page_id=&limit=&offset=`, `/citations/{sha1}/pages` | Postgres + `citation_history` / `revisions` Parquet |
| 4 Lookup | `/templates/report?domain=&template=&parameter_key=&parameter_value=`, `/web-resources?url=` | Postgres + `template_param_index` |
| v1 aliases | the `/api/v1/…` paths over the same handlers, domain defaulted to `API_DEFAULT_DOMAIN` | |
| Report pages | `/citations/{sha1}`, `/citations/{sha1}/pages`, `/templates/report?domain=&template=…` (HTML, `apps/frontend/reports.html`) | — |
| Redirects (302) | `/explorer/…` → the dashboard and report pages; `/api/v1/docs` → `/docs`, `/api/v1/openapi.json` → `/openapi.json` | |

Notes on the response shapes:

* **No build-dependent ids.** Pages are `(domain, page_id)`, revisions `revision_id`,
  citations `normalized_sha1`, citation instances `(domain, page_id, raw_sha1)`,
  templates `(domain, template_name)`, web resources their URL. Database row ids
  (`wiki_templates.id`, `web_resources.id`) are never returned (`docs/design/proposed_api.md`,
  principle 5).
* `global_source_id` is an **interim, per-build** handle: `<run id>.<hash>` for builds made
  with `WCD_RUN_ID`, a bare hash for the 2026-11 build (`run_id` reports
  `API_BASE_RUN_ID` for those). The durable name is **`source_key`** — the standardized
  identifier (`doi:…`, `isbn:…`, `pmid:…`, `pmc:…`, `arxiv:…`, `wikidata:Q…`,
  `librarybase:Q…`, `url:…`, in that order of preference), else `part:<canonical_part_sha1>`,
  a content hash. `GET /api/v2/sources?doi=…` (or `isbn`, `pmid`, `pmc`, `arxiv`,
  `wikidata`, `librarybase`, `url`, `key`) finds sources by it.
* **v1 compatibility**: the `/api/v1` aliases keep `citation_instance_id` (as a string:
  the ids are 64-bit hashes, beyond JavaScript's 2^53 safe integers) and speak the **v1
  database's** template ids — `wiki_template_id` in responses, `/api/v1/template/{id}/report`
  — translated by name through `API_LEGACY_TEMPLATE_IDS` (null / 404 without it).
  `web_resource_id` is gone from v1 too. Remove `routers/v1.py` and the `V1…` models
  when v1 is retired.
* `/citations/{sha1}/history` pages (`limit` ≤ 10,000, default 1,000 on v2 and 10,000
  on v1; `total` counts every revision). Without `page_id` it reads one revision bucket
  per page the citation is on, so it is refused (400) for citations on more than
  `HISTORY_MAX_PAGES` (25) pages — pass `domain` + `page_id`. Before this a citation on
  120k pages took three minutes.
* `/citations/{sha1}/pages` lists the pages citing a normalized text (title, instance
  count, ref names) from Postgres only; `limit=0` returns just the header and `total`.
* Page references (`appears_on_articles`, history entries, template-report rows) carry
  `page_title` from `page_metadata`.
* Template names are normalized like the extractor's (underscores → spaces, first letter
  capitalized) and parameter values compared trimmed, as the v1 explorer did.

Errors are `{"error", "code"}` with 400 (bad URL / parameters), 404
(unknown domain, page, source, citation), 422 (validation) and 503 (a
dataset not built yet, or the site API unreachable for a title lookup).

Settings (environment / `.env`): `DATABASE_URL` or `DB_*`; `PARQUET_DIR`;
`API_DEFAULT_DOMAIN` (en.wikipedia.org); `API_BASE_RUN_ID` (the run bare global source
ids belong to, e.g. `enwiki-20261001`); `API_CORS_ORIGINS` (comma-separated,
default `*`); `API_POOL_SIZE` (Postgres connections, 5);
`WIKIPEDIA_API_USER_AGENT` / `WIKIPEDIA_API_CONTACT_EMAIL` for title lookups;
`API_LEGACY_TEMPLATE_IDS` (optional) — a TSV `id<TAB>name` exported from the v1
database's `wiki_templates`, used only to redirect old `/explorer/template/{id}/report`
links by template name (without it they land on a page explaining the link is stale).
The dashboard in `apps/frontend/` is served at `/` (assets under `/static/`).

Tests (`apps/api/tests`) build the synthetic pipeline database
(`wcd_pipeline.synthetic.build_database`, a few seconds) and exercise every
endpoint; they need `WCD_TEST_DATABASE_URL` like the pipeline's integration
tests. For a real-data check, run the pipeline on `fixtures/shards/ci`
(`test_fixture_ci.py` with `WCD_FIXTURE_WORK` set) and point the service at
that database and Parquet directory.
