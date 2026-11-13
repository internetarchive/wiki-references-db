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
| 2 Page analytics | `/pages/random/summary`, `/pages/{domain}/{page_id}/summary`, `…/timeline`, `…/top-sources`, `/sources/{id}`, `/sources/{id}/pages` | Postgres; timeline reads `page_revision_stats` |
| 3 Revision & citation detail | `…/revisions`, `…/citations?revision_id=&raw=`, `/citations/{sha1}`, `/citations/{sha1}/history?domain=&page_id=&limit=&offset=`, `/citations/{sha1}/pages` | Postgres + `citation_history` / `revisions` Parquet |
| 4 Lookup | `/templates/{id}/report`, `/web-resources?url=` | Postgres |
| v1 aliases | the `/api/v1/…` paths over the same handlers, domain defaulted to `API_DEFAULT_DOMAIN` | |
| Report pages | `/citations/{sha1}`, `/citations/{sha1}/pages`, `/templates/{id}/report` (HTML, `apps/frontend/reports.html`) | — |
| Redirects (302) | `/explorer/…` → the dashboard and report pages; `/api/v1/docs` → `/docs`, `/api/v1/openapi.json` → `/openapi.json` | |

Notes on the response shapes:

* `citation_instance_id` is a **string** (`"9172276549252580379"`): the ids are 64-bit
  hashes, beyond JavaScript's 2^53 safe integers, so a JSON number would be silently
  rounded by browsers and JS clients. This applies to `/api/v1` too.
* `/citations/{sha1}/history` pages (`limit` ≤ 10,000, default 1,000 on v2 and 10,000
  on v1; `total` counts every revision). Without `page_id` it reads one revision bucket
  per page the citation is on, so it is refused (400) for citations on more than
  `HISTORY_MAX_PAGES` (25) pages — pass `domain` + `page_id`. Before this a citation on
  120k pages took three minutes.
* `/citations/{sha1}/pages` lists the pages citing a normalized text (title, instance
  count, ref names) from Postgres only; `limit=0` returns just the header and `total`.
* Page references (`appears_on_articles`, history entries, template-report rows) carry
  `page_title` from `page_metadata`.
* Template ids (`wiki_template_id`) were renumbered relative to the v1 database. Ids in
  this service's responses are consistent with its own endpoints; ids saved from the
  old site are not (see `API_LEGACY_TEMPLATE_IDS`). Template parameter values are
  compared trimmed, as the v1 explorer did.

Errors are `{"error", "code"}` with 400 (bad URL / parameters), 404
(unknown domain, page, source, citation), 422 (validation) and 503 (a
dataset not built yet, or the site API unreachable for a title lookup).

Settings (environment / `.env`): `DATABASE_URL` or `DB_*`; `PARQUET_DIR`;
`API_DEFAULT_DOMAIN` (en.wikipedia.org); `API_CORS_ORIGINS` (comma-separated,
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
