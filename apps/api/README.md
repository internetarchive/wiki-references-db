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
| 3 Revision & citation detail | `…/revisions`, `…/citations?revision_id=&raw=`, `/citations/{sha1}`, `/citations/{sha1}/history` | Postgres + `citation_history` / `revisions` Parquet |
| 4 Lookup | `/templates/{id}/report`, `/web-resources?url=` | Postgres |
| v1 aliases | the `/api/v1/…` paths over the same handlers, domain defaulted to `API_DEFAULT_DOMAIN` | |

Errors are `{"error", "code"}` with 400 (bad URL / parameters), 404
(unknown domain, page, source, citation), 422 (validation) and 503 (a
dataset not built yet, or the site API unreachable for a title lookup).

Settings (environment / `.env`): `DATABASE_URL` or `DB_*`; `PARQUET_DIR`;
`API_DEFAULT_DOMAIN` (en.wikipedia.org); `API_CORS_ORIGINS` (comma-separated,
default `*`); `API_POOL_SIZE` (Postgres connections, 5);
`WIKIPEDIA_API_USER_AGENT` / `WIKIPEDIA_API_CONTACT_EMAIL` for title lookups.
The dashboard in `apps/frontend/` is served at `/` (assets under `/static/`).

Tests (`apps/api/tests`) build the synthetic pipeline database
(`wcd_pipeline.synthetic.build_database`, a few seconds) and exercise every
endpoint; they need `WCD_TEST_DATABASE_URL` like the pipeline's integration
tests. For a real-data check, run the pipeline on `fixtures/shards/ci`
(`test_fixture_ci.py` with `WCD_FIXTURE_WORK` set) and point the service at
that database and Parquet directory.
