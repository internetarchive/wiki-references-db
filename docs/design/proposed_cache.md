### Proposed Caching & Intermediate Data Layers

*Revised 2026-09-10 per `claude/decisions_2026-09-10.md`.*

Items fully specified elsewhere (`page_source_summary`, `citation_instances_denorm`, `global_source_scores`, `page_revision_stats`) are referenced, not repeated.

---

### 1. Page Summary Cache (Postgres table) — Stage 7h

**Problem**: `/pages/{domain}/{page_id}/summary` aggregates the latest revision's citation set, type breakdown, template breakdown and source-kind profile. Scoping to the latest revision needs `citation_history` (Parquet), so this is a cross-engine query — too slow per request.

**Solution**: pre-compute the response per page.

```sql
page_summary_cache (
    domain_id             INTEGER NOT NULL,
    page_id               INTEGER NOT NULL,
    revision_count        INTEGER NOT NULL,
    first_revision_ts     TIMESTAMP,
    latest_revision_ts    TIMESTAMP,
    latest_rev_id         BIGINT,
    reference_count       INTEGER NOT NULL,   -- citation instances at latest revision
    type_breakdown        JSONB,    -- [{citation_type, count}] (six kinds)
    top_templates         JSONB,    -- [{template_name, count}]
    source_profile        JSONB,    -- {total_citation_uses, format_breakdown, source_kind_breakdown, summary}
    updated_at            TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (domain_id, page_id)
)
```

`page_title` / `page_namespace` are not cached; the endpoint joins `page_metadata`.

**Built by**: Stage 7h, DuckDB, batched by `(domain, page_bucket)` with checkpointing (`proposed_data_integrity.md` §2). Inputs per bucket: `revisions` Parquet (revision_count, first/latest, latest_rev_id via `arg_max`), `citation_history` ⋈ `citation_instances_denorm` (resolved) filtered to `revision_id = latest_rev_id` (counts, type breakdown, template breakdown, source-kind classification from identifier columns), `global_source_scores` (for verdict-aware profile text if wanted). Writes to `pg.page_summary_cache`. The classification helpers (`_classify_source_format`, `_classify_source_kind`, `_build_source_profile_summary`) are ported from UVA `page_summary.py`.

**Refresh**: full rebuild after each pipeline run (bounded at O(pages)). Staleness watermark: `latest_rev_id` vs `MAX(revisions.revision_id)` for the page.

**Random page**: `SELECT … FROM page_summary_cache TABLESAMPLE SYSTEM (0.01) LIMIT 1` (optionally `WHERE domain_id = ?`).

---

### 2. Per-Revision Citation Type Counts

Already in `page_revision_stats` (`n_ref_tag … n_external_link`), computed in Stage 8. Nothing further.

---

### 3. DuckDB Connection Pool

Two-phase startup: a writable init pass creates the persistent views in a DuckDB file, then a small pool of read-only connections each `ATTACH` Postgres.

```python
PARQUET_VIEWS = ("citation_history", "named_ref_uses", "revisions", "ref_name_links",
                 "citation_instances_denorm", "global_source_scores", "page_revision_stats")

def init_duckdb_views(db_path, postgres_dsn, parquet_base):
    con = duckdb.connect(db_path)                       # writable
    con.execute(f"ATTACH '{postgres_dsn}' AS pg (TYPE POSTGRES, READ_ONLY)")
    for name in PARQUET_VIEWS:
        con.execute(f"""CREATE VIEW IF NOT EXISTS {name} AS
            SELECT * FROM read_parquet('{parquet_base}/{name}/**/*.parquet.zst',
                                       hive_partitioning=true)""")
    con.execute("""
        CREATE VIEW IF NOT EXISTS domains AS
            SELECT id AS domain_id, value AS domain FROM pg.domains;
        CREATE VIEW IF NOT EXISTS global_sources      AS SELECT * FROM pg.global_sources;
        CREATE VIEW IF NOT EXISTS page_source_summary AS SELECT * FROM pg.page_source_summary;
        CREATE VIEW IF NOT EXISTS page_metadata       AS SELECT * FROM pg.page_metadata;
    """)
    con.close()

class DuckDBPool:
    def __init__(self, db_path, postgres_dsn, parquet_base, size=4):
        init_duckdb_views(db_path, postgres_dsn, parquet_base)
        self._pool = Queue(maxsize=size)
        for _ in range(size):
            con = duckdb.connect(db_path, read_only=True)
            con.execute(f"ATTACH '{postgres_dsn}' AS pg (TYPE POSTGRES, READ_ONLY)")
            self._pool.put(con)

    @contextmanager
    def connection(self):
        con = self._pool.get()
        try: yield con
        finally: self._pool.put(con)
```

Every query against a page-partitioned view must filter on `domain = ? AND page_bucket = ?` (bucket = `page_id % 512`) so DuckDB prunes to one partition.

**Serves**: timeline, revisions list, citation history, citations-at-revision (Group 3), and the 7h rebuild.

**Lifecycle**: init pass + pool at startup; re-run the init pass after Parquet datasets are replaced.

---

### 4. Cross-Page Count Maintenance on `global_sources` — Stage 7e

Sole writer of `n_pages` / `n_total_citations` (7a inserts NULL). Runs immediately after 7d.

```sql
UPDATE global_sources gs
SET n_pages = sub.n_pages, n_total_citations = sub.n_total_citations
FROM (
    SELECT global_source_id,
           COUNT(*)          AS n_pages,          -- one page_source_summary row per (domain, page)
           SUM(n_citations)  AS n_total_citations
    FROM page_source_summary
    GROUP BY global_source_id
) sub
WHERE gs.global_source_id = sub.global_source_id;
```

Followed by 7f (reliability/verdict; independent of the counts but exported together in 7g) and 7g.

**Serves**: `cross_page_count` on top-sources, `total_pages` on source-pages.

---

### 5. Page Title LRU Cache (Application-Level)

`page_metadata` is populated for every page by RevisionChest (Stage 0/3), so misses should be rare. An in-memory LRU keyed `(domain, page_id) → (title, namespace)`, warmed at startup from `page_metadata` for pages present in `page_summary_cache`. The on-demand fetch (UVA `_maybe_fetch_page_title_on_demand`, ported) hits `https://{domain}/w/api.php` and writes the row back to `page_metadata`; it is the exception path only.

---

### Summary Table

| Layer | Storage | Serves | Built / refreshed |
|---|---|---|---|
| `page_summary_cache` | Postgres | `/summary`, `/random/summary` | Stage 7h; full rebuild per run; `latest_rev_id` watermark |
| Per-revision type counts | Parquet columns of `page_revision_stats` | `/timeline` | Stage 8 |
| DuckDB pool | app memory | all DuckDB-backed endpoints | init pass + pool at startup |
| `global_sources.n_pages/n_total_citations` | Postgres columns | `/top-sources`, `/sources/{id}/pages` | Stage 7e, then 7f, 7g |
| `global_source_scores` | Parquet | Stage 8; denorm analytics | Stage 7g |
| Page title LRU | app memory | all page-scoped endpoints | startup warm; on-demand exception path |