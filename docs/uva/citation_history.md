# `citation_history.py` - Workflow

## Purpose

Builds three output tables that capture the full citation history for one or more Wikipedia pages, tracking how citations evolve across revisions. This is the final step in the pipeline as it consumes all upstream outputs and produces the human-queryable history tables.

---

## Inputs and Outputs

**Input:** All upstream pipeline tables: `citations_extracted`, `ref_tag_normalized`, `template_normalized`, `citation_source_map`, `template_citation_map`, `merged_sources`, `global_source_map`, `global_sources`.

**Outputs:**

| Table | Rows | Description |
|---|---|---|
| `citations_resolved` | One per citation per revision | Each citation mapped to its `global_source_id` |
| `page_source_summary` | One per (page, global_source) | Lifetime stats for each source on a page |
| `page_revision_stats` | One per revision | Citation quality metrics over time |

All three tables use DELETE + INSERT per page, making re-runs idempotent.

---

## Step-by-Step Workflow

### Step 1: Build `citations_resolved` (`build_citations_resolved`)

Constructs a UNION ALL SQL query that walks the full matching chain from raw citation text to `global_source_id`. The query has three branches:

**Resolved ref_tags:**
```
citations_extracted
  → ref_tag_normalized       (content → content_sha1)
  → citation_source_map      (content_sha1 + page_id → source_id)
  → merged_sources           (ref_source_id + page_id → merged_source_id)
  → global_source_map        (merged_source_id → global_source_id)
```

**Unresolved ref_tags:**
Citations that have content but no path through the matching chain. Identified using a LEFT JOIN anti-join pattern rather than a correlated NOT EXISTS subquery. The LEFT JOIN allows DuckDB to use a single hash anti-join pass rather than a nested loop per row, which is critical for performance against the citations_extracted table. Stored with `global_source_id = NULL` so they appear in revision stats as `n_unresolved` without being lost.

**Resolved templates:**
```
citations_extracted
  → template_normalized      (raw → content_sha1)
  → template_citation_map    (content_sha1 + page_id → source_id)
  → merged_sources           (tmpl_source_id + page_id → merged_source_id)
  → global_source_map        (merged_source_id → global_source_id)
```

Each branch checks whether the intermediate tables exist before including itself in the UNION.

---

### Step 2: Build `page_source_summary` (`build_page_source_summary`)

Runs a SQL aggregation over `citations_resolved` for the page to compute per-source lifetime statistics:

- `first_rev_id` / `first_seen` : earliest revision the source appeared
- `last_rev_id` / `last_seen` : most recent revision
- `n_revisions_cited` : how many distinct revisions cite this source
- `n_self_closing_uses` : how many times the source was used as a self-closing ref (`<ref name="X"/>`) rather than a full citation
- `is_current` : whether the source was present in the latest revision
- `reliability_score` : numeric score (0-100) from the global source record
- `verdict` : confidence classification (`high_confidence`, `medium_confidence`, `low_confidence`, `questionable`, `likely_not_citation`) from the global source record

Joins to `global_sources` to pull in canonical metadata (author, title, year, identifiers) so the summary table is self-contained for querying.

---

### Step 3: Build `page_revision_stats` (`build_page_revision_stats`)

Computes per-revision citation quality metrics in two phases:

**SQL phase** : groups `citations_resolved` by `rev_id` to compute:
- `n_total_citations` : non-self-closing citations
- `n_self_closing` : self-closing ref reuses
- `n_unique_sources` : distinct resolved sources
- `n_unresolved` : citations with no global_source_id
- `n_high_confidence` : resolved sources with confidence ≥ 0.85

**Python phase** : computes `n_new_sources` (sources appearing for the first time in each revision). This requires cumulative state, for each global_source_id, find the minimum rev_id it appears in, then count how many sources have their minimum in each revision. This cannot be expressed efficiently in a single SQL window function, so it runs as a pandas groupby after the SQL aggregation.

`pct_resolved` = `n_unique_sources / n_total_citations` is added as a final column summarizing citation quality for that revision.

---

### Step 4: Write to DuckDB

Tables are written in batches of 5,000 pages via `build_all_pages_bulk()` rather than one page at a time. Each batch resolves all citations for its pages in a single SQL pass into a temp table `_resolved_all`, then builds `page_source_summary` and `page_revision_stats` from that temp table in two further SQL passes. The temp table is dropped after each batch.

This bulk approach avoids repeated full scans of `citations_extracted` : the per-page approach would scan the all row table once per page, resulting in many scans. The bulk approach scans it once per batch of 5,000 pages.

`citations_resolved` is created as a SQL VIEW after all batches complete, not as a materialized table, as materializing it would produce billions of rows across a full corpus run.

---

## Key Design Decisions

**Pure SQL for the join chain.** The full matching chain from raw citation text to `global_source_id` is six tables deep. Doing this in Python would require loading all intermediate tables into memory and merging them. The SQL UNION ALL approach pushes all join logic into DuckDB and returns only the final resolved rows, more efficient at scale.

**Unresolved citations are kept.** A citation with `global_source_id = NULL` is still stored in `citations_resolved`. This is intentional as unresolved citations contribute to `n_unresolved` in revision stats and tell you how many citations on a page the pipeline couldn't match. Dropping them would give a falsely optimistic resolution rate.

**`n_new_sources` in Python, not SQL.** The new-sources calculation requires knowing, for each source, the minimum revision it appeared in, then counting per revision. While this is expressible in SQL with a window function, the Python groupby is simpler to read and runs on a small in-memory DataFrame (one row per source per page, typically a few hundred rows).

**`is_current` flag.** A source is `is_current = True` if its last appearance was in the most recent revision for the page. This allows simple filtering for "sources active at the time of the last snapshot" without needing to join back to `citations_extracted`.

**Bulk batched processing instead of per-page loop.** Early versions processed pages one at a time, scanning `citations_extracted` for each page independently. With 131M rows and 77k pages from our sample, this produced massive DuckDB temp file spills. The bulk approach groups pages into batches of 5,000 and resolves all citations in the batch in a single filtered SQL pass, reducing `citations_extracted` scans from 77,000 to 16.

**`citations_resolved` is a view, not a table.** An earlier version accidentally materialized this as a 131M row table during one run, consuming ~2 GB. It is now explicitly created as `CREATE VIEW` after all batches complete. Callers that need resolved citation data should query the view directly or filter by `page_id` to avoid full scans.