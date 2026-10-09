# Proposed Matching Pipeline

*Revised 2026-09-10 per `claude/decisions_2026-09-10.md`.*

## Overview

Stage 6 groups citation **parts** (`normalized_citation_parts`, see `proposed_data_model.md`) into sources — first within each page, then across all pages. Stage 7 materializes the results to Postgres and exports the Parquet datasets the serving layer needs.

The code is UVA's `citation_matching/` module (four stages, ~3,230 lines). Principle: **reuse UVA code; adapt inputs, not logic.** UVA's `content_sha1` is our `part_sha1`; UVA's `content` is our `parts.content`; UVA's `ref_tag_normalized` / `template_normalized` columns are all on `normalized_citation_parts`.

Canonical identifier set: DOI, ISBN, PMID, PMC, arXiv, Wikidata QID, LibraryBase (model-only), URL.

---

## Pipeline Position

```
Stages 1–3  extract → normalize wikitext → load
Stage 4     normalized_citation_parts (UVA normalizers)
Stage 5     ref_name_links
Stage 6a    ref_tag_matching     per page, parts with part_type IN ('ref_tag','external_link')
Stage 6b    template_matching    per page, parts with part_type = 'template'
Stage 6c    cross_type_merger    per page; also joins the 6a/6b maps
Stage 6d    cross_page_matcher   global
Stage 7     7a–7h materialization + exports
Stage 8     page_revision_stats (proposed_page_revision_stats.md)
```

---

## Units of Work and Output Layout

- **Bucket, not page.** Stages 6a–6c iterate pages within a `(domain, page_bucket)` partition and write one Parquet directory per bucket (`matching_intermediate/<table>/domain=…/page_bucket=…/`). The bucket is the unit of parallelism, checkpointing and re-run. No per-page files.
- Page list per bucket comes from the bucket's `citation_instances_denorm` partition (`SELECT page_id, COUNT(*) … GROUP BY page_id ORDER BY 2 DESC`), largest-first as UVA does.
- UVA's per-page `DELETE … WHERE page_id = ?` + insert becomes: build the bucket in memory (or a DuckDB temp table), then `COPY … TO bucket_dir (OVERWRITE)`.

---

## Input Query (6a / 6b)

Both matchers need, per page, the unique parts with their normalized fields plus `ref_name`, `n_revisions`, `first_rev_id`. Every input is Parquet: the **unresolved** `citation_instances_denorm` (built at the end of Stage 4 — see `proposed_citation_instances_denorm.md`; `global_source_id` is NULL at that point) and `citation_history`, both read for one bucket partition:

```sql
-- DuckDB; both views scoped to domain=:domain/page_bucket=:bucket
SELECT
    dn.part_sha1, dn.content, dn.norm_content, dn.norm_subtype, dn.id_tier, dn.id_tier_label,
    dn.extracted_url, dn.extracted_isbn, dn.extracted_doi, dn.extracted_pmid, dn.extracted_pmc,
    dn.extracted_arxiv, dn.extracted_wikidata_qid,
    dn.anchor_key, dn.author_key, dn.year, dn.year_reliable, dn.page_ref, dn.title,
    dn.page_id,
    COUNT(DISTINCT ch.revision_id)      AS n_revisions,
    MIN(ch.revision_id)                 AS first_rev_id,
    MIN(dn.reference_name)              AS ref_name
FROM citation_instances_denorm dn
JOIN citation_history ch ON ch.citation_instance_id = dn.citation_instance_id
WHERE dn.part_type IN ('ref_tag', 'external_link')          -- 6b: = 'template'
GROUP BY ALL
```

No `pg.*` table is scanned in Stage 6.

Notes:
- `bare_url` and `external_link` citations go through the ref-tag path; a URL is their signal.
- Self-closing refs are not inputs (no rows). UVA's `ref_name` signal still fires: the *defining* `<ref name="X">…</ref>` instances share the name.

---

## Stage 6a: Per-Page Ref-Tag Matching — UVA `ref_tag_matching.py`

Kept as-is: union-find, `CONFIDENCE` table, `FUZZY_THRESHOLD = 88`, `AUTHOR_YEAR_THRESHOLD = 70`, title-to-title 85, and **all** passes:

| Pass | Signal | Confidence |
|---|---|---|
| 1 | doi / isbn / pmid / **pmc / arxiv / wikidata_qid** (added) / ref_name / url / anchor_key / norm_content | 1.00 / .95 / .95 / .95 / .95 / .95 / .90 / .85 / .80 / .65 |
| 2 | cite_isbn | .88 |
| 3 | author+year+title (year_reliable guard) | .60 |
| 4 | author_containment | .75–.95 |
| 5 | bert (optional; only if `bert_results` exists) | .55 |
| final | fuzzy singleton clustering | .60–.70 |

`norm_content` is UVA's typographic fingerprint from `normalized_citation_parts.norm_content` — **not** `normalized_sha1`.

Adaptation: input query above; three added hard-id columns in the Pass-1 list; `source_id = sha1(f"{page_id}:{root}")[:16]` unchanged (page ids are unique within a domain; the bucket directory carries the domain).

**Output** (bucketed): `sources/` and `citation_source_map/` with UVA's columns, `content_sha1` renamed `part_sha1`, `canonical_sha1` → `canonical_part_sha1`, plus the six identifier columns.

## Stage 6b: Per-Page Template Matching — UVA `template_matching.py`

Signals kept: doi / isbn / pmid (+ pmc / arxiv / wikidata_qid) / ref_name / url / title_year (.70) / author_containment / bert (optional). Input: same query with `part_type = 'template'`; structured fields (`title`, `year`, `author_key`, identifiers) are columns on `normalized_citation_parts` — no `params_json` parsing in the matcher.

**Output**: `template_sources/`, `template_citation_source_map/` (separate from 6a's, as in UVA).

## Stage 6c: Per-Page Cross-Type Merge — UVA `cross_type_merger.py`

Signals kept: doi 1.00 / isbn .95 / pmid .95 (+ pmc / arxiv / wikidata_qid .95) / url .85 / author_year .65, plus UVA's three-pass fuzzy title bridge. Merged sources get `source_type = 'both'`.

6c also joins the two maps into `merged_citation_source_map/` (`part_sha1, page_id, merged_source_id, source_type, match_type, confidence, is_canonical`).

**Output**: `merged_sources/`, `merged_citation_source_map/` (bucketed).

## Stage 6d: Cross-Page Matching — UVA `cross_page_matcher.py`

Kept: hard-id groupby (doi 1.00, isbn/pmid .95, + pmc/arxiv/qid .95), author+year fuzzy with title threshold 85 and `author_key[:4]`+year blocking, no URL cross-page, hard-id sources excluded from fuzzy, deterministic `global_source_id = sha1("global:" + "|".join(sorted(members)))[:16]`, canonical member by UVA `_id_priority` (identifier quality, then citation count), `confidence = None` for singletons.

Also kept: **`_compute_score`** (reliability score + verdict) — but it is *called* in Stage 7f, not here, so scoring is a materialization step with its own validation gate rather than a side effect of matching. `load_merged_sources` gains the three identifier columns and reads `id_tier`, `year_reliable`, `norm_subtype` of the canonical member from `normalized_citation_parts` (UVA reads them from `ref_tag_normalized`).

Scale: UVA loads all `merged_sources` into one DataFrame; at full scale this is the memory risk. Profile on one domain; fall back to Polars lazy / DuckDB groupby for Pass 1 (pure groupby) and Python only for the blocked fuzzy pass.

**Output**: `global_source_map/` (global): `merged_source_id, global_source_id, page_id, match_type, confidence`, plus a `global_sources_stage.parquet` with the representative fields UVA's `build_global_sources` produces (`n_pages`, `n_total_citations`, `page_ids` retained for diagnostics).

---

## Stage 7: Materialization and Exports (sequential)

```
7a  global_sources             Postgres  ← global_sources_stage (counts, score, verdict NULL)
7b  citation_source_resolution Postgres  ← merged_citation_source_map ⋈ global_source_map ⋈ parts
7c  citation_instances_denorm  Parquet   ← Postgres export (proposed_citation_instances_denorm.md)
7d  page_source_summary        Postgres  ← DuckDB: denorm × citation_history × revisions ×
                                            named_ref_uses × ref_name_links, batched by bucket
7e  count refresh              Postgres  ← n_pages, n_total_citations from page_source_summary
7f  reliability + verdict      Postgres  ← UVA _compute_score per global source
7g  global_source_scores       Parquet   ← thin export of 7e/7f columns
7h  page_summary_cache         Postgres  ← DuckDB (proposed_cache.md §1)
```

### 7a `global_sources`

Insert from `global_sources_stage.parquet` (UVA's representative selection, unchanged). `n_pages`, `n_total_citations`, `reliability_score`, `verdict` inserted NULL; `source_type` of the canonical member is stored (needed by 7f).

### 7b `citation_source_resolution`

```sql
INSERT INTO citation_source_resolution
    (normalized_id, part_index, domain_id, page_id, global_source_id,
     source_type, match_type, confidence, is_canonical)
SELECT p.normalized_id, p.part_index, d.id, m.page_id, g.global_source_id,
       m.source_type, m.match_type, m.confidence, m.is_canonical
FROM merged_citation_source_map m
JOIN global_source_map g USING (merged_source_id)
JOIN domains d ON d.value = m.domain
JOIN normalized_citation_parts p ON p.part_sha1 = m.part_sha1;
```

`match_type` / `confidence` are the **per-page** values from the 6a/6b maps (as in UVA's `citations_resolved`), not the cross-page values on `global_source_map` — the latter are NULL for singletons, which would make every single-page source "unresolved-looking" in `n_high_confidence`. The cross-page `match_type`/`confidence` live on `global_sources`.

A part with the same `part_sha1` can appear in more than one `normalized_citations` row (same inner content, different `<ref name>`); the join fans out correctly because the PK includes `(normalized_id, part_index)`.

### 7c `citation_instances_denorm` (resolved re-export)

Re-export — see `proposed_citation_instances_denorm.md`. The same dataset was first exported at the end of Stage 4 without resolution (for Stage 6's inputs); 7c replaces it with `global_source_id`/`source_type`/`match_type`/`confidence` filled from 7b, so 7d and Stage 8 never join `pg.citation_instances` / `pg.normalized_citation_parts` from DuckDB.

### 7d `page_source_summary`

DuckDB, Postgres ATTACHed read-write, batched by `(domain, page_bucket)` with checkpointing (`proposed_data_integrity.md` §2). All large inputs are Parquet views scoped to the bucket partition.

```sql
WITH latest AS (
    SELECT page_id, arg_max(revision_id, revision_timestamp) AS latest_rev_id
    FROM revisions GROUP BY page_id
),
temporal AS (
    SELECT dn.page_id, dn.global_source_id,
           MIN(r.revision_id)              AS first_rev_id,
           MIN(r.revision_timestamp)       AS first_seen,
           MAX(r.revision_id)              AS last_rev_id,
           MAX(r.revision_timestamp)       AS last_seen,
           COUNT(DISTINCT dn.citation_instance_id) AS n_citations,
           COUNT(DISTINCT ch.revision_id)  AS n_revisions_cited,
           BOOL_OR(ch.revision_id = l.latest_rev_id) AS is_current,
           ANY_VALUE(dn.source_type) AS source_type,
           ANY_VALUE(dn.match_type)  AS match_type,
           ANY_VALUE(dn.confidence)  AS confidence
    FROM citation_instances_denorm dn
    JOIN citation_history ch ON ch.citation_instance_id = dn.citation_instance_id
    JOIN revisions r         ON r.revision_id = ch.revision_id
    JOIN latest l            ON l.page_id = dn.page_id
    WHERE dn.global_source_id IS NOT NULL
    GROUP BY dn.page_id, dn.global_source_id
),
reuse AS (
    SELECT dn.page_id, dn.global_source_id, COUNT(*) AS n_self_closing_uses
    FROM named_ref_uses nru
    JOIN ref_name_links rnl                     -- Stage-5 Parquet copy, bucket-scoped
      ON rnl.page_id = nru.page_id AND rnl.ref_name = nru.ref_name
     AND nru.revision_id >= rnl.first_revision_id
     AND (rnl.last_revision_id IS NULL OR nru.revision_id <= rnl.last_revision_id)
    JOIN citation_instances_denorm dn ON dn.citation_instance_id = rnl.defining_instance_id
    WHERE dn.global_source_id IS NOT NULL
    GROUP BY dn.page_id, dn.global_source_id
)
INSERT INTO pg.page_source_summary (…)
SELECT :domain_id, t.page_id, t.global_source_id,
       gs.author_key, gs.year, gs.title, gs.extracted_* …,
       t.source_type, t.match_type, t.confidence,
       t.first_rev_id, t.first_seen, t.last_rev_id, t.last_seen,
       t.n_citations, t.n_revisions_cited, COALESCE(ru.n_self_closing_uses, 0), t.is_current
FROM temporal t
JOIN pg.global_sources gs USING (global_source_id)
LEFT JOIN reuse ru USING (page_id, global_source_id);
```

`revisions` and `ref_name_links` here are the Stage-3 / Stage-5 Parquet copies (bucket-scoped), so `latest` is exact even for revisions with no citations and no `pg.*` table is scanned inside the bucket loop; only `pg.global_sources` (small) is joined.

### 7e Count refresh

Sole writer of `n_pages` / `n_total_citations` (query in `proposed_cache.md` §4). Runs immediately after 7d.

### 7f Reliability score and verdict — UVA scorer

Port `cross_page_matcher._compute_score` unchanged:

```
score = 30·[source_type ∈ {template, both}] + 25·[id_tier > 0] + 15·[author_key]
      + 15·[year_reliable] + 5·[year] + {doi,isbn,pmid: 10; url: 8; author_year: 5;
        title_fuzzy, strong_author_fuzzy, author_title_fuzzy: 3}[match_type]
verdict: template/both → high_confidence; id_tier > 0 → high_confidence;
         norm_subtype = 'noise' → likely_not_citation;
         score ≥ 50 high / ≥ 25 medium / ≥ 10 low / else questionable
```

Inputs per global source: `source_type` (on `global_sources`), `match_type`, `author_key`, `year`, and the canonical member's `id_tier`, `year_reliable`, `norm_subtype` (join `normalized_citation_parts` on `canonical_part_sha1`). PMC/arXiv/QID `match_type` values map to the 10-point bonus. Implemented as one pass in Python (as UVA) or as a single SQL `UPDATE` with the verdict derived in a second `UPDATE` (a SET clause sees pre-update values). The score does not depend on 7e's counts; 7e→7f ordering is kept only so 7g exports both together.

The `n_pages`/`n_total_citations` weighting proposed earlier is **dropped**.

### 7g `global_source_scores`

`COPY (SELECT global_source_id, n_pages, n_total_citations, reliability_score, verdict FROM pg.global_sources) TO …/global_source_scores/*.parquet.zst`. Consumed by Stage 8 and by any denorm-based analytics that need verdicts.

### 7h `page_summary_cache`

`proposed_cache.md` §1.

---

## Orchestration

`matching/pipeline_runner.py` (adapted from UVA's, 359 lines): iterates `(domain, page_bucket)` for 6a–6c with a process pool (one bucket per worker; UVA's per-page `run_page` functions are called unchanged inside), then 6d once, then 7a–7h sequentially.

```bash
python -m matching.pipeline_runner --db-url … --parquet-dir data/parquet --domain en.wikipedia.org \
    --buckets ALL          # or --buckets 17,18 / --page-id 12345 for testing
```

Rebuild strategy: full replace for global tables (7a/7b/7d truncated before Stage 7); bucketed 6a–6c outputs are re-runnable per bucket.

---

## Adaptation Summary

| Change | Scope | Effort |
|---|---|---|
| Input assembled from `normalized_citation_parts` ⋈ `citation_instances` ⋈ `citation_history` (DuckDB, per bucket) | 6a, 6b | Medium |
| Column renames `content_sha1→part_sha1`, `canonical_sha1→canonical_part_sha1` | all | Low |
| PMC / arXiv / Wikidata QID hard-id signals | 6a–6d | Low |
| Bucketed Parquet outputs replacing DuckDB tables | 6a–6d | Medium |
| Process-pool orchestration by bucket | runner | Low–Medium |
| `_compute_score` moved to 7f, reading canonical member fields via `canonical_part_sha1` | 6d/7f | Low |
| 7d query on Parquet inputs (denorm, history, revisions, named_ref_uses) | 7d | Medium |
| Reuse attribution (`named_ref_uses` × `ref_name_links`) | 7d | Low–Medium (new) |
| Exports 7c, 7g | new | Low |

Deferred: incremental matching; out-of-core 6d (profile first); LibraryBase; cross-domain matching (same DOI on two domains — the `global_source_id` scheme supports it; per-page `source_id` must then include the domain, since `page_id` is only unique within a domain; initial build runs per domain); BERT/`author_key_enriched` enrichment (passes retained behind UVA's table-exists checks; no producer in the critical path).

---

## Column Mapping: UVA → Unified

| UVA table.column | Unified |
|---|---|
| `citations_extracted.content` | `normalized_citation_parts.content` |
| `citations_extracted.ref_name` | `citation_instances.reference_name` |
| `citations_extracted.rev_id` | `citation_history.revision_id` (Parquet) |
| `ref_tag_normalized.content_sha1` / `template_normalized.content_sha1` | `normalized_citation_parts.part_sha1` |
| `ref_tag_normalized.*`, `template_normalized.*` | `normalized_citation_parts.*` (same names) |
| `citation_page_map` | `citation_instances (domain_id, page_id, normalized_id)` |
| `sources`, `citation_source_map`, `template_sources`, `template_citation_map`, `merged_sources` | `matching_intermediate/<same>/domain=/page_bucket=/` |
| `global_source_map` | `matching_intermediate/global_source_map/` |
| `global_sources` | `global_sources` (Postgres) + `global_source_scores` (Parquet) |
| `page_source_summary`, `page_revision_stats` | same names (Postgres / Parquet) |
| `wikipedia_pages` | `page_metadata` |