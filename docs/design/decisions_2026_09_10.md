# Decisions: Resolving the Codebase Gaps

*2026-09-10 · Resolves the items in `claude/codebase_gap_assessment.md` §4. Decisions by James; to be applied to the `proposed_*.md` docs.*

Standing assumption for all items: **this is a rebuild.** Existing Postgres data is not migrated in place; the pipeline is re-run from RevisionChest bundles.

## 1. Page identity — `(domain_id, page_id)`

- The page key throughout the schema is the pair **`domain_id` (FK `domains.id`) + `page_id` (wiki-local curid)**. The earlier rule "`page_id` means `documents.id`" is withdrawn.
- Every page-keyed table (`citation_instances`, `revisions`, `page_source_summary`, `page_summary_cache`, `ref_name_links`, `named_ref_uses`, Parquet partitions) carries `domain_id`; unique constraints and indexes are on `(domain_id, page_id, …)`.
- `documents` is decoupled from article identity for the build. It remains the abstract node for *any* document — Wikipedia articles and cited works alike — and is populated/cleaned in the citation-graph phase. Nothing in Stages 1–8 depends on `documents` being correct.
- Terminology: the notes' `wiki` column/partition is renamed **`domain`**. Domain (not language code or project family) is the identifier, so the model extends to non-Wikimedia encyclopedia-style sites.
- API: `page_id` keeps its v1 meaning (curid). Page-scoped v2 endpoints take a domain qualifier (path or query; the article resolver returns both). Response `wiki` field → `domain`.

## 2. Entity tables — RevisionChest owns them (Option A)

- RevisionChest (Postgres mode, or a new `load_all` phase over its `--parquet` metadata file) is the sole writer of `revisions` (all revisions, with bundle offsets), `revision_bundles`, and article-page metadata.
- `build_db.py` / `load_all.py` stop writing `revisions`, and stop writing article-side `documents` / `web_resources` rows.
- `wikipedia_pages` is dropped from the model; title and namespace come from RevisionChest's page metadata (stored on a page-metadata table keyed by `(domain_id, page_id)` — name TBD in the data-model rewrite — rather than on `documents.title`).
- `revisions.revision_timestamp` becomes `TIMESTAMP`; one format.
- RevisionChest's title-keyed `documents` insert is no longer load-bearing; it should be removed or made to write the page-metadata table instead (small Rust change; not blocking).

## 3. `citation_history` — deterministic ids, Parquet-native (Option B)

- `citation_instance_id` = deterministic 64-bit hash of `(domain, page_id, raw_sha1)`, computed in `build_db.py`. Postgres receives the id; no `BIGSERIAL`, no `(page_id, raw_sha1) → id` lookup at load.
- `build_db.py` writes `citation_history` directly in the Tier-2 layout: `domain={domain}/page_bucket={bucket}/…`, columns `(citation_instance_id, revision_id, page_id, domain_id)`.
- The Postgres `citation_history` table is **not created** in the rebuild. Group 3 v2 endpoints read Parquet via DuckDB from day one; the v1 alias period is served by the old database until v2 is live.

## 4. Reliability score / verdict — port UVA's scorer (Option A)

- `_compute_score` from `cross_page_matcher.py` is ported as-is (template/both +30, `id_tier>0` +25, author +15, `year_reliable` +15, year +5, match-type bonus; template or hard-id → `high_confidence`; `noise` → `likely_not_citation`; else ≥50/≥25/≥10).
- Stage 7e is relabelled "reliability score and verdict (UVA scorer)". The `n_pages`/`n_total_citations` weighting proposed in the notes is dropped from the initial build (may be added later as an explicit change).
- Because the scorer needs `id_tier`, `year_reliable`, `norm_subtype` of the canonical member, 7a must carry those (or 7e joins `normalized_citations` on `canonical_sha1` as the notes already do).
- General principle: **reuse UVA code wherever it exists**; adapt inputs rather than rewrite logic.

## 5. Stage 4 input contract — content extraction step (Option A)

- Stage 4 begins with a content-extraction step that derives `content` from `reference_normalized`: for `<ref>` citations strip the wrapper (name stays on `citation_instances.reference_name`); for other kinds pass the item text through.
- `citation_type` taxonomy extended to cover the IA extractor's four candidate kinds: `ref_tag | template | bare_url | endnote | shorthand | external_link` (final enum to be fixed in the data-model rewrite). UVA normalizers run on `ref_tag`/`bare_url`/`shorthand`/`endnote` (ref-tag path) and `template` (template path); `external_link` yields `extracted_url` only.
- Template classification uses the existing `wikis.yaml` `citation_templates` (`prefixes` / `exact`) via `wiki_config.py`. The hard-coded `CITE_TEMPLATES` set in `proposed_extractor.md` is removed.
- UVA's footnote filter (`_is_citation`) is ported into Stage 4 as a classifier input (sets `noise`), not applied at extraction — extraction keeps everything.
- **Bullet-block splitting moves to Stage 4.** One `<ref>` stays one `citation_instance` (offsets, `template_data`, and hashes unchanged); Stage 4 may emit N semantic rows for a multi-citation block, linked to the one instance. Schema: a `normalized_citation_parts` (name TBD) table keyed `(normalized_id, part_index)` carrying the Stage-4 columns; single-citation refs have one part. Matching operates on parts.

## 6. Self-closing refs — reuse events, per occurrence (Option A, extended)

- Self-closing `<ref name="X" />` produce **no** `citation_instances` / `normalized_citations` rows or hashes. Extractor change: stop emitting them as references and stop per-revision text dedup for them.
- `named_ref_uses` is recorded **one row per occurrence**, not as a counter: `(domain_id, page_id, revision_id, ref_name, offset_start)`. This preserves the ability to locate each reuse in the revision text (for reference↔sentence correlation later); per-revision counts are a `GROUP BY`. If volume is a problem, fall back to counts for history and keep offsets for the latest revision only.
- `ref_name_links` with revision ranges (Stage 5) is built new; UVA's page-scoped `build_ref_name_links` is the seed. Name redefinition mid-history starts a new range row.
- The existing extractor test `test_extract_references_self_closing_ref_name` changes to assert the reuse-event output.

## 7. Identifier extraction — arXiv and Wikidata QID in the template path

- Wikidata QID: `{{Cite Q}}` first positional parameter (`Q\d+`). Template normalizer.
- arXiv: `|arxiv=` on `{{cite arXiv}}` / `{{cite journal}}` and the `|eprint=` alias on `{{cite arXiv}}`. Template normalizer. Optional: `arxiv.org/abs/<id>` URLs in the ref-tag path.
- Both feed the hard-identifier passes in 6a–6d as the notes specify. LibraryBase stays model-only.

## 8. ML enrichment — acknowledged, off the critical path

- UVA's optional `bert_results` / `author_key_enriched` inputs (and the `bert` passes that read them) are documented as optional enrichments. No procedure for producing them exists in the repo; they are not part of the initial build. The passes stay in the ported code behind their existing table-exists checks.

## 9. Matching scale-out shape

- 6a–6c outputs are bucketed by `(domain, page_bucket)`; the bucket is the unit of re-run and of parallelism. No per-page files.
- 7c and 8a read `citation_instances_denorm` Parquet (8b) instead of `pg.*` tables via ATTACH; 8b therefore runs before 7c, and `reliability_score`/`verdict` are exported separately after 7e (thin file keyed on `global_source_id`).
- Gate for Phase 2: benchmark one shard end-to-end through the ported matcher.

## Follow-ups

- Rewrite the affected `proposed_*.md` sections (data model, extractor, matching, stats, denorm, cache, API, frontend, integrity) to these decisions.
- Open items F1–F4 in `claude/plan_review.md`: F1 (backfill) is superseded by the rebuild assumption; F2 (refresh cadence / serving during rebuild), F3 (testing), F4 (`GET /sources/{id}`) remain.

## Applied 2026-09-10 — numbering and additions made while applying these decisions

- Stage numbering in the docs is now: 0 RevisionChest · 1–5 · 6a–6d · **7a global_sources, 7b citation_source_resolution, 7c citation_instances_denorm (resolved export), 7d page_source_summary, 7e count refresh, 7f reliability/verdict (UVA scorer), 7g global_source_scores export, 7h page_summary_cache** · **8 page_revision_stats** (single step). Item 9's "8b before 7c" is realised as 7c-before-7d.
- `citation_instances_denorm` is built twice: unresolved at the end of Stage 4 (step 4.6, the matchers' input) and resolved at 7c. Stage 6 reads only Parquet.
- Two further bucketed Parquet copies were added so no `pg.*` table is scanned inside a bucket loop: `revisions` (Stage 3, from RevisionChest metadata) and `ref_name_links` (Stage 5).
- Item 5's parts table is `normalized_citation_parts` keyed `(normalized_id, part_index)`, with `part_sha1 = SHA1(content)` as the direct analogue of UVA's `content_sha1`. `citation_source_resolution` is keyed on the part.
- Item 5's taxonomy is fixed as `ref_tag | template | bare_url | endnote | shorthand | external_link` (`citation_type`, set at extraction); `part_type` (`template | ref_tag | external_link`) records the normalizer path.
- Item 6: `named_ref_uses` is per-occurrence `(page_id, revision_id, ref_name, offset_start)`; `ref_name_links.defining_instance_id` points at `citation_instances.id`.
- Item 3: `citation_instances.id` is the first 8 bytes of `SHA1("{domain}|{page_id}|{raw_sha1}")` as `BIGINT`, with a collision gate in dedup and a documented UUID fallback above ~10⁹ instances.
- `citation_source_resolution.match_type/confidence` are the per-page values (UVA `citations_resolved` semantics), not the cross-page ones, so singleton sources are not NULL-confidence at the page level.
- `wikipedia_pages` → `page_metadata (domain_id, page_id, page_title, page_namespace)`.
- Timeline `named_ref_count` now means reuse events; UI label changes to "reuses".
- `GET /api/v2/sources/{global_source_id}` added (closes F4).