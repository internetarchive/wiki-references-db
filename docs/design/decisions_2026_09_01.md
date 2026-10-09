# Review: Gaps and Inconsistencies in the Proposed Plan — RESOLVED

Original review: 2026-08-31. Decisions made by James and applied to all nine `proposed_*.md` docs on 2026-09-01. This doc now serves as the resolution log; each finding below records the decision and where it landed.

---

## A. Critical / cross-cutting — all resolved

**A1. Multi-wiki vs Postgres keying.** ✔ Resolved via document identity: each Wikipedia article is a distinct `documents` row uniquely identified by its curid URL, and `page_id` throughout the schema means `documents.id` (internal, unique across wikis) — not the raw curid. No wiki columns needed on page-keyed tables. Documented in `proposed_data_model.md` (new "Identity Model" section); API design principle 4 and the frontend URL helpers updated accordingly (the summary response now carries the canonical curid `url` since `page_id` is no longer the curid).

**A2. `bare_url` citations orphaned.** ✔ Resolved: `bare_url` rows are processed by the ref_tag normalizer (Stage 4 branches on `citation_type IN ('ref_tag','bare_url')`) and included in ref_tag matching (Stage 6a). Extractor, matching, and frontend table-mapping updated.

**A3. `reference_type` self-closing conflict.** ✔ Resolved, and superseded by a larger decision (see C7): self-closing refs produce no hash and no citation rows at all — they are reuse events, recorded in `named_ref_uses` Parquet and resolved through `ref_name_links`. `reference_type` keeps its positional meaning; no query infers self-closing status from it anymore.

**A4. Summary endpoint "Postgres-only" claim.** ✔ Resolved by removing the claim: docs now distinguish the cache-serving path (pure Postgres) from the cache rebuild path (Stage 7f; DuckDB + `citation_history`, since latest-revision scoping is not knowable from Postgres alone). Fixed in `proposed_api.md` (endpoint mapping + query-engine section) and `proposed_frontend.md` §6.

**A5. Two migration philosophies.** ✔ Resolved: Tier 3 alias views are retained as a **reference and transitional/validation aid**, with an explicit note that the production path is the planned full rewrite of the frontend's lookup layer. Noted in both `proposed_data_model.md` Tier 3 and `proposed_frontend.md`.

## B. Schema inconsistencies — all resolved

**B1. `extracted_pmc` missing from `global_sources`.** ✔ Column and partial index added; included in the 7a insert description.

**B2/B3. Identifier set drift.** ✔ Canonical set declared in `proposed_data_model.md` and applied everywhere: DOI, ISBN, PMID, PMC, arXiv, Wikidata QID, LibraryBase ID, URL — with the standing caveat that LibraryBase is model-only (no current extraction/matching). `citation_instances_denorm` gained `extracted_pmid`/`extracted_pmc`.

**B4. `ref_name_links` storage + scoping.** ✔ Settled on Postgres; schema reworked to per-revision-range: `(page_id, ref_name, defining_sha1, first_revision_id, last_revision_id)`. The `self_closing_sha1` column is gone (self-closing refs have no hash). Data-flow summary corrected.

**B5. `page_summary_cache` dual schema.** ✔ Clarified in both docs: the data-model listing is the canonical (logical) column model; the cache doc's JSONB schema is the derived production serving representation and the actual DDL. Staleness detection via the `latest_rev_id` watermark (no extra column).

**B6. Stale cache §2.** ✔ Rewritten to state the per-type counts are already in the data model; the long-form file remains a documented future option only.

## C. Pipeline and orchestration — all resolved

**C1. Stage numbering.** ✔ Unified: Stages 1–5 extraction/normalization/links (`proposed_extractor.md`), 6a–6d matching, 7a–7f materialization (`proposed_matching.md`), 8a/8b Parquet export. The extractor doc's old catch-all "Stage 6" box now summarizes 6–8 with correct numbers; integrity and cache docs renumbered to match.

**C2. Stage 6b append.** ✔ 6b writes its own `template_citation_source_map.parquet`; **6c is the joiner**, emitting `merged_citation_source_map.parquet`. No stage shares an output; integrity doc's no-append rule now holds.

**C3. Stage 7c/7f execution strategy.** ✔ Documented alternative in `proposed_data_integrity.md` §2: batch-and-checkpoint at **(wiki, page_bucket)** granularity — hundreds of buckets instead of millions of pages, checkpoint file bounded by bucket count, each batch reads one Parquet partition.

**C4. `page_summary_cache` build unowned.** ✔ Added as **Stage 7f** in the matching orchestrator, cross-referenced from cache, API, denorm, and data-model docs.

**C5. Counts computed twice / stale reliability.** ✔ 7a inserts `n_pages`/`n_total_citations` as NULL; the cache-doc §4 UPDATE (now **Stage 7d**) is their sole writer, always followed by **7e** (reliability + verdict). To break the resulting cycle, `reliability_score`/`verdict` were removed from `page_source_summary` — they live only on `global_sources`, which the top-sources endpoint already joins.

**C6. `revision_signals` contradiction.** ✔ Postgres table dropped; flags written straight to `revision_signals.parquet` at extraction (analytics data, no serving consumer). Stats doc's relationship section updated.

**C7. `named_ref_count` had no source / self-closing noise.** ✔ Self-closing refs are no longer stored as their own citations — they increment the named reference's reuse counter. New extraction output `named_ref_uses` Parquet (page_id, revision_id, ref_name, n_uses) feeds `page_revision_stats.n_self_closing`, which serves the timeline's `named_ref_count`; per-source attribution flows through `ref_name_links` into `page_source_summary.n_self_closing_uses`.

**C8. `wikipedia_pages` never populated.** ✔ Stage 3 load now populates it from the page-metadata data file that accompanies the dumps, at the same time article-document rows are added; the on-demand Wikipedia API fetch is demoted to an exception path (cache §5).

## D. Code-level bugs — all resolved

**D1.** ✔ Reliability/verdict split into two sequential UPDATEs (7e), with a note on why one UPDATE cannot work.
**D2.** ✔ Invalid 7a aggregate SQL removed; replaced with a prose spec (highest-confidence representative via window function/DISTINCT ON, identifier coalescing).
**D3.** ✔ Broken denorm export script removed; replaced with a prose build recipe (including the correct recursive glob for row-count verification).
**D4.** ✔ Single-shot stats build script removed; the batched (wiki, page_bucket) variant is the reference implementation and now also registers the `named_ref_uses` view.
**D5.** ✔ DuckDB pool now uses a writable init pass (with ATTACH so pg.* views bind) to create persistent views, then opens read-only pool connections that only ATTACH.
**D6.** ✔ Integrity §5 now points to the batched variant in the stats doc.

## E. Smaller items — all resolved

- **documents vs wikipedia_pages:** ✔ Language standardized around documents — Wikipedia pages are a type of document; `documents` is authoritative for existence, `wikipedia_pages` is derived display metadata keyed by `documents.id`.
- **Frontend §5c:** ✔ Marked Done (API doc already lists all identifier fields).
- **Extractor cross-references:** ✔ Point to `proposed_matching.md`; stale `notes/` prefixes dropped.
- **Validation placement:** ✔ `citation_type` NULL check moved to the after-Stage-3 gate; new gates added for 7d (counts) and 7e (scores).
- **Stage-4 wording contradiction:** ✔ Reworded — the column list is split into "populated at extraction/load (Stages 1–3)" vs "populated by Stage 4".
- **`n_high_confidence`:** ✔ Wanted and surfaced: new `high_confidence_count` field on timeline points (`proposed_api.md`) and a frontend rendering item (`proposed_frontend.md` §3d).
- **`ref_name` signal join:** ✔ Matching doc now states 6a/6b inputs must join `citation_instances` (source of `page_id` and `reference_name`).

## F. Missing pieces — still open (no doc written yet)

These were identified in the original review and remain undocumented; each would be a new doc or section when the time comes:

1. **Backfill/migration plan** for the existing database (Stage 4 over already-loaded rows, ALTER TABLE/index build strategy on tens of millions of rows). The `page_id = documents.id` decision (A1) also implies checking whether existing rows keyed on raw curids need remapping.
2. **End-to-end refresh cadence/orchestration** — what triggers a full rebuild, expected wall-clock, serving-during-rebuild policy.
3. **Testing strategy** — golden pages, fixture dumps, matching-quality regression detection for the ported UVA code.
4. **`GET /sources/{id}` detail endpoint** — no way to fetch a single global source's metadata without going through a page's top-sources.