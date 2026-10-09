# Wikipedia Citations Database — Executive Summary

*Migration plan overview · revised 2026-09-10 · Companion to the nine `proposed_*.md` design documents, `claude/codebase_gap_assessment.md`, and `claude/decisions_2026-09-10.md`*

---

## Purpose

This document summarizes the goals and shape of the effort to merge the UVA Wikipedia Citations Database project into wiki-references-db as a single system. It is a **rebuild**: the pipeline is re-run from RevisionChest bundles rather than migrated in place.

## Project Goals

**1. Adopt UVA's semantic capabilities on top of wiki-references-db's extraction at scale.** wiki-references-db extracts and syntactically normalizes references from full revision histories; UVA contributes citation classification, identifier and bibliographic-field extraction, per-page and cross-page source matching, and a reliability score/verdict per source. The principle is to reuse UVA code and adapt its inputs, not rewrite its logic.

**2. Adopt the UVA frontend.** The analytics dashboard (page summary, timeline, top sources, cross-page usage) becomes the project frontend, rebranded "Wikipedia Citations Database". Frontend changes are mechanical; the backend query layer is rewritten.

**3. Deliver a V2 API** under `/api/v2` covering every V1 use case, keyed by `(domain, page_id)`, plus the analytics endpoints and a source-detail endpoint.

## Target Architecture

- **Identity**: pages are `(domain_id, page_id)` — domain plus site-local id. `documents` is the abstract document node and is decoupled from page identity until the citation-graph phase.
- **RevisionChest (Stage 0)** owns the entity tables: every revision (with bundle offsets), page titles/namespaces (`page_metadata`), article curid URLs.
- **Postgres** holds entities, `normalized_citations` + the new `normalized_citation_parts` (UVA's semantic columns, one row per citation part so bullet-list blocks can split), `citation_instances` (deterministic hash ids), and the resolution/serving tables (`global_sources`, `citation_source_resolution`, `page_source_summary`, `page_summary_cache`).
- **Parquet** holds `citation_history` (written directly by the extractor, never loaded to Postgres), `named_ref_uses` (self-closing reuse events, one row per occurrence with offset), `revisions`, `citation_instances_denorm`, `global_source_scores`, `page_revision_stats`, and the matching intermediates — all partitioned by `domain` / `page_bucket`.
- **DuckDB** runs every cross-engine step Parquet-to-Parquet and serves the timeline/history endpoints.

## Pipeline

```
0  RevisionChest                      5  ref_name_links (revision ranges)
1  extraction (+ citation_type, instance ids, history/reuse Parquet)
2  syntactic normalization (unchanged) 6a–6d  UVA matching, bucketed
3  dedup + load (+ RevisionChest metadata)   7a–7h  materialize + export (incl. UVA scorer, 7f)
4  UVA normalizers → parts; unresolved denorm export   8  page_revision_stats
```

## Phases

**Phase 0 — Schema and RevisionChest.** New schema (no migration); RevisionChest: `TIMESTAMP` timestamps, `page_metadata`, drop title-keyed documents insert. `build_all.py` passes `--domain`.

**Phase 1 — Stages 1–5.** Extractor changes (`citation_type`, deterministic ids, Tier-2 writers, reuse events), loader changes, Stage 4 driver around the UVA normalizers (+ arXiv / `{{Cite Q}}` params), Stage 5 link builder.

**Phase 2 — Stage 6.** UVA matchers on bucketed Parquet inputs with a process pool; three added identifier signals. Gate: one-shard end-to-end benchmark.

**Phase 3 — Stages 7–8.** Materialization, exports, UVA scorer, cache, stats.

**Phase 4 — V2 API.** Ported V1 endpoints, analytics endpoints, `GET /sources/{id}`; V1 served from the old database until cutover.

**Phase 5 — Frontend.** Backend query rewrite, then the mechanical frontend changes.

## Risks

Cross-page matching memory at full scale (profile, then out-of-core); per-page matcher throughput (benchmark gate); 64-bit instance-id collisions above ~10⁹ instances (gate + UUID fallback); UVA heuristics are English-tuned (other domains degrade gracefully).

## Open Items

Refresh cadence and serving-during-rebuild policy; testing strategy (golden pages, matching-quality regression) — the ported code has no tests; per-domain tuning of the normalizers. See `claude/plan_review.md` (F2, F3; F1 superseded by the rebuild, F4 closed by `GET /sources/{id}`).