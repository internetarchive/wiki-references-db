### Proposed Unified API

*Revised 2026-09-10 per `claude/decisions_2026-09-10.md`.*

Merges the wiki-references-db v1 endpoints with the UVA analytics endpoints under `/api/v2`.

---

### Design Principles

1. **Page-centric**: page-scoped endpoints are keyed by **`(domain, page_id)`**, where `page_id` is the site-local id (the curid) — the same meaning as v1. The domain is a path segment: `/api/v2/pages/{domain}/{page_id}/…`.
2. **Layered detail**: summary → detail → history.
3. **Backward compatible**: every v1 use case is covered; v1 paths remain as aliases during migration (they imply `domain = en.wikipedia.org`).
4. **Domain-aware**: `domain` (e.g. `en.wikipedia.org`) is returned on every response and is what clients use to build site links. Global entities (`global_source_id`, `normalized_sha1`, `wiki_template_id`, URLs) are not domain-scoped.

---

### Endpoint Catalog

#### Group 1: Article Resolution

##### `GET /api/v2/article?url={wikipedia_url}`

Resolves any article URL (curid or title form; title form via the site API as v1's explorer does) to identity + metadata.

| Field | Type | Description |
|---|---|---|
| `domain` | string | |
| `page_id` | int | site-local id |
| `url` | string | canonical curid URL |
| `page_title` | string? | `page_metadata` |
| `page_namespace` | int? | `page_metadata` |
| `revision_count` | int | |
| `latest_revision_id` | int? | |

**Tables**: `web_resources` (curid URL → `numeric_page_id`, `domain_id`), `domains`, `page_metadata`, `revisions`. Replaces v1 `GET /article?url=` (which returned `page_id` + `document_id`; `document_id` is dropped).

---

#### Group 2: Page Analytics (UVA frontend)

All Group 2 responses share a header block: `domain, page_id, page_title, page_namespace, url, revision_count, first_revision_timestamp, latest_revision_timestamp`.

##### `GET /api/v2/pages/random/summary?domain=`
`page_summary_cache` (+ `page_metadata`). Pure Postgres.

##### `GET /api/v2/pages/{domain}/{page_id}/summary`
Header + `latest_revision {rev_id, timestamp, reference_count, citation_type_breakdown[{citation_type,count}], top_templates[{template_name,count}]}` + `source_profile {total_citation_uses, format_breakdown, source_kind_breakdown, summary}`.
**Serving**: `page_summary_cache` ⋈ `page_metadata` — pure Postgres. **Rebuild** (Stage 7h): DuckDB over `revisions`, `citation_history`, `citation_instances_denorm`, `global_source_scores`.

##### `GET /api/v2/pages/{domain}/{page_id}/timeline`
Header + `points[]`, one per revision (including zero-citation revisions):

| Field | Source |
|---|---|
| `rev_id`, `timestamp` | `page_revision_stats` |
| `reference_count` | `n_total_citations` |
| `template_count` | `n_template` |
| `named_ref_count` | **semantics change**: UVA counted instances carrying a `ref_name`; v2 serves `n_self_closing` (reuse events) under this key and the frontend label changes to "reuses" (`proposed_frontend.md` §3d). The old metric is not carried over (could be added to `page_revision_stats` as `n_named` later) |
| `high_confidence_count` | `n_high_confidence` (new) |
| `citation_type_breakdown` | the six `n_*` type columns |

**Tables**: `page_metadata` (Postgres); `page_revision_stats` (Parquet, one bucket partition). Fallback: `revisions` ⋈ `citation_history` ⋈ `citation_instances_denorm` ⋈ `named_ref_uses`, grouped by revision.

##### `GET /api/v2/pages/{domain}/{page_id}/top-sources?limit=10`
Header + `total_sources` + `sources[]`: `global_source_id, source_label, author_key, year, title, extracted_{doi,isbn,pmid,pmc,arxiv,wikidata_qid,librarybase_id,url}, source_type, page_citation_count (n_citations), cross_page_count (gs.n_pages), match_type, confidence, reliability_score, verdict, first_seen, last_seen, n_revisions_cited, n_self_closing_uses, is_current`.
**Tables**: `page_source_summary` ⋈ `global_sources` ⋈ `page_metadata`. Pure Postgres. Order: `n_citations DESC, n_pages DESC` (UVA order).

##### `GET /api/v2/sources/{global_source_id}/pages?exclude_domain=&exclude_page_id=&limit=10&offset=0`
`global_source_id, source_label, total_pages (gs.n_pages)`, `pages[]`: `domain, page_id, page_title, page_namespace, url, citation_count (n_citations), n_revisions_cited, first_seen, last_seen, is_current`.
**Tables**: `page_source_summary` ⋈ `page_metadata` ⋈ `domains`, `global_sources`. Pure Postgres. Cross-domain by construction.

##### `GET /api/v2/sources/{global_source_id}` *(new; closes open item F4)*
One `global_sources` row with `n_pages, n_total_citations, reliability_score, verdict` and the canonical member's `normalized_sha1`/`part_index` (via `citation_source_resolution WHERE is_canonical`). Pure Postgres.

---

#### Group 3: Revision & Citation Detail

##### `GET /api/v2/pages/{domain}/{page_id}/revisions?limit=100&offset=0`
`revisions[] {revision_id, revision_timestamp, parent_revision_id, citation_count}`, `total`. **Tables**: `revisions` (Postgres) for the page; `citation_history` (Parquet, one bucket) for counts. All revisions appear (RevisionChest writes every one).

##### `GET /api/v2/pages/{domain}/{page_id}/citations?revision_id=&raw=false&limit=100&offset=0`
Citations present at a revision. `NormalizedCitationItem` gains: `citation_type`, `parts[] {part_index, part_type, norm_subtype, template_name, template_subtype, extracted_{doi,isbn,pmid,pmc,arxiv,wikidata_qid,url}, global_source_id, source_type, match_type, confidence, reliability_score, verdict}`.
**Tables**: `citation_history` (Parquet) → `citation_instances`, `normalized_citations`, `normalized_citation_parts`, `citation_source_resolution`, `global_sources`, `normalized_citation_web_resources`, `web_resources`, `template_data`, `wiki_templates` (Postgres).

##### `GET /api/v2/citations/{normalized_sha1}`
v1 `CitationDetailResponse` + `citation_type` + `parts[]` as above + `instances[] {domain, page_id, reference_name, reference_type}`.

##### `GET /api/v2/citations/{normalized_sha1}/history?domain=&page_id=`
Unchanged shape; `citation_instances` (Postgres) → `citation_history` (Parquet) → `revisions` (Postgres).

---

#### Group 4: Template & Web Resource Lookup

##### `GET /api/v2/templates/{wiki_template_id}/report?parameter_key=&parameter_value=&limit=&offset=`
Unchanged (`wiki_templates` is already domain-scoped through `domain`).

##### `GET /api/v2/web-resources?url={url}`
Unchanged shape; each referencing citation gains `global_source_id`, `reliability_score`, `verdict` (via `normalized_citation_parts` → `citation_source_resolution` → `global_sources`; a citation with several parts may yield several).

---

### Endpoint → Table Mapping

| Endpoint | Postgres | Parquet (DuckDB) |
|---|---|---|
| `/article?url=` | `web_resources`, `domains`, `page_metadata`, `revisions` | — |
| `/pages/random/summary`, `/pages/{d}/{id}/summary` | `page_summary_cache`, `page_metadata` | — (7h rebuild: `revisions`, `citation_history`, `citation_instances_denorm`, `global_source_scores`) |
| `/pages/{d}/{id}/timeline` | `page_metadata` | `page_revision_stats` |
| `/pages/{d}/{id}/top-sources` | `page_source_summary`, `global_sources`, `page_metadata` | — |
| `/sources/{id}/pages`, `/sources/{id}` | `page_source_summary`, `global_sources`, `page_metadata`, `domains`, `citation_source_resolution` | — |
| `/pages/{d}/{id}/revisions` | `revisions` | `citation_history` |
| `/pages/{d}/{id}/citations` | `citation_instances`, `normalized_citations`, `normalized_citation_parts`, `citation_source_resolution`, `global_sources`, `web_resources`, `normalized_citation_web_resources`, `template_data`, `wiki_templates` | `citation_history` |
| `/citations/{sha1}` | same minus history | — |
| `/citations/{sha1}/history` | `citation_instances`, `revisions` | `citation_history` |
| `/templates/{id}/report` | `wiki_templates`, `template_data`, `normalized_citations` | — |
| `/web-resources?url=` | `web_resources`, `domains`, `normalized_citation_web_resources`, `normalized_citations`, `normalized_citation_parts`, `citation_source_resolution`, `global_sources` | — |

---

### Migration Notes

| v1 path | v2 path |
|---|---|
| `/api/v1/article?url=` | `/api/v2/article?url=` (`document_id` dropped; `domain` added) |
| `/api/v1/article/{page_id}/revisions` | `/api/v2/pages/{domain}/{page_id}/revisions` |
| `/api/v1/article/{page_id}/citations` | `/api/v2/pages/{domain}/{page_id}/citations` |
| `/api/v1/citation/{record_sha1}` | `/api/v2/citations/{normalized_sha1}` |
| `/api/v1/citation/{record_sha1}/history` | `/api/v2/citations/{normalized_sha1}/history` |
| `/api/v1/template/{id}/report` | `/api/v2/templates/{id}/report` |
| `/api/v1/web_resource?url=` | `/api/v2/web-resources?url=` |
| — | `/api/v2/pages/random/summary`, `/pages/{d}/{id}/summary`, `/timeline`, `/top-sources`, `/sources/{id}/pages`, `/sources/{id}` |

**Coexistence.** This is a rebuild: the v2 service runs against the new database; v1 continues to be served from the existing database until v2 is live, then v1 paths become thin aliases (domain defaulted to `en.wikipedia.org`) over v2 handlers, then are removed.

**Query engine.** Groups 1, 4 and the Group 2 summary/top-sources/source-pages endpoints are pure Postgres. Timeline reads `page_revision_stats` Parquet. Group 3 endpoints touch `citation_history` Parquet where the revision dimension is involved. Every Parquet read is pruned to `domain=…/page_bucket=…`.