### Proposed Unified API

*Revised 2026-09-10 per `claude/decisions_2026-09-10.md`.*

Merges the wiki-references-db v1 endpoints with the UVA analytics endpoints under `/api/v2`.

---

### Design Principles

1. **Page-centric**: page-scoped endpoints are keyed by **`(domain, page_id)`**, where `page_id` is the site-local id (the curid) — the same meaning as v1. The domain is a path segment: `/api/v2/pages/{domain}/{page_id}/…`.
2. **Layered detail**: summary → detail → history.
3. **Backward compatible**: every v1 use case is covered; v1 paths remain as aliases during migration (they imply `domain = en.wikipedia.org`).
4. **Domain-aware**: `domain` (e.g. `en.wikipedia.org`) is returned on every response and is what clients use to build site links. Global entities (`global_source_id`, `normalized_sha1`, URLs) are not domain-scoped; templates are `(domain, name)`.
5. **No build-dependent ids** *(2026-11-13)*. Everything the API returns or accepts is named by an upstream id (domain, page id, revision id), a content hash (`normalized_sha1`, `raw_sha1`, `part_sha1`, `canonical_part_sha1`), a name (templates) or a URL (web resources). Database row ids are internal: `wiki_templates.id` and `web_resources.id` are assigned in load order and are never returned; a citation instance is `(domain, page_id, raw_sha1)` (`citation_instances.id` is a hash of exactly that, kept only in the v1 aliases). The one interim id is **`global_source_id`**: `<run id>.<hash>` for builds made with `WCD_RUN_ID`, a bare 16-hex hash for the build before run ids; it changes whenever a source's membership does, so it is a per-build handle. Its durable counterpart is **`source_key`**: the source's standardized identifier as `scheme:value`, in the order doi, isbn, pmid, pmc, arxiv, wikidata, librarybase, url (intrinsic identifiers of the work first, then the knowledge-base items that are created later — LibraryBase being the assigner of last resort — so a key only ever moves toward a more standard identifier), else `part:<canonical_part_sha1>`, a content hash, as the fallback. Wikidata and LibraryBase ids are both Q-ids; the scheme always says which.

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
Header + `total_sources` + `sources[]`: `global_source_id, source_key, source_label, author_key, year, title, extracted_{doi,isbn,pmid,pmc,arxiv,wikidata_qid,librarybase_id,url}, source_type, page_citation_count (n_citations), cross_page_count (gs.n_pages), match_type, confidence, reliability_score, verdict, first_seen, last_seen, n_revisions_cited, n_self_closing_uses, is_current`.
**Tables**: `page_source_summary` ⋈ `global_sources` ⋈ `page_metadata`. Pure Postgres. Order: `n_citations DESC, n_pages DESC` (UVA order).

##### `GET /api/v2/sources/{global_source_id}/pages?exclude_domain=&exclude_page_id=&limit=10&offset=0`
`global_source_id, source_label, total_pages (gs.n_pages)`, `pages[]`: `domain, page_id, page_title, page_namespace, url, citation_count (n_citations), n_revisions_cited, first_seen, last_seen, is_current`.
**Tables**: `page_source_summary` ⋈ `page_metadata` ⋈ `domains`, `global_sources`. Pure Postgres. Cross-domain by construction.

##### `GET /api/v2/sources/{global_source_id}` *(new; closes open item F4)*
One `global_sources` row with `n_pages, n_total_citations, reliability_score, verdict`, `source_key`, `run_id` (the run of a qualified id; `API_BASE_RUN_ID` for a bare one) and the canonical member's `normalized_sha1`/`part_index` (via `citation_source_resolution WHERE is_canonical`). Pure Postgres.

##### `GET /api/v2/sources?{doi|isbn|pmid|pmc|arxiv|wikidata|librarybase|url|key}=&limit=20&offset=0` *(2026-11-13)*
The durable way to refer to a source: global sources whose representative identifier equals the one given (exactly one parameter; `key` takes a `source_key`). Input is normalized to the stored forms (DOI lowercased and stripped of `doi:` / `https://doi.org/`; ISBN-10 and ISBN-13 both tried; `PMC` prefix dropped; Q-ids upper-cased). Response `{identifier, value, total, sources[]}` with the `/sources/{id}` fields. Indexes `idx_gs_*` (DOI and URL hash, `canonical_part_sha1` for `part:`).

---

#### Group 3: Revision & Citation Detail

##### `GET /api/v2/pages/{domain}/{page_id}/revisions?limit=100&offset=0`
`revisions[] {revision_id, revision_timestamp, parent_revision_id, citation_count}`, `total`. **Tables**: `revisions` (Postgres) for the page; `citation_history` (Parquet, one bucket) for counts. All revisions appear (RevisionChest writes every one).

##### `GET /api/v2/pages/{domain}/{page_id}/citations?revision_id=&raw=false&limit=100&offset=0`
Citations present at a revision, each identified by `raw_sha1` (with the page). Templates are `{template_name, parameters}`, links `{url}` (no row ids). `NormalizedCitationItem` gains: `citation_type`, `parts[] {part_index, part_type, norm_subtype, template_name, template_subtype, extracted_{doi,isbn,pmid,pmc,arxiv,wikidata_qid,url}, global_source_id, source_type, match_type, confidence, reliability_score, verdict}`.
**Tables**: `citation_history` (Parquet) → `citation_instances`, `normalized_citations`, `normalized_citation_parts`, `citation_source_resolution`, `global_sources`, `normalized_citation_web_resources`, `web_resources`, `wiki_templates` (Postgres); template parameters from Parquet `template_params` / `template_param_index` (plan §20).

##### `GET /api/v2/citations/{normalized_sha1}`
v1 `CitationDetailResponse` + `citation_type` + `parts[]` as above + `instances[] {domain, page_id, raw_sha1, reference_name, reference_type}`.

##### `GET /api/v2/citations/{normalized_sha1}/history?domain=&page_id=&limit=1000&offset=0`
v1 shape + `total` and `page_title`; refused (400) without `page_id` for citations on more than 25 pages. `citation_instances` (Postgres) → `citation_history` (Parquet) → `revisions` (Postgres).

##### `GET /api/v2/citations/{normalized_sha1}/pages?limit=100&offset=0` *(2026-11-13)*
Pages whose citations normalize to the text: `{domain, page_id, page_title, url, n_instances, reference_names}`, title order. Postgres only.

---

#### Group 4: Template & Web Resource Lookup

##### `GET /api/v2/templates/report?domain=&template=&parameter_key=&parameter_value=&limit=&offset=`
*(2026-11-13: was `/templates/{wiki_template_id}/report`.)* Templates are named: `template` is normalized like the extractor (underscores → spaces, first letter capitalized), the value compared trimmed. `wiki_templates.id` stays an internal join key for `template_value_counts`. The v1 alias `/api/v1/template/{id}/report` takes the **v1 database's** template id and translates it by name (`API_LEGACY_TEMPLATE_IDS`); v1 responses report that id as `wiki_template_id` (null when unknown).

##### `GET /api/v2/web-resources?url={url}`
v1 shape without `web_resource_id` (the URL is the key); each referencing citation gains `global_source_id`, `reliability_score`, `verdict` (via `normalized_citation_parts` → `citation_source_resolution` → `global_sources`; a citation with several parts may yield several).

---

### Endpoint → Table Mapping

| Endpoint | Postgres | Parquet (DuckDB) |
|---|---|---|
| `/article?url=` | `web_resources`, `domains`, `page_metadata`, `revisions` | — |
| `/pages/random/summary`, `/pages/{d}/{id}/summary` | `page_summary_cache`, `page_metadata` | — (7h rebuild: `revisions`, `citation_history`, `citation_instances_denorm`, `global_source_scores`) |
| `/pages/{d}/{id}/timeline` | `page_metadata` | `page_revision_stats` |
| `/pages/{d}/{id}/top-sources` | `page_source_summary`, `global_sources`, `page_metadata` | — |
| `/sources?doi=…` (lookup) | `global_sources` | — |
| `/sources/{id}/pages`, `/sources/{id}` | `page_source_summary`, `global_sources`, `page_metadata`, `domains`, `citation_source_resolution` | — |
| `/pages/{d}/{id}/revisions` | `revisions` | `citation_history` |
| `/pages/{d}/{id}/citations` | `citation_instances`, `normalized_citations`, `normalized_citation_parts`, `citation_source_resolution`, `global_sources`, `web_resources`, `normalized_citation_web_resources`, `wiki_templates` | `citation_history`, `template_params` |
| `/citations/{sha1}` | same minus history | — |
| `/citations/{sha1}/history` | `citation_instances`, `revisions` | `citation_history` |
| `/templates/report` | `wiki_templates` (name → internal id), `template_value_counts`, `normalized_citations` | `template_param_index` (one `search_bucket`); citations listed by `normalized_sha1` |
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