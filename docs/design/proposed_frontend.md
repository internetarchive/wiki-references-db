# Proposed Frontend Adaptation

*Revised 2026-09-10 per `claude/decisions_2026-09-10.md`.*

## Overview

The UVA analytics frontend (`src/analytics_service/frontend/`: `index.html` 229 lines, `app.js` 1,070 lines, `styles.css`) is a single-page dashboard making four API calls. Frontend changes are mechanical; the substantive work is the backend query layer (`page_summary.py`, 990 lines), rewritten against the unified model (`proposed_api.md`, `proposed_cache.md`). UVA's `main.py` response models (`PageSummaryResponse`, `TimelinePointResponse`, `TopSourceResponse`, …) are kept and extended, so the JSON shape the frontend sees is UVA's plus new fields.

---

## 1. API base and domain-qualified paths

`app.js` calls bare paths. V2 paths carry `/api/v2` and the domain:

```javascript
const API_BASE = "/api/v2";
let currentDomain = "en.wikipedia.org";

const pagePath = (domain, pageId, tail) =>
  `${API_BASE}/pages/${encodeURIComponent(domain)}/${encodeURIComponent(pageId)}/${tail}`;
```

Call sites (six): `loadPage` internals (~1030 summary, ~1032 timeline, ~1033 top-sources), `formatRelatedPageFacts`' source-usage fetch (~402), and the three `loadPage` callers (~1056, ~1060, ~1068). `/pages/random/summary` becomes `${API_BASE}/pages/random/summary` (optionally `?domain=`).

The page-id input gains a domain field (default `en.wikipedia.org`); the URL hash carries `domain/page_id` so links are shareable.

---

## 2. Site URLs from `domain`

`page_id` is the site-local id, so the existing helpers only need the domain substituted:

```javascript
function formatWikipediaUrlFromPageId(pageId) {
  return `https://${currentDomain}/?curid=${encodeURIComponent(pageId)}`;
}
function formatRevisionUrl(revId) {
  return `https://${currentDomain}/w/index.php?oldid=${encodeURIComponent(revId)}`;
}
function formatRevisionDiffUrl(fromRevId, toRevId) {
  if (!fromRevId) return formatRevisionUrl(toRevId);
  return `https://${currentDomain}/w/index.php?title=Special:Diff&diff=${encodeURIComponent(toRevId)}&oldid=${encodeURIComponent(fromRevId)}`;
}
```

Set `currentDomain = summary.domain` in `renderPage()`. Related-page links in the source-usage list use each page's own `domain` (the endpoint is cross-domain).

---

## 3. Response Shape Adaptation

**3a. `n_revisions_cited`** on source-usage pages: returned by v2; no change.

**3b. `domain` on all responses**: consumed by §1–2.

**3c. `source_label`** on source-usage response: available, unused.

**3d. Timeline — `named_ref_count` semantics change.** UVA's `named_ref_count` was "citations with a ref name". V2 serves `n_self_closing` (reuse events) under the same key. Change the three UI strings that say "named refs" (inspector summary ~176, peak stat label, hover tooltip ~823) to "reuses", and rename the stat element (`timelinePeakNamedRefs` → `timelinePeakReuses`). If the old metric is wanted, it can be added to `page_revision_stats` later as `n_named`.

**3e. Timeline — `high_confidence_count` (new).** Render as an additional series or a tooltip line ("N high-confidence sources").

**3f. Type breakdown** now has six kinds; `formatCitationTypeBreakdownSummary` already iterates whatever it receives. Add display labels for `endnote`, `shorthand`, `external_link`.

---

## 4. Branding

| File | Line | Current | Updated |
|---|---|---|---|
| `index.html` | 6 | `<title>Wikipedia Citation Analytics</title>` | `<title>Wikipedia Citations Database</title>` |
| `index.html` | 15 | eyebrow `UVA Wikipedia Citations Database` | `Wikipedia Citations Database` |
| `index.html` | 16–19 | h1 / hero copy | keep h1; remove "UVA" from copy |

Backend error strings that name UVA tables (`citations_extracted`, `ref_tag_normalized`, `template_normalized`, `wikipedia_pages`) are replaced by unified names (`citation_instances_denorm`, `normalized_citation_parts`, `page_metadata`).

---

## 5. Identifier Badges and Links

Replace the `if / else if` chain (lines 473–488) and the link list (496–523) with data-driven loops over the canonical set:

```javascript
const IDENTIFIERS = [
  { field: "extracted_doi",          label: "DOI",        href: v => `https://doi.org/${encodeURIComponent(v)}` },
  { field: "extracted_isbn",         label: "ISBN",       href: v => `https://openlibrary.org/search?isbn=${encodeURIComponent(v)}` },
  { field: "extracted_pmid",         label: "PMID",       href: v => `https://pubmed.ncbi.nlm.nih.gov/${encodeURIComponent(v)}/` },
  { field: "extracted_pmc",          label: "PMC",        href: v => `https://www.ncbi.nlm.nih.gov/pmc/articles/PMC${encodeURIComponent(v)}/` },
  { field: "extracted_arxiv",        label: "arXiv",      href: v => `https://arxiv.org/abs/${encodeURIComponent(v)}` },
  { field: "extracted_wikidata_qid", label: "Wikidata",   href: v => `https://www.wikidata.org/wiki/${encodeURIComponent(v)}` },
  { field: "extracted_librarybase_id", label: "LibraryBase", href: null },
  { field: "extracted_url",          label: "Link",       href: v => v },
];
```

Badges: one per present identifier (plus the existing verdict badge). Links: `createIdentifierLink(label, value, href ? href(value) : null)` — it already renders a `<span>` when `href` is null. `extracted_pmc` is digits-only (UVA normalizer), hence the `PMC` prefix in the URL.

---

## 6. Backend Query Layer Rewrite

### Table mapping

| UVA query target | Unified | Notes |
|---|---|---|
| `citations_extracted` (summary/timeline aggregates) | `page_summary_cache` (serving) / `page_revision_stats` (timeline) | live aggregation replaced by materialized data |
| `ref_tag_normalized`, `template_normalized` | `normalized_citation_parts` | |
| `wikipedia_pages` | `page_metadata` | keyed `(domain_id, page_id)` |
| `page_source_summary` | `page_source_summary` | + `domain_id`; reliability/verdict via `global_sources` |
| `global_sources` | `global_sources` | |
| `global_source_map` ⋈ `merged_sources` (source_type, page_citation_count) | `page_source_summary.source_type`, `.n_citations` | the 3-hop chain is gone |

### Endpoint → query

- **summary**: one Postgres lookup on `page_summary_cache` ⋈ `page_metadata`. Rebuild path is Stage 7h.
- **timeline**: one DuckDB read of `page_revision_stats` for `domain=…/page_bucket=…` filtered by `page_id`; each row is a point.
- **top-sources**: `page_source_summary` ⋈ `global_sources` ⋈ `page_metadata`, ordered `n_citations DESC, n_pages DESC`.
- **source pages**: `page_source_summary` ⋈ `page_metadata` ⋈ `domains` filtered by `global_source_id`, excluding `(exclude_domain, exclude_page_id)`.

### Engine

Postgres (psycopg / SQLAlchemy) for the three Postgres-only endpoints; the DuckDB pool (`proposed_cache.md` §3) for the timeline and Group 3.

---

## 7. Implementation Sequence

1. Backend: port UVA `main.py` response models; rewrite `page_summary.py` handlers per §6; keep UVA's frontend as the test harness.
2. `API_BASE` + domain-qualified paths; domain input; hash routing.
3. URL helpers on `currentDomain`.
4. Branding.
5. Identifier badges/links.
6. Timeline: "reuses" relabel; `high_confidence_count` series; six-kind breakdown labels.
7. Smoke test against the v2 backend.

---

## Summary of Changes by File

| File | Change | Effort |
|---|---|---|
| `app.js` | `API_BASE`, `pagePath`, domain input/hash, six call sites | Low |
| `app.js` | URL helpers on `currentDomain` | Low |
| `app.js` | data-driven identifier badges/links | Low |
| `app.js` | timeline relabel + `high_confidence_count` + six-kind labels | Low |
| `index.html` | branding, domain field | Low |
| `main.py` | models extended (`domain`, `url`, new identifier fields, `high_confidence_count`, `parts`); error strings | Low |
| `page_summary.py` | full rewrite of the query layer | **High** |