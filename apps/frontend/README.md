# apps/frontend — the dashboard

The page-level citation analytics dashboard: the UVA analytics frontend
(`UVAWikipediaCitationsDatabase/src/analytics_service/frontend`, commit
`29f3ae3`) adapted to the `/api/v2` service per
`docs/design/proposed_frontend.md`. Plain HTML, CSS and JavaScript — no build
step. `wcd-api` serves this directory at `/` (`/static/…` for the assets)
whenever it exists next to `apps/api`; any static file server works too, as
long as `/api/v2` is reachable on the same origin.

Changes from the UVA original:

* `API_BASE = /api/v2`; every page call is domain-qualified
  (`/pages/{domain}/{page_id}/…`); the form has a wiki-domain field and the
  page field accepts a page ID **or an article URL** (resolved through
  `/api/v2/article`). Links carry `?domain=…&page_id=…`; `#domain/page_id`
  is accepted too.
* Site links (page, revision, diff) are built from the loaded page's `domain`;
  related-page links use each page's own domain (the source-pages endpoint is
  cross-domain).
* Branding: Wikipedia Citations Database; no engine names in the copy.
* Identifier badges and links are data-driven over the canonical set (DOI,
  ISBN, PMID, PMC, arXiv, Wikidata, LibraryBase, URL).
* Timeline: "named refs" became "reuses" (`named_ref_count` now counts
  self-closing `<ref name=…/>` reuse events), `high_confidence_count` is a
  second (dashed) series, a tooltip line and a highlight stat; the six
  citation types have display labels.
* Errors follow the v2 `{"error", "code"}` shape (FastAPI's `detail` is still
  understood).

Ported from the v1 Citation Explorer (`legacy/explorer.py`):

* **Citations section** on the dashboard: the citations at the latest revision (or the
  revision picked with "Citations at this revision" in the timeline inspector), 25 at a
  time, with badges, the normalized text, first/last seen, extracted links and template
  parameters. Each card links to the report pages.
* **Report pages** (`reports.html` + `reports.js`, served by `wcd-api` at the paths
  below; the script routes on the path):
  * `/citations/{sha1}?domain=&page_id=` — every revision containing the citation, on
    one page or (for citations on ≤ 25 pages) every page. Was `/explorer/citation/{sha1}/report`.
  * `/citations/{sha1}/pages?domain=&page_id=` — the pages citing it, by title, the
    current page highlighted. Was `/explorer/citation/{sha1}/other-articles`.
  * `/templates/report?domain=&template=&parameter_key=&parameter_value=` — citations whose
    template (by name) has that parameter value ("Find matches" on a parameter). Was
    `/explorer/template/{id}/report`.
  * `/templates/report?legacy_id=…` — landing page for an old template link whose id
    could not be translated.
* `?url=<article URL>` loads a page by URL (old `/explorer/article?url=` links redirect here).

The explorer's revision slider is not ported; the timeline plays that role.

ECharts is loaded from jsDelivr as before; without it the page degrades to the
textual timeline inspector and highlights.
