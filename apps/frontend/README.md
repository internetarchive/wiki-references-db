# apps/frontend — the article page and the report pages

Plain HTML, CSS and JavaScript, no build step and no third-party scripts.
`wcd-api` serves this directory at `/` (`/static/…` for the assets) whenever it
exists next to `apps/api`; any static file server works too, as long as
`/api/v2` is reachable on the same origin.

## The article page (`index.html`, `app.js`, `article.css`)

"Look up a Wikipedia article": paste an article URL (any tracked wiki), pick a
random article, or look up by domain and page ID. The page then shows, top to
bottom:

* **The article**: title, wiki, links to the article and to this report, the
  number of sources cited now and removed, and a strip of edits per year over
  the article's tracked history (from `/timeline`).
* **Sources on this article** (`/api/v2/pages/{d}/{id}/sources`), filtered by
  All / Current / Removed and sorted by time on page, uses, or recently added.
  Each source is a card:
  * its title (linked to its URL or identifier), its durable `source_key`
    (`doi:…`, `isbn:…`, …; a `part:` content hash when it has no standard
    identifier), author, year and publication, and badges for its kind and
    identifiers ("Not yet identified" when it has neither title nor
    identifier: the wikitext leads the card instead);
  * a bar of its time on the page, on the same timeline as the strip above:
    one segment per stretch on the page (absences under a week are merged),
    a mark where it was converted to short-form citations, a red mark where it
    was removed. Every segment and mark links to that revision's diff;
  * "Added … · removed … · restored … · still cited · N years on the page · N uses
    in the latest revision", each date a diff link (long churn is summarised);
  * every wikitext form it is cited in on the page: full citation,
    bibliography entry, short form (`{{sfn}}`), named reuse
    (`<ref name="…" />`), and earlier forms (struck through, the rest folded),
    each with its dates and uses, links to the revisions and pages with that
    wikitext, and its template parameters with **Find matches** links;
  * other articles citing the source.

  Removed sources follow in their own section, dashed, with their last
  wikitext.
* **Article metrics**: references over time (an inline SVG from `/timeline`),
  the largest changes (diff links), citation types and templates in the latest
  revision, and source kinds (from `/summary`).

Links carry `?domain=…&page_id=…` (plus `status`/`sort` when not the defaults);
`#domain/page_id` and `?url=<article URL>` (where old `/explorer/article?url=`
links redirect) are accepted too.

## Report pages (`reports.html`, `reports.js`)

Served by `wcd-api` at these paths; the script routes on the path:

* `/citations/{sha1}?domain=&page_id=`: every revision containing the citation,
  on one page or (for citations on ≤ 25 pages) every page. Was
  `/explorer/citation/{sha1}/report`.
* `/citations/{sha1}/pages?domain=&page_id=`: the pages citing it, by title,
  the current page highlighted. Was `/explorer/citation/{sha1}/other-articles`.
* `/sources/{global_source_id}/pages?domain=&page_id=`: the pages citing a
  source, most citations first (the page you came from is left out).
* `/templates/report?domain=&template=&parameter_key=&parameter_value=`:
  citations whose template (by name) has that parameter value. Was
  `/explorer/template/{id}/report`.
* `/templates/report?legacy_id=…`: landing page for an old template link whose
  ID could not be translated.

The report pages keep the earlier stylesheet (`styles.css`).
