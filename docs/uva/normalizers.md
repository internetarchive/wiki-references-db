# `reftag_normalizer.py` - Workflow

## Purpose

Transforms raw `ref_tag` citation strings from `citations_extracted` into a structured lookup table (`ref_tag_normalized`). Operates on **unique content strings only**. The output table is keyed on `content_sha1` and joined back to `citations_extracted` later.

---

## Inputs and Outputs

**Input:** `citations_extracted` table in DuckDB, one row per citation per revision. The script reads only rows where `citation_type = 'ref_tag'` and `content IS NOT NULL`.

**Outputs:**

- `ref_tag_normalized` : one row per unique ref_tag content string, with structural classification, extracted fields, and identifier metadata
- `ref_name_links` : one row per `(page_id, ref_name)` pair, mapping named references to their canonical `content_sha1`

---

## Step-by-Step Workflow

### Step 1: Load unique content strings

```
citations_extracted
  → SELECT DISTINCT content WHERE citation_type = 'ref_tag'
```

Deduplication happens at the SQL level. The same citation string appearing across thousands of revisions is processed exactly once.

---

### Step 2: Classify each string (Python, batched)

Each unique string passes through a **classifier chain** in priority order. The first classifier that matches wins; unmatched strings fall through to `prose_citation`.

| Priority | Classifier | Matches |
|---|---|---|
| 1 | `_classify_noise` | Too short, wiki table markup, non-citation content |
| 2 | `_classify_wikilink` | `[[#anchor\|Label]], p. N` internal bibliography links |
| 3 | `_classify_bare_url` | `[http://... Label]` external links |
| 4 | `_classify_shorthand_template` | `{{harvnb}}`, `{{sfn}}`, `{{r}}`, `{{rp}}` |
| 5 | `_classify_author_date`       | `Smith (2003) p. 45` shorthand prose |
| 6 | `_classify_italic_citation`   | `Author, ''Title'', Publisher, Year` italic-formatted prose citations |
| 7 | `_classify_cite_html`         | VisualEditor `<cite class="citation...">` entries |
| 8 | `_classify_prose`             | Everything else, i.e. free-form prose citations |

Each classifier returns a dict of extracted fields:
`norm_subtype`, `anchor_key`, `anchor_label`, `url_anchor_text`,
`author_key`, `year`, `year_reliable`, `page_ref`, `title`.

---

### Step 3: Content normalization (`normalize_content`)

Runs **before** classification to produce `norm_content`: the cleaned, fingerprint-ready version of the string used for deduplication matching.

Applied in this order:

1. **Error template stripping**: removes `{{Citation not found}}`,
   `{{Dead link}}`, `{{Citation needed}}` etc. that editors append to
   citations and that would otherwise prevent two otherwise identical strings
   from matching
2. **HTML comment stripping**: removes `<!-- ... -->`
3. **HTML entity decoding**: `&ndash;` → `–`, `&nbsp;` → ` ` etc.
4. **Wikilink collapsing**: `[[Target|Display]]` → `Display`
5. **Italic/bold marker stripping**: removes `''` and `'''`
6. **HTML tag stripping**: removes `<ref>`, `<span>` etc.
7. **Whitespace collapsing**: multiple spaces into single space
8. **Typographic normalization**: standardizes page ref spacing
   (`p. 20` → `p.20`), normalizes dashes in ranges (`239–240` → `239-240`),
   strips trailing punctuation

---

### Step 4: Year extraction (`_extract_year`, `_year_reliable`)

Each string gets two year-related fields:

**`year`**: extracted using a priority hierarchy:
1. Reliable pattern (Author (YYYY), standalone (YYYY), harvnb param,
   month-prefixed year, ISO date) → returns the year from that pattern
2. Bare scan fallback → any 4-digit year in range 1500–2029

This ensures citations like `Allan Nevins, ''Prologue to Civil War, 1859-60'' (1950)` extracts `year=1950` (from the parenthesized publication year) rather than `1859` (from the title subject matter).

**`year_reliable`**: boolean flag. `True` if the extracted year came from a reliable pattern (parenthesized, harvnb param, author-date format). `False` if it came from a bare scan that may have hit a subject-matter year in the title. The matching pipeline uses this flag as a guard, at least one citation in each comparison pair must have `year_reliable = True` before grouping on year.

---

### Step 5: Identifier extraction (`_extract_identifiers`)

Runs **independently** of structural classification on every string, regardless of subtype. Assigns an `id_tier` and populates identifier fields.

| Tier | Label | Signal |
|---|---|---|
| 1 | `template_wrapped` | `{{cite}}` template params: url, doi, isbn, pmid, pmc |
| 2 | `url_ref` | `http(s)://` URL regex scan |
| 3 | `isbn_ref` | `ISBN` prefix or `[[Special:BookSources/...]]` wikilink |
| 4 | `doi_ref` | `10.xxxx/` pattern or `[[doi:...]]` wikilink |
| 5 | `pmid_pmc_ref` | `PMID`/`PMC` prefix + digits |
| 0 | `no_identifier` | Nothing found — deferred to ML/fuzzy matching |

The wikilink-encoded forms (`Special:BookSources`, `[[doi:...]]`) are handled explicitly because VisualEditor renders `<cite>` HTML with these patterns and they are invisible to plain-text regex.

---

### Step 6: Write to DuckDB

Results are written via a **SQL CTE chain** rather than a direct DataFrame insert. This assigns `normalization_rule` in SQL based on `norm_subtype`, keeping the mapping logic in the database rather than Python.

In `append` mode, only content SHA1s not already present in the table are inserted, making the script safe to re-run on new shards without duplicating existing rows.

---

### Step 7: Build `ref_name_links`

After the normalized table is written, a second SQL query builds the `ref_name_links` table. For each `(page_id, ref_name)` pair, it finds the earliest revision where that named ref appeared with content and records the `content_sha1` of that citation.

This enables the matching pipeline to resolve self-closing refs (`<ref name="X"/>`) back to their defining citation, which the extractor has already handled, but the links table provides a fast lookup path for downstream.

---

## Key Design Decisions

**Unique strings only.** Processing unique strings instead of raw rows makes the classification step fast and the output table compact. All revision-level information (revision count, first/last seen) is computed during matching, not here.

**`year_reliable` flag instead of discarding unreliable years.** The bare scan is kept as a fallback since it often produces the correct year. The flag lets the matching pipeline make fine-grained decisions about when to trust the extracted year rather than discarding potentially useful data.

**Error template stripping before normalization.** `{{Citation not found}}` and similar maintenance templates appended by editors must be stripped in pass 0, before any other processing, so that `{{harvnb|Donald|1997|pp=598}}` and `{{harvnb|Donald|1997|pp=598}}{{Citation not found}}` produce the same `norm_content` fingerprint and get matched together.

**Identifier extraction independent of subtype.** A `prose_citation` string may contain an ISBN or DOI embedded in its text. Running identifier extraction on all strings regardless of subtype ensures these are captured and exposed to the matching pipeline's hard-identifier signals.

---

# `template_normalizer.py` - Workflow

## Purpose

Transforms raw template citation strings from `citations_extracted` into a structured lookup table (`template_normalized`). Unlike ref_tags which are free-form text, templates already contain structured params JSON parsed by `mwparserfromhell` at extraction time. This script normalizes those params into typed, cleaned columns with consistent field names regardless of which template name or param aliases the editor used.

---

## Inputs and Outputs

**Input:** `citations_extracted` table in DuckDB, one row per citation per revision. The script reads only rows where `citation_type = 'template'` and `raw IS NOT NULL`.

**Outputs:**

- `template_normalized` : one row per unique template string, with subtype classification, extracted bibliographic fields, and identifier metadata
- `template_ref_name_links` : one row per `(page_id, ref_name)` pair, mapping named template references to their canonical `content_sha1`

---

## Step-by-Step Workflow

### Step 1: Load unique template strings

```
citations_extracted
  → SELECT DISTINCT raw WHERE citation_type = 'template'
```

Deduplication is done in SQL using `GROUP BY raw`. The query also retrieves `ANY_VALUE(template_name)` and `ANY_VALUE(params)`, the pre-parsed JSON params from the extractor.

---

### Step 2: Classify each template into a subtype

Each template is classified using `_classify_subtype()`, which checks the template name first and falls back to param-based detection:

| Subtype | Matched by |
|---|---|
| `cite_news` | Template name matches `cite news`, `cite newspaper` |
| `cite_web` | Template name matches `cite web`, `cite podcast`, `cite tweet` |
| `cite_journal` | Template name matches `cite journal`, `cite magazine`; or `journal=` param present |
| `cite_book` | Template name matches `cite book`, `cite encyclopaedia`, `cite thesis`; or `isbn=` param present |
| `cite_other` | Everything else. `{{citation}}`, custom templates, etc. |

The name check takes priority over the param check. This handles cases where an editor uses `{{cite web}}` without a URL (which would fail param detection) or `{{citation}}` with an ISBN (which would be misclassified without the name check).

---

### Step 3: Extract bibliographic fields (`extract_template`)

Each template's `params` JSON is unpacked and normalized into clean typed fields. Each field is resolved by checking a list of known aliases in order. The first non-empty value wins.

**Author fields:**

`author_last` checks: `last`, `last1`, `surname`, `surname1`, `author`, `author1`, `authors`, `last-first`, `author-last`

If none of those yield a value and `author=` contains a comma, the value is split on the first comma: `"Smith, John"` to `author_last=Smith`, `author_first=John`. If no comma is present, the last word of the author string is taken as the surname.

`author_key` is `author_last.lower()`. The lowercase surname used as the grouping key in matching.

**Title:** checks `title`, `chapter`, `article`, `entry`, `contribution`

**Journal:** checks `journal`, `periodical`, `newspaper`, `magazine`, `work`, `website`. For non-journal subtypes, the `publisher` value is prevented from bleeding into the journal field.

**Year:** scans `year`, `date`, `publication-date`, `pubdate` in order and extracts the first 4-digit year found. The raw `date` string is also stored separately for traceability.

**Publisher / Location / Edition / Volume / Issue / Pages:** each resolved from their respective alias lists.

---

### Step 4: Extract identifiers

Runs on every template regardless of subtype. Normalizes each identifier type:

**URL**: checks `url`, `archive-url`, `archiveurl`, `chapter-url`, `chapterurl`, `contribution-url`. Normalizes `http://` to `https://`. Flags archive URLs (`web.archive.org`, `archive.ph` etc.) with `is_archive_url = True`.

**ISBN**: checks `isbn`, `ISBN`, `isbn13`, `isbn10`, and the generic `id=` field. Strips hyphens, spaces, and the `ISBN` prefix. Validates as 10 or 13 digit string.

**DOI**: checks `doi`, `DOI`, and `id=`. Strips `doi:` prefix and `https://doi.org/` prefix. Validates that the result starts with `10.`.

**PMID / PMC**: checks `pmid`/`PMID` and `pmc`/`PMC` respectively.

**`id_tier`**: set to `1` (`template_wrapped`) if any identifier was found, `0` (`no_identifier`) otherwise. This mirrors the ref_tag normalizer's tier system so both tables can be joined uniformly in the matching pipeline.

---

### Step 5: Write to DuckDB

Results are written as a flat DataFrame. In `append` mode, only content SHA1s not already present in the table are inserted, making the script safe to re-run on new shards without duplicating existing rows.

Unlike the ref_tag normalizer, there is no SQL CTE post-processing step. `normalization_rule` is assigned directly in Python during `extract_template()` based on the subtype (`book_params_extracted`, `journal_params_extracted`, `web_params_extracted`, `news_params_extracted`, `template_params_extracted`).

---

### Step 6: Build `template_ref_name_links`

After the normalized table is written, a second SQL query builds `template_ref_name_links`. For each `(page_id, ref_name)` pair, it finds the earliest revision where that named ref appeared with content and records the `content_sha1` of that template citation.

This mirrors `ref_name_links` for the ref_tag side and enables the matching pipeline to resolve self-closing template refs back to their defining citation.

---

## Key Design Decisions

**Templates use `raw` not `content`.** In `citations_extracted`, ref_tags store their text in `content` and templates store their wikitext in `raw`. The normalizer explicitly reads `raw` as the unique key and join path.

**Params are pre-parsed.** The extractor already ran `mwparserfromhell` and stored the result as a JSON dict in `params`. The normalizer doesn't parse wikitext, it only normalizes the already-extracted param values. This is in contrast to the ref_tag normalizer which runs `mwparserfromhell` live on shorthand templates.

**Param alias resolution.** Wikipedia editors are inconsistent, some use `last=`, some use `surname=`, some use `author=`. The alias lists ensure the same field is found regardless of which convention the editor followed. The alias ordering reflects frequency of use across Wikipedia.

**`author=` splitting.** The `author=` param sometimes contains a full name like `"Smith, John"` rather than using separate `last=` and `first=` params. The comma-split handles this without requiring the editor to have used the canonical param names.

**`id_tier` mirrors reftag normalizer.** Both tables use the same tier system so the matching pipeline can treat ref_tag and template identifiers uniformly in the cross-type merge step. Templates are always at least Tier 1 since the extractor only creates template rows for structured `{{cite}}` calls which typically have at least one identifier.

**`italicized_citation` title extraction.** Citations matching the pattern `Author, ''Title'', Publisher, Year` have their title extracted directlfrom the italic-delimited segment. The extracted title is stored in the `title` column and is used downstream in cross-type and cross-page fuzzy matching as a stronger signal than full content comparison. All other subtypes leave `title` as NULL. Title extraction for general prose citations requires more research. 