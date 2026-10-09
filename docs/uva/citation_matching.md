# `ref_tag_matching.py` - Workflow

## Purpose

Groups unique ref_tag citation strings on a page into **original sources** i.e. citations that refer to the same real-world work. Operates on the output of `reftag_normalizer.py` and writes two tables: `sources` (one row per unique source per page) and `citation_source_map` (one row per citation string mapping it to its source).

---

## Inputs and Outputs

**Input:** `citations_extracted`, `ref_tag_normalized`.

**Outputs:**

- `sources` : one row per unique source per page, with canonical citation fields and match metadata
- `citation_source_map` : maps each `content_sha1` to a `source_id` with `match_type` and `confidence`

---

## Step-by-Step Workflow

### Step 1: Load page citations (`load_page_citations`)

Joins `citations_extracted` to `ref_tag_normalized` for a single page, grouping by unique `content_sha1` to count revisions.

---

### Step 2: Union-find initialization

A `UnionFind` data structure is initialized with all `content_sha1` values for the page. Each citation starts as its own component. Signals are applied in confidence order, each one unions citations that share a common value, collapsing them into the same component.

---

### Step 3: Signal passes

**Pass 1: Hard identifiers (exact match)**

| Signal | Column | Confidence |
|---|---|---|
| `doi` | `extracted_doi` | 1.00 |
| `isbn` | `extracted_isbn` | 0.95 |
| `pmid` | `extracted_pmid` | 0.95 |
| `ref_name` | `ref_name` | 0.90 |
| `url` | `extracted_url` | 0.85 |
| `anchor_key` | `anchor_key` | 0.80 |
| `norm_content` | `norm_content` | 0.65 |

For each signal, citations sharing the same value are unioned together. This is a simple groupby.

**Pass 2 : cite_isbn (confidence 0.88)**

Connects `cite_html` VisualEditor bibliography entries (which have ISBNs) to their corresponding inline shorthand citations (which share the same `author_key` and `year` but have no identifier). Without this signal, a `<cite class="citation book">Belz (1998)...ISBN 9780823217687</cite>` and `Belz (1998), p. 86.` would remain separate sources even though they likely refer to the same book.

**Pass 3: author+year+title (normalizer fields, confidence 0.60)**

Groups citations sharing `(author_key, year)` where their content scores ≥ 70 on `token_sort_ratio`. When both citations have an extracted title (from the `italicized_citation` classifier), title-to-title comparison is used at a tighter threshold of 85 rather than comparing full `norm_content` strings. This reduces false positives for prose citations where the full content strings differ substantially in length and structure but the titles match cleanly. Two guards apply:

- **Title-first comparison**: if both sources have `title` populated, `token_sort_ratio` on normalized titles must meet threshold 85 before falling back to `norm_content` at threshold 70
- **Year reliability guard**: at least one citation in each pair must have `year_reliable=True`

---

### Step 4: Build sources from components

After all signal passes, `uf.components()` returns the final groupings. For each component:

**Canonical citation selection** : `_richness()` ranks citations by identifier quality: DOI > ISBN > PMID > URL > none, with revision count as a tiebreaker within each tier. The richest citation becomes the canonical and its fields are stored as the source's representative metadata.

**Source ID** : deterministic 16-character SHA1 hash of `page_id:root_sha1`.

**Match type and confidence** : taken from the highest-confidence signal that fired for this component's root node. Singletons (components of size 1) get `match_type='singleton'` and `confidence=None`.

---

### Step 5: Fuzzy singleton pass (`fuzzy_cluster`, `merge_fuzzy`)

After the union-find pass, remaining singletons are run through greedy fuzzy clustering on `norm_content`:

1. **Strip page refs** : remove `p. N`, `pp. N-M`, `vol. N`, `ch. N` before comparison so `Donald (1995) p. 21` and `Donald (1995) pp. 55-58` are seen as the same citation
2. **Year-mismatch guard** : two citations with incompatible years are never compared, preventing `Donald (1995)` from matching `Donald (1992)`
3. **Greedy clustering** : each citation is assigned to the first existing cluster where the best member scores above `threshold` (default 88)
4. **Confidence scaling** : score 88 -> confidence 0.60, score 100 -> 0.70

Matched clusters are merged back into `sources` and `citation_source_map`,
replacing the individual singleton rows.

---

### Step 6: Write to DuckDB

`sources` and `citation_source_map` are written with `DELETE WHERE page_id` then INSERT, making the operation idempotent. Re-running for a page safely replaces existing results.

---

## Key Design Decisions

**Union-find over iterative grouping.** Multiple signals may connect the same citations via different paths, DOI connects A to B, URL connects B to C, so A, B, C should all be in the same source. Union-find handles transitive closure automatically. A naive "group by value" approach would require multiple passes and miss transitive connections.

**Signals applied in confidence order, highest-confidence match wins.** The `edge_info` dict on each union-find root tracks only the highest-confidence signal that contributed to that component. If DOI connects two citations, the source is labeled `doi` regardless of whether a URL signal also fired, the strongest evidence determines the label.

**`year_reliable` guard prevents false positives from title years.** Prose citations frequently contain years that refer to historical events rather than publication dates — `''The Prologue to Civil War, 1859-60'' (1950)` has year=1950 in parentheses (reliable) and year=1859 in the title (unreliable). Without the guard, `1859` would incorrectly group this with Civil War-era citations.

**Richness-first canonical selection.** A citation that appeared in 800 revisions as `Donald (1995)` would previously win over a structured citation with an ISBN that appeared only twice. Now the richest identifier wins, the canonical citation is the one that carries the most information for downstream use.

**Fuzzy pass only on singletons.** Running fuzzy comparison on all citation strings would be too slow. Running it only on the singletons per page (after hard signal matching) keeps the fuzzy pass fast while still catching informal citations that couldn't be matched by any structured signal.

# `template_matching.py` - Workflow

## Purpose

Groups unique template citation strings on a page into **original sources**  i.e. citations that refer to the same real-world work. Operates on the output of `template_normalizer.py` and mirrors the approach of `ref_tag_matching.py`, but adapted for templates which have structured params (author, title, year, DOI, ISBN etc.) rather than free-form text.

---

## Inputs and Outputs

**Input:** `citations_extracted`, `template_normalized`.

**Outputs:**

- `template_sources` : one row per unique source per page, with canonical citation fields and match metadata
- `template_citation_map` : maps each `content_sha1` to a `source_id` with `match_type` and `confidence`

---

## Step-by-Step Workflow

### Step 1: Load page templates (`load_page_templates`)

Joins `citations_extracted` to `template_normalized` for a single page using the raw wikitext column as the join key, unlike ref_tags which store their text in `content`, templates store their full wikitext in `raw`.

Groups by unique `content_sha1` to count the number of revisions each template citation appeared in.

---

### Step 2: Union-find initialization

A `UnionFind` data structure is initialized with all `content_sha1` values for the page. Each template citation starts as its own component. Signal passes are applied in confidence order, collapsing citations that share common values into the same component.

---

### Step 3: Signal passes

**Pass 1: Hard identifiers (exact match)**

| Signal | Column | Confidence |
|---|---|---|
| `doi` | `extracted_doi` | 1.00 |
| `isbn` | `extracted_isbn` | 0.95 |
| `pmid` | `extracted_pmid` | 0.95 |
| `ref_name` | `ref_name` | 0.90 |
| `url` | `extracted_url` | 0.85 |

For each signal, citations sharing the same value are unioned together. The vast majority of templates (~95%) have at least one hard identifier, so most grouping happens here.

**Pass 2: title_year (confidence 0.70)**

Groups citations sharing `(normalized_title, year)`. Title normalization strips punctuation and lowercases before comparison so `"Prologue to Civil War"` and `"Prologue to Civil War."` are treated as the same title. Only titles of 5 or more characters after normalization are compared.

This signal has higher confidence than the equivalent fuzzy pass in `ref_tag_matching.py` because template title params are already structured and editor-verified, there is no need for fuzzy matching. An exact normalized title match is sufficient.

---

### Step 4: Build sources from components

After all signal passes, `uf.components()` returns the final groupings. For each component:

**Canonical citation selection** : `_richness()` ranks citations by identifier quality: DOI > ISBN > PMID > URL > none, with revision count as a tiebreaker within each tier. The richest citation becomes the canonical and its fields are stored as the source's representative metadata.

**Source ID** : deterministic 16-character SHA1 hash of `page_id:tmpl:root_sha1`. The `tmpl:` prefix distinguishes template source IDs from ref_tag source IDs, which is necessary since both live in `merged_sources` after the cross-type merger.

**Match type and confidence** : taken from the highest-confidence signal that fired for this component's root node. Singletons get `match_type='singleton'` and `confidence=None`.

---

### Step 5: Write to DuckDB

`template_sources` and `template_citation_map` are written with `DELETE WHERE page_id` then INSERT, making the operation idempotent. Re-running for a page safely replaces existing results.

---

## Key Design Decisions

**`raw` not `content` as the join key.** Templates store their full wikitext in `raw` rather than `content`. This is a consequence of the extraction design as `mwparserfromhell` parses the template from the raw wikitext and stores both the raw string and the structured params. The join must use `raw` to match back to `template_normalized`.

**No fuzzy pass.** Unlike `ref_tag_matching.py`, there is no greedy fuzzy singleton pass at the end. Templates have structured params so if two templates refer to the same work, they will almost certainly share a DOI, ISBN, URL, or at minimum a matching title+year. Fuzzy text comparison on template wikitext would match on template boilerplate rather than citation content.

**title_year at 0.70 confidence.** Title+year matching is higher confidence for templates than for ref_tags because template params are structured and editor-curated. A `{{cite book|title=X|year=2003}}` and `{{cite book|title=X|year=2003|pages=45}}` are almost certainly the same book.

**`tmpl:` prefix in source ID.** Template and ref_tag sources are merged in the cross-type merger step. The prefix ensures their SHA1-based source IDs never collide, since both are derived from `page_id` and a citation SHA1.

# `cross_type_merger.py` - Workflow

## Purpose

Merges ref_tag sources and template sources for a page into a single unified `merged_sources` table. Runs after both within-page matchers have completed.

The core problem: the same real-world book or article may be cited on a page both as a structured `{{cite book}}` template and as an informal ref_tag like `Donald (1995) p. 21`. The within-page matchers handle each type separately, so these two citations end up as separate sources even though they refer to the same work. This script resolves that.

---

## Inputs and Outputs

**Input:** `sources` (from `ref_tag_matching.py`) and `template_sources` (from `template_matching.py`) tables in DuckDB, both for a single page.

**Output:**

- `merged_sources` : one row per unified source per page, with `source_type` indicating whether it came from ref_tags only, templates only, or both sides merged together

---

## Step-by-Step Workflow

### Step 1: Load sources for the page (`load_sources`)

Loads both `sources` and `template_sources` for the page. The template query renames `author_last` to `author_key` to align with the ref_tag schema so both sides can be compared on a common field.

---

### Step 2: Union-find initialization

Keys for both source sets are prefixed to keep them distinct:

- `rt:<source_id>` : ref_tag source
- `tm:<source_id>` : template source

A `UnionFind` is initialized with all keys from both sides. Signal passes then union matching pairs across the two sets.

---

### Step 3: Signal passes

**Pass 1: Hard identifier signals (exact match)**

| Signal | Column | Confidence |
|---|---|---|
| `doi` | `extracted_doi` | 1.00 |
| `isbn` | `extracted_isbn` | 0.95 |
| `pmid` | `extracted_pmid` | 0.95 |
| `url` | `extracted_url` | 0.85 |

For each identifier, `_build_identifier_index()` maps identifier values to their prefixed source IDs on each side. Any rt and tm sources sharing the same identifier value are unioned together.

**Pass 2: author+year signal (confidence 0.65)**

Groups rt and tm sources that share `(author_key, year)` after lowercasing and stripping whitespace. Both sides must have both fields for this pass to fire.

This is intentionally the weakest signal and there is no fuzzy title comparison added on top of it. The cross-type merge is conservative by design. `Donald (1995) p. 21` and `{{cite book|last=Donald|year=1995}}` are a plausible match on author+year alone, but `Smith (2003)` matching `{{cite book|last=Smith|year=2003|title=Unrelated Book}}` would be a false positive. Adding fuzzy title would only partially guard against this and would introduce new failure modes.

---

### Step 4: Build merged source rows

After all signal passes, `uf.components()` returns the final groupings. 

For each component:

**source_type** : `'both'` if the component contains at least one rt and one tm source, `'ref_tag'` if all members are rt, `'template'` if all members are tm. Unmatched sources pass through as their original type.

**Canonical row selection** : `_id_priority()` picks the source with the best identifier (DOI > ISBN > PMID > URL > none). This determines the `canonical_sha1` stored on the merged source.

**Field propagation** : template rows are ordered first, then ref_tag rows. `_best_field()` returns the first non-null value across this ordered list for each field. This means the template side wins for structured metadata (title, author from params) while the ref_tag side fills in any fields the template is missing. The final merged source gets the best of both sides.

**Merged source ID** : deterministic 16-character SHA1 hash of `page_id:merged:` + sorted member keys.

---

### Step 5: Write to DuckDB

`merged_sources` is written with `DELETE WHERE page_id` then INSERT, making the operation idempotent. Re-running for a page safely replaces existing results.

---

## Key Design Decisions

**Conservative by design.** This merger only has five signals, none of which is fuzzy. The within-page matchers (ref_tag and template) already handle fuzzy matching within their own type. The cross-type merge's job is to connect sources that are genuinely the same work, not to speculatively group anything that looks similar. A missed cross-type merge is cheaper than a false positive that incorrectly collapses two different sources.

**Prefixed key space.** Using `rt:` and `tm:` prefixes lets both source types share a single `UnionFind` without key collisions, and makes it trivial to split components back into their constituent sides when building output rows.

**Field propagation rather than picking one side.** A ref_tag source matched to a template source often has complementary information and the template may have a DOI and title. `_best_field()` with template-first ordering means the merged source carries the richest available metadata from either side, rather than discarding half of what was found.

**`source_type='both'`** is the key output to track. Only 1–3% of sources on a typical page are cross-type matches, but each one represents a real citation that would otherwise appear twice in the global source count, once in the ref_tag graph and once in the template graph.

# `pipeline_runner.py` - Workflow

## Purpose

Orchestrates the full per-page citation matching pipeline across all pages in the database. Calls three matching scripts in sequence for each page and collects timing stats. This is the main entry point for the matching phase of the pipeline.

---

## Scripts Called

| Phase | Script | Function called |
|---|---|---|
| 1 | `ref_tag_matching.py` | `run_page(con, page_id)` |
| 2 | `template_matching.py` | `run_page(con, page_id)` |
| 3 | `cross_type_merger.py` | `run_page(con, page_id)` |

All three must be in the same directory or on the Python path.

---

## Inputs and Outputs

**Input:** `citations_extracted` table, used to discover which pages exist and how many citations each has.

**Outputs (per page, written by the called scripts):**

| Table | Written by |
|---|---|
| `sources` | `ref_tag_matching.py` |
| `citation_source_map` | `ref_tag_matching.py` |
| `template_sources` | `template_matching.py` |
| `template_citation_map` | `template_matching.py` |
| `merged_sources` | `cross_type_merger.py` |

---

## Step-by-Step Workflow

### Step 1: Page discovery

`get_all_page_ids()` queries `citations_extracted` for all distinct `page_id` values, ordered by citation count descending. The largest pages are processed first so that slow outliers (pages 307, 737) surface early rather than at the end of a long run. `--limit N` restricts to the N largest pages for testing.

### Step 2: Per-page matching loop

For each page, three phases run in sequence:

**Phase 1 : ref_tag matching** (`ref_tag_matching.run_page`) Loads unique ref_tag citations for the page, runs signal passes (doi, isbn, pmid, ref_name, url, anchor_key, norm_content, author_year_title), then fuzzy clusters remaining singletons. Writes `sources` and `citation_source_map`.

**Phase 2 : template matching** (`template_matching.run_page`) Same approach for template citations. Signals are doi, isbn, pmid, ref_name, url, title_year. Writes `template_sources` and `template_citation_map`.

**Phase 3 : cross-type merge** (`cross_type_merger.run_page`) Merges the ref_tag and template sources for the page into `merged_sources`. Signals are doi, isbn, pmid, url, author_year. Only runs if write mode is on and `cross_type_merger` was successfully imported.

Each phase can be skipped independently with `--reftag-only` or `--template-only` for debug purposes.

### Step 3: Progress reporting

Every `--log-every` pages (default: 10), prints a one-line summary showing page ID, citation and source counts for each type, per-phase timing, and estimated time remaining.

### Step 4: Final summary and benchmark

After all pages, prints totals for citations, sources, and CPU time by type. With `--benchmark`, also prints per-phase timing percentiles, match type breakdowns, and the 10 slowest pages.

### Step 5: Intermediate table pruning (optional)

With `--prune`, calls `prune_intermediate_tables()` after the run completes. Deletes rows from `ref_name_links`, `template_ref_name_links`, `citation_source_map`, and `template_citation_map` for all pages that have completed through `global_source_map` (the cross-page matching step). Pages not yet fully processed are unaffected.

---

## CLI Flags

| Flag | Description |
|---|---|
| `--db` | Path to DuckDB database (required) |
| `--page-id N` | Process a single page only |
| `--limit N` | Process only the N largest pages |
| `--dry-run` | Run matching without writing to DuckDB |
| `--benchmark` | Print detailed timing breakdown at the end |
| `--reftag-only` | Skip template matching (Phase 2 and 3) |
| `--template-only` | Skip ref-tag matching (Phase 1) |
| `--log-every N` | Print progress every N pages (default: 10) |
| `--prune` | Prune intermediate table rows after run |

---

## Key Design Decisions

**Pages processed largest-first.** Ordering by citation count descending means slow outlier pages fail or surface early. If a page causes an error or runs unexpectedly long, you find out in the first few minutes rather than at hour 3 of a multi-hour run.

**Three separate scripts, one orchestrator.** Each matching script (`ref_tag_matching`, `template_matching`, `cross_type_merger`) is independently runnable and testable on a single page. The runner's only job is to call them in the right order and collect stats. It contains no matching logic itself.

**`--prune` is opt-in, not automatic.** Pruning intermediate rows is safe but irreversible within a session. Making it opt-in means a failed or partial run never accidentally cleans up rows that are still needed for debugging.

**Write mode is all-or-nothing per run.** `--dry-run` skips all writes including Phase 3. This is intentional as a dry run that wrote merged_sources but not sources would leave the database in an inconsistent state.

# `cross_page_matcher.py` - Workflow

## Purpose

Deduplicates sources across all pages by assigning a single `global_source_id` to each unique real-world work cited anywhere in the dataset.

After `pipeline_runner.py` runs, the same book cited on 500 different Wikipedia pages exists as 500 separate rows in `merged_sources`, one per page. This script collapses those 500 rows into a single `global_sources` row and records the mapping in `global_source_map`. It is the first step that operates across the entire dataset rather than page by page.

---

## Inputs and Outputs

**Input:** `merged_sources` table, the unified per-page source table built by `cross_type_merger.py`.

**Outputs:**

- `global_sources` : one row per unique real-world work across all pages, with canonical metadata and page count
- `global_source_map` : maps each `merged_source_id` to its `global_source_id`, one row per page-level source

Both output tables are fully replaced on every run as there is no append mode. This is intentional since cross-page matching must be recomputed holistically whenever new shards are added.

---

## Step-by-Step Workflow

### Step 1: Load merged sources

`load_merged_sources()` reads all rows from `merged_sources` in one query: author, year, title, identifiers, citation counts, and canonical SHA1. All cross-page matching happens in Python against this in-memory DataFrame.

At scale, this DataFrame fits comfortably in memory. DuckDB handles the SQL aggregation efficiently before handing off to Python.

---

### Step 2: Union-find initialization

A `UnionFind` is initialized with all `merged_source_id` values from the DataFrame. Each row starts as its own component. The two signal passes then union rows that refer to the same real-world work.

---

### Step 3: Pass 1 - Hard identifier matching

For each of DOI (confidence 1.00), ISBN (0.95), and PMID (0.95):
- Groups all `merged_source_id` values sharing the same identifier value
- Unions any group of 2 or more into the same component

These identifiers are globally unique by definition as two sources sharing a DOI are the same paper. No fuzzy comparison is needed or applied. This pass is fast because it is a simple groupby operation on the DataFrame.

---

### Step 4: Pass 2 - Author+year fuzzy blocking

Pass 2 only fires for sources that have no hard identifier AND have an extracted title. Sources without a title are excluded from this pass entirely and remain as singletons. This guards against false positives from common surnames — `jones 2025` matching across 200 unrelated articles was observed when the no-title branch was active.

Within each `(author_key, year)` block:
- If both sources have a title, `token_sort_ratio` on normalized titles must meet the threshold (default 85) to union them
- If neither has a title, author+year alone is sufficient at confidence 0.55
- Confidence scales linearly from 0.60 at threshold=85 to 0.70 at score=100

The threshold is 85 here versus 70 in `ref_tag_matching.py`. Cross-page matching is riskier because common surnames like `Smith` or `David` paired with a common year can produce false positives across unrelated articles. The higher threshold is the primary guard against this.

---

### Step 5: Build global sources

`build_global_sources()` iterates over union-find components and builds one row per component:

**Canonical selection** : `_id_priority()` picks the member with the best identifier (DOI > ISBN > PMID > URL > none), with total citation count as a tiebreaker. The canonical row's SHA1 and metadata fields become the representative for the global source.

**`n_pages`** : count of distinct `page_id` values across all members in the component.

**`n_total_citations`** : sum of `n_total_citations` across all members, counting every occurrence of this work across the entire dataset.

**`page_ids`** : JSON array of sorted page IDs for easy querying of which pages cite this work.

**Global source ID** : deterministic 16-character SHA1 hash of `global:` + sorted member IDs. Rebuilding with the same data produces the same IDs.

Singletons (components of size 1) get `match_type='singleton'` and `confidence=None` as they are real sources, just not cited on multiple pages.

---

### Step 6: Write to DuckDB

Both output tables are dropped and recreated from scratch on every run. This is the correct behavior because cross-page matching is a global operation because adding one new shard can change which sources merge across pages, so partial updates would be incorrect.

---

## Key Design Decisions

**Full replace, not append.** Per-page tables use DELETE + INSERT so pages can be re-run independently. Global tables use DROP + CREATE because the entire global matching result can change when any new data is added. There is no safe way to append to `global_sources` without re-running the full pass.

**Higher fuzzy threshold than within-page.** Within a single page, the same author appears consistently (editors cite the same book in similar ways). Across pages, the same author surname can refer to entirely different people. Threshold 85 vs 70 is a deliberate conservative choice. Missed cross-page merges are acceptable; incorrect merges that collapse different works are not.

**Pass 1 before Pass 2.** Hard-identifier sources are excluded from the fuzzy pass entirely. This prevents a source with a known DOI from being incorrectly re-grouped on author+year with a different source that happens to share the author's name and publication year.

**No URL as a cross-page signal.** URLs appear in Pass 1 for within-page matching but not in cross-page hard identifier matching. URLs are less stable than DOI/ISBN/PMID as they can be archive URLs, redirect URLs, or simply change over time. Using them for cross-page grouping would introduce false positives at a rate that DOI/ISBN/PMID do not. Requires more testing, may be viable matching.