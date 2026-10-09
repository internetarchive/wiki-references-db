### Proposed Unified Data Model

*Revised 2026-09-10 per `claude/decisions_2026-09-10.md`. This is a **rebuild** model: the pipeline is re-run from RevisionChest bundles; no in-place migration of the existing database is assumed.*

The model is organized into three tiers: **Postgres** for entity lookups and API serving, **Parquet** for high-volume analytical/history data, and **DuckDB** as the query engine that unifies both for materialization and for the analytics endpoints.

---

### Identity Model: `(domain_id, page_id)`

A page (Wikipedia article, or an article on any encyclopedia-style site) is identified by the pair **`domain_id`** (FK to `domains.id`, e.g. the row for `en.wikipedia.org`) and **`page_id`** (the site-local numeric page id — the curid for MediaWiki). This pair is the key of every page-scoped table. `page_id` never means anything other than the site-local id.

Domain — not language code, not project family — is the site identifier, so the model extends to non-Wikimedia sites without change.

**`documents` is decoupled from page identity for the build.** `documents` remains the abstract node for *any* document — articles and cited works alike — and will be populated and reconciled in the citation-graph phase. No table in Stages 0–8 depends on `documents` being correct or complete. `normalized_citations.appears_on_article` is therefore replaced by `(appears_on_domain_id, appears_on_page_id)`.

**Parquet vs Postgres keying.** Postgres tables carry `domain_id`. Parquet datasets carry the domain **string** as a hive partition column (`domain=en.wikipedia.org/`) because they are written by the extractor before Postgres has assigned ids; cross-engine joins go through the `domains` table (`domains.value = domain`).

**`page_bucket`.** Every page-partitioned Parquet dataset is sub-partitioned by `page_bucket = page_id % 512`. The bucket is the unit of batching, checkpointing, and re-run throughout Stages 6–8.

---

### Writers of the Entity Tables

| Table | Sole writer |
|---|---|
| `domains`, `containers` | Stage 3 load (from staging), plus RevisionChest's `domains` insert (same `value` key; idempotent) |
| `revisions`, `revision_bundles`, `page_metadata` | **RevisionChest** (Stage 0), via its Postgres mode or a Stage 3 load of its `--parquet` metadata file |
| `web_resources` (article curid URLs) | RevisionChest (Stage 0) |
| `web_resources` (cited URLs), `normalized_citation_web_resources` | Stage 3 load |
| `documents` | Nothing in Stages 0–8 (citation-graph phase). RevisionChest's title-keyed `documents` insert is removed. |
| all citation tables | Stage 3 load / Stages 4–7 |

`build_db.py` / `load_all.py` no longer write `revisions` or article-side `documents`/`web_resources` rows.

---

### Canonical Identifier Set

**DOI, ISBN, PMID, PMC, arXiv, Wikidata QID, LibraryBase ID, URL.**

- DOI/ISBN/PMID/PMC/URL: extracted by both UVA normalizers (existing code).
- **arXiv**: template path only — `|arxiv=` on `{{cite arXiv}}` / `{{cite journal}}`, `|eprint=` alias on `{{cite arXiv}}`. Optional ref-tag path: `arxiv.org/abs/<id>` URLs.
- **Wikidata QID**: template path only — `{{Cite Q}}` first positional parameter (`Q\d+`).
- **LibraryBase**: model-only; NULL in the initial build.

---

### Tier 1: Postgres (Entity & Serving Layer)

#### Existing tables (unchanged)

```sql
documents              -- abstract document node; not used for page identity in the build
web_resources          -- URLs with domain, archive links, availability
domains                -- domain names (the site identifier)
containers             -- periodicals, journals
wiki_templates         -- template names per domain
revision_bundles       -- pointers to .mwrev.zst files (written by RevisionChest)
template_data          -- per-parameter rows for template search
normalized_citation_web_resources
```

#### Modified / new tables

```sql
-- revisions: written by RevisionChest for EVERY revision (including revisions
-- with no references). revision_timestamp becomes a real TIMESTAMP.
revisions (
    revision_id           BIGINT PRIMARY KEY,
    domain_id             INTEGER REFERENCES domains(id) NOT NULL,   -- NEW
    page_id               INTEGER NOT NULL,                          -- site-local id
    parent_revision_id    BIGINT,
    revision_timestamp    TIMESTAMP NOT NULL,                        -- was VARCHAR
    found_in_bundle       INTEGER REFERENCES revision_bundles(id),
    offset_begin          BIGINT,
    length                BIGINT
)
CREATE INDEX idx_revisions_page_ts    ON revisions (domain_id, page_id, revision_timestamp);
CREATE INDEX idx_revisions_page_revid ON revisions (domain_id, page_id, revision_id);

-- page_metadata: replaces the proposed wikipedia_pages. Title/namespace for
-- every page, from RevisionChest's page metadata (Stage 0/3). The on-demand
-- site-API fetch (proposed_cache.md §5) is the exception path only.
page_metadata (
    domain_id             INTEGER REFERENCES domains(id) NOT NULL,
    page_id               INTEGER NOT NULL,
    page_title            VARCHAR,
    page_namespace        INTEGER,
    PRIMARY KEY (domain_id, page_id)
)

-- normalized_citations: one row per unique normalized reference text.
-- Semantic (Stage 4) columns live on normalized_citation_parts, not here.
normalized_citations (
    id                    SERIAL PRIMARY KEY,
    normalized_sha1       CHAR(40) UNIQUE NOT NULL,
    reference_normalized  TEXT NOT NULL,             -- full wikitext incl. <ref …> wrapper
    appears_on_domain_id  INTEGER REFERENCES domains(id) NOT NULL,   -- replaces appears_on_article
    appears_on_page_id    INTEGER NOT NULL,
    citation_type         VARCHAR NOT NULL           -- extraction-time kind, see below
)

-- citation_type (set in Stage 1, per reference, from the extractor's candidate class):
--   'ref_tag'        <ref>…</ref> whose content is not a citation template or a bare URL
--   'template'       <ref>…</ref> whose content is (or embeds) a citation template per wikis.yaml
--   'bare_url'       <ref>…</ref> whose content is only a URL / [url label]
--   'endnote'        list item in a reference section (outside <ref>)
--   'shorthand'      standalone {{sfn}}-family template outside <ref>
--   'external_link'  bracketed or bare external link outside <ref>, not in a reference section

-- normalized_citation_parts: Stage 4 output. One row per semantic part of a
-- normalized citation. Bullet-block splitting happens HERE (a <ref> holding a
-- bullet list of N citations yields N parts); every other citation has exactly
-- one part (part_index = 0). part_sha1 is the SHA1 of the extracted part
-- content and is the direct analogue of UVA's content_sha1.
normalized_citation_parts (
    normalized_id         INTEGER REFERENCES normalized_citations(id) NOT NULL,
    part_index            SMALLINT NOT NULL DEFAULT 0,
    part_sha1             CHAR(40) NOT NULL,          -- SHA1(content)
    content               TEXT NOT NULL,              -- ref inner content / item text (no <ref> wrapper)
    part_type             VARCHAR NOT NULL,           -- 'template' | 'ref_tag' | 'external_link'
                                                      -- (which normalizer path handled it)
    -- ref_tag path (UVA reftag_normalizer)
    norm_subtype          VARCHAR,          -- wikilink_ref | bare_url | shorthand_template |
                                            -- author_date | prose_citation | noise |
                                            -- italicized_citation | cite_html
    norm_content          VARCHAR,          -- UVA typographic-normalized fingerprint
    anchor_key            VARCHAR,
    anchor_label          VARCHAR,
    url_anchor_text       VARCHAR,
    page_ref              VARCHAR,
    -- template path (UVA template_normalizer)
    template_name         VARCHAR,          -- normalized, e.g. 'cite journal'
    template_subtype      VARCHAR,          -- cite_book | cite_journal | cite_web | cite_news | cite_other
    params_json           JSONB,            -- raw template params (NULL for ref_tag path)
    author_last           VARCHAR,
    author_first          VARCHAR,
    journal               VARCHAR,
    date                  VARCHAR,
    publisher             VARCHAR,
    location              VARCHAR,
    edition               VARCHAR,
    volume                VARCHAR,
    issue                 VARCHAR,
    pages                 VARCHAR,
    -- shared
    author_key            VARCHAR,
    title                 VARCHAR,
    year                  VARCHAR,
    year_reliable         BOOLEAN,
    extracted_doi         VARCHAR,
    extracted_isbn        VARCHAR,
    extracted_pmid        VARCHAR,
    extracted_pmc         VARCHAR,
    extracted_arxiv       VARCHAR,
    extracted_wikidata_qid VARCHAR,
    extracted_librarybase_id VARCHAR,       -- model-only
    extracted_url         VARCHAR,
    is_archive_url        BOOLEAN,
    id_tier               SMALLINT,         -- UVA semantics: 0 none, 1 template_wrapped,
                                            -- 2 url, 3 isbn, 4 doi, 5 pmid/pmc (a branch
                                            -- label, NOT a linear quality scale)
    id_tier_label         VARCHAR,
    PRIMARY KEY (normalized_id, part_index)
)
CREATE INDEX idx_ncp_part_sha1 ON normalized_citation_parts (part_sha1);
CREATE INDEX idx_ncp_doi   ON normalized_citation_parts (extracted_doi)  WHERE extracted_doi  IS NOT NULL;
CREATE INDEX idx_ncp_isbn  ON normalized_citation_parts (extracted_isbn) WHERE extracted_isbn IS NOT NULL;
CREATE INDEX idx_ncp_pmid  ON normalized_citation_parts (extracted_pmid) WHERE extracted_pmid IS NOT NULL;
CREATE INDEX idx_ncp_pmc   ON normalized_citation_parts (extracted_pmc)  WHERE extracted_pmc  IS NOT NULL;
CREATE INDEX idx_ncp_arxiv ON normalized_citation_parts (extracted_arxiv) WHERE extracted_arxiv IS NOT NULL;
CREATE INDEX idx_ncp_qid   ON normalized_citation_parts (extracted_wikidata_qid) WHERE extracted_wikidata_qid IS NOT NULL;
CREATE INDEX idx_ncp_id_tier ON normalized_citation_parts (id_tier);

-- citation_instances: one row per unique raw reference text on a page.
-- id is DETERMINISTIC: the first 8 bytes of SHA1("{domain}|{page_id}|{raw_sha1}")
-- as a signed BIGINT, computed in build_db.py — so citation_history Parquet can
-- be written with the id at extraction time, with no Postgres round trip.
-- Self-closing <ref name="X"/> produce NO rows here (see named_ref_uses).
citation_instances (
    id                    BIGINT PRIMARY KEY,         -- deterministic hash
    domain_id             INTEGER REFERENCES domains(id) NOT NULL,
    page_id               INTEGER NOT NULL,
    normalized_id         INTEGER REFERENCES normalized_citations(id) NOT NULL,
    raw_sha1              CHAR(40) NOT NULL,
    reference_type        SMALLINT NOT NULL DEFAULT 0, -- 0=other, 1=inline, 2=endnote (unchanged)
    reference_name        VARCHAR,
    UNIQUE (domain_id, page_id, raw_sha1)
)
CREATE INDEX idx_ci_page       ON citation_instances (domain_id, page_id);
CREATE INDEX idx_ci_normalized ON citation_instances (normalized_id);
CREATE INDEX idx_ci_norm_named ON citation_instances (normalized_id)
    WHERE reference_name IS NOT NULL AND reference_name <> '';

-- citation_history: NOT a Postgres table in the rebuild (Tier 2 only).
```

Hash-collision note for `citation_instances.id`: a 64-bit hash over N instances has expected collisions ≈ N²/2⁶⁵ (≈0.25 at N = 3×10⁹). The dedup step (`proposed_data_integrity.md` §4) verifies `COUNT(DISTINCT id) = COUNT(DISTINCT (domain, page_id, raw_sha1))` and aborts on collision. If the instance count approaches 10⁹, switch the id to a 128-bit `UUID` (first 16 bytes of the same SHA1) before the build; DuckDB, Parquet and Postgres all support it natively at a cost of 8 extra bytes per history row.

#### New tables: Semantic Resolution

```sql
global_sources (
    global_source_id      VARCHAR PRIMARY KEY,        -- UVA: sha1("global:"+sorted members)[:16]
    match_type            VARCHAR,          -- 'doi','isbn','pmid','pmc','arxiv','wikidata_qid',
                                            -- 'author_year_fuzzy','singleton'
    confidence            DOUBLE PRECISION, -- NULL for singletons (UVA semantics)
    canonical_part_sha1   CHAR(40),         -- UVA canonical_sha1
    source_type           VARCHAR,          -- 'ref_tag' | 'template' | 'both' (of canonical member)
    author_key            VARCHAR,
    year                  VARCHAR,
    title                 VARCHAR,
    extracted_doi         VARCHAR,
    extracted_isbn        VARCHAR,
    extracted_pmid        VARCHAR,
    extracted_pmc         VARCHAR,
    extracted_arxiv       VARCHAR,
    extracted_wikidata_qid VARCHAR,
    extracted_librarybase_id VARCHAR,
    extracted_url         VARCHAR,
    n_pages               INTEGER,          -- Stage 7e (sole writer)
    n_total_citations     INTEGER,          -- Stage 7e (sole writer)
    reliability_score     DOUBLE PRECISION, -- Stage 7f, UVA _compute_score
    verdict               VARCHAR           -- Stage 7f: high_confidence | medium_confidence |
                                            -- low_confidence | questionable | likely_not_citation
)
-- partial indexes on each extracted_* identifier, plus verdict and reliability_score

-- Flattened part → global source resolution (replaces UVA's 3-hop chain).
citation_source_resolution (
    normalized_id         INTEGER NOT NULL,
    part_index            SMALLINT NOT NULL,
    domain_id             INTEGER NOT NULL,
    page_id               INTEGER NOT NULL,
    global_source_id      VARCHAR REFERENCES global_sources(global_source_id) NOT NULL,
    source_type           VARCHAR,          -- 'ref_tag' | 'template' | 'both'
    match_type            VARCHAR,          -- PER-PAGE match (6a/6b/6c), as in UVA citations_resolved;
    confidence            DOUBLE PRECISION, -- the cross-page values are on global_sources
    is_canonical          BOOLEAN,
    PRIMARY KEY (normalized_id, part_index, domain_id, page_id),
    FOREIGN KEY (normalized_id, part_index) REFERENCES normalized_citation_parts
)
CREATE INDEX idx_csr_global ON citation_source_resolution (global_source_id);
CREATE INDEX idx_csr_page   ON citation_source_resolution (domain_id, page_id);

-- Per-page source summary (serves the frontend). reliability_score/verdict are
-- NOT stored here; endpoints join global_sources.
page_source_summary (
    domain_id             INTEGER NOT NULL,
    page_id               INTEGER NOT NULL,
    global_source_id      VARCHAR NOT NULL,
    author_key            VARCHAR,
    year                  VARCHAR,
    title                 VARCHAR,
    extracted_doi/isbn/pmid/pmc/arxiv/wikidata_qid/librarybase_id/url  VARCHAR,
    source_type           VARCHAR,
    match_type            VARCHAR,
    confidence            DOUBLE PRECISION,
    first_rev_id          BIGINT,
    first_seen            TIMESTAMP,
    last_rev_id           BIGINT,
    last_seen             TIMESTAMP,
    n_citations           INTEGER,          -- citation instances for this source on this page
    n_revisions_cited     INTEGER,
    n_self_closing_uses   INTEGER,          -- reuse events attributed via ref_name_links
    is_current            BOOLEAN,
    PRIMARY KEY (domain_id, page_id, global_source_id)
)
CREATE INDEX idx_pss_global  ON page_source_summary (global_source_id);
CREATE INDEX idx_pss_current ON page_source_summary (domain_id, page_id) WHERE is_current;

-- Ref-name links: name → defining citation, scoped by revision range.
ref_name_links (
    domain_id             INTEGER NOT NULL,
    page_id               INTEGER NOT NULL,
    ref_name              VARCHAR NOT NULL,
    defining_instance_id  BIGINT NOT NULL,            -- citation_instances.id of <ref name="X">…</ref>
    first_revision_id     BIGINT NOT NULL,
    last_revision_id      BIGINT,                     -- NULL = still current
    PRIMARY KEY (domain_id, page_id, ref_name, first_revision_id)
)

-- Serving cache; production DDL in proposed_cache.md §1.
page_summary_cache (
    domain_id             INTEGER NOT NULL,
    page_id               INTEGER NOT NULL,
    … (see proposed_cache.md)
    PRIMARY KEY (domain_id, page_id)
)
```

---

### Tier 2: Parquet (Analytical / History Layer)

```
data/parquet/
  citation_history/            ← written by build_db.py (Stage 1), ~28.8B rows
    domain={domain}/page_bucket={b}/*.parquet.zst
  named_ref_uses/              ← written by build_db.py (Stage 1); one row per self-closing occurrence
    domain={domain}/page_bucket={b}/*.parquet.zst
  revision_signals/            ← written by build_db.py (Stage 1)
    domain={domain}/*.parquet.zst
  ref_name_links/              ← Stage 5: bucketed Parquet copy of the Postgres table (for 7d)
    domain={domain}/page_bucket={b}/*.parquet.zst
  revisions/                   ← Stage 3: re-partitioned copy of RevisionChest's page/revision
    domain={domain}/page_bucket={b}/*.parquet.zst      metadata (rev_id, page_id, parent, timestamp)
                                 so Stages 7d/8 never join a ~10⁹-row pg.revisions from DuckDB
  matching_intermediate/       ← Stages 6a–6d; bucketed
    sources/                     domain={domain}/page_bucket={b}/
    citation_source_map/         domain={domain}/page_bucket={b}/
    template_sources/            domain={domain}/page_bucket={b}/
    template_citation_source_map/ domain={domain}/page_bucket={b}/
    merged_sources/              domain={domain}/page_bucket={b}/
    merged_citation_source_map/  domain={domain}/page_bucket={b}/
    global_source_map/           (global; not bucketed)
  citation_instances_denorm/   ← Postgres → Parquet export, built twice: end of Stage 4
    domain={domain}/page_bucket={b}/*.parquet.zst   (unresolved; Stage 6 input) and Stage 7c (resolved)
  global_source_scores/        ← Stage 7g export (thin: id, n_pages, n_total_citations, score, verdict)
    *.parquet.zst
  page_revision_stats/         ← Stage 8
    domain={domain}/page_bucket={b}/*.parquet.zst
```

```
citation_history.parquet
├── citation_instance_id  INT64      (deterministic hash; see citation_instances)
├── revision_id           INT64
├── page_id               INT32
└── partitioned by: domain, page_bucket

named_ref_uses.parquet             (one row per <ref name="X"/> occurrence)
├── page_id               INT32
├── revision_id           INT64
├── ref_name              VARCHAR
├── offset_start          INT32      (character offset in the revision text)
└── partitioned by: domain, page_bucket
   -- per-revision counts are GROUP BY page_id, revision_id, ref_name.
   -- Fallback if volume is prohibitive: keep per-occurrence rows only for the
   -- latest revision of each page and (page_id, revision_id, ref_name, n_uses)
   -- counts for history.

revision_signals.parquet
├── revision_id, page_id
├── has_ref_tag, has_named_ref, has_self_closing_ref, has_references_tag,
│   has_cite_template, has_citation_template, has_shorthand_template  BOOLEAN
└── partitioned by: domain

citation_instances_denorm.parquet   (Stage 7c; see proposed_citation_instances_denorm.md)
├── citation_instance_id  INT64
├── page_id               INT32
├── raw_sha1, normalized_sha1  CHAR(40)
├── reference_type        INT8
├── reference_name        VARCHAR
├── citation_type         VARCHAR    (extraction kind)
├── part_index            INT16
├── part_sha1             CHAR(40)
├── part_type, norm_subtype, template_name, template_subtype  VARCHAR
├── extracted_doi/isbn/pmid/pmc/arxiv/wikidata_qid/librarybase_id/url  VARCHAR
├── author_key, title, year  VARCHAR
├── year_reliable         BOOLEAN
├── id_tier               INT8
├── global_source_id      VARCHAR    (NULL if unresolved)
├── source_type, match_type  VARCHAR
├── confidence            DOUBLE
└── partitioned by: domain, page_bucket
   -- NO reliability_score/verdict: this export runs before Stage 7f.
   -- Join global_source_scores for those.

global_source_scores.parquet       (Stage 7g)
├── global_source_id      VARCHAR
├── n_pages               INT32
├── n_total_citations     INT32
├── reliability_score     DOUBLE
└── verdict               VARCHAR

page_revision_stats.parquet        (Stage 8)
├── page_id               INT32
├── rev_id                INT64
├── timestamp             TIMESTAMP
├── n_total_citations     INT32
├── n_self_closing        INT32      (reuse events, from named_ref_uses)
├── n_unique_sources      INT32
├── n_unresolved          INT32
├── n_high_confidence     INT32
├── n_new_sources         INT32
├── pct_resolved          DOUBLE
├── n_ref_tag, n_template, n_bare_url, n_endnote, n_shorthand, n_external_link  INT32
└── partitioned by: domain, page_bucket

matching_intermediate/sources.parquet            (6a; UVA `sources`)
├── source_id, page_id, canonical_part_sha1, norm_subtype, id_tier,
│   extracted_* (canonical set), anchor_key, author_key, year, title,
│   n_citations, match_type, confidence
matching_intermediate/citation_source_map.parquet (6a; UVA `citation_source_map`)
├── part_sha1, page_id, source_id, match_type, confidence, is_canonical
matching_intermediate/merged_sources.parquet     (6c; UVA `merged_sources`)
├── merged_source_id, page_id, source_type, ref_source_id, tmpl_source_id,
│   canonical_part_sha1, author_key, year, title, extracted_* (canonical set),
│   n_ref_citations, n_tmpl_citations, n_total_citations, match_type, confidence
```

---

### Tier 3: DuckDB Query Layer

DuckDB is the engine for every cross-engine step (Stages 7d, 7h, 8) and for the analytics endpoints that touch Parquet. Postgres is ATTACHed for small point lookups; the large joins are Parquet-to-Parquet (`citation_instances_denorm` × `citation_history` × `named_ref_uses`), not Parquet-to-`pg.*`.

```python
con = duckdb.connect()
con.execute("ATTACH 'dbname=wiki_references …' AS pg (TYPE POSTGRES, READ_ONLY);")
for name in ("citation_history", "named_ref_uses", "revisions", "ref_name_links",
             "citation_instances_denorm", "global_source_scores", "page_revision_stats"):
    con.execute(f"""CREATE VIEW {name} AS
        SELECT * FROM read_parquet('data/parquet/{name}/**/*.parquet.zst',
                                   hive_partitioning=true)""")
# domain string ↔ domain_id
con.execute("CREATE VIEW domains AS SELECT id AS domain_id, value AS domain FROM pg.domains")
```

**Transitional alias views** (validation aid only; not the production path) map UVA frontend tables onto the unified model:

| UVA table | Unified source |
|---|---|
| `citations_extracted` | `citation_instances_denorm` ⋈ `citation_history` |
| `ref_tag_normalized`, `template_normalized` | `normalized_citation_parts` (Postgres) |
| `page_source_summary`, `global_sources` | Postgres via ATTACH |
| `global_source_map` | `citation_source_resolution` |
| `merged_sources` | `matching_intermediate/merged_sources` |
| `wikipedia_pages` | `page_metadata` |

---

### Key Design Decisions

**Why `(domain_id, page_id)` instead of `documents.id`?** Page identity must be correct from the first load; `documents` is the abstract document node and is reconciled later. Keying pages by their site-local id under an explicit domain keeps page identity independent of the document graph and keeps the v1 `page_id` meaning.

**Why a parts table?** UVA's normalizers operate on the inner content of a `<ref>` and may split a bullet-list block into several citations. Keeping `normalized_citations` as the one-row-per-reference-text entity (with hashes, offsets and `template_data` intact) and putting semantic output on `normalized_citation_parts` lets the UVA code run unchanged on `parts.content` while the syntactic layer is untouched. `part_sha1` is UVA's `content_sha1`.

**Why deterministic instance ids?** So `citation_history` — the only table too large for Postgres — never needs a Postgres round trip: `build_db.py` writes it in its final Parquet layout.

**Why do self-closing refs get no citation rows?** A `<ref name="X" />` has no content; storing it as a citation is repetitive noise. It is a reuse event, recorded per occurrence (with its offset, so it can still be located in the text) in `named_ref_uses`, and resolved to its defining citation through `ref_name_links`.

**Why `citation_history` in Parquet only?** ~28.8B rows × ~16 bytes; Parquet+zstd with page-bucket pruning serves the timeline and history queries; Postgres would need ~700 GB+ with indexes.

**Why `page_source_summary` in Postgres?** Most-hit table; bounded at O(pages × sources_per_page); point lookups by `(domain_id, page_id)`.

---

### Data Flow Summary

```
Stage 0: RevisionChest
    → .mwrev.zst bundles + page/revision metadata (→ revisions, revision_bundles,
      page_metadata, article web_resources in Postgres)

Stages 1–3: Extraction + Load (build_db.py / dedup_parquet.py / load_all.py)
    → Parquet staging (citation_instances, normalized_citations, ncwr, template_data, …)
    → Tier-2 Parquet written directly: citation_history, named_ref_uses, revision_signals
    → Postgres load: domains, web_resources (cited URLs), normalized_citations,
      citation_instances, ncwr, wiki_templates, template_data
      (+ RevisionChest metadata → revisions, page_metadata if not loaded in Stage 0)
    → revisions Parquet (re-partitioned from RevisionChest metadata)

Stage 4: Semantic Normalization (UVA normalizers)
    → content extraction (strip <ref> wrapper) → bullet split → classify part
    → reftag_normalizer / template_normalizer → normalized_citation_parts
    → citation_instances_denorm Parquet (unresolved) for Stage 6

Stage 5: Ref-Name Link Resolution → ref_name_links (Postgres + bucketed Parquet copy)

Stage 6: Matching (UVA, bucketed)
    → 6a/6b per-page ref_tag & template matching → 6c merge → 6d global dedup

Stage 7: Materialization + exports (sequential)
    7a global_sources          7b citation_source_resolution
    7c citation_instances_denorm Parquet export
    7d page_source_summary (DuckDB: denorm × citation_history × named_ref_uses × ref_name_links)
    7e count refresh           7f reliability + verdict (UVA scorer)
    7g global_source_scores Parquet export
    7h page_summary_cache

Stage 8: page_revision_stats Parquet (DuckDB: denorm × citation_history × named_ref_uses × global_source_scores)

Serving (FastAPI): Postgres point lookups; DuckDB over Parquet for timeline/history.
```