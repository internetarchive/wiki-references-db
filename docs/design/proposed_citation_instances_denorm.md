### Proposed Build Step: `citation_instances_denorm` Parquet

*Revised 2026-09-10 per `claude/decisions_2026-09-10.md`.*

---

#### Purpose

`citation_instances_denorm` is the Parquet flattening of `citation_instances` ⋈ `normalized_citations` ⋈ `normalized_citation_parts` ⋈ `citation_source_resolution`. It exists so that every DuckDB step that needs per-instance semantic fields — the matchers (Stage 6), `page_source_summary` (7d), `page_summary_cache` (7h), `page_revision_stats` (Stage 8), and the timeline fallback — reads Parquet-to-Parquet and never joins large `pg.*` tables through the Postgres scanner (which does table scans, not index lookups).

It is built **twice** with the same layout:

| Build | When | `global_source_id` etc. |
|---|---|---|
| **Unresolved** | end of Stage 4 (step 4.6) | NULL |
| **Resolved** | Stage 7c, after 7b | filled from `citation_source_resolution` |

The resolved build fully replaces the unresolved one. It is the UVA `citations_extracted` analogue (minus the revision dimension, which lives in `citation_history`).

---

#### Schema

Grain: **one row per (citation instance, part)**. An instance whose normalized citation has N parts contributes N rows; single-citation refs contribute one.

```
citation_instances_denorm.parquet
├── citation_instance_id  INT64
├── page_id               INT32
├── raw_sha1              CHAR(40)
├── normalized_id         INT32
├── normalized_sha1       CHAR(40)
├── reference_type        INT8
├── reference_name        VARCHAR
├── citation_type         VARCHAR     (extraction kind: ref_tag|template|bare_url|endnote|shorthand|external_link)
├── part_index            INT16
├── part_sha1             CHAR(40)
├── content               VARCHAR     (part content; needed by the matchers)
├── part_type             VARCHAR     (template | ref_tag | external_link)
├── norm_subtype          VARCHAR
├── norm_content          VARCHAR
├── template_name         VARCHAR
├── template_subtype      VARCHAR
├── anchor_key            VARCHAR
├── page_ref              VARCHAR
├── extracted_doi / isbn / pmid / pmc / arxiv / wikidata_qid / librarybase_id / url  VARCHAR
├── is_archive_url        BOOLEAN
├── author_key            VARCHAR
├── title                 VARCHAR
├── year                  VARCHAR
├── year_reliable         BOOLEAN
├── id_tier               INT8
├── id_tier_label         VARCHAR
├── global_source_id      VARCHAR     (NULL if unresolved / before 7c)
├── source_type           VARCHAR
├── match_type            VARCHAR
├── confidence            DOUBLE
└── partitioned by: domain, page_bucket
```

`reliability_score` / `verdict` are **not** here — 7c runs before 7f. Join `global_source_scores` (7g) on `global_source_id` when they are needed.

---

#### Query

```sql
COPY (
    SELECT
        d.value                        AS domain,
        ci.page_id % 512               AS page_bucket,
        ci.id                          AS citation_instance_id,
        ci.page_id, ci.raw_sha1,
        nc.id                          AS normalized_id,
        nc.normalized_sha1,
        ci.reference_type, ci.reference_name, nc.citation_type,
        p.part_index, p.part_sha1, p.content, p.part_type,
        p.norm_subtype, p.norm_content, p.template_name, p.template_subtype,
        p.anchor_key, p.page_ref,
        p.extracted_doi, p.extracted_isbn, p.extracted_pmid, p.extracted_pmc,
        p.extracted_arxiv, p.extracted_wikidata_qid, p.extracted_librarybase_id,
        p.extracted_url, p.is_archive_url,
        p.author_key, p.title, p.year, p.year_reliable, p.id_tier, p.id_tier_label,
        csr.global_source_id, csr.source_type, csr.match_type, csr.confidence
    FROM pg.citation_instances ci
    JOIN pg.domains d                     ON d.id = ci.domain_id
    JOIN pg.normalized_citations nc       ON nc.id = ci.normalized_id
    JOIN pg.normalized_citation_parts p   ON p.normalized_id = nc.id
    LEFT JOIN pg.citation_source_resolution csr
           ON csr.normalized_id = p.normalized_id AND csr.part_index = p.part_index
          AND csr.domain_id = ci.domain_id AND csr.page_id = ci.page_id
    WHERE ci.domain_id = :domain_id
) TO 'data/parquet/citation_instances_denorm'
  (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000,
   PARTITION_BY (domain, page_bucket), OVERWRITE_OR_IGNORE);
```

For the unresolved build the `LEFT JOIN` is dropped and the four resolution columns are literal NULLs of the right type.

This is the one place the pipeline scans the large Postgres citation tables from DuckDB; it is a sequential full scan per domain, which the Postgres scanner does efficiently. Run per domain; each domain's output directory is independent.

---

#### Pipeline Position

```
Stage 4.6  unresolved build   ← after normalized_citation_parts is loaded
Stage 6    matchers read it (+ citation_history)
Stage 7b   citation_source_resolution
Stage 7c   resolved build     ← replaces 4.6 output
Stage 7d/7h, Stage 8 read it
```

---

#### Existing Code

None builds this dataset. `dedup_parquet.py` shows the `COPY (…) TO` pattern; UVA's `build_citations_resolved()` is the join-shape reference (its 3-hop chain is our single `citation_source_resolution`).

---

#### New Code Required

One function, `export_citation_instances_denorm(domain, resolved: bool)`, in `materialize_parquet.py`, called from the Stage 4 driver and from the matching runner (7c). Verify with `SELECT COUNT(*) FROM read_parquet('…/**/*.parquet')` against `SELECT COUNT(*) FROM citation_instances ci JOIN normalized_citation_parts p USING (normalized_id) WHERE domain_id = :d`.

---

#### Refresh Strategy

Full rebuild per domain after any matching run. Incremental rebuild (re-export only buckets whose `citation_source_resolution` rows changed) is possible because the layout is already bucketed; deferred with incremental matching.