### Proposed Build Step: `page_revision_stats` Parquet (Stage 8)

*Revised 2026-09-10 per `claude/decisions_2026-09-10.md`.*

---

#### What it produces

Per-revision citation quality metrics for every page. Schema (from `proposed_data_model.md`):

```
page_revision_stats.parquet
├── page_id               INT32
├── rev_id                INT64
├── timestamp             TIMESTAMP
├── n_total_citations     INT32       -- citation instances present (reuse events are not citations)
├── n_self_closing        INT32       -- <ref name="X"/> reuse events (named_ref_uses rows)
├── n_unique_sources      INT32       -- distinct global_source_ids
├── n_unresolved          INT32       -- instances with no global_source_id
├── n_high_confidence     INT32       -- distinct sources with confidence >= 0.85
├── n_new_sources         INT32       -- sources first appearing in this revision
├── pct_resolved          DOUBLE
├── n_ref_tag, n_template, n_bare_url,
│   n_endnote, n_shorthand, n_external_link  INT32   -- by citation_type
└── partitioned by: domain, page_bucket
```

`n_high_confidence` is served on the timeline as `high_confidence_count`. Note that `confidence` is NULL for singleton sources (UVA semantics), so singletons never count as high-confidence; that matches UVA's behaviour.

**Counting rule.** A citation instance with N parts is still **one** citation for `n_total_citations` and the per-type counts (`COUNT(DISTINCT citation_instance_id)`); parts matter only for source resolution (`n_unique_sources`, `n_unresolved`, `n_high_confidence`, `n_new_sources` count over parts).

**Every revision appears**, including revisions with zero citations, because the revision list comes from the Stage-3 `revisions` Parquet (RevisionChest metadata), not from `citation_history`. A revision that removes the last citation is a row with zeros.

---

#### Dependencies (all Parquet, all bucketed)

| Dataset | Produced by |
|---|---|
| `revisions` | Stage 3 (RevisionChest metadata, re-partitioned) |
| `citation_history` | Stage 1 |
| `named_ref_uses` | Stage 1 |
| `citation_instances_denorm` (resolved) | Stage 7c |
| `global_source_scores` | Stage 7g (not needed by this query; listed because the timeline may join it) |

No Postgres access.

---

#### Existing code

UVA `citation_history.py::build_page_revision_stats()` (per-page) and the bulk CTE path (`rev_agg`, `source_first`, `new_per_rev` over `_resolved_all`, lines 675–726). The bulk path is the model below. Differences: input is `citation_instances_denorm` ⋈ `citation_history` instead of `citations_extracted`; `is_self_closing` (always FALSE in UVA) is replaced by `named_ref_uses`; six per-type counts; every revision present; partitioned Parquet output.

---

#### Build query (per bucket; all views scoped to `domain=:domain/page_bucket=:bucket`)

```sql
WITH resolved AS (
    SELECT dn.page_id, ch.revision_id AS rev_id, dn.citation_instance_id,
           dn.citation_type, dn.part_index, dn.global_source_id, dn.confidence
    FROM citation_history ch
    JOIN citation_instances_denorm dn ON dn.citation_instance_id = ch.citation_instance_id
),
rev_agg AS (
    SELECT page_id, rev_id,
        COUNT(DISTINCT citation_instance_id)                                   AS n_total_citations,
        COUNT(DISTINCT global_source_id)                                       AS n_unique_sources,
        COUNT(DISTINCT citation_instance_id) FILTER (WHERE global_source_id IS NULL) AS n_unresolved,
        COUNT(DISTINCT global_source_id) FILTER (WHERE confidence >= 0.85)     AS n_high_confidence,
        COUNT(DISTINCT citation_instance_id) FILTER (WHERE citation_type = 'ref_tag')       AS n_ref_tag,
        COUNT(DISTINCT citation_instance_id) FILTER (WHERE citation_type = 'template')      AS n_template,
        COUNT(DISTINCT citation_instance_id) FILTER (WHERE citation_type = 'bare_url')      AS n_bare_url,
        COUNT(DISTINCT citation_instance_id) FILTER (WHERE citation_type = 'endnote')       AS n_endnote,
        COUNT(DISTINCT citation_instance_id) FILTER (WHERE citation_type = 'shorthand')     AS n_shorthand,
        COUNT(DISTINCT citation_instance_id) FILTER (WHERE citation_type = 'external_link') AS n_external_link
    FROM resolved GROUP BY page_id, rev_id
),
self_closing AS (
    SELECT page_id, revision_id AS rev_id, COUNT(*) AS n_self_closing
    FROM named_ref_uses GROUP BY page_id, revision_id
),
source_first AS (
    SELECT page_id, global_source_id, MIN(rev_id) AS first_rev_id
    FROM resolved WHERE global_source_id IS NOT NULL
    GROUP BY page_id, global_source_id
),
new_per_rev AS (
    SELECT page_id, first_rev_id AS rev_id, COUNT(*) AS n_new_sources
    FROM source_first GROUP BY page_id, first_rev_id
)
SELECT
    r.page_id, r.revision_id AS rev_id, r.revision_timestamp AS timestamp,
    CAST(COALESCE(a.n_total_citations, 0) AS INTEGER) AS n_total_citations,
    CAST(COALESCE(sc.n_self_closing, 0)   AS INTEGER) AS n_self_closing,
    CAST(COALESCE(a.n_unique_sources, 0)  AS INTEGER) AS n_unique_sources,
    CAST(COALESCE(a.n_unresolved, 0)      AS INTEGER) AS n_unresolved,
    CAST(COALESCE(a.n_high_confidence, 0) AS INTEGER) AS n_high_confidence,
    CAST(COALESCE(n.n_new_sources, 0)     AS INTEGER) AS n_new_sources,
    ROUND((COALESCE(a.n_total_citations,0) - COALESCE(a.n_unresolved,0))
          / GREATEST(COALESCE(a.n_total_citations,0), 1.0), 4) AS pct_resolved,
    CAST(COALESCE(a.n_ref_tag,0)       AS INTEGER) AS n_ref_tag,
    CAST(COALESCE(a.n_template,0)      AS INTEGER) AS n_template,
    CAST(COALESCE(a.n_bare_url,0)      AS INTEGER) AS n_bare_url,
    CAST(COALESCE(a.n_endnote,0)       AS INTEGER) AS n_endnote,
    CAST(COALESCE(a.n_shorthand,0)     AS INTEGER) AS n_shorthand,
    CAST(COALESCE(a.n_external_link,0) AS INTEGER) AS n_external_link
FROM revisions r
LEFT JOIN rev_agg a       ON a.page_id = r.page_id AND a.rev_id = r.revision_id
LEFT JOIN self_closing sc ON sc.page_id = r.page_id AND sc.rev_id = r.revision_id
LEFT JOIN new_per_rev n   ON n.page_id = r.page_id AND n.rev_id = r.revision_id
ORDER BY r.page_id, r.revision_id
```

`revisions` drives the output, so the earlier "revision with only self-closing refs" edge case disappears: such a revision has `n_total_citations = 0` and `n_self_closing > 0`.

---

#### Reference implementation

```python
def build_page_revision_stats(parquet_base: str, domain: str, output_dir: str):
    con = duckdb.connect()
    buckets = con.execute(f"""
        SELECT DISTINCT page_bucket
        FROM read_parquet('{parquet_base}/revisions/domain={domain}/**/*.parquet.zst',
                          hive_partitioning=true) ORDER BY 1""").fetchall()
    for (bucket,) in buckets:
        part = f"domain={domain}/page_bucket={bucket}"
        for name in ("revisions", "citation_history", "named_ref_uses", "citation_instances_denorm"):
            con.execute(f"""CREATE OR REPLACE VIEW {name} AS
                SELECT * FROM read_parquet('{parquet_base}/{name}/{part}/*.parquet.zst')""")
        con.execute(f"""COPY ({STATS_QUERY}) TO '{output_dir}/{part}/stats.parquet'
                        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)""")
```

`named_ref_uses` may have no files for a bucket; guard with an empty-view fallback.

---

#### New code required

| Component | Status |
|---|---|
| Build query | Adapted from UVA bulk CTE path |
| Per-bucket runner | New (~30 lines) |
| Integration | Step in `materialize_parquet.py`, after 7c (only needs the resolved denorm; independent of 7d–7h) |

---

#### Relationship to `revision_signals`

Extraction-time markup flags stay in their own Parquet dataset; joinable on `revision_id` at query time, not included here.

#### Performance

Every input is a single bucket partition of Parquet; the join is `citation_history` (largest) to a bucket of the denorm on `citation_instance_id`. No Postgres traffic. Buckets are independent — run several in parallel.