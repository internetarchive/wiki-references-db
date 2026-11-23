### Proposed Incremental Refresh

*Drafted 2026-11-22 from the SCA-389 enwiki run (plan §TBD). Status: proposal.*

A refresh applies new revisions (RevisionChest `sync` bundles and metadata) to a
built domain. Today the only refresh is the runbook's: extract the new bundles
as shards, then re-run the whole domain from `dedup`. This proposal replaces
that with a refresh whose cost follows the size of the change, in two phases.

---

### 1. Why

Measured on the enwiki-20261101 build (897.8M revisions, 19.3M pages), with
the 2026-11-01 → 22 sync (2.66M revisions):

| Window | Revisions | Pages touched | Buckets touched | History on those pages |
|---|---|---|---|---|
| 1 hour | 2,814 | 1,840 | 496 / 512 | — |
| 1 day (Nov 21) | 106,042 | 70,245 (0.36%) | 512 | 4.9% of all revisions |
| 3 weeks | 2,661,591 | 1,301,367 (6.7%) | 512 | 41.6% |

* **The bucket is the wrong unit.** An hour of edits touches nearly every
  bucket, so "re-run the buckets a bundle touched" (the deferred design in
  `proposed_matching.md`) saves nothing.
* **The page is the right unit, and it is still not free.** Busy pages are
  large, so a day's touched pages hold 4.9% of all history. Re-computing
  them in full costs about 5% of a rebuild, a 20× saving. A three-week catch-up
  saves only 2.4×, so for long gaps a full rebuild stays competitive.
* **Global work dominates once page work shrinks.** Of the base build's 43 h,
  6d took 3.3 h and 7a–7g took 11.3 h. Each of them reads the whole domain. A
  day's touched pages cite 5.15M of 122.2M global sources (4.2%), which
  appear on 8.14M of 131.9M page–source pairs (6.2%); the most cited of them
  is on 8,871 pages. That is an upper bound on what phase 2 re-runs for a
  day, since most of those components will not change.

Base build step times, for reference: stage0-load 2.2 h, dedup 2.2 h, load
9.0 h, gate 3 2.3 h, parts 2.6 h, denorm-export 2.8 h, ref-name-links 2.1 h,
6a–6c 24.7 h, 6d 3.3 h, 7a 0.5 h, 7b 2.6 h, 7c 3.1 h, 7d 2.7 h, 7e 0.6 h,
7f 1.2 h, 7g 0.1 h, 7h 1.5 h, revision-stats 0.7 h.

### 2. The contract

**An incremental refresh produces the same database as a full build over the
same inputs.** Same rows, same ids except `normalized_citations.id` (a
sequence, already not reproducible between builds), same gate results. Global
source ids are already per-build handles (`wcd_schema.ids`,
`qualify_global_source_id`), so a component whose membership changes may get a
new id; one whose membership does not change must not.

This is testable on the ci fixture: build from shards A, refresh with shard B,
and compare against a full build of A+B, table by table. That equivalence test
is the gate for every step below, and it comes first.

### 3. The refresh set

* **New shards:** the bundles the refresh extracts.
* **T, the touched pages:** every `page_id` with a revision in the new metadata.
  Written once per refresh as `refresh/<id>/pages.parquet`.
* **R, the retyped pages:** every page citing a citation whose merged
  `citation_type` the refresh changes (a new occurrence with a smaller type).
  The citation's type and parts are in each citing page's denorm rows, so these
  pages' page-local outputs change though no new revision touched them. `dedup
  --previous` writes them to `changes/retyped_pages.parquet`; the page-set steps
  run for T ∪ R. Phase 1 as first written ran T alone, which the ci fixture did
  not catch (it has no retyped citation) and `test_refresh_load.py` now does.
* **Affected pages (phase 2):** T ∪ R plus every page citing a global source
  whose component changes (§5).

### 4. Phase 1: page-local and append-only steps

Every step below is page-local or append-only today in what it *computes*; what
is missing is a writer that replaces a page set instead of a domain.

| Step | Today | Phase 1 |
|---|---|---|
| stage0-load | Postgres upserts (already incremental); `revisions/` rebuilt from all metadata | `--metadata` takes only the new files; `revisions/` gains one file per metadata file in each bucket, named after it (as extract names shard files). Duplicate revisions are impossible from `sync` (it drops repeated rcids). |
| extract | per shard | unchanged |
| dedup | whole staging set, one file per table | dedups only the new shards' staging (`deduped-<refresh>/`) |
| load | upserts that overwrite with the batch's values | upserts that **merge** as dedup does: `LEAST` for `appears_on_page_id`, `citation_type`, `reference_type`, `reference_name`, so the result is independent of load order. It records the `normalized_id`s whose `citation_type` changed. The collision gate also checks new instance ids against Postgres. |
| load (templates) | replaces the domain's `template_params`, `template_param_index`, `template_value_counts` | `template_params` rewritten for T's buckets; `template_param_index` gets the new citations' rows as an extra file per search bucket (citations are immutable, so rows only append); `template_value_counts` incremented. |
| gate 3 | whole domain (2.3 h) | the checks that can be, on the new rows; the whole-domain checks stay in the full build |
| parts | `--resume` | `--resume` plus the changed-`citation_type` list from load |
| denorm-export (unresolved) | rmtree domain | rewrite T's rows in each bucket |
| ref-name-links | DELETE domain | DELETE + insert T's pages; rewrite T's rows in each bucket file |
| 6a–6c | per bucket | per bucket, only T's pages; the bucket's `data-*.parquet` rewritten as its untouched pages' rows plus T's new rows |
| 7h | per bucket checkpoint, DELETE by page list | T only |
| revision-stats | per bucket | T only in phase 1 for its own fields; pages whose sources changed in 6d are added in phase 2 |

Rewriting a bucket's Parquet file still reads and writes the untouched pages'
rows, but only as I/O; the computation is for T alone. A shared helper
(`tier.replace_pages(dataset, domain, bucket, pages, new_rows)`) does this for
every page-partitioned dataset.

In phase 1, 6d and 7a–7g still run over the whole domain. Estimated cost of a
one-day refresh: page-local steps about 2 h, 6d and 7a–7g about 15 h. That is
about 17 h, down from 43 h. It is not yet enough for a daily cadence.

### 5. Phase 2: affected components

6d groups the domain's merged sources into components (hard-id unions, then
fuzzy author+year+title unions). A component can only change if one of its
merged sources changed, or a changed source shares a blocking key with it.

1. **Changed sources.** The merged sources of T before the refresh (from the
   previous `merged_sources`) and after (from the re-run of 6a–6c). Sources
   that exist in both with the same id are unchanged.
2. **Closure.** Persist 6d's blocking keys (each hard id, each fuzzy key) per
   merged source as a Parquet index. From the changed sources' keys, collect
   every source sharing a key, and repeat until no new source joins. Their
   current components, plus the changed sources, are the affected set.
3. **Recompute** the union-find over the affected set only, in the same
   `(page_id, merged_source_id)` order as the full run. Components outside the
   affected set are untouched and keep their ids.
4. **Downstream.** Affected global sources are those removed or created.
   7a upserts and deletes them. 7b, 7c (resolved) and 7d re-run for the
   affected pages (every page citing one of them). 7e, 7f and 7g re-run for the
   affected global sources only. Revision-stats re-runs for the affected pages.

Open points for phase 2, to settle before building:

* **Order dependence.** 6d's union-find keeps edge information (`match_type`,
  `confidence`) per root and drops the loser's. The result depends on the order
  unions are applied. Recomputing a subset in the same relative order should
  give the same answer, but this has to be proven by the equivalence test, not
  assumed.
* **`canon_type` reaches across components:** `source_type` comes from the first
  source in the domain with the same `canonical_sha1`. The closure has to
  include sources that share a `canonical_sha1`, or `canon_type` has to become a
  property of the component.
* **Giant components.** If one fuzzy key joins a large part of the domain (a
  common title or author), the closure may pull in much of it. Measure the
  closure size for a day of edits on the base build before building.
* **7b's orphan delete and 7e/7f** run across all domains today; they need
  domain or id-set filters.

### 6. When to rebuild instead

A refresh keeps the build's history but not its code. A change to extraction,
normalization or the matchers still means the re-run the runbook lists. A
refresh whose touched pages hold more than about a third of the domain's history
(about two weeks of enwiki edits) is better as a full rebuild of Stages 3–8.

### 7. Plan

1. **Done.** The A + B = AB equivalence test on the ci fixture
   (`test_refresh_ci.py`), with today's full re-run as the refresh. It did not
   pass trivially: 7b's delete of unreferenced global sources broke a foreign
   key on any re-run, because `page_source_summary` still referred to them
   until 7d. The delete now runs at the end of 7d.
2. **Done.** Load merges (`LEAST`) and the Postgres-aware collision gate.
   Today's full path never loads a partial staging set, so this changes no
   current output; it makes a partial load safe. Still open from this item: a
   changed `citation_type` must re-do that citation's parts, which `--resume`
   skips and whose deletion `fk_csr_part` blocks until 7b re-runs.
3. Phase 1 writers, one step at a time, each kept equal by the test.
   **Done:** dedup (`--previous`, merging the previous deduped set, which
   replaces the per-row `LEAST` design above as the source of exactness: the
   change set carries merged values, so a changed minimum reaches pages the
   refresh did not touch), load (`--changes`, with an incremental template
   phase), parts (`--redo-from`), denorm-export, ref-name-links and 6a–6c
   (`--pages-from`, for T ∪ R). `test_refresh_load.py` covers the changed-minimum cases
   the ci fixture does not have. **Left:** stage0-load's `revisions/` rebuild,
   a gate 3 for the new rows, 7h and revision-stats for a page set.
4. Measure the 6d closure for a day of edits on the enwiki build.
5. Phase 2.
