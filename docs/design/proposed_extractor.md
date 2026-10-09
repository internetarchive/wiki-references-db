# Proposed Extraction and Normalization Pipeline

*Revised 2026-09-10 per `claude/decisions_2026-09-10.md`. Rebuild model; stage numbering is shared with `proposed_matching.md` (Stages 6–7) and `proposed_page_revision_stats.md` (Stage 8).*

## Current Pipeline (wiki-references-db, as it exists)

```
RevisionChest (Rust) → .mwrev.zst bundles
   (+ page/revision metadata → SQLite | Parquet | Postgres: domains, documents,
      web_resources, revision_bundles, revisions)
  → build_db.py: get_revisions_from_mwrev_zst()   (parses RevisionChest's line format;
                                                   no mwxml anywhere)
  → refs_extractor/article.py: extract_references()
      - hand-written scanner: <ref> tags (incl. self-closing), standalone {{sfn}},
        list items in reference sections (wikis.yaml), external links
      - templates via mwparserfromhell; URLs via regex
      - classify_reference_type() → 0 other / 1 inline / 2 endnote, using
        wikis.yaml citation_templates {prefixes, exact}
  → refs_normalizer/syntax.py: normalize_wikitext() → normalized_sha1 (name= attribute kept)
  → Parquet staging (per shard) → dedup_parquet.py → load_all.py → Postgres
```

Known gaps in the current pipeline that this proposal fixes: `build_all.py` never passes `--domain` (everything is stamped `en.wikipedia.org`); `revisions` rows are emitted per reference and only for revisions containing references; `citation_history` instance ids are assigned by Postgres; self-closing refs are stored as citations; article `documents` rows are inserted without dedup.

### Strengths to preserve

| Component | Why keep it |
|---|---|
| **Syntactic normalizer** (`syntax.py`) | Canonical text + `normalized_sha1` for deduplication. Unchanged. |
| **Extractor** (`article.py`) | Four candidate kinds (ref tags, standalone shorthand, reference-section list items, external links) with offsets; per-domain config in `wikis.yaml`. |
| **URL/domain extraction** | `web_resources`, `domains`, `ncwr`. Unchanged. |
| **Template parameter storage** (`template_data`) | Arbitrary parameter search. Unchanged. |
| **Entity/occurrence split** | `normalized_citations` + `citation_instances`. Kept; semantic columns go on a new `normalized_citation_parts`. |
| **Parquet staging → dedup → load** | Kept; extended. |

## UVA Code Adopted

| Component | Source file | Reuse |
|---|---|---|
| Ref-tag semantic normalizer | `citation_normalize/reftag_normalizer.py` | As-is, called on `parts.content` |
| Template semantic normalizer | `citation_normalize/template_normalizer.py` | As-is + arXiv/QID params |
| Footnote filter | `wcd_extract/citation_extractor.py::_is_citation` | Ported into Stage 4 as a classifier input (→ `noise`); extraction keeps everything |
| Bullet-block detection/split | `wcd_extract/citation_extractor.py::_is_multi_block`, `iter_citations` | Ported into Stage 4 (not Stage 1) |
| Per-revision signal flags | `wcd_extract/citation_extractor.py::_extract_flags` | Regexes reused in Stage 1 |
| `build_ref_name_links` | both normalizers | Seed for Stage 5 (rewritten with revision ranges) |

## Proposed Unified Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 0: RevisionChest (existing; owns the entity tables)       │
│  - .mwrev.zst bundles per dump file                             │
│  - page/revision metadata → Postgres (revisions for EVERY       │
│    revision with bundle offsets; page_metadata title/ns;        │
│    article curid web_resources; revision_bundles; domains)      │
│    or → --parquet metadata file loaded in Stage 3               │
│  CHANGES: revision_timestamp written as TIMESTAMP; the          │
│  title-keyed documents insert is removed; page title/ns go to   │
│  page_metadata keyed (domain_id, page_id).                      │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 1: Extraction (build_db.py + refs_extractor/article.py)   │
│  KEPT: candidate scanning, offsets, reference_name,             │
│        reference_type, URL/template extraction                  │
│  ADDED:                                                         │
│  - --domain is REQUIRED and forwarded by build_all.py (one      │
│    bundle directory per domain; the .mwrev.zst header has no    │
│    domain)                                                      │
│  - citation_type per reference (6 kinds, see below)             │
│  - citation_instance_id = int64(SHA1(f"{domain}|{page_id}|      │
│    {raw_sha1}")[:8])                                            │
│  - citation_history written DIRECTLY in Tier-2 layout           │
│    (domain=/page_bucket=), with the instance id                 │
│  - self-closing <ref name="X"/>: NO reference emitted; one      │
│    named_ref_uses row per occurrence (page_id, revision_id,     │
│    ref_name, offset_start); per-revision text dedup no longer   │
│    applies to them                                              │
│  - revision_signals flags → Parquet                             │
│  REMOVED: revisions rows, article documents/web_resources rows  │
│  OUTPUT per reference: raw text, offsets, reference_name,       │
│    reference_type, citation_type, templates[], urls[]           │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 2: Syntactic Normalization (UNCHANGED)  syntax.py         │
│  normalized_sha1, raw_sha1, reference_normalized                │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 3: Dedup + Postgres Load (dedup_parquet.py, load_all.py)  │
│  - staging dedup keys gain domain: (domain, page_id, raw_sha1)  │
│  - NEW gate: instance-id collision check                        │
│  - load: domains, web_resources (cited), normalized_citations   │
│    (appears_on_domain_id/page_id, citation_type),               │
│    citation_instances (deterministic id), ncwr, wiki_templates, │
│    template_data                                                │
│  - NEW: load RevisionChest metadata → revisions, page_metadata  │
│    (if Stage 0 ran with --parquet); write revisions Parquet     │
│    (domain=/page_bucket=) for Stages 7d/8                       │
│  - citation_history / named_ref_uses / revision_signals are     │
│    never loaded to Postgres                                     │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 4: Semantic Normalization (UVA normalizers)               │
│  Input: normalized_citations (reference_normalized,             │
│         citation_type)                                          │
│  4.1 Content extraction                                         │
│      ref_tag/template/bare_url: strip the <ref …>…</ref>        │
│        wrapper → inner content (name stays on                   │
│        citation_instances.reference_name)                       │
│      endnote: strip leading list markers                        │
│      shorthand / external_link: text as-is                      │
│  4.2 Bullet-block split (UVA _is_multi_block / iter_citations)  │
│      → N parts, else 1 part (part_index 0)                      │
│  4.3 Per part: part_type                                        │
│      'template' if content starts with / embeds a citation      │
│        template per wikis.yaml citation_templates               │
│      'external_link' if citation_type = external_link           │
│      else 'ref_tag'                                             │
│  4.4 Normalizers (unique content strings only, as in UVA)       │
│      ref_tag → reftag_normalizer (+ footnote filter → noise)    │
│      template → template_normalizer (+ |arxiv=, |eprint=,       │
│        {{Cite Q}} positional QID)                               │
│      external_link → extracted_url only                         │
│  4.5 Write normalized_citation_parts (part_sha1 = SHA1(content))│
│  4.6 Export citation_instances_denorm Parquet (unresolved) —    │
│      Stage 6 reads only Parquet                                 │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 5: Ref-Name Link Resolution                               │
│  ref_name_links: (domain_id, page_id, ref_name,                 │
│  defining_instance_id, first_revision_id, last_revision_id)     │
│  Built from citation_instances (reference_name IS NOT NULL) ×   │
│  citation_history × revisions: for each (page, name) walk       │
│  revisions in timestamp order; a change of defining instance    │
│  closes the current range and opens a new one.                  │
│  Seed: UVA build_ref_name_links (page-scoped, earliest rev).    │
│  Output: Postgres table + bucketed Parquet copy (read by 7d).   │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
  Stages 6–8: proposed_matching.md, proposed_citation_instances_denorm.md,
              proposed_page_revision_stats.md, proposed_cache.md
```

## `citation_type` (Stage 1)

Set per reference from the extractor's candidate class plus the existing `wikis.yaml` template config:

```python
from wiki_config import get_citation_template_prefixes, get_citation_template_exact

def classify_citation_type(candidate_kind, content, templates, domain):
    """candidate_kind is which scanner produced the reference:
       'ref' | 'sfn' | 'list_item' | 'external_link' (already known in extract_references)."""
    if candidate_kind == 'sfn':
        return 'shorthand'
    if candidate_kind == 'list_item':
        return 'endnote'
    if candidate_kind == 'external_link':
        return 'external_link'
    # candidate_kind == 'ref'
    names = [t['template_name'].lower().strip().replace('_', ' ') for t in (templates or [])]
    prefixes = get_citation_template_prefixes(domain)
    exact = set(get_citation_template_exact(domain))
    if any(n in exact or any(n.startswith(p) for p in prefixes) for n in names):
        return 'template'
    inner = strip_ref_wrapper(content).strip()
    if inner and _is_bare_url(inner):        # http://… or [http://… label] and nothing else
        return 'bare_url'
    return 'ref_tag'
```

No hard-coded template set; per-domain behaviour comes from `wikis.yaml`, which already carries `citation_templates: {prefixes, exact}` and `reference_sections` for `en`, `it`, `af`.

## Self-closing refs

Extraction records each `<ref name="X" />` as a reuse event with its offset — one row per occurrence — and emits no reference for it. Existing test `test_extract_references_self_closing_ref_name` is rewritten to assert the reuse-event output. Reuse resolves to the defining citation via `ref_name_links` (Stage 5), and counts roll up in Stages 7d and 8.

## Per-revision signal flags

Same seven booleans as UVA, computed with UVA's `_FLAG_RES` regexes over the revision text, written to `revision_signals.parquet` (Tier 2). No Postgres table.

## Multi-Domain Considerations

- Identity is `(domain_id, page_id)`; Parquet partitions carry the domain string.
- `build_all.py -d <dir> --domain <domain>`: one bundle directory per domain. RevisionChest's `--parquet` metadata carries no domain either, so the same `--domain` is passed when loading it in Stage 3.
- Template classification (Stages 1 and 4) uses `wikis.yaml`; UVA's shorthand-template list (`sfn|harv|harvnb|harvp|r|rp`) should move into `wikis.yaml` as `shorthand_templates` per domain.
- The UVA normalizers contain English-specific heuristics (author-date prose patterns, `en.wikipedia.org` URL handling); they run as-is for `en.wikipedia.org` and degrade to `prose_citation`/`noise` elsewhere. Per-domain tuning is out of scope for the initial build.

## Summary: What Changes, What Stays

| Component | Status | Notes |
|---|---|---|
| RevisionChest | **Modified (small)** | TIMESTAMP timestamps; drop title-keyed documents insert; write `page_metadata` |
| `build_all.py` | **Modified** | Pass `--domain` |
| `build_db.py` | **Modified** | citation_type, deterministic instance id, Tier-2 `citation_history`/`named_ref_uses`/`revision_signals` writers, no revisions/article rows |
| `refs_extractor/article.py` | **Modified** | Return candidate kind; self-closing → reuse events with offsets |
| `refs_normalizer/syntax.py` | **Unchanged** | |
| `dedup_parquet.py` | **Modified** | Domain in keys; collision gate |
| `load_all.py` | **Modified** | New columns; RevisionChest metadata load; revisions Parquet export; no documents/revisions writes |
| Stage 4 driver | **New (thin)** | Content extraction + split + dispatch to UVA normalizers → `normalized_citation_parts` |
| UVA normalizers | **Ported as-is** | + arXiv/QID template params |
| Stage 5 `ref_name_links` | **New** | Revision-range builder |