# Database Model  
Wikipedia Citations Database

---

# Overview

This document describes the current database model for the Wikipedia Citations Database pipeline.

The model is designed around a layered architecture:

1. **Revision-level evidence**
2. **Identifier-level observations**
3. **Normalized identifiers**
4. **Canonical source entities**
5. **Observation-to-source mappings**

This separation allows the pipeline to scale, preserve provenance, and support future extensions such as fuzzy matching, structure-aware linking, and citation graph analysis.

---

# Design Principles

## 1. Preserve provenance
Every layer retains a path back to:
- revision (`rev_id`)
- page (`page_id`)

## 2. Separate observation from identity
The model distinguishes between:
- what was observed in a revision
- how it was normalized
- which canonical source it maps to

## 3. Deterministic first
The current canonicalization model is based on exact normalized identifiers.

## 4. Support future expansion
The model is designed to support later additions such as:
- parser-based structure extraction
- shorthand citation resolution
- fingerprinting and candidate matching
- fuzzy canonicalization

---

# Entity Relationship Summary

```text
revisions_extracted
        │
        │ explode identifier arrays
        ▼
citations_observed
        │
        │ normalize values
        ▼
citations_normalized
        │
        │ exact canonicalization
        ├──────────────► sources_canonical
        │
        └──────────────► observation_source_map
```

---

# Core Tables

---

## 1. revisions_extracted

### Purpose
Stores the raw revision-level output from the fast extraction stage.

### Grain
**One row per Wikipedia revision**

### Role
This is the first structured representation of extracted citation evidence.

### Key Fields

| Column | Description |
|---|---|
| `page_id` | Wikipedia page identifier |
| `ns` | Namespace |
| `rev_id` | Revision identifier |
| `parent_rev_id` | Parent revision identifier |
| `timestamp` | Revision timestamp |
| `user_id` | User identifier |
| `has_ref_tag` | Whether `<ref>` appears in the revision |
| `has_references_tag` | Whether `<references>` appears |
| `has_cite_template` | Whether a `cite` template pattern appears |
| `has_citation_template` | Whether a `citation` template pattern appears |
| `has_named_ref` | Whether named refs appear |
| `has_self_closing_ref` | Whether self-closing refs appear |
| `has_shorthand_template` | Whether shorthand citation templates appear |
| `urls` | Array of extracted URLs |
| `dois` | Array of extracted DOIs |
| `isbns` | Array of extracted ISBNs |
| `pmids` | Array of extracted PMIDs |
| `pmcs` | Array of extracted PMCs |
| `arxivs` | Array of extracted arXiv IDs |

### Notes
- This table is revision-centric, not citation-centric.
- Identifier arrays are exploded in the next stage.

---

## 2. citations_observed

### Purpose
Stores one row per observed identifier extracted from a revision.

### Grain
**One row per identifier observation**

### Role
This is the first long-form citation table.

### Key Fields

| Column | Description |
|---|---|
| `obs_id` | Unique observation identifier |
| `page_id` | Wikipedia page identifier |
| `ns` | Namespace |
| `rev_id` | Revision identifier |
| `parent_rev_id` | Parent revision identifier |
| `timestamp` | Revision timestamp |
| `user_id` | User identifier |
| `has_ref_tag` | Structural flag from revision |
| `has_references_tag` | Structural flag from revision |
| `has_cite_template` | Structural flag from revision |
| `has_citation_template` | Structural flag from revision |
| `has_named_ref` | Structural flag from revision |
| `has_self_closing_ref` | Structural flag from revision |
| `has_shorthand_template` | Structural flag from revision |
| `citation_type` | Identifier type (`url`, `doi`, `isbn`, `pmid`, `pmc`, `arxiv`) |
| `citation_value_raw` | Raw observed identifier value |

### Notes
- This table preserves revision-level provenance.
- `obs_id` is the stable key for downstream transformations.

---

## 3. citations_normalized

### Purpose
Stores normalized versions of observed identifiers.

### Grain
**One row per identifier observation**  
(same grain as `citations_observed`)

### Role
Provides deterministic cleaned identifiers for grouping and exact canonicalization.

### Key Fields

| Column | Description |
|---|---|
| `obs_id` | Observation identifier |
| `page_id` | Wikipedia page identifier |
| `ns` | Namespace |
| `rev_id` | Revision identifier |
| `parent_rev_id` | Parent revision identifier |
| `timestamp` | Revision timestamp |
| `user_id` | User identifier |
| `has_ref_tag` | Structural flag |
| `has_references_tag` | Structural flag |
| `has_cite_template` | Structural flag |
| `has_citation_template` | Structural flag |
| `has_named_ref` | Structural flag |
| `has_self_closing_ref` | Structural flag |
| `has_shorthand_template` | Structural flag |
| `citation_type` | Identifier type |
| `citation_value_raw` | Raw observed value |
| `citation_value_norm` | Normalized value |
| `normalization_status` | `normalized` or `failed` |
| `normalization_rule` | Rule used or failure category |

### Notes
- This table is still observation-level.
- Normalization does not yet imply source identity.

---

## 4. sources_canonical

### Purpose
Stores canonical source entities based on exact normalized identifier matching.

### Grain
**One row per canonical source**

### Role
Represents the current source identity layer.

### Key Fields

| Column | Description |
|---|---|
| `canonical_source_id` | Canonical source identifier |
| `source_type` | Source type (`url`, `doi`, `isbn`, `pmid`, `pmc`, `arxiv`) |
| `canonical_key` | Exact normalized identifier used for grouping |
| `canonical_status` | Current status (`exact`) |
| `canonical_rule` | Rule used to create canonical source |
| `supporting_observation_count` | Number of observations supporting this source |
| `supporting_revision_count` | Number of distinct revisions containing this source |
| `supporting_page_count` | Number of distinct pages containing this source |

### Notes
- This is the current canonical source layer.
- It is exact-match only.
- No fuzzy matching is applied here yet.

---

## 5. observation_source_map

### Purpose
Links identifier observations to canonical sources.

### Grain
**One row per observation-to-source mapping**

### Role
Preserves the distinction between observation and entity.

### Key Fields

| Column | Description |
|---|---|
| `obs_id` | Observation identifier |
| `canonical_source_id` | Canonical source identifier |
| `mapping_method` | Method used (`exact_identifier`) |
| `mapping_confidence` | Confidence score (currently `1.0`) |
| `mapping_rule` | Rule used for mapping |

### Notes
- This table is essential for traceability.
- It allows future remapping without changing the raw observation tables.

---

# Relationships

## revisions_extracted → citations_observed
- One revision can produce many identifier observations.
- Relationship: **one-to-many**

## citations_observed → citations_normalized
- Each observed identifier maps to one normalized record.
- Relationship: **one-to-one**

## citations_normalized → sources_canonical
- Many normalized observations can refer to the same canonical source.
- Relationship: **many-to-one**

## citations_normalized → observation_source_map
- Each normalized observation may map to one canonical source.
- Relationship: **one-to-one** in the current exact-match model

## sources_canonical → observation_source_map
- One canonical source can be linked to many observations.
- Relationship: **one-to-many**

---

# Current Model Diagram

# Updated Data Model Diagram

```text
┌──────────────────────────────────────────────────────────────────────┐
│                        revisions_extracted                          │
│----------------------------------------------------------------------│
│ Grain: one row per revision                                          │
│                                                                      │
│ page_id                                                              │
│ ns                                                                   │
│ rev_id                                                               │
│ parent_rev_id                                                        │
│ timestamp                                                            │
│ user_id                                                              │
│                                                                      │
│ has_ref_tag                                                          │
│ has_references_tag                                                   │
│ has_cite_template                                                    │
│ has_citation_template                                                │
│ has_named_ref                                                        │
│ has_self_closing_ref                                                 │
│ has_shorthand_template                                               │
│                                                                      │
│ urls[]                                                               │
│ dois[]                                                               │
│ isbns[]                                                              │
│ pmids[]                                                              │
│ pmcs[]                                                               │
│ arxivs[]                                                             │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                │ explode identifier arrays
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│                         citations_observed                          │
│----------------------------------------------------------------------│
│ Grain: one row per identifier observation                            │
│                                                                      │
│ obs_id (PK)                                                          │
│                                                                      │
│ page_id                                                              │
│ ns                                                                   │
│ rev_id                                                               │
│ parent_rev_id                                                        │
│ timestamp                                                            │
│ user_id                                                              │
│                                                                      │
│ has_ref_tag                                                          │
│ has_references_tag                                                   │
│ has_cite_template                                                    │
│ has_citation_template                                                │
│ has_named_ref                                                        │
│ has_self_closing_ref                                                 │
│ has_shorthand_template                                               │
│                                                                      │
│ citation_type                                                        │
│ citation_value_raw                                                   │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                │ normalize identifier values
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        citations_normalized                         │
│----------------------------------------------------------------------│
│ Grain: one row per identifier observation                            │
│                                                                      │
│ obs_id (PK/FK)                                                       │
│                                                                      │
│ page_id                                                              │
│ ns                                                                   │
│ rev_id                                                               │
│ parent_rev_id                                                        │
│ timestamp                                                            │
│ user_id                                                              │
│                                                                      │
│ has_ref_tag                                                          │
│ has_references_tag                                                   │
│ has_cite_template                                                    │
│ has_citation_template                                                │
│ has_named_ref                                                        │
│ has_self_closing_ref                                                 │
│ has_shorthand_template                                               │
│                                                                      │
│ citation_type                                                        │
│ citation_value_raw                                                   │
│ citation_value_norm                                                  │
│ normalization_status                                                 │
│ normalization_rule                                                   │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                │ exact canonicalization
                    ┌───────────┴────────────────┐
                    │                            │
                    ▼                            ▼
┌──────────────────────────────────────┐   ┌───────────────────────────┐
│          sources_canonical           │   │   observation_source_map  │
│--------------------------------------│   │---------------------------│
│ Grain: one row per canonical source  │   │ Grain: one row per        │
│                                      │   │ observation-to-source map │
│ canonical_source_id (PK)             │   │                           │
│ source_type                          │   │ obs_id (FK)               │
│ canonical_key                        │   │ canonical_source_id (FK)  │
│ canonical_status                     │   │ mapping_method            │
│ canonical_rule                       │   │ mapping_confidence        │
│ supporting_observation_count         │   │ mapping_rule              │
│ supporting_revision_count            │   └───────────────────────────┘
│ supporting_page_count                │
└──────────────────────────────────────┘
```

---

# Current Constraints and Limitations

## 1. Identifier-centric model
The current model captures identifier observations, not full parsed citation boundaries.

## 2. Exact canonicalization only
Canonical sources currently rely on exact normalized identifier matches.

## 3. No structure-aware resolution yet
Named refs, shorthand templates, and citation template argument structure are not yet integrated into canonicalization.

## 4. Fingerprinting is not active in the production path
Fingerprinting has been moved to legacy and may return later as a feature layer for unresolved cases.

---

# Planned Future Extensions

---

## structure_extracted (future)
Possible future table for parser-based enrichment on sampled or targeted revisions.

Potential fields:
- `rev_id`
- `named_ref_key`
- `ref_body`
- `template_type`
- `author_tokens`
- `year_token`
- `title_tokens`

---

## citation_match_features (future)
Possible future table for fingerprinting / candidate matching.

Potential fields:
- `obs_id`
- `fingerprint_type`
- `fingerprint_value`
- `feature_version`

---

## canonical_source_candidates (future)
Possible future table for fuzzy or heuristic candidate matches.

Potential fields:
- `obs_id`
- `candidate_canonical_source_id`
- `candidate_method`
- `candidate_score`

---

# Summary

The current database model supports a clean and scalable workflow:

- revision evidence is preserved
- observations are explicit
- normalization is deterministic
- canonicalization is exact and traceable
- future fuzzy and structure-aware methods can be added without redesigning the core model

This makes the model appropriate both for:
- large-scale pipeline execution
- future research and enrichment
- downstream analytics and source graph construction

---