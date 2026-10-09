# Pipeline Documentation  
Wikipedia Citations Database

---

# Overview

This document describes the end-to-end data pipeline for extracting, normalizing, and canonicalizing citations from Wikipedia RevisionChest dumps.

The pipeline is designed for:

- scalability (HPC + Slurm)
- high recall extraction
- clear separation of stages
- reproducibility
- preservation of provenance

---

# High-Level Pipeline

```text
RevisionChest (.mwrev.zst)
        │
        ▼
Stage 1 – Fast Extraction (regex, streaming)
        │
        ▼
JSONL (1 row per revision)
        │
        ▼
Stage 2 – Ingestion (DuckDB)
        │
        ▼
revisions_extracted
        │
        ▼
Stage 3 – Observed Citations
        │
        ▼
citations_observed
        │
        ▼
Stage 4 – Normalization
        │
        ▼
citations_normalized
        │
        ▼
Stage 5 – Canonicalization
        │
        ▼
sources_canonical + observation_source_map
```

---

# Detailed Pipeline Stages

---

## Stage 1 – Fast Citation Extraction

### Purpose
Extract citation identifiers and structural signals from raw Wikipedia revisions.

### Approach
- streaming processing (one revision at a time)
- regex-based extraction
- avoids `mwparserfromhell` for performance

### Input
- `.mwrev.zst` RevisionChest shards

### Output
- JSONL file (one row per revision)

### Output Fields
- revision metadata (page_id, rev_id, timestamp)
- structural flags:
  - has_ref_tag
  - has_named_ref
  - has_shorthand_template
- identifier arrays:
  - urls[]
  - dois[]
  - isbns[]
  - pmids[]
  - pmcs[]
  - arxivs[]

### Execution
```bash
sbatch scripts/submit_array.sh configs/mwrev_three_test.txt
```

### Output Location
```
/scratch/$USER/citations_output/<job_id>/
```

---

## Stage 2 – JSONL → DuckDB Ingestion

### Purpose
Load extracted revision-level data into DuckDB.

### Input
- JSONL files from Stage 1

### Output
- `revisions_extracted` table

### Execution
```bash
python src/wcd_ingest/ingest_jsonl_to_duckdb.py \
  --job-id <JOB_ID> \
  --db "/scratch/$USER/wcd_db/wcd.duckdb"
```

---

## Stage 3 – Build citations_observed

### Purpose
Convert revision-level arrays into row-level citation observations.

### Transformation
- explode identifier arrays into individual rows

### Output Table
`citations_observed`

### Structure
- obs_id
- page_id
- rev_id
- citation_type
- citation_value_raw

### Execution
```bash
python src/wcd_ingest/build_citations_observed.py \
  --db "/scratch/$USER/wcd_db/wcd.duckdb"
```

---

## Stage 4 – Normalization

### Purpose
Standardize identifiers for consistent grouping.

### Normalization Rules
- DOI normalization (case, prefix, formatting)
- ISBN cleanup (hyphens, formatting)
- URL cleanup (protocol, trailing slashes, noise)
- PMID / PMC / arXiv standardization

### Output Table
`citations_normalized`

### Fields
- citation_value_norm
- normalization_status
- normalization_rule

### Execution
```bash
python src/wcd_ingest/build_citations_normalized.py \
  --db "/scratch/$USER/wcd_db/wcd.duckdb"
```

---

## Stage 5 – Canonicalization (Exact Match)

### Purpose
Create canonical source entities using deterministic grouping.

### Method
Group by:
- citation_type
- citation_value_norm

### Outputs

### sources_canonical
- canonical_source_id
- source_type
- canonical_key
- supporting_observation_count
- supporting_revision_count
- supporting_page_count

### observation_source_map
- obs_id
- canonical_source_id

### Execution
```bash
python src/wcd_ingest/build_sources_canonical.py \
  --db "/scratch/$USER/wcd_db/wcd.duckdb" \
  --temp-dir "/scratch/$USER/duckdb_tmp"
```

---

# Key Design Principles

## 1. Scalability
- Stage 1 avoids heavy parsing
- Slurm array jobs parallelize extraction
- DuckDB handles large-scale analytics

## 2. Separation of Concerns
Each stage is independent:
- extraction
- ingestion
- transformation
- normalization
- canonicalization

## 3. Deterministic Identity First
- canonicalization is exact-match only
- avoids premature fuzzy matching
- provides clean baseline

## 4. Provenance Preservation
All stages retain:
- revision-level linkage (rev_id)
- page-level linkage (page_id)

---

# Structure Extraction (Future / Sidecar)

A secondary pipeline will:

- use `mwparserfromhell`
- run on filtered or sampled revisions
- extract:
  - named references
  - shorthand citations (sfn, harv)
  - citation templates

This will support:

- advanced canonicalization
- citation graph enrichment

---

# Legacy Components

Moved to `legacy/`:

- fingerprint-based pipeline
- parser-based full extraction

These are retained for reference but not used in the production path.

---

# Current Status

- Fast extractor validated on multiple shards
- ~68M citation observations processed
- normalization successfully reduces identifier variance
- ~150K canonical sources created
- ~68M observations mapped to canonical sources

---

# Next Steps

## Near-Term
- validation queries on canonical sources
- analysis of unmatched observations
- scaling to larger shard counts

## Future
- fuzzy canonicalization / entity resolution
- structure-aware citation linking
- graph construction and analytics

---