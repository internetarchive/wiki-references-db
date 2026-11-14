# UVA Wikipedia Citations Database

UVA Spring 2026 Capstone Project. Extracting, normalizing, and resolving citations across Wikipedia revision history at scale.

Wikipedia is one of the most widely used sources of information in the world, yet the structure, quality, and verifiability of its citations vary widely across articles and over time. This project aims to contribute to the development of a comprehensive Wikipedia Citations Database by extracting, normalizing, and validating citations across Wikipedia article revisions. Using large-scale Wikipedia revision dumps, the project focuses on identifying what constitutes a citation, resolving shorthand and ambiguous references, and inferring real-world sources from free-text citations using rule-based, heuristic, and fuzzy matching/model assisted approaches. The resulting workflow and tools will support efforts to improve citation reliability, link persistence, and provenance tracking across Wikipedia, while producing a scalable methodology that can be applied to larger datasets in the future.

---

## Contents

- [Setting Up Environment](#setting-up-our-environment)
- [Pipeline Overview](#pipeline-overview)
- [Running the Pipeline](#running-the-pipeline)
  - [Stage 1: Extraction (HPC)](#stage-1-extraction-hpc)
  - [Stage 2–6: Database Pipeline](#stage-26-database-pipeline)
- [Frontend](#frontend)
- [Pipeline Scripts Reference](#pipeline-scripts-reference)
- [Repository Structure](#repository-structure)
- [Connecting Rivanna to GitHub](#connecting-your-rivanna-session-to-the-main-github-repo)

---

## Setting Up Our Environment

Currently using conda to manage environments.

Clone this repo. For Rivanna, use SSH:
```bash
git clone git@github.com:scatter-llc/UVAWikipediaCitationsDatabase.git
cd UVAWikipediaCitationsDatabase
```

Load conda on Rivanna:
```bash
module load miniforge/24.11.3-py3.12
```

Create and activate the environment:
```bash
conda create -n wiki_citation -y
conda activate wiki_citation
conda install python=3.12 -y
pip install -r requirements.txt
```

For JupyterLab kernel support on Rivanna:
```bash
python -m ipykernel install --name wiki_citation --display-name wiki_citation --prefix ~/.local
```

---

## Pipeline Overview

```
RevisionChest shards (.mwrev.zst)
        │
        ▼
Stage 1 - Extraction          citation_extractor.py
        │                     Streaming regex-based extraction
        ▼
JSONL.zst (1 row per citation per revision)
        │
        ▼
Stage 2 - Ingestion           ingest_to_duckdb.py
        │                     DuckDB table: citations_extracted
        ▼
Stage 3 - Normalization       reftag_normalizer.py
        │                     template_normalizer.py
        │                     Tables: ref_tag_normalized, template_normalized
        │                             ref_name_links, template_ref_name_links
        ▼
Stage 4 - Matching            pipeline_runner.py
        │                     Per-page: ref_tag matching → template matching
        │                               → cross-type merge
        │                     Tables: sources, citation_source_map,
        │                             template_sources, template_citation_map,
        │                             merged_sources
        ▼
Stage 5 - History             cross_page_matcher.py + citation_history.py
        │                     Global deduplication across all pages
        │                     Tables: global_sources, global_source_map,
        │                             page_source_summary, page_revision_stats
        │                     View:   citations_resolved
        ▼
Stage 6 - Prune               database_prune.py
                              Removes intermediate rows to reduce DB size
```

### Citation Types

Citations are classified at extraction time as:

- `template` : `{{cite book}}`, `{{cite journal}}`, `{{cite web}}`, etc. Fully structured params extracted via mwparserfromhell and cached.
- `ref_tag` : plain prose citations, shorthand templates (`{{harvnb}}`, `{{sfn}}`), bare URLs, and wikilink refs. Stored as raw text for normalization.

### Ref-tag Subtypes

| Subtype | Example |
|---|---|
| `prose_citation` | `Haley, The Spirit of Japanese Law, page 63` |
| `bare_url` | `[https://bbc.com/article BBC News]` |
| `wikilink_ref` | `[[#schiff2006\|Schiff, 2006]], p. 5` |
| `shorthand_template` | `{{harvnb\|Ward\|2004\|p=62}}` |
| `author_date` | `Smith (2003) p. 45` |
| `italicized_citation` | `Heider, ''Anarchism: Left, Right and Green'', AK Press, 1994` |
| `cite_html` | `<cite class="citation book">...</cite>` |
| `noise` | `ibid`, `wiki` |

### Hard Identifiers Extracted

| Identifier | Description |
|---|---|
| URL | External hyperlinks |
| DOI | Digital Object Identifiers |
| ISBN | Book identifiers |
| PMID | PubMed identifiers |
| PMC | PubMed Central identifiers |
| arXiv | Preprint identifiers |

### Soft Metadata Fields

Author surname, publication year, and title. They are used for fuzzy matching when hard identifiers are absent.

---

## Running the Pipeline

### Stage 1: Extraction (HPC)

Extraction runs as a Slurm job array with one task per shard, and all tasks run in parallel.

**Step 1 — Build the shard file list:**
```bash
# Use absolute paths
realpath /path/to/shards/*.mwrev.zst > configs/mwrev_files.txt
cat configs/mwrev_files.txt   # verify
```

**Step 2 — Submit the array job:**
```bash
./scripts/submit_array.sh configs/mwrev_files.txt
```

Optional environment overrides:
```bash
PARTITION=standard \
ACCOUNT=msds_ds6015 \
MAX_CONCURRENT=3 \
TIME_LIMIT=2:00:00 \
CPUS_PER_TASK=4 \
MEM_PER_TASK=32G \
./scripts/submit_array.sh configs/mwrev_files.txt
```

**Monitor progress:**
```bash
# Watch live logs
tail -f logs/cite_extract_<ArrayJobID>_*.out

# Check completion
ls /scratch/$USER/citations_output/<ArrayJobID>/*.done | wc -l

# Check job states
sacct -j <ArrayJobID> --format=JobID,State,Elapsed
```

Each shard produces three files:
```
*.citations.jsonl.zst ← citation data (feeds Stage 2)
*.citations.jsonl.zst.done ← resume marker
*.citations.jsonl.zst.meta.json ← provenance (git SHA, timestamps, exit code)
```

---

### Stage 2–6: Database Pipeline

**Default database path**: if `DB_PATH` is not explicitly set, all stage 
scripts default to `./wcd_db/wcd.duckdb` relative to the repository root. 
Always pass `DB_PATH` explicitly when running any stage script to avoid 
unintended fallback behavior.

All database stages are orchestrated by a single driver script.

**Replace mode. This builds DB from scratch if first run:**
```bash
DB_MODE=replace \
INPUT_GLOB="/scratch/$USER/citations_output/<ArrayJobID>/*.citations.jsonl.zst" \
DB_PATH="./wcd_db/wcd.duckdb" \
./scripts/run_database_pipeline.sh
```

- **Replace mode** rebuilds the database from scratch using the provided input ZST files. **REPLACE MODE WILL WIPE ALL INGESTED DATA** : only pages present in the input glob will exist in the database after ingest completes. If you need to rerun normalization or matching without re-ingesting, use `SKIP_INGEST=1` to preserve the existing`citations_extracted` table and only rerun downstream stages.

**Append run. This adds new shard(s) to existing DB:**
```bash
DB_MODE=append \
INPUT_GLOB="/scratch/$USER/citations_output/<NewJobID>/*.citations.jsonl.zst" \
DB_PATH="./wcd_db/wcd.duckdb" \
./scripts/run_database_pipeline.sh
```

In append mode, the log system automatically skips already-processed pages at every stage. Only new shard pages are normalized and matched. Cross-page matching and history always rerun globally.

**Skip individual stages if needed:**
```bash
SKIP_INGEST=1 \
SKIP_NORM=1 \
DB_MODE=replace \
DB_PATH="./wcd_db/wcd.duckdb" \
./scripts/run_database_pipeline.sh
```

---

## Frontend

The analytics frontend is a small static HTML/CSS/JS app served directly by the FastAPI analytics service from `src/analytics_service/frontend/`.

Run it locally with:
```bash
DB_PATH=./output/citations_v8.duckdb \
uvicorn src.analytics_service.main:app --reload
```

Then open [http://127.0.0.1:8000](http://127.0.0.1:8000). If your DuckDB file lives somewhere else, point `DB_PATH` at that file instead.

---

## Pipeline Scripts Reference

| Script | Purpose |
|---|---|
| `scripts/submit_array.sh` | Submit Slurm job array for extraction |
| `scripts/run_extract_array.sh` | Worker script: runs extractor on one shard |
| `scripts/run_stage2_ingest.sh` | Load JSONL.zst into DuckDB |
| `scripts/run_stage3_normalize.sh` | Run ref-tag and template normalization |
| `scripts/run_stage4_matching.sh` | Run per-page matching pipeline |
| `scripts/run_stage5_history.sh` | Run cross-page matching and citation history |
| `scripts/run_database_pipeline.sh` | Full orchestrator for stages 2–6 |
| `src/pipeline_log.py` | Log system for crash recovery and append skipping |

### Log Files

The pipeline tracks completed pages per stage in `output/logs/`:

```
output/logs/ingest_completed.txt
output/logs/reftag_norm_completed.txt
output/logs/tmpl_norm_completed.txt
output/logs/matching_completed.txt
output/logs/history_completed.txt
```

On append runs, stages skip pages already listed in their log file. On replace runs, logs are cleared and all pages reprocess. If a run crashes mid-way, it resumes from the last completed page on the next run.

To force reprocess a single page:
```bash
# Remove it from the relevant log
sed -i '/^12345$/d' output/logs/matching_completed.txt
```

---

## Repository Structure

```
configs/
  mwrev_files.txt             shard file list for array jobs

scripts/
  submit_array.sh             Slurm array job submitter
  run_extract_array.sh        per-shard extraction worker
  run_stage2_ingest.sh        ingest stage
  run_stage3_normalize.sh     normalization stage
  run_stage4_matching.sh      matching stage
  run_stage5_history.sh       history stage
  run_database_pipeline.sh    full pipeline driver

src/
  pipeline_log.py             shared log system
  wcd_extract/
    citation_extractor.py     streaming citation extractor
  wcd_ingest/
    ingest_to_duckdb.py       DuckDB ingestion
  citation_normalize/
    reftag_normalizer.py      ref-tag normalization
    template_normalizer.py    template normalization
  citation_matching/
    pipeline_runner.py        per-page matching orchestrator
    ref_tag_matching.py       ref-tag source matching
    template_matching.py      template source matching
    cross_type_merger.py      ref-tag + template cross-type merge
    cross_page_matcher.py     global cross-page deduplication
  citation_history/
    citation_history.py       citation history aggregation
  database_prune.py           intermediate table pruning

output/
  logs/                       pipeline completion logs
  *.duckdb                    DuckDB databases

configs/
  mwrev_files.txt             shard paths for extraction
```

---

## Key Design Decisions

**Streaming extraction**: revisions are processed one at a time with no full-text buffering. Uses regex over raw wikitext rather than full parsing, trading some recall on edge cases for the throughput needed to process hundreds of millions of revisions.

**Multi-citation block splitting**: ref tags containing bullet-delimited lists of multiple citations are detected and split into individual records at extraction time, each processed independently through the full classification pipeline.

**Separation of concerns**: extraction, normalization, and matching are fully independent stages. Each can be rerun or replaced without affecting the others.

**DuckDB**: columnar analytical database used throughout. Handles multi-gigabyte tables efficiently without a server process.

**Union-Find matching**: citations are grouped into sources using union-find with signal passes in descending confidence order: DOI (1.00) → ISBN (0.95) → PMID (0.95) → ref_name (0.90) → URL (0.85) → author+year (0.65) → fuzzy title (0.60).

**Source reliability scoring**: each global source is assigned a score (0–100) and a verdict based on: template presence (+30), hard identifier tier (+25), author key (+15), reliable year (+15), year presence (+5), and match signal quality (+3–10). Sources matched to or originating from a template receive an immediate `high_confidence` verdict. Verdicts: `high_confidence`, `medium_confidence`, `low_confidence`, `questionable`, `likely_not_citation`. Scores are computed once per global source in `cross_page_matcher.py` and stored on `global_sources`.

**Fuzzy title matching**: a three-pass fuzzy matcher bridges ref_tag prose citations and structured templates when hard identifiers are absent: title alone, author+title combined, and strong author with weak title. When an italicized_citation title has been extracted, title-to-title comparison is used at a tighter threshold (80) rather than title-to-full-content (65), improving precision.

**Provenance preservation**: every stage retains revision-level and page-level lineage throughout, enabling full citation history reconstruction.

**Crash recovery**: the log system records completed pages per stage. A crashed run resumes from the last completed page on the next run with no duplicate processing.

---

## Current Pipeline Results (5 shards)

| Metric | Value |
|---|---|
| Total citation rows | 127,999,215 |
| Pages processed | 77,290 |
| Unique revisions | 3,008,253 |
| Unique ref-tag strings | 305,784 |
| Template resolution rate | 100% |
| Ref-tag identifier coverage | 64.5% |
| Global sources | 931,038 |
| Multi-page sources | 17,569 (1.9%) |
| Cross-type matches | 56,238 |
| High confidence sources | 94.6% |
| Citations linked to global source | 89.1% |

---

## Connecting Your Rivanna Session to the Main GitHub Repo

Accept the collaborator invitation for the GitHub repo, then:

```bash
# Generate SSH key on Rivanna
ssh-keygen -t ed25519 -C "your@uva.edu"

# Copy the public key
cat ~/.ssh/id_ed25519.pub
```

Go to https://github.com/settings/keys, click **New SSH key**, and paste the key.

Back on Rivanna:
```bash
git remote set-url origin git@github.com:scatter-llc/UVAWikipediaCitationsDatabase.git
```

You can now push and pull normally.

---

## Authors

UVA MSDS Capstone Project - Spring 2026

Wikipedia Citations Database
