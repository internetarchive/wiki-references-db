# wiki-references-db
The `wiki-references-db` is a component in the third-generation Wikipedia Citations Database. It draws off of IARI <https://github.com/internetarchive/iari>, a service that analyzes references that appear on Wikipedia articles.

`wiki-references-db` builds a database of wiki articles (as identified by domain and page ID) and the strings of raw wikitext comprising the references that appear on them.
* "References" are defined broadly to include anything from bare external links to in-line citations (though the coverage of each will vary.)
* Reference strings are normalized before being hashed and stored in the database. Normalization eliminates unnecessary white space, alphabetizes the named parameters, turns underscores into spaces, and makes other stylistic transformations. The goal is to uniquely identify references on the basis of their content, treating two reference strings as the same if they otherwise mean the same thing.
* A reference is identifed by its `record_sha1` hash, made by taking the SHA-1 hash of the domain, numeric page ID, and normalized reference. This uniquely identifies the reference (accounting for variations in wikitext formatting) in the context of the original page and wiki it appeared on. Separately, a `reference_sha1` hash is available if you wish to search for the same reference string across articles regardless of context. (This might be useful to, for instance, look up certain calls of a Cite Q template.)
* This project avoids inferring semantics or other data attributes from the content of these reference strings. This builds the initial structure, and leaves the rest to derivation processes.
* The history of a reference's appearance on a page between revisions is tracked in the `citation_histories` table; you can use this to track a reference being inserted, removed, and re-inserted.

## Setup

1. Clone and enter the repository:

```
git clone https://github.com/internetarchive/wiki-references-db
cd wiki-references-db
```

2. Create and activate a virtual environment:

```
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies:

```
pip3 install -r requirements.txt
```

4. Create a `.env` file with your configuration (see `example.env` for all options):

```
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_db_user
DB_PASS=your_db_password
REVISION_BUNDLES_DIR=/path/to/revision/bundles
STAGING_DIR=./staging
```

5. Initialize the database schema (tables are defined via SQLAlchemy models):

```
python3 init_db.py
```

For bulk loading, create tables without secondary indexes first, then add them after loading (see `init_db.py` flags below).

## Pipeline Overview

The data pipeline has three phases that transform compressed revision bundles (`.mwrev.zst` files) into a populated PostgreSQL database:

```
build_all.py  →  dedup_parquet.py  →  load_all.py
 (Phase 1)         (Phase 1.5)        (Phase 2)
```

**Phase 1 — Extract & Stage (`build_all.py` / `build_db.py`):** Reads `.mwrev.zst` revision bundles, extracts and normalizes references, and writes the derived rows as Parquet files into a staging directory. This phase is CPU-bound and does not touch the database.

**Phase 1.5 — Deduplicate (`dedup_parquet.py`):** Consolidates the per-bundle staged Parquet files across all shards, removes duplicate rows using DuckDB, and writes a single deduplicated Parquet file per table into a `deduped/` subdirectory under the staging directory.

**Phase 2 — Load (`load_all.py`):** Reads the deduplicated Parquet files (via DuckDB) and bulk-inserts them into PostgreSQL, respecting foreign-key ordering. ON CONFLICT upserts handle residual duplicates or re-runs.

## Usage

### Phase 1: Stage derived data

Process all `.mwrev.zst` files in a directory with concurrent jobs:

```
python3 build_all.py -d /path/to/wiki/dumps -o ./staging --jobs 4
```

Or process a single file directly:

```
python3 build_db.py /path/to/file.mwrev.zst -o ./staging/my-shard
```

### Phase 1.5: Deduplicate staged files

```
python3 dedup_parquet.py -d ./staging
```

### Phase 2: Load into PostgreSQL

```
python3 load_all.py -d ./staging
```

## CLI Reference

### `build_all.py` (launcher)

Spawns one `build_db.py` subprocess per `.mwrev.zst` file, running up to `--jobs` in parallel. Skips shards that already have a `DONE.txt` marker and cleans up incomplete shards (those with `STARTED.txt` but no `DONE.txt`).

| Flag | Default | Description |
|------|---------|-------------|
| `-d, --directory` | *(required)* | Directory containing `.mwrev.zst` files |
| `-o, --staging-dir` | `STAGING_DIR` env or `./staging` | Directory to write staged Parquet files |
| `-j, --jobs` | `8` | Number of concurrent jobs |
| `--metrics-interval` | `METRICS_INTERVAL` env or `10` | Seconds between status prints |

Environment variable `BATCH_SIZE` (default `1000`) is forwarded to each `build_db.py` worker.

### `build_db.py` (worker)

Parses a single `.mwrev.zst` file and writes derived rows as Parquet files into the specified staging directory.

| Flag | Default | Description |
|------|---------|-------------|
| `file` | *(required, positional)* | Single `.mwrev.zst` file to process |
| `-o, --staging-dir` | *(required)* | Directory to write staged Parquet files |
| `--domain` | `en.wikipedia.org` | Wiki domain for curid URLs |
| `--batch-size` | `1000` | Revisions per processing batch |
| `--format` | `parquet` | Output format (`parquet` or `jsonl`) |
| `--worker-id` | `00` | Worker ID for parallel runs |

### `dedup_parquet.py`

Consolidates and deduplicates staged Parquet files across all shards using DuckDB. Writes one deduplicated Parquet file per table into the `deduped/` subdirectory. Skips tables that already have a done marker.

Resume-safety markers:
- `.done-<table>` indicates a completed table and is used for skipping on rerun.
- `.running-<table>` is written while a table is in progress; if found on restart, a stale-marker warning is logged and processing resumes.

`citation_histories` is handled as consolidation (pass-through) rather than global `DISTINCT` dedup. This avoids high-memory global dedup and relies on load-time conflict handling.

| Flag | Default | Description |
|------|---------|-------------|
| `-d, --staging-dir` | `STAGING_DIR` env or `./staging` | Staging directory containing raw Parquet files |
| `--memory-limit` | `DEDUP_MEMORY_LIMIT` env or `8GB` | DuckDB memory limit |
| `--temp-dir` | `DEDUP_TEMP_DIR` env or *(auto)* | DuckDB temp/spill directory |
| `--threads` | `DEDUP_THREADS` env or DuckDB default | DuckDB execution threads |
| `--preserve-insertion-order` | `DEDUP_PRESERVE_INSERTION_ORDER` env or `false` | DuckDB insertion-order preservation |
| `--max-temp-dir-size` | `DEDUP_MAX_TEMP_DIRECTORY_SIZE` env or DuckDB default | DuckDB max temp spill size |
| `--tables` | all tables | Only dedup these tables (space-separated) |

### `load_all.py`

Loads deduplicated Parquet files from the `deduped/` subdirectory into PostgreSQL using DuckDB for efficient reading.

| Flag | Default | Description |
|------|---------|-------------|
| `-d, --staging-dir` | `STAGING_DIR` env or `./staging` | Staging directory containing `deduped/` |
| `--batch-size` | `LOAD_BATCH_SIZE` env or `5000` | Rows per INSERT batch |
| `--tables` | all tables | Load only the specified table(s) |

### Other Scripts

| Script | Description |
|--------|-------------|
| `init_db.py` | Creates all database tables defined in `models.py` (see index flags below) |
| `purge.py` | Drops all database tables (destructive!) |
| `app.py` | Runs the Flask web application (API + Explorer UI) on port 12121 |

### Run Explorer with Gunicorn

After installing dependencies and configuring your `.env`, you can run the app (including the `/explorer/` UI) with Gunicorn:

```
gunicorn --bind 0.0.0.0:12121 app:app
```

Then open:

```
http://localhost:12121/explorer/
```

### `init_db.py`

Creates database tables and manages secondary indexes. By default, creates all tables with indexes.

| Flag | Description |
|------|-------------|
| `--table TABLE` | Create only the specified table |
| `--no-indexes` | Create tables without secondary (non-unique) indexes, for faster bulk loading |
| `--add-indexes` | Create secondary indexes on existing tables (run after bulk loading) |
| `--drop-indexes` | Drop secondary (non-unique) indexes from existing tables |

Optimized bulk-loading workflow:

```
python3 init_db.py --no-indexes          # 1. Create tables without secondary indexes
python3 build_all.py -d /path/to/dumps -o ./staging --jobs 4
python3 dedup_parquet.py -d ./staging
python3 load_all.py -d ./staging          # 2. Bulk load data
python3 init_db.py --add-indexes          # 3. Build indexes after loading
```

## Environment Variables

All environment variables are loaded from a `.env` file via `python-dotenv`. See `example.env` for a complete reference.

| Variable | Used By | Default | Description |
|----------|---------|---------|-------------|
| `DB_HOST` | load_all, app, init_db, purge | — | PostgreSQL host |
| `DB_PORT` | load_all, app, init_db, purge | — | PostgreSQL port |
| `DB_NAME` | load_all, app, init_db, purge | — | PostgreSQL database name |
| `DB_USER` | load_all, app, init_db, purge | — | PostgreSQL user |
| `DB_PASS` | load_all, app, init_db, purge | — | PostgreSQL password |
| `REVISION_BUNDLES_DIR` | — | — | Directory where `.mwrev.zst` bundle files are stored |
| `STAGING_DIR` | build_all, dedup_parquet, load_all | `./staging` | Directory for staged Parquet files |
| `BATCH_SIZE` | build_all → build_db | `1000` | Revisions per batch in build_db workers |
| `METRICS_INTERVAL` | build_all | `10` | Seconds between status prints |
| `DEDUP_MEMORY_LIMIT` | dedup_parquet | `8GB` | DuckDB memory limit for dedup |
| `DEDUP_TEMP_DIR` | dedup_parquet | — | DuckDB temp/spill directory |
| `DEDUP_THREADS` | dedup_parquet | DuckDB default | DuckDB execution threads |
| `DEDUP_PRESERVE_INSERTION_ORDER` | dedup_parquet | `false` | DuckDB insertion-order preservation |
| `DEDUP_MAX_TEMP_DIRECTORY_SIZE` | dedup_parquet | DuckDB default | DuckDB max temp spill size |
| `LOAD_BATCH_SIZE` | load_all | `5000` | Rows per INSERT batch when loading into Postgres |
| `WIKIPEDIA_API_USER_AGENT` | explorer | `WikiReferencesDB/1.0` | Primary product token used in MediaWiki API `User-Agent` headers |
| `WIKIPEDIA_API_CONTACT_EMAIL` | explorer | — | Contact email appended in parentheses in MediaWiki API `User-Agent` headers |
| `WIKIPEDIA_API_SECONDARY_USER_AGENT` | explorer | — | Optional secondary product token appended to the MediaWiki API `User-Agent` |

## Revision Bundles

This project loads `.mwrev.zst` files which are compressed bundles of MediaWiki revisions. These files are produced by RevisionChest. Set `REVISION_BUNDLES_DIR` in your `.env` to the directory where these bundle files are stored.
