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

## Pipeline Overview

The data pipeline has three phases that transform compressed revision bundles (`.mwrev.zst` files) into a populated PostgreSQL database:

```
build_all.py  →  dedup_staged.py  →  load_all.py
 (Phase 1)         (Phase 1.5)        (Phase 2)
```

**Phase 1 — Derive & Stage (`build_all.py` / `build_db.py`):** Reads `.mwrev.zst` revision bundles, extracts and normalizes references, and writes the derived rows as compressed JSONL files (`*.jsonl.zst`) into a staging directory. This phase is CPU-bound and does not touch the database.

**Phase 1.5 — Deduplicate (`dedup_staged.py`):** Consolidates the per-bundle staged files across all shards, removes duplicate rows using an ephemeral SQLite index, and writes deduplicated shards into a `deduped/` subdirectory under the staging directory.

**Phase 2 — Load (`load_all.py`):** Reads the deduplicated JSONL files and bulk-inserts them into PostgreSQL, respecting foreign-key ordering. ON CONFLICT upserts handle residual duplicates or re-runs.

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
python3 dedup_staged.py -d ./staging
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
| `-o, --staging-dir` | `STAGING_DIR` env or `./staging` | Directory to write staged JSONL.zst files |
| `-j, --jobs` | `8` | Number of concurrent jobs |
| `--metrics-interval` | `METRICS_INTERVAL` env or `10` | Seconds between status prints |

Environment variable `BATCH_SIZE` (default `1000`) is forwarded to each `build_db.py` worker.

### `build_db.py` (worker)

Parses a single `.mwrev.zst` file and writes derived rows as JSONL.zst files into the specified staging directory.

| Flag | Default | Description |
|------|---------|-------------|
| `file` | *(required, positional)* | Single `.mwrev.zst` file to process |
| `-o, --staging-dir` | *(required)* | Directory to write staged JSONL.zst files |
| `--domain` | `en.wikipedia.org` | Wiki domain for curid URLs |
| `--batch-size` | `1000` | Revisions per processing batch |

### `dedup_staged.py`

Consolidates and deduplicates staged JSONL.zst files across all shards. Uses a temporary SQLite database to track seen keys per table.

| Flag | Default | Description |
|------|---------|-------------|
| `-d, --staging-dir` | `STAGING_DIR` env or `./staging` | Staging directory to read from |
| `--shard-size` | `DEDUP_SHARD_SIZE` env or `2000000` | Max rows per output shard file |
| `--batch-size` | `DEDUP_BATCH_SIZE` env or `10000` | Rows per SQLite INSERT OR IGNORE batch |
| `--tables` | all tables | Only dedup these tables (space-separated) |

### `load_all.py`

Loads deduplicated staged files from the `deduped/` subdirectory into PostgreSQL.

| Flag | Default | Description |
|------|---------|-------------|
| `-d, --staging-dir` | `STAGING_DIR` env or `./staging` | Staging directory containing `deduped/` |
| `--batch-size` | `LOAD_BATCH_SIZE` env or `5000` | Rows per INSERT batch |

### Other Scripts

| Script | Description |
|--------|-------------|
| `init_db.py` | Creates all database tables defined in `models.py` |
| `purge.py` | Drops all database tables (destructive!) |
| `app.py` | Runs the Flask web application (API + Explorer UI) on port 12121 |

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
| `STAGING_DIR` | build_all, dedup_staged, load_all | `./staging` | Directory for staged JSONL.zst files |
| `BATCH_SIZE` | build_all → build_db | `1000` | Revisions per batch in build_db workers |
| `METRICS_INTERVAL` | build_all | `10` | Seconds between status prints |
| `DEDUP_SHARD_SIZE` | dedup_staged | `2000000` | Max rows per deduplicated output shard |
| `DEDUP_BATCH_SIZE` | dedup_staged | `10000` | Rows per SQLite INSERT OR IGNORE batch |
| `LOAD_BATCH_SIZE` | load_all | `5000` | Rows per INSERT batch when loading into Postgres |

## Revision Bundles

This project loads `.mwrev.zst` files which are compressed bundles of MediaWiki revisions. These files are produced by RevisionChest. Set `REVISION_BUNDLES_DIR` in your `.env` to the directory where these bundle files are stored.
