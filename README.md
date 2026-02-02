# wiki-references-db
The `wiki-references-db` is a component in the third-generation Wikipedia Citations Database. It draws off of IARI <https://github.com/internetarchive/iari>, a service that analyzes references that appear on Wikipedia articles.

`wiki-references-db` builds a database of wiki articles (as identified by domain and page ID) and the strings of raw wikitext comprising the references that appear on them.
* "References" are defined broadly to include anything from bare external links to in-line citations (though the coverage of each will vary.)
* Reference strings are normalized before being hashed and stored in the database. Normalization eliminates unnecessary white space, alphabetizes the named parameters, turns underscores into spaces, and makes other stylistic transformations. The goal is to uniquely identify references on the basis of their content, treating two reference strings as the same if they otherwise mean the same thing.
* A reference is identifed by its `record_sha1` hash, made by taking the SHA-1 hash of the domain, numeric page ID, and normalized reference. This uniquely identifies the reference (accounting for variations in wikitext formatting) in the context of the original page and wiki it appeared on. Separately, a `reference_sha1` hash is available if you wish to search for the same reference string across articles regardless of context. (This might be useful to, for instance, look up certain calls of a Cite Q template.)
* This project avoids inferring semantics or other data attributes from the content of these reference strings. This builds the initial structure, and leaves the rest to derivation processes.
*  The `wikireferences` table tracks both present and historical references for a given page. You can distinguish between present and former references by the `latest_revision` column. The history of a reference's appearance on a page between revisions is tracked in the `history` table; you can use this to track a reference being inserted, removed, and re-inserted.

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

4. Create a `.env` file with your database connection details (loaded via `python-dotenv`):

```
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=your-database-name
DB_USER=your-database-username
DB_PASS=your-database-password
REVISION_BUNDLES_DIR=/path/to/revision/bundles  # Directory where compressed revision bundle files are stored
```

5. Initialize the database schema (tables are defined via SQLAlchemy models):

```
python3 init_db.py
```

## Usage

Build database entries from a single compressed revision bundle file (`.mwrev.zst`):

```
python3 build_db.py /path/to/file.mwrev.zst
```

Batch-process a directory of `.mwrev.zst` files:

```
python3 build_all.py -d /path/to/wiki/dumps
```

Optional: limit concurrency (default 150):

```
python3 build_all.py -d /path/to/wiki/dumps -p 50
```

### Revision Bundles

This project loads `.mwrev.zst` files which are compressed bundles of MediaWiki revisions. These files are produced by RevisionChest. Each bundle is tracked in the database with an auto-incrementing `id` and its `file_path`. Individual `revisions` rows may point to the bundle they were found in (`found_in_bundle`) and record byte offsets (`offset_begin`/`offset_end`) for fast extraction.

Set `REVISION_BUNDLES_DIR` in your `.env` to the directory where these bundle files are stored.
