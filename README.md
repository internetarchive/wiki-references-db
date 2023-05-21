# wiki-references-db
The `wiki-references-db` is a component in the third-generation Wikipedia Citations Database. It draws off of IARI <https://github.com/internetarchive/iari>, a service that analyzes references that appear on Wikipedia articles.

`wiki-references-db` builds a database of wiki articles (as identified by domain and page ID) and the strings of raw wikitext comprising the references that appear on them.
* "References" are defined broadly to include anything from bare external links to in-line citations (though the coverage of each will vary.)
* Reference strings are normalized before being hashed and stored in the database. Normalization eliminates unnecessary white space, alphabetizes the named parameters, turns underscores into spaces, and makes other stylistic transformations. The goal is to uniquely identify references on the basis of their content, treating two reference strings as the same if they otherwise mean the same thing.
* A reference is identifed by its `record_md5` hash, made by taking the MD5 hash of the domain, numeric page ID, and normalized reference. This uniquely identifies the reference (accounting for variations in wikitext formatting) in the context of the original page and wiki it appeared on. Separately, a `reference_md5` hash is available if you wish to search for the same reference string across articles regardless of context. (This might be useful to, for instance, look up certain calls of a Cite Q template.)
* This project avoids inferring semantics or other data attributes from the content of these reference strings. This builds the initial structure, and leaves the rest to derivation processes.
*  The `wikireferences` table tracks both present and historical references for a given page. You can distinguish between present and former references by the `latest_revision` column. The history of a reference's appearance on a page between revisions is tracked in the `history` table; you can use this to track a reference being inserted, removed, and re-inserted.

## Setup

1. `git clone https://github.com/internetarchive/wiki-references-db`

2. `cd wiki-references-db`

3. `python3 -m venv venv`

4. `source venv/bin/activate`

5. `pip3 install -r requirements.txt`

6. Load `schema.sql` into a Postgres database.

7. Create a `credentials.py` file similar to this:

```
DB = "your-database-name"
DBUSER = "your-database-username"
DBPASS = "your-database-password"
HOST = "127.0.0.1"
PORT = 5432
```

## Usage

Build database of references appearing on the latest version of English Wikipedia: `python3 build-db.py`
