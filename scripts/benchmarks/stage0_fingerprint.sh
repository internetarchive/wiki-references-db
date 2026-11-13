#!/bin/bash
# Order-independent fingerprints of Stage-0 output: the Postgres tables it loads and the
# revisions Parquet. Two loads of the same dump must print the same lines whatever their
# row order, job count or file layout (all but `parquet_files`); this is how changes to
# stage0-load were checked against the baseline (implementation plan §18).
#
# Usage: stage0_fingerprint.sh [DOMAIN]    (needs DATABASE_URL and PARQUET_DIR; default en.wikipedia.org)
set -euo pipefail
domain=${1:-en.wikipedia.org}
: "${DATABASE_URL:?set DATABASE_URL}" "${PARQUET_DIR:?set PARQUET_DIR}"
repo=$(cd "$(dirname "$0")/../.." && pwd)
psql "${DATABASE_URL/postgresql+psycopg2/postgresql}" -tA -F' ' <<'SQL'
SET max_parallel_workers_per_gather = 8;
SELECT 'revisions', count(*), sum(hashtextextended(concat_ws(',', r.revision_id, r.page_id, r.parent_revision_id,
       r.revision_timestamp, b.file_path, r.offset_begin, r.length), 0)::numeric)
  FROM revisions r LEFT JOIN revision_bundles b ON b.id = r.found_in_bundle;
SELECT 'page_metadata', count(*), sum(hashtextextended(concat_ws(',', page_id, page_title, page_namespace), 0)::numeric) FROM page_metadata;
SELECT 'web_resources', count(*), sum(hashtextextended(concat_ws(',', url, url_hash, numeric_page_id, numeric_namespace_id), 0)::numeric) FROM web_resources;
SQL
glob="$PARQUET_DIR/revisions/domain=$domain/*/*.parquet"
uv run --frozen --project "$repo" python - "$glob" <<'PY'
import sys
import duckdb
c = duckdb.connect()
g = sys.argv[1]
print("parquet", *c.execute(f"SELECT count(*), sum(hash(revision_id, page_id, parent_revision_id, revision_timestamp, "
                            f"page_bucket)::HUGEINT) FROM read_parquet('{g}', hive_partitioning=true)").fetchone())
print("parquet_files", c.execute(f"SELECT count(DISTINCT filename) FROM read_parquet('{g}', filename=true)").fetchone()[0])
PY
