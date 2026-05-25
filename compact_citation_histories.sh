#!/usr/bin/env bash
set -euo pipefail

STAGING_DIR="${STAGING_DIR:-./staging}"
OUTPUT_DIR="$STAGING_DIR/deduped"
TABLE="citation_histories"
MAX_LINES="${DEDUP_SHARD_SIZE:-2000000}"

mkdir -p "$OUTPUT_DIR"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [compact] $*"
}

# Collect files into an array
mapfile -d '' FILES < <(
    find "$STAGING_DIR" -name "*-${TABLE}.jsonl.zst" \
        -not -path "*/deduped/*" -not -path "*/intermediate/*" -print0 \
    | sort -z
)

TOTAL=${#FILES[@]}
if [ "$TOTAL" -eq 0 ]; then
    log "No ${TABLE} files found in ${STAGING_DIR}, nothing to do."
    exit 0
fi

log "Found ${TOTAL} ${TABLE} file(s) to process."

# Process each file individually: decompress, validate/filter JSON, stream out.
# - jq -R reads each line as a raw string (not as JSON), so a malformed line
#   won't cause a parse error that kills jq.
# - fromjson? attempts to parse; the ? makes parse failures produce null
#   instead of an error.  // empty drops those nulls silently.
# - Files that fail zstd decompression are skipped (zstd exits non-zero,
#   but the || continue handles it and the partial output is still valid
#   validated JSON from jq).
FILE_NUM=0
for f in "${FILES[@]}"; do
    FILE_NUM=$((FILE_NUM + 1))
    log "Processing file ${FILE_NUM}/${TOTAL}: $(basename "$f")" >&2
    zstd -dcq "$f" 2>/dev/null | jq -c -R 'fromjson? // empty' || {
        log "  WARN: error processing $(basename "$f"), continuing" >&2
    }
done \
  | split -l "$MAX_LINES" -d -a 8 \
      --filter='
          echo "$(date "+%Y-%m-%d %H:%M:%S") [compact] Writing shard: $(basename "$FILE").jsonl.zst" >&2
          zstd -c > "$FILE.jsonl.zst"
      ' \
      - "$OUTPUT_DIR/${TABLE}-"

log "Done."
