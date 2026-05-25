#!/usr/bin/env bash
set -euo pipefail

STAGING_DIR="${STAGING_DIR:-./staging}"
OUTPUT_DIR="$STAGING_DIR/deduped"
TABLE="citation_histories"
MAX_LINES="${DEDUP_SHARD_SIZE:-2000000}"

mkdir -p "$OUTPUT_DIR"

cat "$STAGING_DIR"/*-${TABLE}.jsonl.zst \
  | zstd -dc \
  | jq -c '.' \
  | split -l "$MAX_LINES" -d -a 8 \
      --filter='zstd -c > "$FILE.jsonl.zst"' \
      - "$OUTPUT_DIR/${TABLE}-"