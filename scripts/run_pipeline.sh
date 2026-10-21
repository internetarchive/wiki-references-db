#!/bin/bash
# Run the whole pipeline (Stages 0–8, each with its gate) on one domain, with per-step
# timings and a memory sampler — the rehearsal-shard driver, usable for any build.
#
#   DATABASE_URL=… PARQUET_DIR=… scripts/run_pipeline.sh <bundles-dir> <metadata-dir-or-file> [domain] [jobs]
#
# Writes <PARQUET_DIR>/../run.log, timings.txt and mem.log next to the Parquet directory
# (override with RUN_DIR). Stops at the first failing step. The database schema is
# recreated unless KEEP_SCHEMA=1. A single large bundle can be cut into pieces first
# with scripts/split_bundle.py so that --jobs applies to Stage 1.
set -o pipefail
BUNDLES=${1:?bundles dir}; META=${2:?metadata}; D=${3:-en.wikipedia.org}; JOBS=${4:-4}
: "${DATABASE_URL:?set DATABASE_URL}"; : "${PARQUET_DIR:?set PARQUET_DIR}"
RUN_DIR=${RUN_DIR:-$(dirname "$PARQUET_DIR")}; STAGING=${STAGING_DIR:-$RUN_DIR/staging}
mkdir -p "$RUN_DIR" "$PARQUET_DIR" "$STAGING"
LOG=$RUN_DIR/run.log; TIMINGS=$RUN_DIR/timings.txt; MEM=$RUN_DIR/mem.log
step() { local name="$1"; shift; local t0=$(date +%s); echo "== $(date '+%F %T') START $name" | tee -a "$LOG"
  if "$@" >> "$LOG" 2>&1; then echo "== $(date '+%F %T') OK $name $(( $(date +%s) - t0 ))s" | tee -a "$LOG"; echo "$name $(( $(date +%s) - t0 ))" >> "$TIMINGS"
  else echo "== $(date '+%F %T') FAIL $name $(( $(date +%s) - t0 ))s (see $LOG)" | tee -a "$LOG"; echo "$name FAILED" >> "$TIMINGS"; kill $SAMPLER 2>/dev/null; exit 1; fi; }
( while true; do ps -eo rss,args --sort=-rss | grep -E "wcd_pipeline|wcd " | grep -v grep | head -4 | awk -v t="$(date '+%T')" '{printf "%s %d MB %s\n", t, $1/1024, substr($0, index($0,$2), 70)}' >> "$MEM"; sleep 60; done ) &
SAMPLER=$!
if [ -z "$KEEP_SCHEMA" ]; then
  psql "$DATABASE_URL" -qc "DROP SCHEMA public CASCADE; CREATE SCHEMA public;" >> "$LOG" 2>&1 || { echo "cannot reset schema"; exit 1; }
fi
step init-db        uv run wcd init-db --no-indexes
step stage0-load    uv run wcd stage0-load --domain "$D" --metadata "$META"
step extract-all    uv run wcd extract-all -d "$BUNDLES" -o "$STAGING" --domain "$D" --jobs "$JOBS"
step dedup          uv run wcd dedup -d "$STAGING"
step load           uv run wcd load -d "$STAGING"
step gate3          uv run wcd gate 3 --domain "$D" --all-buckets
step add-indexes    uv run wcd init-db --add-indexes
step parts          uv run wcd parts --domain "$D"
step denorm-export  uv run wcd denorm-export --domain "$D"
step ref-name-links uv run wcd ref-name-links --domain "$D"
step match          uv run wcd match --domain "$D" --jobs "$JOBS"
step materialize    uv run wcd materialize --domain "$D"
step revision-stats uv run wcd revision-stats --domain "$D" --jobs "$JOBS"
kill $SAMPLER 2>/dev/null
echo "== $(date '+%F %T') ALL DONE" | tee -a "$LOG"; echo "peak worker memory:"; sort -k2 -n -r "$MEM" | head -3
