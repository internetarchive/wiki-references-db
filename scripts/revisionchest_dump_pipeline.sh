#!/bin/bash
# Run RevisionChest on each dump file of a full-history dump as soon as its download is
# verified (a <file>.ok marker beside it), up to $JOBS files at once, so Stage 0's bundle
# build overlaps the download. Per-file statistics go to $STATS (TSV). Re-runnable: files
# with a done or failed marker are skipped; ones left running by an interrupted run are redone.
# Stops when every file is done, when the download has stopped, or when free disk falls
# under $MIN_FREE_GB.
#
#   DUMPS=…/dumps/enwiki-20261001 BUNDLES=…/bundles/enwiki-20261001-ns0 META=…/meta/enwiki-20261001-ns0 \
#   STATE=…/state-ns0 TOTAL=980 scripts/revisionchest_dump_pipeline.sh
#
# Variables: DUMPS, BUNDLES, META, STATE (required); TOTAL (dump files expected; required);
# JOBS (12), DOMAIN (en.wikipedia.org), NAMESPACE (0), MIN_FREE_GB (40),
# BIN (rust/revisionchest/target/release/RevisionChest), STATS ($STATE/stats.tsv), LOG ($STATE/pipeline.log),
# DOWNLOADER (a pgrep -f pattern for the download process; default fetch_one.sh).
# Used for enwiki-20261001 (980 files, 12 jobs); see the implementation plan, §17.
set -u
: "${DUMPS:?}" "${BUNDLES:?}" "${META:?}" "${STATE:?}" "${TOTAL:?}"
repo=$(cd "$(dirname "$0")/.." && pwd)
JOBS=${JOBS:-12}
DOMAIN=${DOMAIN:-en.wikipedia.org}
NAMESPACE=${NAMESPACE:-0}
MIN_FREE_GB=${MIN_FREE_GB:-40}
BIN=${BIN:-$repo/rust/revisionchest/target/release/RevisionChest}
STATS=${STATS:-$STATE/stats.tsv}
LOG=${LOG:-$STATE/pipeline.log}
DOWNLOADER=${DOWNLOADER:-fetch_one.sh}
mkdir -p "$BUNDLES" "$META" "$STATE"/{done,running,failed,time}
[ -f "$STATS" ] || printf 'file\tstart\tend\twall_s\tuser_s\tsys_s\tcpu_pct\tmaxrss_kb\trevisions\tdump_bytes\tbundle_bytes\tmeta_bytes\texit\n' > "$STATS"

run_one() {
  local n=$1 stem=${1%.7z}
  local t="$STATE/time/$n.txt" out="$STATE/time/$n.out"
  local s e rc
  s=$(date +%s.%N)
  /usr/bin/time -v "$BIN" build "$DUMPS/$n" -o "$BUNDLES" --parquet "$META/$stem.parquet" \
      --domain "$DOMAIN" --namespace "$NAMESPACE" > "$out" 2> "$t"
  rc=$?
  e=$(date +%s.%N)
  local revs; revs=$(grep -o 'Finished .*: [0-9]* revisions' "$t" "$out" 2>/dev/null | grep -o '[0-9]* revisions' | cut -d' ' -f1 | tail -1)
  printf '%s\t%s\t%s\t%.1f\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$n" \
    "$(date -d @${s%.*} +%FT%T)" "$(date -d @${e%.*} +%FT%T)" "$(echo "$e - $s" | bc)" \
    "$(awk -F': ' '/User time/{print $2}' "$t")" "$(awk -F': ' '/System time/{print $2}' "$t")" \
    "$(awk -F': ' '/Percent of CPU/{print $2}' "$t" | tr -d %)" "$(awk -F': ' '/Maximum resident/{print $2}' "$t")" \
    "${revs:-}" "$(stat -c %s "$DUMPS/$n")" "$(stat -c %s "$BUNDLES/$stem.mwrev.zst" 2>/dev/null || echo 0)" \
    "$(stat -c %s "$META/$stem.parquet" 2>/dev/null || echo 0)" "$rc" >> "$STATS"
  rm -f "$STATE/running/$n"
  if [ $rc -eq 0 ]; then touch "$STATE/done/$n"; echo "$(date +%T) done $n" >> "$LOG"
  else touch "$STATE/failed/$n"; echo "$(date +%T) FAILED($rc) $n" >> "$LOG"; fi
}

echo "$(date +%T) pipeline start, JOBS=$JOBS" >> "$LOG"
rm -f "$STATE"/running/*
last_df=0
while :; do
  for ok in "$DUMPS"/*.7z.ok; do
    [ -e "$ok" ] || continue
    n=$(basename "${ok%.ok}")
    [ -e "$STATE/done/$n" ] || [ -e "$STATE/failed/$n" ] || [ -e "$STATE/running/$n" ] && continue
    while [ "$(jobs -rp | wc -l)" -ge "$JOBS" ]; do wait -n; done
    touch "$STATE/running/$n"
    run_one "$n" &
  done
  now=$(date +%s)
  if (( now - last_df >= 300 )); then
    echo "$(date +%T) disk $(df -h "$BUNDLES" | awk 'NR==2{print $3" used, "$4" free"}') ok=$(ls "$DUMPS"/*.ok | wc -l) done=$(ls "$STATE/done" | wc -l) failed=$(ls "$STATE/failed" | wc -l) running=$(jobs -rp | wc -l)" >> "$LOG"
    avail=$(df --output=avail -B1G "$BUNDLES" | tail -1)
    if (( avail < MIN_FREE_GB )); then echo "$(date +%T) STOP: under $MIN_FREE_GB GB free" >> "$LOG"; wait; exit 2; fi
    last_df=$now
  fi
  nok=$(ls "$DUMPS"/*.ok 2>/dev/null | wc -l)
  ndone=$(( $(ls "$STATE/done" | wc -l) + $(ls "$STATE/failed" | wc -l) ))
  if (( nok >= TOTAL && ndone >= TOTAL )); then break; fi
  if ! pgrep -f "$DOWNLOADER" >/dev/null && (( ndone >= nok )) && [ "$(jobs -rp | wc -l)" -eq 0 ]; then
    echo "$(date +%T) download no longer running with $nok/$TOTAL verified; stopping" >> "$LOG"; break
  fi
  sleep 20
done
wait
echo "$(date +%T) pipeline end: done=$(ls "$STATE/done" | wc -l) failed=$(ls "$STATE/failed" | wc -l)" >> "$LOG"
