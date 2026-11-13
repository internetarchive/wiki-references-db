#!/bin/bash
# Extract one sample bundle with `wcd extract`, timing it; appends a line to RESULTS_DIR/results.tsv:
#   name, wall s, user s, system s, max RSS KB, exit code
#
# Usage: run_one.sh BUNDLE RESULTS_DIR      (run from the repository; DOMAIN defaults to en.wikipedia.org)
b=$1; out=$2; name=$(basename "$b" .mwrev.zst)
domain=${DOMAIN:-en.wikipedia.org}
repo=$(cd "$(dirname "$0")/../../.." && pwd)
mkdir -p "$out/staging/$name" "$out/time" "$out/log"
s=$(date +%s.%N)
/usr/bin/time -v -o "$out/time/$name.txt" \
    uv run --frozen --project "$repo" wcd extract "$b" -o "$out/staging/$name" \
    --domain "$domain" --parquet-dir "$out/parquet" > "$out/log/$name.log" 2>&1
rc=$?
e=$(date +%s.%N)
t="$out/time/$name.txt"
printf '%s\t%.1f\t%s\t%s\t%s\t%s\n' "$name" "$(echo "$e - $s" | bc)" \
  "$(awk -F': ' '/User time/{print $2}' "$t")" "$(awk -F': ' '/System time/{print $2}' "$t")" \
  "$(awk -F': ' '/Maximum resident/{print $2}' "$t")" "$rc" >> "$out/results.tsv"
