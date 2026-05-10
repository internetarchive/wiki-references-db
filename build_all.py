import os
import re
import subprocess
import queue
import time
import argparse
import threading
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

max_jobs = 8

def sort_key(filepath):
    # Sort by file size, smallest first
    try:
        return os.path.getsize(filepath)
    except OSError:
        return float('inf')

# Regex to parse a metrics line emitted by build_db.py's print_metrics().
# Example:
#   2026-03-10 21:00:00 [1/10] [metrics] elapsed=0h5m12s | queue=3/32 | parsers_done=2/4 | tables: articles=100, revisions=5000
_METRICS_RE = re.compile(
    r"(?P<prefix>\[.*?\])\s+"           # log-prefix like [1/10]
    r"\[(?P<kind>[^\]]+)\]\s+"          # [metrics] or [final]
    r"elapsed=(?P<elapsed>\S+)\s*\|\s*"
    r"queue=(?P<queue>\S+)\s*\|\s*"
    r"parsers_done=(?P<parsers>\S+)\s*\|\s*"
    r"tables:\s*(?P<tables>.*)"
)

def parse_tables(tables_str):
    """Parse 'key=val, key=val' into a dict of {str: int}."""
    result = {}
    if not tables_str or not tables_str.strip():
        return result
    for pair in tables_str.split(","):
        pair = pair.strip()
        if "=" in pair:
            k, v = pair.split("=", 1)
            try:
                result[k.strip()] = int(v.strip())
            except ValueError:
                pass
    return result


class ProcessSlot:
    """Tracks a subprocess."""
    def __init__(self, process, log_prefix, filepath):
        self.process = process
        self.log_prefix = log_prefix
        self.filepath = filepath
        self.finished = False


def reader_thread(slot):
    """Read stdout from a subprocess line-by-line, silencing metrics."""
    for raw in slot.process.stdout:
        line = raw.strip()
        if not line:
            continue
        if _METRICS_RE.search(line):
            continue
        # Non-metrics output: print through with timestamp
        print(line, flush=True)


def aggregate_and_print(slots, finished_count, total_files):
    """Print simplified status focusing on shards done and in progress."""
    active = len(slots)
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    print(
        f"{ts} [build_all] {finished_count}/{total_files} shards done ({active} in progress)",
        flush=True,
    )


def main():
    parser = argparse.ArgumentParser(description="Spawn build_db.py for each .mwrev.zst file in a directory.")
    parser.add_argument("-d", "--directory", required=True, help="Directory containing .mwrev.zst files")
    parser.add_argument("-o", "--staging-dir", default=os.environ.get('STAGING_DIR', './staging'),
                        help="Directory to write staged JSONL.zst files (default: STAGING_DIR env or ./staging)")
    parser.add_argument("-j", "--jobs", type=int, default=max_jobs, help="Number of concurrent jobs/files to process (default: 8)")
    parser.add_argument("--metrics-interval", type=float, default=float(os.environ.get("METRICS_INTERVAL", "10")),
                        help="Seconds between aggregated metrics prints (default: 10 or METRICS_INTERVAL env)")
    args = parser.parse_args()

    directory = args.directory
    if not os.path.isdir(directory):
        raise SystemExit(f"Provided path is not a directory: {directory}")

    files = [
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if os.path.isfile(os.path.join(directory, f)) and f.endswith('.mwrev.zst')
    ]
    files.sort(key=sort_key)

    all_slots = []
    finished_count = 0
    process_queue = queue.Queue(maxsize=args.jobs)
    metrics_interval = args.metrics_interval
    last_agg_print = time.time()

    for counter, file in enumerate(files):
        while process_queue.full():
            time.sleep(0.1)
            newly_done = cleanup_finished_processes(process_queue, all_slots)
            finished_count += newly_done
            all_slots[:] = [s for s in all_slots if not s.finished]
            now = time.time()
            if now - last_agg_print >= metrics_interval:
                aggregate_and_print(all_slots, finished_count, len(files))
                last_agg_print = now

        log_prefix = f"[{counter+1}/{len(files)}]"
        # Each job gets a subdirectory under the staging dir named after the input file
        # Strip the full .mwrev.zst suffix (not just the last extension)
        basename = os.path.basename(file)
        for suffix in ('.mwrev.zst',):
            if basename.endswith(suffix):
                basename = basename[:-len(suffix)]
                break
        else:
            basename = os.path.splitext(basename)[0]
        job_staging_dir = os.path.join(args.staging_dir, basename)
        process = subprocess.Popen([
            "python3", "build_db.py", file,
            "-o", job_staging_dir,
            "--parse-procs", os.environ.get("PARSE_PROCS", "4"),
            "--write-procs", os.environ.get("WRITE_PROCS", "1"),
            "--batch-size", os.environ.get("BATCH_SIZE", "1000"),
            "--queue-max", os.environ.get("QUEUE_MAX", "32"),
            "--metrics-interval", os.environ.get("METRICS_INTERVAL", "5"),
            "--log-prefix", log_prefix,
        ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        slot = ProcessSlot(process, log_prefix, file)
        slot.job_staging_dir = job_staging_dir
        all_slots.append(slot)
        process_queue.put(slot)

        # Start a reader thread to consume stdout without blocking
        t = threading.Thread(target=reader_thread, args=(slot,), daemon=True)
        t.start()

        # Write STARTED.txt with current timestamp
        os.makedirs(job_staging_dir, exist_ok=True)
        with open(os.path.join(job_staging_dir, 'STARTED.txt'), 'w') as f:
            f.write(datetime.now(timezone.utc).isoformat() + '\n')

        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {log_prefix} [start] {file}", flush=True)

    while not process_queue.empty():
        newly_done = cleanup_finished_processes(process_queue, all_slots)
        finished_count += newly_done
        all_slots[:] = [s for s in all_slots if not s.finished]
        now = time.time()
        if now - last_agg_print >= metrics_interval:
            aggregate_and_print(all_slots, finished_count, len(files))
            last_agg_print = now
        time.sleep(0.1)

    # Final aggregate
    aggregate_and_print(all_slots, finished_count, len(files))


def cleanup_finished_processes(process_queue, all_slots):
    newly_done = 0
    for _ in range(process_queue.qsize()):
        slot = process_queue.get()
        if slot.process.poll() is None:
            process_queue.put(slot)
        else:
            slot.finished = True
            newly_done += 1
            # Write DONE.txt with current timestamp
            if hasattr(slot, 'job_staging_dir') and slot.job_staging_dir:
                os.makedirs(slot.job_staging_dir, exist_ok=True)
                with open(os.path.join(slot.job_staging_dir, 'DONE.txt'), 'w') as f:
                    f.write(datetime.now(timezone.utc).isoformat() + '\n')
            # Release subprocess resources
            slot.process.stdout.close()
            slot.process.wait()
            slot.process = None
    return newly_done

if __name__ == "__main__":
    main()
