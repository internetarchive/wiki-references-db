import os
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
    """Sort by file size, smallest first."""
    try:
        return os.path.getsize(filepath)
    except OSError:
        return float('inf')


class ProcessSlot:
    """Tracks a subprocess."""
    def __init__(self, process, log_prefix, filepath, job_staging_dir):
        self.process = process
        self.log_prefix = log_prefix
        self.filepath = filepath
        self.job_staging_dir = job_staging_dir
        self.finished = False


def reader_thread(slot):
    """Read stdout from a subprocess line-by-line."""
    for raw in slot.process.stdout:
        line = raw.strip()
        if not line:
            continue
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
    parser.add_argument("-j", "--jobs", type=int, default=max_jobs,
                        help="Number of concurrent jobs/files to process (default: 8)")
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

    # Clean up incomplete shards (STARTED but not DONE)
    if os.path.isdir(args.staging_dir):
        for subdir in os.listdir(args.staging_dir):
            subdir_path = os.path.join(args.staging_dir, subdir)
            if not os.path.isdir(subdir_path):
                continue
            started = os.path.exists(os.path.join(subdir_path, 'STARTED.txt'))
            done = os.path.exists(os.path.join(subdir_path, 'DONE.txt'))
            if started and not done:
                print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} [build_all] Clearing incomplete shard: {subdir}", flush=True)
                for fname in os.listdir(subdir_path):
                    if fname.endswith('.jsonl.zst'):
                        os.remove(os.path.join(subdir_path, fname))
                # Also remove stale STARTED.txt so it gets a fresh timestamp
                os.remove(os.path.join(subdir_path, 'STARTED.txt'))

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
        basename = os.path.basename(file)
        for suffix in ('.mwrev.zst',):
            if basename.endswith(suffix):
                basename = basename[:-len(suffix)]
                break
        else:
            basename = os.path.splitext(basename)[0]
        job_staging_dir = os.path.join(args.staging_dir, basename)

        # Skip already-completed shards
        if os.path.exists(os.path.join(job_staging_dir, 'DONE.txt')):
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {log_prefix} [skip] {file} (already done)", flush=True)
            finished_count += 1
            continue

        process = subprocess.Popen([
            "python3", "build_db.py", file,
            "-o", job_staging_dir,
            "--batch-size", os.environ.get("BATCH_SIZE", "1000"),
        ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        slot = ProcessSlot(process, log_prefix, file, job_staging_dir)
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
