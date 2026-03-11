import os
import re
import subprocess
import queue
import time
import argparse

max_jobs = 8

def sort_key(file_name):
    # For .mwrev.zst files, try to extract numeric hints if present; else by name
    match = re.search(r"(\d+)", file_name)
    if match:
        return int(match.group(1))
    return float('inf')  # Fallback

def main():
    parser = argparse.ArgumentParser(description="Spawn build_db.py for each .mwrev.zst file in a directory.")
    parser.add_argument("-d", "--directory", required=True, help="Directory containing .mwrev.zst files")
    parser.add_argument("-j", "--jobs", type=int, default=max_jobs, help="Number of concurrent jobs/files to process (default: 8)")
    args = parser.parse_args()

    directory = args.directory
    if not os.path.isdir(directory):
        raise SystemExit(f"Provided path is not a directory: {directory}")

    files = [
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if os.path.isfile(os.path.join(directory, f)) and f.endswith('.mwrev.zst')
    ]
    files.sort(key=lambda f: sort_key(os.path.basename(f)))
    #files.reverse()
    process_queue = queue.Queue(maxsize=args.jobs)

    for counter, file in enumerate(files):
        while process_queue.full():
            time.sleep(0.1)
            cleanup_finished_processes(process_queue)
        # Keep DB concurrency bounded inside each job via --write-procs.
        # Use more parser procs inside the job to leverage CPU while protecting Postgres.
        process = subprocess.Popen([
            "python3", "build_db.py", file,
            "--parse-procs", os.environ.get("PARSE_PROCS", "4"),
            "--write-procs", os.environ.get("WRITE_PROCS", "1"),
            "--batch-size", os.environ.get("BATCH_SIZE", "1000"),
            "--queue-max", os.environ.get("QUEUE_MAX", "32"),
            "--metrics-interval", os.environ.get("METRICS_INTERVAL", "5"),
        ] + (["--tune-db"] if os.environ.get("TUNE_DB", "0") in ("1", "true", "TRUE") else []))
        process_queue.put(process)
        print(f"[{str(counter+1)}/{str(len(files))}] {file}")

    while not process_queue.empty():
        cleanup_finished_processes(process_queue)
        time.sleep(0.1)

def cleanup_finished_processes(process_queue):
    for _ in range(process_queue.qsize()):
        process = process_queue.get()
        if process.poll() is None:
            process_queue.put(process)

if __name__ == "__main__":
    main()
