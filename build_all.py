import os
import re
import subprocess
import queue
import time
import argparse

max_processes = 150

def sort_key(file_name):
    # For .mwrev.zst files, try to extract numeric hints if present; else by name
    match = re.search(r"(\d+)", file_name)
    if match:
        return int(match.group(1))
    return float('inf')  # Fallback

def main():
    parser = argparse.ArgumentParser(description="Spawn build_db.py for each .mwrev.zst file in a directory.")
    parser.add_argument("-d", "--directory", required=True, help="Directory containing .mwrev.zst files")
    parser.add_argument("-p", "--processes", type=int, default=max_processes, help="Max concurrent processes (default: 150)")
    args = parser.parse_args()

    directory = args.directory
    if not os.path.isdir(directory):
        raise SystemExit(f"Provided path is not a directory: {directory}")

    global max_processes
    max_processes = args.processes
    files = [
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if os.path.isfile(os.path.join(directory, f)) and f.endswith('.mwrev.zst')
    ]
    files.sort(key=lambda f: sort_key(os.path.basename(f)))
    #files.reverse()
    process_queue = queue.Queue(maxsize=max_processes)

    for counter, file in enumerate(files):
        while process_queue.full():
            time.sleep(0.1)
            cleanup_finished_processes(process_queue)
        process = subprocess.Popen(["python3", "build_db.py", file])
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
