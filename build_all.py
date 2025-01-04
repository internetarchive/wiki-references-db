import os
import re
import subprocess
import queue
import time

directory = "./sources/"
max_processes = 30

def sort_key(file_name):
    match = re.search(r"history(\d+).*?p(\d+)", file_name)
    if match:
        history_num = int(match.group(1))
        p_num = int(match.group(2))
        return (history_num, p_num)
    return (float('inf'), float('inf'))  # Fallback

def main():
    files = [os.path.join(directory, f) for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
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
