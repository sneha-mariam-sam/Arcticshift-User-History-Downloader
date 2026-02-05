import os
import sys
import json
import logging
import time
import requests
import threading
from datetime import datetime
from queue import Queue, Empty

# ================= CONFIGURATION =================
INPUT_FILE = #[insert file path]
COMPLETED_FILE = #[insert file path]
OUTPUT_DIR = #[insert file path]

BASE_URL = "https://arctic-shift.photon-reddit.com"
END_DATE_TIMESTAMP = int(datetime(2024, 12, 31, 23, 59, 59).timestamp() * 1000) #Can be changed based on individual requirements

CONCURRENT_USERS = 5
MAX_RETRIES = 5
BACKOFF_FACTOR = 2
# =================================================

file_lock = threading.Lock()
shutdown_event = threading.Event()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("api_download.log", encoding='utf-8')
    ]
)
logger = logging.getLogger("arctic_api")

def check_file_integrity(filepath):
    if not os.path.exists(filepath): return False
    size = os.path.getsize(filepath)
    if size == 0: return True
    try:
        with open(filepath, 'rb') as f:
            first_line = f.readline().strip()
            if not first_line: return False
            f.seek(0, os.SEEK_END)
            filesize = f.tell()
            seek_back = min(filesize, 1024)
            f.seek(-seek_back, os.SEEK_END)
            last_chunk = f.read().splitlines()
            if not last_chunk: return False
            last_line = last_chunk[-1].strip()
        return first_line.startswith(b'{') and last_line.endswith(b'}')
    except Exception: return False

def fetch_with_retry(url, params=None):
    if shutdown_event.is_set(): return None
    for attempt in range(MAX_RETRIES):
        if shutdown_event.is_set(): return None
        try:
            response = requests.get(url, params=params, timeout=60, headers={"User-Agent": "ArcticShiftDownloader/1.0"})
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                wait = int(response.headers.get("X-RateLimit-Reset", 10))
                logger.warning(f"Rate limited. Waiting {wait}s...")
                time.sleep(wait)
            else:
                logger.warning(f"HTTP {response.status_code} for {url}. Attempt {attempt+1}/{MAX_RETRIES}")
        except Exception as e:
            logger.warning(f"Request error: {e}. Attempt {attempt+1}/{MAX_RETRIES}")
        
        if attempt < MAX_RETRIES - 1:
            # Check shutdown during sleep
            for _ in range(BACKOFF_FACTOR ** attempt):
                if shutdown_event.is_set(): return None
                time.sleep(1)
    return None

def download_stream(username, data_type, start_date_iso):
    filename = f"u_{username}_{data_type}.jsonl"
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    if check_file_integrity(filepath):
        logger.info(f"  [Skipped] {filename} exists.")
        return True

    temp_filepath = filepath + ".tmp"
    try:
        dt = datetime.fromisoformat(start_date_iso.replace('Z', '+00:00'))
        current_date_ms = int(dt.timestamp() * 1000)
    except Exception as e:
        logger.error(f"  [Error] Date parsing failed for {username}: {e}")
        return False
    
    base_search_url = f"{BASE_URL}/api/{data_type}/search"
    params = {
        "author": username,
        "limit": "auto",
        "sort": "asc",
        "before": END_DATE_TIMESTAMP,
        "meta-app": "download-tool-automation"
    }

    count = 0
    try:
        with open(temp_filepath, 'w', encoding='utf-8') as f:
            while not shutdown_event.is_set():
                params["after"] = current_date_ms
                data = fetch_with_retry(base_search_url, params)
                if shutdown_event.is_set(): return False
                
                if not data or "data" not in data:
                    logger.error(f"  [Error] Failed to fetch data for {username} {data_type}")
                    return False
                
                items = data["data"]
                if not items: break
                
                for item in items:
                    f.write(json.dumps(item) + "\n")
                
                count += len(items)
                last_item = items[-1]
                new_date_ms = int(last_item["created_utc"] * 1000)
                if new_date_ms == current_date_ms: new_date_ms += 1000
                current_date_ms = new_date_ms
                
                if count % 1000 == 0:
                    logger.info(f"  {username} {data_type}: {count} items downloaded...")

        if shutdown_event.is_set():
            if os.path.exists(temp_filepath): os.remove(temp_filepath)
            return False

        if os.path.exists(filepath): os.remove(filepath)
        os.rename(temp_filepath, filepath)
        logger.info(f"  [Success] Saved {count} {data_type} for {username}")
        return True
    except Exception as e:
        logger.error(f"  [Critical Error] {username} {data_type}: {e}")
        if os.path.exists(temp_filepath): os.remove(temp_filepath)
        return False

def process_user(username):
    if shutdown_event.is_set(): return
    logger.info(f"Processing user: {username}")
    
    min_date_url = f"{BASE_URL}/api/utils/min"
    params = {"author": username, "meta-app": "download-tool-automation"}
    data = fetch_with_retry(min_date_url, params)
    
    if shutdown_event.is_set(): return
    if not data or data.get("data") is None:
        logger.warning(f"  [Invalid] User {username} not found.")
        complete_and_remove(username)
        return

    start_date_iso = data["data"]
    
    if download_stream(username, "posts", start_date_iso) and \
       download_stream(username, "comments", start_date_iso):
        complete_and_remove(username)

def complete_and_remove(username):
    with file_lock:
        completed = set()
        if os.path.exists(COMPLETED_FILE):
            with open(COMPLETED_FILE, 'r', encoding='utf-8') as f:
                completed = {l.strip() for l in f if l.strip()}
        completed.add(username)
        sorted_completed = sorted(list(completed), key=lambda x: x.lower())
        with open(COMPLETED_FILE, 'w', encoding='utf-8') as f:
            for u in sorted_completed: f.write(f"{u}\n")
        
        if os.path.exists(INPUT_FILE):
            with open(INPUT_FILE, 'r', encoding='utf-8') as f:
                lines = [l.strip() for l in f if l.strip() and l.strip() != username]
            with open(INPUT_FILE, 'w', encoding='utf-8') as f:
                for l in lines: f.write(f"{l}\n")

def worker(q):
    while not shutdown_event.is_set():
        try:
            username = q.get(timeout=1)
        except Empty:
            continue
            
        if username is None:
            q.task_done()
            break
        try:
            process_user(username)
        except Exception as e:
            logger.error(f"Worker error for {username}: {e}")
        finally:
            q.task_done()

def main():
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR, exist_ok=True)
    if not os.path.exists(INPUT_FILE):
        logger.error(f"Input file not found: {INPUT_FILE}")
        return

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        all_users = [l.strip() for l in f if l.strip()]

    completed = set()
    if os.path.exists(COMPLETED_FILE):
        with open(COMPLETED_FILE, 'r', encoding='utf-8') as f:
            completed = {l.strip() for l in f if l.strip()}
    
    todo = [u for u in all_users if u not in completed]
    logger.info(f"Starting API Downloader. Todo: {len(todo)}")

    if not todo: return

    q = Queue()
    for u in todo: q.put(u)
    for _ in range(CONCURRENT_USERS): q.put(None)

    threads = []
    for _ in range(CONCURRENT_USERS):
        t = threading.Thread(target=worker, args=(q,))
        t.daemon = True # Allows script to exit if main thread is killed
        t.start()
        threads.append(t)

    try:
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(timeout=0.5)
    except KeyboardInterrupt:
        logger.info("\n[INTERRUPT] Stopping all workers... Please wait.")
        shutdown_event.set()
        # Give threads a moment to finish current chunk
        for t in threads:
            t.join(timeout=2)
        logger.info("Stopped.")
        sys.exit(0)

    logger.info("--- ALL DOWNLOADS COMPLETED ---")

if __name__ == "__main__":
    main()
