import json
import urllib.request
import urllib.error
import threading
import time
import uuid
import sys

BASE_URL = "http://localhost:8000"

def run_long_query(query_id, duration=5):
    url = f"{BASE_URL}/query"
    query = f"SELECT pg_sleep({duration})"
    body = {
        "query": query,
        "query_id": query_id  # Proposed feature
    }
    
    print(f"[{query_id}] Starting long query ({duration}s)...")
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    
    start_time = time.time()
    try:
        with urllib.request.urlopen(req) as response:
            status = response.getcode()
            body_text = response.read().decode("utf-8")
            elapsed = time.time() - start_time
            print(f"[{query_id}] Finished in {elapsed:.2f}s with status {status}")
            return status, body_text
    except urllib.error.HTTPError as e:
        elapsed = time.time() - start_time
        print(f"[{query_id}] Failed in {elapsed:.2f}s with status {e.code}")
        return e.code, e.read().decode("utf-8")
    except Exception as e:
        print(f"[{query_id}] Exception: {e}")
        return 999, str(e)

def cancel_query(query_id, delay=1):
    time.sleep(delay)
    url = f"{BASE_URL}/query/cancel"
    body = {"query_id": query_id}
    
    print(f"[{query_id}] distinct cancel request sending...")
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    
    try:
        with urllib.request.urlopen(req) as response:
            status = response.getcode()
            print(f"[{query_id}] Cancel requested. Status: {status}")
            return status
    except urllib.error.HTTPError as e:
        print(f"[{query_id}] Cancel failed. Status: {e.code}")
        return e.code
    except Exception as e:
        print(f"[{query_id}] Cancel Exception: {e}")
        return 999

def test_cancellation():
    query_id = str(uuid.uuid4())
    
    # Thread 1: Run Query
    t_query = threading.Thread(target=run_long_query, args=(query_id, 10))
    t_query.start()
    
    # Thread 2: Cancel Query after 2 seconds
    t_cancel = threading.Thread(target=cancel_query, args=(query_id, 2))
    t_cancel.start()
    
    t_query.join()
    t_cancel.join()

if __name__ == "__main__":
    print("Starting Query Cancellation Test...")
    test_cancellation()
