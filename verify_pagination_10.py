import json
import urllib.request
import sys

BASE_URL = "http://localhost:8000"

def test_pagination():
    print("--- Testing Pagination (Limit 10) ---")
    
    # 1. Test Default Limit (should be 10)
    query = "SELECT * FROM generate_series(1, 25) as n"
    body = {"query": query} # No limit/offset specified
    
    req = urllib.request.Request(
        f"{BASE_URL}/query", 
        data=json.dumps(body).encode("utf-8"), 
        headers={"Content-Type": "application/json"}, 
        method="POST"
    )
    
    with urllib.request.urlopen(req) as res:
        data = json.loads(res.read())
        
        # Check rows
        rows = len(data["data"])
        total = data["total_rows"]
        print(f"Page 1: Returned {rows} rows. Total: {total}")
        
        if rows != 10:
            print(f"FAIL: Expected 10 rows, got {rows}")
            return False
            
        if total != 25:
             print(f"FAIL: Expected 25 total rows, got {total}")
             return False

    # 2. Test Offset (Page 2)
    body = {"query": query, "offset": 10}
    req = urllib.request.Request(
        f"{BASE_URL}/query", 
        data=json.dumps(body).encode("utf-8"), 
        headers={"Content-Type": "application/json"}, 
        method="POST"
    )
    
    with urllib.request.urlopen(req) as res:
        data = json.loads(res.read())
        first_val = data["data"][0]["n"]
        print(f"Page 2 First Value: {first_val}")
        
        if str(first_val) != "11":
             print(f"FAIL: Expected first value 11, got {first_val}")
             return False
             
    print("PASS")
    return True

if __name__ == "__main__":
    if test_pagination():
        sys.exit(0)
    sys.exit(1)
