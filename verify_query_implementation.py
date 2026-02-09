import json
import urllib.request
import urllib.error
import sys

BASE_URL = "http://localhost:8000"

def test_query(name, query, limit, offset, expected_status, expected_check=None):
    print(f"--- Test: {name} ---")
    print(f"Query: {query} | Limit: {limit} | Offset: {offset}")
    
    url = f"{BASE_URL}/query"
    body = {"query": query}
    if limit is not None:
        body["limit"] = limit
    if offset is not None:
        body["offset"] = offset
        
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    
    try:
        with urllib.request.urlopen(req) as response:
            status = response.getcode()
            body_text = response.read().decode("utf-8")
            res_data = json.loads(body_text)
            
            print(f"Status: {status}")
            if status != expected_status:
                print(f"FAIL: Expected status {expected_status}, got {status}")
                print(body_text)
                return False
                
            if expected_check:
                if not expected_check(res_data):
                    print(f"FAIL: Check failed. Response: {res_data}")
                    return False
            
            print("PASS")
            return True
            
    except urllib.error.HTTPError as e:
        status = e.code
        body_text = e.read().decode("utf-8")
        print(f"Status: {status}")
        
        if status != expected_status:
           print(f"FAIL: Expected status {expected_status}, got {status}")
           print(body_text)
           return False
           
        print("PASS")
        return True
        
    except Exception as e:
        print(f"FAIL: Exception {e}")
        return False

def main():
    tests = [
        (
            "Basic SELECT", 
            "SELECT 1 as val", 
            500, 0,
            200, 
            lambda d: d["data"][0]["val"] == "1" and d["total_rows"] == 1
        ),
        (
            "CTE Support", 
            "WITH cte AS (SELECT 'test' as t) SELECT * FROM cte", 
            500, 0,
            200, 
            lambda d: d["data"][0]["t"] == "test" and d["total_rows"] == 1
        ),
        (
            "Deny DROP", 
            "DROP TABLE users", 
            500, 0,
            400, 
            None
        ),
        (
            "Deny ISO Update",
            "UPDATE users SET x=1", 
            500, 0,
            400,
            None
        ),
         (
            "Pagination: Page 1", 
            "SELECT * FROM generate_series(1, 20) as n", 
            10, 0,
            200, 
            lambda d: len(d["data"]) == 10 and d["data"][0]["n"] == "1" and d["has_more"] is True and d["total_rows"] == 20
        ),
        (
            "Pagination: Page 2", 
            "SELECT * FROM generate_series(1, 20) as n", 
            10, 10,
            200, 
            lambda d: len(d["data"]) == 10 and d["data"][0]["n"] == "11" and d["has_more"] is False and d["total_rows"] == 20
        ),
        (
            "Pagination: End of Data", 
            "SELECT * FROM generate_series(1, 20) as n", 
            10, 20,
            200, 
            lambda d: len(d["data"]) == 0 and d["has_more"] is False and d["total_rows"] == 20
        ),
        (
            "Default Limit (Unbounded)", 
            "SELECT * FROM generate_series(1, 600) as n", 
            None, None, # Use defaults -> limit 500
            200, 
            lambda d: len(d["data"]) == 500 and d["has_more"] is True and d["total_rows"] == 600
        ),
    ]

    passed = 0
    for args in tests:
        if test_query(*args):
            passed += 1
            
    print(f"\nTotal: {len(tests)}, Passed: {passed}")
    sys.exit(0 if passed == len(tests) else 1)

if __name__ == "__main__":
    main()
