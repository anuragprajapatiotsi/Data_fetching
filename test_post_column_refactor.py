import json
import urllib.request
import urllib.error
from uuid import uuid4

def request(method, url, data=None):
    req = urllib.request.Request(url, method=method)
    req.add_header('Content-Type', 'application/json')
    if data:
        req.data = json.dumps(data).encode()
    try:
        with urllib.request.urlopen(req) as response:
            return response.status, json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        print(f"HTTPError {e.code}: {raw}")
        try:
            return e.code, json.loads(raw)
        except:
            return e.code, None
    except Exception as e:
        print(f"Request failed: {e}")
        return 500, None

def test_post_column_refactor():
    BASE_URL = "http://127.0.0.1:8000/datasets"
    
    # 1. Create Dataset
    print("Creating dataset...")
    status, dataset = request("POST", BASE_URL + "/", {"name": "Column Refactor Test"})
    if status != 201:
        print("Failed to create dataset")
        return
    dataset_id = dataset["id"]
    print(f"Dataset: {dataset_id}")

    # 2. Add Table
    print("Adding table...")
    _, table = request("POST", f"{BASE_URL}/{dataset_id}/tables", {"table_name": "users", "alias": "u"})
    table_id = table["id"]
    print(f"Table ID: {table_id}")

    # 3. Create Column using dataset_table_id
    print("Creating column...")
    col_payload = {
        "dataset_table_id": table_id,
        "column_name": "created_at",
        "role": "Dimension",
        "definition_code": "DIM_002",
        "display_name": "Order Month"
    }
    status, column = request("POST", f"{BASE_URL}/{dataset_id}/columns", col_payload)
    
    if status == 200:
        print("VERIFIED: Column created successfully with dataset_table_id")
        print(f"Column: {column}")
    else:
        print(f"FAILURE: Failed to create column. Status: {status}, Response: {column}")

    # 4. Test Invalid Payload (old format)
    print("Testing invalid payload (table_name)...")
    bad_payload = {
        "table_name": "users", # No longer supported
        "column_name": "fail_col"
    }
    status, err = request("POST", f"{BASE_URL}/{dataset_id}/columns", bad_payload)
    if status == 422: # Validation Error
        print("VERIFIED: Old format rejected with 422 (Schema Validation Error)")
    else:
        print(f"FAILURE: Expected 422, got {status}")

if __name__ == "__main__":
    test_post_column_refactor()
