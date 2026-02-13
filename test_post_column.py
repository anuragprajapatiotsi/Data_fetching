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

def test_post_column():
    BASE_URL = "http://127.0.0.1:8000/datasets"
    
    # 1. Create Dataset
    print("Creating dataset...")
    status, dataset = request("POST", BASE_URL + "/", {"name": "Test POST Column Dataset"})
    if status != 201:
        print("Failed to create dataset:", status, dataset)
        return
    dataset_id = dataset["id"]
    print(f"Dataset created: {dataset_id}")

    # 2. Add Table
    print("Adding table...")
    status, table = request("POST", f"{BASE_URL}/{dataset_id}/tables", {
        "table_name": "users", 
        "position_x": 0, 
        "position_y": 0
    })
    if status != 201:
        print("Failed to add table:", status, table)
        return
    table_id = table["id"]
    print(f"Table added: {table_id}")

    # 3. Create Column via POST
    print("Creating column via POST...")
    col_req = {
        "table_id": table_id,
        "column_name": "email",
        "role": "Dimension",
        "definition_code": "DIM_EMAIL",
        "display_name": "User Email"
    }
    status, column = request(
        "POST", 
        f"{BASE_URL}/{dataset_id}/columns", 
        col_req
    )
    if status != 200:
        print("Failed to create column:", status, column)
        return
    print("Column created:", column)

    if column.get("column_name") == "email" and column.get("dataset_table_id") == table_id:
        print("SUCCESS: POST column verified!")
    else:
        print("FAILURE: Column details specific mismatch.")

if __name__ == "__main__":
    test_post_column()
