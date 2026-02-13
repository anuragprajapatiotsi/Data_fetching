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

def test_joins_refactor():
    BASE_URL = "http://127.0.0.1:8000/datasets"
    
    # 1. Create Dataset
    print("Creating dataset...")
    status, dataset = request("POST", BASE_URL + "/", {"name": "Join Refactor Test"})
    if status != 201:
        print("Failed to create dataset")
        return
    dataset_id = dataset["id"]
    print(f"Dataset: {dataset_id}")

    # 2. Add Tables
    print("Adding tables...")
    _, t1 = request("POST", f"{BASE_URL}/{dataset_id}/tables", {"table_name": "users", "alias": "u"})
    _, t2 = request("POST", f"{BASE_URL}/{dataset_id}/tables", {"table_name": "orders", "alias": "o"})
    
    t1_id = t1["id"]
    t2_id = t2["id"]
    print(f"Tables: {t1_id} (users), {t2_id} (orders)")

    # 3. Add Join using IDs
    print("Adding join...")
    join_payload = {
        "left_dataset_table_id": t1_id,
        "left_column": "id",
        "right_dataset_table_id": t2_id,
        "right_column": "user_id",
        "join_type": "inner"
    }
    status, join = request("POST", f"{BASE_URL}/{dataset_id}/joins", join_payload)
    if status != 201:
        print("Failed to add join:", status, join)
        return
    
    join_id = join["id"]
    print(f"Join added: {join['id']}")
    
    if join["left_dataset_table_id"] == t1_id and join["right_dataset_table_id"] == t2_id:
        print("VERIFIED: Response contains correct table IDs")
    else:
        print("FAILURE: IDs mismatch in response")

    # 4. Verify GET joins
    print("Fetching joins...")
    status, joins = request("GET", f"{BASE_URL}/{dataset_id}/joins")
    if status == 200:
        target = next((j for j in joins if j["id"] == join_id), None)
        if target:
            print(f"Join found in list: {target['id']}")
        else:
            print("FAILURE: Join not found in list")
    
    # 5. Test Invalid Join (tables not in dataset)
    print("Testing invalid join...")
    bad_payload = join_payload.copy()
    bad_payload["left_dataset_table_id"] = str(uuid4()) # Random ID
    status, err = request("POST", f"{BASE_URL}/{dataset_id}/joins", bad_payload)
    if status == 400:
        print("VERIFIED: Invalid join rejected with 400")
    else:
        print(f"FAILURE: Invalid join returned {status}")

if __name__ == "__main__":
    test_joins_refactor()
