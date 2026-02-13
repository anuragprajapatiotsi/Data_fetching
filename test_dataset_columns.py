import json
import urllib.request
import urllib.error

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



def test_columns():
    BASE_URL = "http://127.0.0.1:8000/datasets"
    
    # 1. Create Dataset
    print("Creating dataset...")
    # Add trailing slash for create if needed, or rely on distinct handling
    status, dataset = request("POST", BASE_URL + "/", {"name": "Test Column Dataset"})
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

    # 3. Save Column Metadata
    print("Saving column metadata...")
    col_data = {
        "column_name": "id",
        "role": "Dimension",
        "definition_code": "DIM_001",
        "display_name": "User ID"
    }
    status, column = request(
        "PUT", 
        f"{BASE_URL}/{dataset_id}/tables/{table_id}/columns/id", 
        col_data
    )
    if status != 200:
        print("Failed to save column:", status, column)
        return
    print("Column saved successfully.")

    # 4. Verify in Dataset Fetch
    print("Fetching dataset to verify columns...")
    status, tables = request("GET", f"{BASE_URL}/{dataset_id}/tables")
    if status == 200:
        target_table = next((t for t in tables if t["id"] == table_id), None)
        if target_table:
            cols = target_table.get("columns", [])
            print("Table columns:", cols)
            if cols and cols[0]["role"] == "Dimension" and cols[0]["display_name"] == "User ID":
                print("SUCCESS: Column metadata verified!")
            else:
                print("FAILURE: Column metadata missing or incorrect.")
        else:
            print("FAILURE: Table not found in response.")
    else:
         print("Failed to get tables:", status, tables)

if __name__ == "__main__":
    test_columns()
