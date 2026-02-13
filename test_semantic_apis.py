import json
import urllib.request
import urllib.error
import asyncio
from uuid import uuid4
from sqlalchemy import text
from app.core.database import engine

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

async def setup_physical_tables():
    print("Setting up physical tables (users_test, orders_test)...")
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS orders_test"))
        await conn.execute(text("DROP TABLE IF EXISTS users_test"))
        
        await conn.execute(text("""
            CREATE TABLE users_test (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50),
                country VARCHAR(50)
            )
        """))
        await conn.execute(text("""
            CREATE TABLE orders_test (
                id SERIAL PRIMARY KEY,
                user_id INTEGER,
                amount INTEGER,
                created_at DATE
            )
        """))
        
        await conn.execute(text("INSERT INTO users_test (username, country) VALUES ('alice', 'USA'), ('bob', 'UK')"))
        await conn.execute(text("INSERT INTO orders_test (user_id, amount, created_at) VALUES (1, 100, '2024-01-01'), (1, 200, '2024-01-02'), (2, 50, '2024-01-03')"))
    print("Physical tables created.")

def test_semantic_apis():
    BASE_URL = "http://127.0.0.1:8000/datasets"
    
    # 1. Create Dataset
    print("Creating dataset...")
    status, dataset = request("POST", BASE_URL + "/", {"name": "Semantic API Test"})
    dataset_id = dataset["id"]
    
    # 2. Add Tables
    print("Adding tables...")
    _, t1 = request("POST", f"{BASE_URL}/{dataset_id}/tables", {"table_name": "users_test", "alias": "u"})
    _, t2 = request("POST", f"{BASE_URL}/{dataset_id}/tables", {"table_name": "orders_test", "alias": "o"})
    t1_id = t1["id"]
    t2_id = t2["id"]
    
    # 3. Add Join
    print("Adding join...")
    join_payload = {
        "left_dataset_table_id": t1_id,
        "left_column": "id",
        "right_dataset_table_id": t2_id,
        "right_column": "user_id",
        "join_type": "inner"
    }
    request("POST", f"{BASE_URL}/{dataset_id}/joins", join_payload)
    
    # 4. Add Columns (1 Dimension, 1 Indicator)
    print("Adding columns...")
    # Dimension: Country
    request("POST", f"{BASE_URL}/{dataset_id}/columns", {
        "dataset_table_id": t1_id,
        "column_name": "country",
        "role": "Dimension",
        "display_name": "User Country"
    })
    # Indicator: Amount
    request("POST", f"{BASE_URL}/{dataset_id}/columns", {
        "dataset_table_id": t2_id,
        "column_name": "amount",
        "role": "Indicator",
        "display_name": "Total Revenue"
    })
    
    # 5. Test GET Columns
    print("Testing GET /columns...")
    status, cols = request("GET", f"{BASE_URL}/{dataset_id}/columns")
    if status == 200 and len(cols) == 2:
        print("VERIFIED: Retrieved 2 columns.")
    else:
        print(f"FAILURE: GET columns failed. Status: {status}, Data: {cols}")

    # 6. Test GET Preview
    print("Testing GET /preview...")
    status, preview = request("GET", f"{BASE_URL}/{dataset_id}/preview")
    if status == 200:
        print("Preview Result:", preview)
        # Expect: Alice(USA): 300, Bob(UK): 50
        # Rows should be [['USA', 300], ['UK', 50]] (order depends on DB)
        rows = preview["rows"]
        if ["USA", 300] in rows or [300, "USA"] in rows: # order of cols might vary
             print("VERIFIED: Data aggregation correct.")
        else:
             # Check distinct values
             vals = [r[1] for r in rows] # assuming amount is 2nd
             if 300 in vals and 50 in vals:
                  print("VERIFIED: Data aggregation correct.")
             else:
                  print("FAILURE: Validation of rows failed.")
    else:
        print(f"FAILURE: Preview failed. Status: {status}, Data: {preview}")

if __name__ == "__main__":
    # Run async setup then sync test
    loop = asyncio.new_event_loop()
    loop.run_until_complete(setup_physical_tables())
    test_semantic_apis()
