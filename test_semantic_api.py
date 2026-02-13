import urllib.request
import json
import sys

API_URL = "http://127.0.0.1:8000/semantic-modeling/column-types"

def test_api():
    try:
        # Test without filter
        print(f"Testing GET {API_URL}...")
        with urllib.request.urlopen(API_URL) as response:
            print("Status:", response.status)
            if response.status == 200:
                data = json.loads(response.read().decode())
                print("Response (first 2):", data[:2])
            else:
                print("Error:", response.read().decode())

        # Test with filter
        filter_url = f"{API_URL}?type=Dimension"
        print(f"\nTesting GET {filter_url}...")
        with urllib.request.urlopen(filter_url) as response:
            print("Status:", response.status)
            if response.status == 200:
                data = json.loads(response.read().decode())
                print(f"Response count: {len(data)}")
                if data:
                    print("First filtered item:", data[0])
            else:
                print("Error:", response.read().decode())

    except urllib.error.URLError as e:
        print("Failed to connect:", e)
    except Exception as e:
        print("An error occurred:", e)

if __name__ == "__main__":
    test_api()
