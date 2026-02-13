import urllib.request
import urllib.error
import json

def check_server():
    url = "http://127.0.0.1:8000/datasets"
    print(f"Checking {url}...")
    try:
        with urllib.request.urlopen(url) as response:
            print(f"Status: {response.status}")
            print("Server is UP and reachable.")
            data = response.read().decode()
            try:
                json_data = json.loads(data)
                print(f"Response: {str(json_data)[:100]}...")
            except:
                print(f"Response (raw): {data[:100]}...")
    except urllib.error.URLError as e:
        print(f"Failed to connect: {e}")
        if hasattr(e, 'reason'):
            print(f"Reason: {e.reason}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    check_server()
