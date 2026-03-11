import requests
import json

base_url = "http://127.0.0.1:5050"

try:
    print("Checking /api/local/status...")
    resp = requests.get(f"{base_url}/api/local/status")
    print(f"Status: {resp.status_code}, Data: {resp.json()}")

    print("\nChecking /api/local/accounts...")
    resp = requests.get(f"{base_url}/api/local/accounts")
    print(f"Status: {resp.status_code}")
    data = resp.json()
    print(f"Data: {json.dumps(data, indent=2)}")
    
except Exception as e:
    print(f"Error connecting to agent: {e}")
