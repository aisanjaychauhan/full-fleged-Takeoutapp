import requests
import json

server_url = "http://127.0.0.1:5005"
token = "Okm7jWy_6oz2R7eqD3rB40BQe6YKf6CYt9QMBG_reFXVOD7l7BJdeQ"

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

try:
    print(f"Testing {server_url}/api/agent/accounts...")
    resp = requests.get(f"{server_url}/api/agent/accounts", headers=headers)
    print(f"Status: {resp.status_code}")
    print(f"Response: {resp.text}")

    print(f"\nTesting {server_url}/api/agent/credentials...")
    resp = requests.get(f"{server_url}/api/agent/credentials", headers=headers)
    print(f"Status: {resp.status_code}")
    print(f"Response: {resp.text}")

except Exception as e:
    print(f"Error: {e}")
