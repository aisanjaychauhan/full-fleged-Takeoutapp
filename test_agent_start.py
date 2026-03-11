import requests
import json
import time

agent_url = "http://127.0.0.1:5050"

def log(msg):
    print(msg)

try:
    log("Requesting local start in visible mode...")
    payload = {
        "mode": "create",
        "settings": {
            "browser_type": "chrome",
            "showBrowser": True
        }
    }
    resp = requests.post(f"{agent_url}/api/local/start", json=payload)
    print(f"Status: {resp.status_code}")
    print(f"Response: {resp.text}")

    if resp.status_code == 200:
        log("Automation started. Waiting 10 seconds to check browser status...")
        for i in range(10):
            time.sleep(1)
            status = requests.get(f"{agent_url}/api/local/status").json()
            print(f"T+{i+1}s Status: {status}")
            if not status.get('running'):
                 break
        
        log("Checking logs for activity...")
        logs = requests.get(f"{agent_url}/api/local/logs").json()
        print(f"Logs: {json.dumps(logs[-5:], indent=2)}")

except Exception as e:
    log(f"Error: {e}")
