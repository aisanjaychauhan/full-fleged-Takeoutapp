import requests
import re

BASE_URL = "http://127.0.0.1:5005"

def get_session():
    session = requests.Session()
    # Get login page to extract CSRF token
    resp = session.get(f"{BASE_URL}/login")
    match = re.search(r'name="csrf_token" value="(.*)"', resp.text)
    if not match:
        print("[FAIL] CSRF token not found on login page.")
        return None
    csrf_token = match.group(1)
    
    # Login as admin
    resp = session.post(f"{BASE_URL}/login", data={
        "username": "admin", 
        "password": "admin123",
        "csrf_token": csrf_token
    }, allow_redirects=False)
    
    if resp.status_code in [200, 302]:
        print("[INFO] Logged in as admin.")
        return session
    else:
        print(f"[FAIL] Login failed: {resp.status_code}")
        return None

def verify_reports(session):
    if not session: return
    
    print("\nTesting CSV Report...")
    resp = session.get(f"{BASE_URL}/api/admin/audit/backup_history_report/csv")
    if resp.status_code == 200 and "text/csv" in resp.headers.get("Content-Type", ""):
        print("[PASS] CSV Report generated successfully.")
    else:
        print(f"[FAIL] CSV Report failed: {resp.status_code}")
        # print(resp.text)

    print("\nTesting PDF Report...")
    resp = session.get(f"{BASE_URL}/api/admin/audit/backup_history_report/pdf")
    if resp.status_code == 200 and "application/pdf" in resp.headers.get("Content-Type", ""):
        print("[PASS] PDF Report generated successfully.")
    else:
        print(f"[FAIL] PDF Report failed: {resp.status_code}")
        # print(resp.text)

if __name__ == "__main__":
    session = get_session()
    verify_reports(session)
