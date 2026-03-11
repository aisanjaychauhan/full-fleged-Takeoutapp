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
        # print(resp.text)
        return None

def test_date_validation(session):
    print("\nTesting date validation in Backup Report API...")
    if not session: return
    # Invalid date format should return 400
    resp = session.get(f"{BASE_URL}/api/admin/audit/backup_history_report/csv?start=INVALID&end=2026-01-01")
    if resp.status_code == 400:
        print("[PASS] Correct: Invalid date returned 400.")
    else:
        print(f"[FAIL] Failed: Invalid date returned {resp.status_code}.")

def test_avatar_magic_bytes(session):
    print("\nTesting avatar magic bytes validation...")
    if not session: return
    files = {'avatar': ('malicious.png', b'not an image content', 'image/png')}
    try:
        resp = session.post(f"{BASE_URL}/api/user/avatar", files=files)
        if resp.status_code == 400:
            print("[PASS] Correct: Magic byte check caught non-image file.")
        else:
            print(f"[FAIL] Result: {resp.status_code}")
    except Exception as e:
        print(f"Error: {e}")

def test_avatar_size_limit(session):
    print("\nTesting avatar size limit...")
    if not session: return
    large_data = b"x" * (3 * 1024 * 1024)
    files = {'avatar': ('large.png', large_data, 'image/png')}
    try:
        resp = session.post(f"{BASE_URL}/api/user/avatar", files=files)
        if resp.status_code == 400 and "too large" in resp.text.lower():
            print("[PASS] Correct: 3MB file rejected.")
        elif resp.status_code == 413:
             print("[PASS] Correct: 3MB file rejected by Flask (MAX_CONTENT_LENGTH).")
        else:
            print(f"[FAIL] Result: {resp.status_code}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    session = get_session()
    test_date_validation(session)
    test_avatar_magic_bytes(session)
    test_avatar_size_limit(session)
