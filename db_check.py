
import sqlite3
import json
import os

DB_FILE = "c:/Users/skcha/.gemini/antigravity/scratch/takeout_web_app_v2_backup/users.db"

def check_jobs():
    if not os.path.exists(DB_FILE):
        print(f"DB file {DB_FILE} not found")
        return
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    jobs = conn.execute("SELECT * FROM agent_jobs LIMIT 5").fetchall()
    count = conn.execute("SELECT COUNT(*) FROM agent_jobs").fetchone()[0]
    conn.close()
    
    print(f"Total jobs: {count}")
    for job in jobs:
        print(dict(job))

if __name__ == "__main__":
    check_jobs()
