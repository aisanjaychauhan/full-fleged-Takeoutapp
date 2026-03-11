"""
Takeout Automator — Windows Agent
Runs locally on a team member's laptop.
Connects to a Central Server for account data and job reporting.
All Selenium automation runs locally; downloads go to local machine.
"""

import json
import os
import sys
import socket
import uuid
import threading
import time
import traceback
import secrets
import webbrowser
from functools import wraps
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, Response

import requests as http_requests  # for talking to central server

# ── Local config ──────────────────────────────────────────────────────
AGENT_PORT = 5050
CONFIG_FILE = "agent_config.json"
AGENT_ID = str(uuid.uuid4())  # unique per installation; overwritten from config

# ── EXE Compatibility ──────────────────────────────────────────────────
if getattr(sys, 'frozen', False):
    # Running in a PyInstaller bundle
    template_folder = os.path.join(sys._MEIPASS, 'templates')
    static_folder = os.path.join(sys._MEIPASS, 'static')
    app = Flask(__name__, template_folder=template_folder, static_folder=static_folder)
else:
    # Running in normal Python environment
    app = Flask(__name__)

# Bandit B105: Use environment variable or random token for secret key
app.secret_key = os.getenv("AGENT_SECRET_KEY", secrets.token_hex(32))

# ── Runtime state ─────────────────────────────────────────────────────
agent_logs = []
agent_running = False
agent_stop_event = threading.Event()
log_lock = threading.Lock()
active_ctx = None
last_screenshot = ""
last_accounts_cache = []
show_browser_setting = True  # Global visibility state

# ── Playwright Installer ──────────────────────────────────────────────

def ensure_browsers():
    try:
        print("Checking browser dependencies... (this may take a minute on first run)")
        import subprocess
        import sys
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        print(f"Non-fatal warning: Could not auto-install chromium via playwright: {e}")

threading.Thread(target=ensure_browsers, daemon=True).start()

# ── Config helpers ────────────────────────────────────────────────────

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_config(cfg):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)

def is_configured():
    cfg = load_config()
    return bool(cfg.get("server_url") and cfg.get("agent_token"))

def server_headers():
    cfg = load_config()
    return {"Authorization": f"Bearer {cfg.get('agent_token', '')}",
            "Content-Type": "application/json"}

def server_url():
    return load_config().get("server_url", "").rstrip("/")

# ── Heartbeat thread ──────────────────────────────────────────────────

def heartbeat_loop():
    while True:
        time.sleep(30)
        if is_configured():
            try:
                http_requests.post(
                    f"{server_url()}/api/agent/heartbeat",
                    headers=server_headers(),
                    timeout=5
                )
            except Exception:
                pass

threading.Thread(target=heartbeat_loop, daemon=True).start()

# ── Job Polling Loop ─────────────────────────────────────────────────

def job_polling_loop():
    """Polls the central server for assigned jobs."""
    while True:
        time.sleep(10)
        if is_configured() and not agent_running:
            try:
                cfg = load_config()
                resp = http_requests.get(
                    f"{server_url()}/api/agent/jobs/next",
                    headers=server_headers(),
                    timeout=5
                )
                if resp.status_code == 200:
                    data = resp.json()
                    job = data.get("job")
                    if job:
                        # Convert job to format expected by local_start logic
                        email = job["email"]
                        mode = job["mode"]
                        job_id = job["id"]
                        
                        # Fetch full credentials for this account
                        cred_resp = http_requests.get(
                            f"{server_url()}/api/agent/credentials",
                            headers=server_headers(),
                            timeout=8
                        )
                        all_creds = cred_resp.json()
                        target_acc = next((a for a in all_creds if a["email"] == email), None)
                        
                        if target_acc:
                            print(f"POLLER: Found job {job_id} for {email} in {mode} mode.")
                            trigger_local_automation([target_acc], mode, {"showBrowser": show_browser_setting}, job_id=job_id)
            except Exception as e:
                print(f"POLLER ERROR: {e}")

threading.Thread(target=job_polling_loop, daemon=True).start()

# ── Logging helper ────────────────────────────────────────────────────

def emit_log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{ts}] {msg}"
    try:
        print(entry) 
    except Exception:
        # Fallback for consoles that can't handle emojis/special chars
        try:
            print(entry.encode('ascii', 'replace').decode())
        except Exception:
            pass
    with log_lock:
        agent_logs.append(entry)
        if len(agent_logs) > 2000:
            agent_logs.pop(0)

# ── Routes: Setup ─────────────────────────────────────────────────────

@app.route("/")
def index():
    if not is_configured():
        return redirect(url_for("setup"))
    return redirect(url_for("dashboard"))

@app.route("/setup")
def setup():
    return render_template("agent_setup.html")

@app.route("/api/agent/connect", methods=["POST"])
def agent_connect():
    """First-run: user provides server URL + invite token."""
    data = request.get_json()
    srv = data.get("server_url", "").strip().rstrip("/")
    # Auto-prepend http:// if no scheme is provided (e.g. user types "10.1.208.246:5010")
    if srv and not srv.startswith(("http://", "https://")):
        srv = "http://" + srv
    invite_token = data.get("invite_token", "").strip()

    if not srv or not invite_token:
        return jsonify({"error": "Server URL and invite token are required"}), 400

    cfg = load_config()
    a_id = cfg.get("agent_id") or str(uuid.uuid4())

    try:
        resp = http_requests.post(
            f"{srv}/api/agent/register",
            json={
                "invite_token": invite_token,
                "agent_id": a_id,
                "hostname": socket.gethostname()
            },
            timeout=10
        )
        result = resp.json()
    except Exception as e:
        return jsonify({"error": f"Could not reach server: {e}"}), 502

    if not result.get("success"):
        return jsonify({"error": result.get("error", "Registration failed")}), 400

    save_config({
        "server_url": srv,
        "agent_id": a_id,
        "agent_token": result["agent_token"],
        "username": result["username"]
    })

    return jsonify({"success": True, "username": result["username"]})

@app.route("/api/agent/disconnect", methods=["POST"])
def agent_disconnect():
    if os.path.exists(CONFIG_FILE):
        os.remove(CONFIG_FILE)
    return jsonify({"success": True})

@app.route("/api/local/disconnect", methods=["POST"])
def local_disconnect():
    if os.path.exists(CONFIG_FILE):
        os.remove(CONFIG_FILE)
    return jsonify({"success": True})

# ── Routes: Dashboard ─────────────────────────────────────────────────

@app.route("/dashboard")
def dashboard():
    if not is_configured():
        return redirect(url_for("setup"))
    cfg = load_config()
    return render_template("agent_dashboard.html",
                           username=cfg.get("username", ""),
                           server_url=cfg.get("server_url", ""),
                           hostname=socket.gethostname(),
                           agent_id=cfg.get("agent_id", ""))

@app.route("/api/agent/info")
def agent_info():
    cfg = load_config()
    return jsonify({
        "configured": is_configured(),
        "username": cfg.get("username", ""),
        "server_url": cfg.get("server_url", ""),
        "hostname": socket.gethostname(),
        "agent_id": cfg.get("agent_id", "")
    })

@app.route("/api/local/settings", methods=["GET"])
def local_settings():
    """Retrieve agent-specific automation parameters."""
    cfg = load_config()
    return jsonify({
        "username": cfg.get("username", "unknown"),
        "hostname": socket.gethostname()
    })

@app.route("/api/local/stats")
def local_stats():
    """Proxy stats to the central server."""
    try:
        cfg = load_config()
        resp = http_requests.get(
            f"{server_url()}/api/agent/stats",
            headers={"Authorization": f"Bearer {cfg.get('agent_token', '')}"},
            timeout=8
        )
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 502

@app.route("/api/local/status")
def local_status():
    """Show current agent execution state."""
    global agent_running, agent_logs, last_screenshot
    return jsonify({
        "running": agent_running,
        "log_count": len(agent_logs),
        "has_screenshot": bool(last_screenshot)
    })

@app.route("/api/local/accounts")
def local_accounts():
    """Fetch this user's accounts from the central server, merging local status."""
    global last_accounts_cache
    try:
        cfg = load_config()
        resp = http_requests.get(
            f"{server_url()}/api/agent/accounts",
            headers={"Authorization": f"Bearer {cfg.get('agent_token', '')}"},
            timeout=8
        )
        server_data = resp.json()
        
        # Merge local status if running
        if last_accounts_cache:
            for s_acc in server_data:
                for l_acc in last_accounts_cache:
                    if s_acc.get('email') == l_acc.get('email'):
                        # Prefer local status if set
                        if l_acc.get('create_status'):
                            s_acc['create_status'] = l_acc['create_status']
                        if l_acc.get('download_status'):
                            s_acc['download_status'] = l_acc['download_status']
                        break
        
        last_accounts_cache = server_data
        return jsonify(server_data)
    except Exception as e:
        # If server is unreachable, return last known local cache
        if last_accounts_cache:
            return jsonify(last_accounts_cache)
        return jsonify({"error": str(e)}), 502

@app.route("/api/local/add_account", methods=["POST"])
def local_add_account():
    """Manually add a target account to the central server."""
    try:
        data = request.get_json()
        cfg = load_config()
        resp = http_requests.post(
            f"{server_url()}/api/add_target_account",
            json=data,
            headers={"Authorization": f"Bearer {cfg.get('agent_token', '')}"},
            timeout=15
        )
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 502

@app.route("/api/local/upload_csv", methods=["POST"])
def local_upload_csv():
    """Proxy CSV upload to central server."""
    try:
        if 'file' not in request.files:
            return jsonify({"message": "No file part"}), 400
        file = request.files['file']
        if file.filename == '':
            return jsonify({"message": "No selected file"}), 400
        
        cfg = load_config()
        # Relay the file to Central Server
        resp = http_requests.post(
            f"{server_url()}/api/upload_csv",
            files={'file': (file.filename, file.stream, 'text/csv')},
            headers={"Authorization": f"Bearer {cfg.get('agent_token', '')}"},
            timeout=30
        )
        return jsonify(resp.json())
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 502

@app.route("/api/local/delete_accounts", methods=["POST"])
def local_delete_accounts():
    """Proxy batch deletion to the central server."""
    try:
        data = request.get_json()
        cfg = load_config()
        resp = http_requests.post(
            f"{server_url()}/api/delete_accounts_bulk",
            json={"emails": data.get("emails", [])},
            headers={"Authorization": f"Bearer {cfg.get('agent_token', '')}"},
            timeout=10
        )
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 502

@app.route("/api/local/export_csv")
def local_export_csv():
    """Proxy CSV export to the central server."""
    try:
        cfg = load_config()
        resp = http_requests.get(
            f"{server_url()}/api/agent/accounts/export",
            headers={"Authorization": f"Bearer {cfg.get('agent_token', '')}"},
            stream=True,
            timeout=15
        )
        return Response(
            resp.iter_content(chunk_size=1024),
            content_type=resp.headers.get("content-type"),
            headers={"Content-Disposition": resp.headers.get("Content-Disposition")}
        )
    except Exception as e:
        return str(e), 502

# ── Routes: Automation ────────────────────────────────────────────────

def trigger_local_automation(accounts, mode, settings, job_id=None):
    global agent_running, agent_stop_event
    
    agent_stop_event.clear()
    agent_running = True
    agent_logs.clear()

    def run_worker():
        global agent_running, last_screenshot
        last_screenshot = ""  # Clear stale state from previous run
        cfg = load_config()
        emit_log(f"🚀 Agent starting in [{mode.upper()}] mode for {len(accounts)} accounts...")

        for acc in accounts:
            if agent_stop_event.is_set():
                emit_log("🛑 Stopped by user.")
                break
            email = acc["email"]
            emit_log(f"▶ Processing: {email}")

            acc_data = acc 
            job_logs = []

            class LocalContext:
                def __init__(self, log_list, current_email):
                    self.logs = log_list
                    self.stop_event = agent_stop_event
                    self.pause_event = threading.Event()
                    self.pause_event.set()
                    self.frame_b64 = ""
                    self.log_file = None
                    self.server_url = server_url()
                    self.headers = server_headers()
                    self.current_email = current_email
                
                def emit_log(self, msg, **kwargs):
                    emit_log(msg)
                    self.logs.append(msg)

                def update_status(self, email, status, mode="create"):
                    global last_accounts_cache
                    for acc in last_accounts_cache:
                        if acc.get('email') == email:
                            if mode == "create":
                                acc['create_status'] = status
                            else:
                                acc['download_status'] = status
                            break
                    # Also update on central server
                    try:
                        http_requests.post(
                            f"{self.server_url}/api/agent/status/update",
                            json={"email": email, "status": status, "mode": mode},
                            headers=self.headers,
                            timeout=5
                        )
                    except: pass

            ctx = LocalContext(job_logs, email)
            global active_ctx
            active_ctx = ctx

            try:
                # Run locally
                def sync_screenshots():
                    global last_screenshot
                    while agent_running and not agent_stop_event.is_set():
                        if hasattr(ctx, 'frame_b64') and ctx.frame_b64:
                            last_screenshot = ctx.frame_b64
                        time.sleep(1)
                
                ss_thread = threading.Thread(target=sync_screenshots, daemon=True)
                ss_thread.start()

                run_settings = {**settings, "_run_owner": cfg.get("username", "admin")}
                # Ensure showBrowser from our global state is used if not in settings
                run_settings["showBrowser"] = settings.get("showBrowser", show_browser_setting)

                emit_log(f"🔧 Initializing [{run_settings.get('browser_type', 'chrome')}] driver (Visibility: {'Visible' if run_settings.get('showBrowser') else 'Hidden'})...")
                if mode == "create":
                    import takeout_runner
                    takeout_runner.run_all(run_settings, ctx, data_source="agent", agent_accounts=[acc_data])
                else:
                    import takeout_downloader
                    takeout_downloader.run_all(run_settings, ctx, data_source="agent", agent_accounts=[acc_data])

                final_status = "done"
                emit_log(f"✅ Completed: {email}")
            except Exception as e:
                final_status = "failed"
                tb_str = traceback.format_exc()
                job_logs.append(f"❌ Error: {e}")
                ctx.emit_log(f"❌ Failed: {email} — {e}")
                ctx.emit_log(f"💥 FATAL EXCEPTION:\n{tb_str}")
                traceback.print_exc()

            if job_id:
                try:
                    http_requests.post(
                        f"{server_url()}/api/agent/jobs/report",
                        json={"job_id": job_id, "status": final_status, "logs": "\n".join(job_logs)},
                        headers=server_headers(),
                        timeout=5
                    )
                except: pass

        emit_log("🏁 All accounts processed.")
        agent_running = False

    threading.Thread(target=run_worker, daemon=True).start()

@app.route("/api/local/start", methods=["POST"])
def local_start():
    global agent_running

    if agent_running:
        return jsonify({"error": "Already running"}), 400

    data = request.get_json()
    mode = data.get("mode", "create")
    settings = data.get("settings", {})

    try:
        cfg = load_config()
        resp = http_requests.get(
            f"{server_url()}/api/agent/credentials",
            headers={"Authorization": f"Bearer {cfg.get('agent_token', '')}"},
            timeout=8
        )
        accounts = resp.json()
    except Exception as e:
        return jsonify({"error": f"Could not fetch accounts: {e}"}), 502

    if not accounts:
        return jsonify({"error": "No accounts found on the server for your user."}), 400

    global last_accounts_cache
    last_accounts_cache = accounts
    
    trigger_local_automation(accounts, mode, settings)
    return jsonify({"started": True})

@app.route("/api/local/stop", methods=["POST"])
def local_stop():
    global agent_running
    agent_stop_event.set()
    agent_running = False
    return jsonify({"stopped": True})


@app.route("/api/local/pause", methods=["POST"])
def local_pause():
    global active_ctx
    if active_ctx:
        active_ctx.pause_event.clear()
        return jsonify({"paused": True})
    return jsonify({"error": "No active session"}), 400

@app.route("/api/local/resume", methods=["POST"])
def local_resume():
    global active_ctx
    if active_ctx:
        active_ctx.pause_event.set()
        return jsonify({"resumed": True})
    return jsonify({"error": "No active session"}), 400

@app.route("/api/local/logs")
def local_logs():
    def generate():
        sent = 0
        while True:
            with log_lock:
                new_logs = agent_logs[sent:]
                sent = len(agent_logs)
            for line in new_logs:
                yield f"data: {json.dumps(line)}\n\n"
            time.sleep(0.5)
    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.route("/api/local/toggle_browser", methods=["POST"])
def local_toggle_browser():
    global show_browser_setting
    data = request.get_json()
    show_browser_setting = data.get("show", True)
    print(f"TOGGLE: show_browser_setting is now {show_browser_setting}")
    return jsonify({"success": True, "show": show_browser_setting})

if __name__ == "__main__":
    # Ensure data dir exists
    if not os.path.exists("data"):
        os.makedirs("data")
    
    # Start job polling in the background if configured
    if is_configured():
        emit_log("📡 Initializing job polling loop...")
        poll_thread = threading.Thread(target=job_polling_loop, daemon=True)
        poll_thread.start()
    else:
        emit_log("⚠️ Agent not configured. Polling disabled.")

    print(f"Agent Dashboard running at: http://localhost:5006")
    # Open browser automatically on startup (optional but helpful)
    # threading.Timer(1.5, lambda: webbrowser.open("http://localhost:5006")).start()
    app.run(port=5006, debug=False, host='0.0.0.0')
