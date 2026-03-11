import os
import time
import tempfile
import traceback
import sqlite3
import glob
import pyautogui
import threading
import concurrent.futures
import gspread
pyautogui.FAILSAFE = False
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# from webdriver_manager.chrome import ChromeDriverManager
# from webdriver_manager.firefox import GeckoDriverManager
# from webdriver_manager.microsoft import EdgeChromiumDriverManager
from cryptography.fernet import Fernet
from dotenv import load_dotenv

# Email utility
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email(to_email, subject, html_body):
    smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", 587))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_password = os.getenv("SMTP_PASSWORD", "")
    if not smtp_user or not smtp_password: return False
    msg = MIMEMultipart("alternative")
    msg["Subject"], msg["From"], msg["To"] = subject, f"Takeout Automator <{smtp_user}>", to_email
    msg.attach(MIMEText(html_body, "html"))
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.ehlo(); server.starttls(); server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, to_email, msg.as_string())
        return True
    except: return False

load_dotenv()
_fernet_key = os.getenv('FERNET_KEY')
_fernet = Fernet(_fernet_key.encode()) if _fernet_key else None

def decrypt_password(stored: str) -> str:
    if _fernet is None:
        return stored
    try:
        return _fernet.decrypt(stored.encode()).decode()
    except Exception:
        return stored

TAKEOUT_URL = "https://takeout.google.com/settings/takeout/downloads"

def run_all(settings, context, data_source="csv", **kwargs):
    class UserCancelledException(BaseException):
        pass

    DB_FILE = "users.db"
    run_owner = settings.get('_run_owner', 'admin')

    log_lock = threading.Lock()
    
    # --- Tracking for Notifications ---
    stats = {'success': 0, 'failed': 0, 'skipped': 0, 'errors': []}

    def check_cancel():
        if context and getattr(context, 'stop_event', None) and context.stop_event.is_set():
            raise UserCancelledException("Cancelled by user")

    def check_pause():
        if context and getattr(context, 'pause_event', None):
            if not context.pause_event.is_set():
                if context and context.logs is not None:
                    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                    context.logs.append(f"[{timestamp}] ⏸️ Script paused. Waiting to resume...")
            context.pause_event.wait()

    def send_account_notification(email, status_msg, error_msg=""):
        if not settings.get('notify_on_complete', False):
            return

        # ── Recipient = agent's own email injected by agent.py (notify_to_email) ──
        # sender = SMTP_USER (e.g. sanjay.chauhan@tothenew.com)
        # recipient = agent operator (e.g. adobe-admin@tothenew.com)
        recipient_email = (
            settings.get('notify_to_email') or         # agent's registered email (primary)
            os.getenv("SMTP_USER", "") or              # fallback: SMTP user itself
            email                                       # last resort: target account
        )
        if not recipient_email: return

        mode_name = "Download Backups"
        subject = f"[{'ERROR' if error_msg else 'SUCCESS'}] Takeout Automation: {email}"

        html = f"<h2>Automation Session: {mode_name}</h2>"
        html += f"<p><strong>Target Account:</strong> {email}</p>"
        if error_msg:
            html += f"<h3 style='color:red;'>Task Failed</h3><p><strong>Error:</strong> {error_msg}</p>"
        else:
            html += f"<h3 style='color:green;'>Task Completed Successfully</h3><p><strong>Status:</strong> {status_msg}</p>"

        try:
            send_email(recipient_email, subject, html)
            emit_log(f"📧 Notification sent to {recipient_email} for account {email}")
        except Exception as e:
            emit_log(f"⚠️ Failed to send notification email: {e}")

    def emit_log(msg, bypass_cancel=False):
        if not bypass_cancel: 
            check_cancel()
            check_pause()
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        formatted = f"[{timestamp}] {msg}"
        with log_lock:
            try:
                print(formatted)
            except Exception:
                pass # Ignore console print errors for emojis on Windows
            if context:
                if hasattr(context, "emit_log"):
                    context.emit_log(msg)
                else:
                    context.logs.append(formatted)
                
                log_f = getattr(context, 'log_file', None)
                if log_f:
                    try:
                        with open(log_f, "a", encoding="utf-8") as f:
                            f.write(formatted + "\n")
                    except Exception:
                        pass
        
    def save_step_screenshot(driver, name):
        check_cancel()
        check_pause()
        try:
            if context:
                context.frame_b64 = driver.get_screenshot_as_base64()
        except Exception as e:
            if str(e) == "Cancelled by user": raise
            emit_log(f"⚠️ Screenshot update error: {e}")

    def safe_get_url(driver):
        """Safely get the current URL; returns empty string if driver is invalid/closed."""
        try:
            url = driver.current_url
            return (url or '').lower()
        except Exception:
            return ''

    def get_delay(seconds):
        speed = settings.get('speed', 'safe')
        if speed == 'fast':
            return seconds * 0.5
        return seconds

    def click_element_js(driver, element):
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
        time.sleep(get_delay(0.3))
        driver.execute_script("arguments[0].click();", element)
        time.sleep(get_delay(0.6))

    # --- State for dynamic Sheet indexing ---
    state = {'status_col': 5} # Downloader typically uses col 5

    def update_status(email, text, row_idx=None, sheet=None, bypass_cancel=False):
        if not bypass_cancel: 
            check_cancel()
            check_pause()
        try:
            if data_source == "csv":
                conn = sqlite3.connect(DB_FILE, timeout=10.0)
                conn.execute("UPDATE target_accounts SET download_status = ? WHERE email = ? AND owner = ?", (text, email, run_owner))
                conn.commit()
                conn.close()
            elif data_source == "agent" and context:
                # Update central server via API
                from requests import post as http_post
                try:
                    http_post(
                        f"{context.server_url}/api/agent/status/update",
                        json={"email": email, "status": text, "mode": "download"},
                        headers=context.headers,
                        timeout=5
                    )
                    # ALSO update local agent cache for immediate UI feedback
                    if hasattr(context, 'update_status'):
                        context.update_status(email, text, mode="download")
                except Exception as e:
                    emit_log(f"⚠️ Server status update failed: {e}")
            elif data_source == "sheet" and sheet and row_idx:
                sheet.update_cell(row_idx, state['status_col'], text)
        except Exception as e:
            if str(e) == "Cancelled by user": raise
            emit_log(f"⚠️ Failed to update status: {e}")
        emit_log(f"📊 [{email}] {text}")

    def update_backup_codes(email, codes_list, row_idx=None, sheet=None):
        codes_str = ",".join(codes_list)
        try:
            if data_source == "csv":
                conn = sqlite3.connect(DB_FILE, timeout=10.0)
                conn.execute("UPDATE target_accounts SET backup_codes = ? WHERE email = ? AND owner = ?", (codes_str, email, run_owner))
                conn.commit()
                conn.close()
            elif data_source == "agent" and context:
                from requests import post as http_post
                try:
                    http_post(
                        f"{context.server_url}/api/agent/backup_codes/update",
                        json={"email": email, "backup_codes": codes_str},
                        headers=context.headers,
                        timeout=5
                    )
                except Exception as e:
                    emit_log(f"⚠️ Server codes update failed: {e}")
            elif data_source == "sheet" and sheet and row_idx:
                pass
        except Exception as e:
            emit_log(f"⚠️ Failed to update backup codes: {e}")

    def create_driver(email=""):
        browser_type = settings.get('browser_type', 'chrome').lower()
        show_browser = settings.get('showBrowser', True)
        
        emit_log(f"🛠️ Driver Factory: Type={browser_type}, Visible={show_browser}")
        custom_path = settings.get('download_path', '').strip()
        download_dir = custom_path if custom_path and os.path.isabs(custom_path) else os.path.join(os.path.expanduser("~"), "Downloads")
        if email:
            safe_email = email.replace("@", "_").replace(".", "_")
            download_dir = os.path.join(download_dir, safe_email)
        os.makedirs(download_dir, exist_ok=True)
        download_dir_win = download_dir.replace('/', '\\')

        if browser_type == 'firefox':
            opts = FirefoxOptions()
            opts.add_argument("-private")
            if not show_browser:
                opts.add_argument("-headless")
            opts.set_preference("browser.download.dir", download_dir)
            opts.set_preference("browser.download.folderList", 2)
            opts.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/zip,application/octet-stream")
            return webdriver.Firefox(options=opts)

        elif browser_type == 'edge':
            opts = EdgeOptions()
            opts.add_argument("--inprivate")
            opts.add_argument("--window-size=1200,900")
            if not show_browser:
                opts.add_argument("--headless=new")
            opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
            prefs = {
                "download.default_directory": download_dir_win,
                "download.prompt_for_download": False,
                "profile.default_content_setting_values.automatic_downloads": 1
            }
            opts.add_experimental_option("prefs", prefs)
            return webdriver.Edge(options=opts)

        else:  # Default to Chrome
            opts = ChromeOptions()
            if not show_browser:
                emit_log("🕶️ Enabling Headless Mode (Chrome)")
                opts.add_argument("--headless=new")
                opts.add_argument("--disable-gpu")
                opts.add_argument("--window-size=1920,1080")
                opts.add_argument("--remote-debugging-port=9222")
            else:
                emit_log("📺 Enabling Visible Mode (Chrome)")
            opts.add_argument("--window-size=1200,900")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            opts.add_argument("--disable-popup-blocking")
            opts.add_argument("--disable-notifications")
            opts.add_argument("--disable-extensions")
            opts.add_argument("--disable-blink-features=AutomationControlled")
            opts.add_argument("--no-zygote")
            opts.add_argument("--no-first-run")
            opts.add_argument("--no-default-browser-check")

            opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
            opts.add_experimental_option("useAutomationExtension", False)
            temp_profile = tempfile.mkdtemp()
            opts.add_argument(f"--user-data-dir={temp_profile}")

            prefs = {
                "download.default_directory": download_dir_win,
                "savefile.default_directory": download_dir_win,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": False,
                "safebrowsing.disable_download_protection": True,
                "profile.default_content_setting_values.automatic_downloads": 1,
                "profile.default_content_settings.popups": 0,
                "plugins.always_open_pdf_externally": True,
                "sync_promo.show_on_first_run_allowed": False,
                "browser.show_home_button": False,
                "signin.allowed_results_history": 0
            }
            opts.add_experimental_option("prefs", prefs)

            try:
                driver = webdriver.Chrome(options=opts)
            except Exception as e:
                emit_log(f"❌ CRITICAL ERROR: Chrome failed to launch: {e}")
                emit_log("💡 Tip: Ensure Google Chrome is installed and compatible with Selenium 4.")
                raise

            if show_browser:
                try:
                    driver.maximize_window()
                    driver.set_window_position(0, 0)
                except Exception as e:
                    emit_log(f"⚠️ Could not position window: {e}")
            return driver


    def handle_2fa_flow(driver, backup_code, email, row_idx=None, sheet=None):
        wait = WebDriverWait(driver, 30)
        save_step_screenshot(driver, f"{email}_2fa_start")
        update_status(email, "Handling 2FA challenge", row_idx, sheet)

        backup_xpath = "//*[@data-challengetype='12'] | //*[contains(text(), '8-digit backup code')] | //*[contains(text(), 'Enter one of your 8-digit backup codes')] | //*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '8-digit backup code')]"
        try_another_xpath = "//*[contains(text(), 'Try another way')] | //*[contains(text(), 'More ways to sign in')] | //*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'try another way')]"
        understand_xpath = "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'i understand')]"

        try:
            emit_log("Analyzing 2FA challenge options (10s timeout)...")
            # Use a shorter timeout to see IF we are even on a 2FA page
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, f"{backup_xpath} | {try_another_xpath} | {understand_xpath}"))
            )
        except Exception:
            # If no 2FA elements found, check if we are already on the Takeout page or another stage
            curr_url = safe_get_url(driver)
            if "takeout" in curr_url or "myaccount.google.com" in curr_url:
                emit_log("No 2FA challenge detected or already logged in. Skipping 2FA flow.")
                return
            
            # If we are stuck on a page that isn't 2FA but also isn't our target, log and continue
            emit_log("No recognized 2FA elements found. Proceeding to verify login status...")
            return

        try:
            time.sleep(1)
            
            # 1. Handle "I understand" welcome screen
            understand_btns = driver.find_elements(By.XPATH, understand_xpath)
            if understand_btns and understand_btns[0].is_displayed():
                click_element_js(driver, understand_btns[0])
                emit_log("✅ Clicked 'I understand'")
                time.sleep(3)

            # 2. Look for Backup Code option, otherwise click 'Try Another Way'
            for attempt in range(2):
                backup_btns = driver.find_elements(By.XPATH, backup_xpath)
                visible_backup = [b for b in backup_btns if b.is_displayed()]
                
                if visible_backup:
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", visible_backup[0])
                    driver.execute_script("arguments[0].click();", visible_backup[0])
                    emit_log("✅ Clicked backup code option")
                    save_step_screenshot(driver, f"{email}_clicked_backup_option")
                    time.sleep(2)
                    break
                else:
                    try_another_btns = driver.find_elements(By.XPATH, try_another_xpath)
                    visible_try = [t for t in try_another_btns if t.is_displayed()]
                    if visible_try and attempt == 0:
                        emit_log("Backup option not visible. Clicking 'Try another way'...")
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", visible_try[0])
                        driver.execute_script("arguments[0].click();", visible_try[0])
                        time.sleep(3)
                    elif attempt == 1:
                        raise Exception("8-digit backup code option not found after 'Try another way'")
            
        except Exception as e:
            if str(e) == "Cancelled by user": raise
            emit_log(f"⚠️ 2FA Menu Error: {e}")
            save_step_screenshot(driver, f"{email}_2fa_menu_failed")
            raise Exception(f"Failed to locate the 8-digit backup code option: {e}")

        try:
            emit_log("Waiting for backup code input box...")
            backup_input = WebDriverWait(driver, 25).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//input[@type='tel' or @name='idvPin' or @type='number']")
                )
            )
            backup_input.clear()
            backup_input.send_keys(backup_code)
            save_step_screenshot(driver, f"{email}_backup_code_entered")
            emit_log("✅ Backup code entered")

            verify_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//*[contains(text(),'Verify') or contains(text(),'Next')]")
                )
            )
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", verify_btn)
            driver.execute_script("arguments[0].click();", verify_btn)
            emit_log("✅ Clicked Verify")
            save_step_screenshot(driver, f"{email}_clicked_verify")

            # --- Detect rejection after clicking Verify ---
            time.sleep(4)
            error_xpaths = [
                "//*[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'wrong code')]",
                "//*[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'invalid')]",
                "//*[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'incorrect')]",
                "//*[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'code has already been used')]",
                "//*[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'try again')]"
            ]
            for xpath in error_xpaths:
                err_els = driver.find_elements(By.XPATH, xpath)
                visible_errs = [e for e in err_els if e.is_displayed()]
                if visible_errs:
                    err_text = visible_errs[0].text.strip()
                    emit_log(f"⚠️ Google rejected backup code: '{err_text}'")
                    save_step_screenshot(driver, f"{email}_code_rejected")
                    raise Exception(f"Backup code rejected by Google: {err_text}")

        except Exception as e:
            if str(e) == "Cancelled by user": raise
            emit_log(f"⚠️ Could not verify backup code: {e}")
            save_step_screenshot(driver, f"{email}_backup_code_failed")
            raise Exception(f"Failed to input or verify the 8-digit backup code: {e}")

        update_status(email, "2FA completed - proceeding to Takeout", row_idx, sheet)


    def login_and_takeout(email, password, backup_code, row_idx=None, sheet=None):
        driver = None
        emit_log(f"=== Processing {email} ===")
        try:
            check_cancel()
            browser_type = settings.get('browser_type', 'chrome').capitalize()
            update_status(email, f"Starting {browser_type}", row_idx, sheet)
            driver = create_driver(email)
            if context:
                context.driver = driver
            wait = WebDriverWait(driver, 30)

            driver.get(TAKEOUT_URL)
            save_step_screenshot(driver, f"{email}_takeout_open")

            # --- Login Bypass Check ---
            curr_url = safe_get_url(driver)
            if "takeout.google.com" in curr_url and "accounts.google.com" not in curr_url:
                emit_log(f"✅ [{email}] Already logged in via session. Skipping credentials entry.")
            else:
                el_email = wait.until(EC.visibility_of_element_located((By.ID, "identifierId")))
                el_email.clear()
                el_email.send_keys(email)
                click_element_js(driver, driver.find_element(By.ID, "identifierNext"))
                save_step_screenshot(driver, f"{email}_email_entered")

            # --- Password Entry with Retries ---
            password_accepted = False
            for pwd_attempt in range(3):
                try:
                    el_pwd = wait.until(EC.visibility_of_element_located((By.NAME, "Passwd")))
                    el_pwd.clear()
                    el_pwd.send_keys(password)
                    click_element_js(driver, driver.find_element(By.ID, "passwordNext"))
                    time.sleep(3)
                    
                    # Check for "Wrong password" message
                    wrong_pwd_xpath = "//*[contains(text(), 'Wrong password') or contains(text(), 'incorrect') or contains(text(), 'try again')]"
                    err_els = driver.find_elements(By.XPATH, wrong_pwd_xpath)
                    visible_errs = [e for e in err_els if e.is_displayed()]
                    
                    if visible_errs:
                        err_msg = visible_errs[0].text
                        emit_log(f"⚠️ Password attempt {pwd_attempt+1} rejected for {email}: {err_msg}")
                        save_step_screenshot(driver, f"{email}_download_pwd_rejected_{pwd_attempt+1}")
                        if pwd_attempt < 2:
                            emit_log(f"Retrying password ({pwd_attempt+2}/3)...")
                            continue
                        else:
                            raise Exception(f"Incorrect password for {email} after 3 attempts.")
                    else:
                        password_accepted = True
                        break
                except Exception as e:
                    if "incorrect password" in str(e).lower(): raise e
                    emit_log(f"⚠️ Password entry glitch (attempt {pwd_attempt+1}): {e}")
                    if pwd_attempt == 2: raise e
                    time.sleep(2)

            if not password_accepted:
                raise Exception(f"Failed to authenticate password for {email}")

            save_step_screenshot(driver, f"{email}_password_accepted")
            update_status(email, "Password accepted", row_idx, sheet)

            handle_2fa_flow(driver, backup_code, email, row_idx, sheet)

            # --- Robust Post-2FA Navigation ---
            emit_log("Verifying successful login and navigating to Takeout...")
            
            # Wait for any redirect away from common login endpoints
            for _ in range(10): 
                time.sleep(1)
                curr_url = safe_get_url(driver)
                if not curr_url:
                    emit_log(f"⚠️ Browser window appears closed during post-login wait.")
                    break
                if "takeout.google.com" in curr_url and "accounts.google.com" not in curr_url:
                    break
            
            # Navigate to the specific Downloads section
            driver.get(TAKEOUT_URL)
            time.sleep(3)
            
            # Final verification that we are NOT on a sign-in page
            if "accounts.google.com" in safe_get_url(driver):
                save_step_screenshot(driver, f"{email}_login_loop_detected")
                emit_log("❌ Error: Redirected to sign-in page after 2FA. Potential session expiration or incorrect backup code.")
                raise Exception("Login Loop Detected: Redirected to sign-in page after 2FA.")

            save_step_screenshot(driver, f"{email}_downloads_loaded")
            update_status(email, "Downloads page loaded", row_idx, sheet)

            safe_email = email.replace("@", "_").replace(".", "_")
            # MUST match the path used in create_driver() for Chrome prefs
            custom_path = settings.get('download_path', '').strip()
            base_download_dir = custom_path if custom_path and os.path.isabs(custom_path) else os.path.join(os.path.expanduser("~"), "Downloads")
            download_dir = os.path.join(base_download_dir, safe_email)
            os.makedirs(download_dir, exist_ok=True)
            emit_log(f"[{email}] Download monitoring dir: {download_dir}")

            # --- Handle "Summary" page with a "Continue" button (seen for some accounts) ---
            # Google sometimes shows a Summary page at /manage with "Completed with errors"
            # and a blue Continue button before showing the actual downloads list.
            try:
                emit_log("Checking for Summary page with Continue button...")
                for _ in range(8):  # Poll for up to 8 seconds
                    curr_url = safe_get_url(driver)
                    # Only act when on the /manage page (not already on /downloads)
                    if "manage" in curr_url or "takeout.google.com" in curr_url:
                        # Look for a "Continue" button (rendered as a <button> or <a> with that text)
                        continue_btns = driver.find_elements(
                            By.XPATH,
                            "//*[self::button or self::a][normalize-space(.)='Continue' or normalize-space(text())='Continue']"
                        )
                        visible_continue = [b for b in continue_btns if b.is_displayed()]
                        if visible_continue:
                            click_element_js(driver, visible_continue[0])
                            emit_log("✅ Clicked 'Continue' on Summary page — proceeding to downloads.")
                            save_step_screenshot(driver, f"{email}_summary_continue_clicked")
                            time.sleep(3)
                            break
                    time.sleep(1)
            except Exception as e:
                emit_log(f"Summary-page Continue check skip: {e}")

            # If redirected to /manage, exports may be collapsed. Expand them.
            try:
                emit_log("Checking for collapsed exports on /manage...")
                clicked = False
                for _ in range(15): # Poll for up to 15 seconds
                    # Search by XPath for the exact text "Completed"
                    completed_elements = driver.find_elements(By.XPATH, "//*[normalize-space(text())='Completed' or normalize-space(text())='Show exports']")
                    for el in completed_elements:
                        if el.is_displayed():
                            click_element_js(driver, el)
                            emit_log("Clicked 'Completed/Show exports' text label to expand latest export.")
                            clicked = True
                            time.sleep(3)
                            break
                    if clicked: break
                    
                    # Fallback to finding the expand arrow icon
                    arrow_elements = driver.find_elements(By.XPATH, "//*[local-name()='path' and starts-with(@d, 'M10 6L8.59')]")
                    for el in arrow_elements:
                        if el.is_displayed():
                            click_element_js(driver, el)
                            emit_log("Clicked expand arrow to reveal latest export.")
                            clicked = True
                            time.sleep(3)
                            break
                    if clicked: break
                    time.sleep(1)
            except Exception as e:
                emit_log(f"Export expansion skip: {e}")

            download_btns = []
            for _ in range(20):
                # The earliest verified working selector for downloads
                download_btns = driver.execute_script('''
                    return [...document.querySelectorAll('a')]
                           .filter(el => el.href && el.href.includes('takeout/download') && !(el.textContent || '').toLowerCase().includes('report'))
                ''')
                if download_btns: break
                time.sleep(1)

            if not download_btns:
                emit_log(f"[{email}] No download buttons found. Is the export ready?")
                update_status(email, "❌ No downloads found", row_idx, sheet)
            else:
                emit_log(f"Found {len(download_btns)} download button(s).")
                
                # Take a BASELINE snapshot of completed files before ANY downloads start
                baseline_files = set(f for f in glob.glob(os.path.join(download_dir, "*")) 
                                     if not f.endswith(".crdownload") and not f.endswith(".tmp"))
                baseline_count = len(baseline_files)
                emit_log(f"[{email}] Baseline files in download dir: {baseline_count}")
                
                for index, btn in enumerate(download_btns):
                    emit_log(f"Clicking download {index + 1} of {len(download_btns)}...")
                    
                    # Snapshot BEFORE clicking - count completed (non-temp) files
                    pre_click_files = set(f for f in glob.glob(os.path.join(download_dir, "*"))
                                          if not f.endswith(".crdownload") and not f.endswith(".tmp"))
                    
                    # Give it a tiny buffer before clicking
                    time.sleep(2)
                    
                    # Refresh element reference in case of DOM changes
                    try:
                        current_btns = driver.execute_script('''
                            return [...document.querySelectorAll('a')]
                                   .filter(el => el.href && el.href.includes('takeout/download') && !(el.textContent || '').toLowerCase().includes('report'))
                        ''')
                        if index < len(current_btns):
                            click_element_js(driver, current_btns[index])
                        else:
                            click_element_js(driver, btn)
                    except Exception:
                        try:
                            click_element_js(driver, btn)
                        except Exception:
                            pass
                    
                    # Check for Re-auth prompt
                    try:
                        # Sometimes Google asks for the email again before the password
                        el_email = driver.find_elements(By.ID, "identifierId")
                        if el_email and el_email[0].is_displayed():
                            emit_log(f"[{email}] Re-authentication required. Entering email...")
                            el_email[0].clear()
                            el_email[0].send_keys(email)
                            click_element_js(driver, driver.find_element(By.ID, "identifierNext"))
                            time.sleep(3)
                            
                        el_pwd = driver.find_elements(By.NAME, "Passwd")
                        if el_pwd and el_pwd[0].is_displayed():
                            emit_log(f"[{email}] Re-authentication required. Entering password...")
                            el_pwd[0].clear()
                            el_pwd[0].send_keys(password)
                            click_element_js(driver, driver.find_element(By.ID, "passwordNext"))
                            time.sleep(3)
                            # Handle 2FA if it appears again
                            try:
                                handle_2fa_flow(driver, backup_code, email, row_idx, sheet)
                            except Exception as e:
                                emit_log(f"[{email}] 2FA re-auth check: {e}")
                                raise e
                            
                            # Wait to return to downloads
                            WebDriverWait(driver, 30).until(EC.url_contains("downloads"))
                            time.sleep(5)
                    except Exception as e:
                        pass
                    
                    emit_log(f"[{email}] Waiting for download {index + 1} of {len(download_btns)} to finish...")
                    
                    # Wait for a NEW completed file to appear (compared to pre-click snapshot)
                    download_start_time = time.time()
                    while True:
                        check_cancel()
                        check_pause()
                        
                        current_all = set(glob.glob(os.path.join(download_dir, "*")))
                        completed_files = set(f for f in current_all 
                                              if not f.endswith(".crdownload") and not f.endswith(".tmp"))
                        temp_files = [f for f in current_all 
                                      if f.endswith(".crdownload") or f.endswith(".tmp")]
                        
                        # New completed files since we clicked
                        new_completed = completed_files - pre_click_files
                        
                        # Total new files since baseline (for status display)
                        total_new = len(completed_files) - baseline_count
                        
                        if new_completed and not temp_files:
                            emit_log(f"[{email}] Download {index + 1} confirmed: {os.path.basename(list(new_completed)[0])}")
                            break
                        
                        # Also check: if total completed files >= expected, we're done
                        if total_new >= (index + 1) and not temp_files:
                            emit_log(f"[{email}] Download {index + 1} confirmed (by total count: {total_new} new files)")
                            break
                            
                        # Safety timeout per file (15 mins)
                        if time.time() - download_start_time > 900:
                            emit_log(f"[{email}] ⚠️ Timeout waiting for download {index+1}.")
                            break
                            
                        update_status(email, f"Downloading: {total_new}/{len(download_btns)}", row_idx, sheet)
                        time.sleep(3)
                        
                    time.sleep(2) # Buffer for Windows lock

                
                # FINAL check if all files downloaded
                final_files_count = len([f for f in glob.glob(os.path.join(download_dir, "*")) if not f.endswith(".crdownload")])
                emit_log(f"✅ Total files in directory: {final_files_count}")
                
                # Mass Rename correctly
                try:
                    all_files = glob.glob(os.path.join(download_dir, "*"))
                    takeout_files = [f for f in all_files if "takeout" in os.path.basename(f).lower() and os.path.isfile(f)]
                    takeout_files.sort(key=os.path.getctime)
                    
                    email_prefix = email.split('@')[0]
                    for i, zf in enumerate(takeout_files):
                        ext = os.path.splitext(zf)[1]
                        new_name = f"{email_prefix}-{(i + 1):03d}{ext}"
                        new_path = os.path.join(download_dir, new_name)
                        try:
                            os.rename(zf, new_path)
                            emit_log(f"[{email}] Renamed: {os.path.basename(zf)} -> {new_name}")
                        except Exception as e:
                            emit_log(f"[{email}] ⚠️ Could not rename {os.path.basename(zf)}: {e}")
                except Exception as e:
                    emit_log(f"[{email}] ⚠️ Mass renaming failed: {e}")

                emit_log(f"✅ All {len(download_btns)} files downloaded for {email}")
                update_status(email, "✅ Download Complete", row_idx, sheet)
                stats['success'] += 1
                send_account_notification(email, "Export downloaded successfully", "")


        except UserCancelledException as e:
            emit_log(f"🛑 Cancelled while processing {email}", bypass_cancel=True)
            update_status(email, "❌ Cancelled", row_idx, sheet, bypass_cancel=True)
            raise e
        except Exception as e:
            if str(e) == "Cancelled by user": raise
            emit_log(f"❌ Error: {e}")
            traceback.print_exc()
            if driver:
                save_step_screenshot(driver, f"{email}_error")
            update_status(email, f"❌ Error: {e}", row_idx, sheet)
            stats['failed'] += 1
            stats['errors'].append(f"{email}: {e}")
            send_account_notification(email, "", str(e))
            raise e
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
            emit_log(f"Browser closed for {email}")
            if context: context.frame_b64 = ""

    def process_row(row_dict, row_idx=None, sheet=None):
        check_cancel()
        
        # Ensure name-based access works regardless of if row_dict is sqlite3.Row or dict
        def get_val(key, default=""):
            try:
                val = row_dict[key]
                return (str(val).strip() if val is not None else default)
            except (KeyError, IndexError, TypeError):
                return row_dict.get(key, default) if hasattr(row_dict, 'get') else default

        email = get_val('email')
        password = decrypt_password(get_val('password'))
        raw_backups = get_val('backup_codes')
        status = get_val('download_status', get_val('create_status'))

        # LOGGING FOR DEBUGGING FIELD SWAPS
        masked_pwd = "*" * len(password) if password else "EMPTY"
        masked_codes = f"...{raw_backups[-8:]}" if len(raw_backups) > 8 else raw_backups
        emit_log(f"DEBUG: Processing row for {email} | Pwd Length: {len(password)} | Codes: {masked_codes}")

        if not email or not password:
            emit_log(f"Skipping row for {email or 'Unknown'}: Missing Email or Password")
            stats['skipped'] += 1
            return
            
        if "✅" in status or "Export" in status or "Success" in status or "Successful" in status:
            emit_log(f"Skipping {email} (already downloaded)")
            stats['skipped'] += 1
            return
            
        backup_list = [c.strip() for c in raw_backups.split(",")] if raw_backups else [""]
        if not backup_list: backup_list = [""]

        emit_log(f"=== Beginning processing for {email} ===")
        update_status(email, "Starting backup...", row_idx, sheet)
        
        # Implement smart fallback for Backup Codes
        success = False
        last_error = None
        
        current_codes = list(backup_list)
        
        for attempt in range(len(backup_list)):
            if not current_codes: break
            target_code = current_codes[0]
            
            try:
                if len(current_codes) > 1:
                    emit_log(f"[{email}] Attempt {attempt+1} using 2FA code ending in ...{target_code[-4:] if len(target_code)>4 else target_code}")
                login_and_takeout(email, password, target_code, row_idx, sheet)
                
                # Consume successful code
                current_codes.pop(0)
                update_backup_codes(email, current_codes, row_idx, sheet)
                
                success = True
                break
            except UserCancelledException:
                raise
            except Exception as e:
                last_error = str(e)
                err_lower = str(e).lower()
                
                # Consolidated 2FA failure detection
                is_2fa_rejected = any(kw in err_lower for kw in ['wrong code', 'invalid', 'incorrect', 'already been used', 'rejected'])
                is_2fa_timeout = any(kw in err_lower for kw in ['failed to locate', 'failed to input', 'login loop'])
                
                if is_2fa_rejected or is_2fa_timeout:
                    reason = "EXPLICIT REJECTION" if is_2fa_rejected else "LOGIC/TIMEOUT ERROR"
                    emit_log(f"[{email}] ⚠️ 2FA Failure ({reason}): {e}. Consuming code and retrying...")
                    current_codes.pop(0)
                    update_backup_codes(email, current_codes, row_idx, sheet)
                    if current_codes:
                        emit_log(f"[{email}] {len(current_codes)} codes remaining. Trying next...")
                        continue
                    else:
                        break
                else:
                    emit_log(f"[{email}] ❌ Execution failed (non-2FA): {e}")
                    raise e
                    
        if not success and last_error:
            emit_log(f"[{email}] 🛑 Critical Failure: All backup codes exhausted. Last error: {last_error}")
            update_status(email, f"❌ All backup codes failed", row_idx, sheet)
            stats['failed'] += 1
            stats['errors'].append(f"{email} (2FA): {last_error}")
            send_account_notification(email, "", last_error)
            
        emit_log(f"=== Completed session for {email} ===")

    def run_csv(rows):
        """Process CSV rows concurrently using a thread pool."""
        concurrent_limit = int(settings.get('concurrent_sessions', 3))
        emit_log(f"🚀 Starting parallel processing with up to {concurrent_limit} concurrent sessions...")

        def safe_process(row_dict):
            """Wrapper to propagate cancellation from worker threads."""
            try:
                check_cancel()
                process_row(row_dict)
            except UserCancelledException:
                raise
            except Exception as e:
                emit_log(f"❌ Error processing {row_dict.get('email', '?')}: {e}")
                traceback.print_exc()

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_limit) as pool:
            futures = {pool.submit(safe_process, dict(row)): dict(row) for row in rows}
            for future in concurrent.futures.as_completed(futures):
                row_dict = futures[future]
                try:
                    future.result()  # Re-raise any exception from the worker
                except UserCancelledException:
                    pool.shutdown(wait=False, cancel_futures=True)
                    raise
                except Exception as e:
                    emit_log(f"❌ Session for {row_dict.get('email', '?')} finished with error: {e}")

        emit_log("🏁 All sessions completed.")

    try:
        check_cancel()
        
        if data_source == "csv":
            emit_log("Authenticating with local SQLite Database...")
            conn = sqlite3.connect(DB_FILE)
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT email, password, backup_codes, download_status FROM target_accounts WHERE owner = ?", (run_owner,)).fetchall()
            conn.close()
            
            emit_log(f"✅ Connected to Database. Found {len(rows)} accounts.")
            run_csv(rows)

        elif data_source == "agent":
            agent_accounts = kwargs.get("agent_accounts", [])
            run_csv(agent_accounts)
                        
        elif data_source == "sheet":
            spreadsheet_id = settings.get("spreadsheet_id", "")
            sheet_name = settings.get("sheet_name", "")
            credentials_file = settings.get("service_json_path", "")
            
            emit_log("Authenticating with Google Sheets...")
            creds = Credentials.from_service_account_file(
                credentials_file,
                scopes=["https://www.googleapis.com/auth/spreadsheets"]
            )
            client = gspread.authorize(creds)
            sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
            emit_log("✅ Connected to Google Sheet")
            
            sheet_rows = sheet.get_all_values()
            if not sheet_rows:
                emit_log("⚠️ Sheet is empty!")
                return
            
            headers = [h.strip().lower() for h in sheet_rows[0]]
            emit_log(f"DEBUG: Sheet Headers: {headers}")
            
            def find_idx(candidates):
                for c in candidates:
                    if c in headers: return headers.index(c)
                return -1
                
            email_idx = find_idx(['email', 'username', 'e-mail'])
            pwd_idx = find_idx(['password', 'pass', 'pwd'])
            code_idx = find_idx(['backupcodes', 'backup codes', 'backup_codes', 'backup', '2fa', 'mfa'])
            status_idx = find_idx(['download_status', 'downloadstatus', 'status', 'create_status', 'createstatus'])
            
            if status_idx != -1:
                state['status_col'] = status_idx + 1
            else:
                state['status_col'] = 5 # Default
            
            emit_log(f"✅ Found {len(sheet_rows)-1} accounts in sheet.")

            row_dicts = []
            for idx, row in enumerate(sheet_rows[1:], start=2):
                row_dicts.append({
                    'email': row[email_idx].strip() if email_idx != -1 and len(row) > email_idx else "",
                    'password': row[pwd_idx].strip() if pwd_idx != -1 and len(row) > pwd_idx else "",
                    'backup_codes': row[code_idx].strip() if code_idx != -1 and len(row) > code_idx else "",
                    'download_status': row[status_idx].strip() if status_idx != -1 and len(row) > status_idx else "",
                    '_row_idx': idx,
                    '_sheet': sheet
                })

            concurrent_limit = int(settings.get('concurrent_sessions', 3))
            emit_log(f"🚀 Starting parallel processing with up to {concurrent_limit} concurrent sessions...")

            def safe_process_sheet(row_dict):
                try:
                    check_cancel()
                    process_row(row_dict, row_dict.pop('_row_idx', None), row_dict.pop('_sheet', None))
                except UserCancelledException:
                    raise
                except Exception as e:
                    emit_log(f"❌ Error processing {row_dict.get('email', '?')}: {e}")
                    traceback.print_exc()

            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_limit) as pool:
                futures = {pool.submit(safe_process_sheet, rd): rd for rd in row_dicts}
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                    except UserCancelledException:
                        pool.shutdown(wait=False, cancel_futures=True)
                        raise
                    except Exception as e:
                        emit_log(f"❌ Sheet session error: {e}")

        emit_log("🏁 All target accounts processed.")
        emit_log("=== All Tasks Finished ===")
    except UserCancelledException:
        emit_log("🛑 Execution aborted by User.", bypass_cancel=True)
    except Exception as e:
        emit_log(f"❌ Unhandled Exception: {e}")
        traceback.print_exc()
    finally:
        if context:
            context.frame_b64 = ""

