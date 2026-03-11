import os
import time
import tempfile
import traceback
import sqlite3
import gspread
import threading
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

TAKEOUT_URL = "https://takeout.google.com"

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

        mode_name = "Create Backups"
        subject = f"[{'ERROR' if error_msg else 'SUCCESS'}] Takeout Automation: {email}"

        html = f"<h2>Automation Session: {mode_name}</h2>"
        html += f"<p><strong>Target Account:</strong> {email}</p>"
        if error_msg:
            html += f"<h3 style='color:red;'>Task Failed</h3><p><strong>Error:</strong> {error_msg}</p>"
        else:
            html += f"<h3 style='color:green;'>Task Completed Successfully</h3><p><strong>Status:</strong> {status_msg}</p>"

        try:
            send_email(recipient_email, subject, html)
            emit_log(f"\U0001f4e7 Notification sent to {recipient_email} for account {email}")
        except Exception as e:
            emit_log(f"\u26a0\ufe0f Failed to send notification email: {e}")

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
    state = {'status_col': 4}

    def update_status(email, text, row_idx=None, sheet=None, bypass_cancel=False):
        if not bypass_cancel: 
            check_cancel()
            check_pause()
        try:
            if data_source == "csv":
                conn = sqlite3.connect(DB_FILE, timeout=10.0)
                conn.execute("UPDATE target_accounts SET create_status = ? WHERE email = ? AND owner = ?", (text, email, run_owner))
                conn.commit()
                conn.close()
            elif data_source == "agent" and context:
                # Update central server via API
                from requests import post as http_post
                try:
                    http_post(
                        f"{context.server_url}/api/agent/status/update",
                        json={"email": email, "status": text, "mode": "create"},
                        headers=context.headers,
                        timeout=5
                    )
                    # ALSO update local agent cache for immediate UI feedback
                    if hasattr(context, 'update_status'):
                        context.update_status(email, text, mode="create")
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
                # Need to find the correct column for backup codes
                # For simplicity in this function, we assume the caller handled it if needed
                # or we just skip sheet update here if it's too complex to re-crawl headers
                pass
        except Exception as e:
            emit_log(f"⚠️ Failed to update backup codes: {e}")

    def create_driver():
        browser_type = settings.get('browser_type', 'chrome').lower()
        show_browser = settings.get('showBrowser', True)
        
        emit_log(f"🛠️ Driver Factory: Type={browser_type}, Visible={show_browser}")
        
        if browser_type == 'firefox':
            opts = FirefoxOptions()
            opts.add_argument("-private")
            if not show_browser:
                opts.add_argument("-headless")
            return webdriver.Firefox(options=opts)

        elif browser_type == 'edge':
            opts = EdgeOptions()
            opts.add_argument("--inprivate")
            if not show_browser:
                opts.add_argument("--headless=new")
            opts.add_argument("--window-size=1200,900")
            opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
            return webdriver.Edge(options=opts)

        else:  # Default to Chrome
            opts = ChromeOptions()
            opts.add_argument("--incognito")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            opts.add_argument("--disable-popup-blocking")
            opts.add_argument("--disable-notifications")
            opts.add_argument("--disable-extensions")
            opts.add_argument("--no-first-run")
            opts.add_argument("--no-default-browser-check")

            if not show_browser:
                emit_log("🕶️ Enabling Headless Mode (Chrome)")
                opts.add_argument("--headless=new")
                opts.add_argument("--disable-gpu")
                opts.add_argument("--window-size=1920,1080")
                opts.add_argument("--remote-debugging-port=9222")
            else:
                emit_log("📺 Enabling Visible Mode (Chrome)")

            opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
            opts.add_experimental_option("useAutomationExtension", False)
            temp_profile = tempfile.mkdtemp()
            opts.add_argument(f"--user-data-dir={temp_profile}")

            prefs = {
                "profile.default_content_settings.popups": 0,
                "profile.default_content_setting_values.automatic_downloads": 1,
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
            
            # If we are stuck on a page that isn't 2FA but also isn't our target, log and continue (runner will fail later if needed)
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

    def set_export_size_50gb(driver, email_tag):
        save_step_screenshot(driver, f"{email_tag}_before_size_change")
        try:
            select_el = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//select[contains(@aria-label,'Export size') or contains(@name,'size')]")
                )
            )
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", select_el)
            time.sleep(1)
            driver.execute_script("""
                const sel = arguments[0];
                const opt = [...sel.options].find(o => /50\\s*GB/i.test(o.text));
                if (opt) { sel.value = opt.value; sel.dispatchEvent(new Event('change',{bubbles:true})); }
            """, select_el)
            save_step_screenshot(driver, f"{email_tag}_set_50gb_via_select")
            emit_log("✅ Export size changed via dropdown.")
            return True
        except Exception as e:
            if str(e) == "Cancelled by user": raise
            emit_log(f"⚠️ Dropdown method failed: {e}")

        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            option = driver.execute_script("""
                return [...document.querySelectorAll('label, span, div[role=radio], div[role=option]')]
                       .find(el => /50\\s*GB/i.test(el.innerText || ''));
            """)
            if option:
                driver.execute_script("arguments[0].click();", option)
                save_step_screenshot(driver, f"{email_tag}_set_50gb_via_click")
                emit_log("✅ Export size changed via label.")
                return True
        except Exception as e:
            if str(e) == "Cancelled by user": raise
            emit_log(f"⚠️ Fallback click failed: {e}")

        save_step_screenshot(driver, f"{email_tag}_size_change_failed")
        emit_log("⚠️ Export size not changed (default used).")
        return False

    def login_and_takeout(email, password, backup_code, row_idx=None, sheet=None):
        driver = None
        emit_log(f"=== Processing {email} ===")
        try:
            check_cancel()
            browser_type = settings.get('browser_type', 'chrome').capitalize()
            update_status(email, f"Starting {browser_type}", row_idx, sheet)
            driver = create_driver()
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
                        save_step_screenshot(driver, f"{email}_pwd_rejected_{pwd_attempt+1}")
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
            
            # Navigate to the specific Takeout section to be sure
            driver.get(TAKEOUT_URL)
            time.sleep(3)
            
            # Final verification that we are NOT on a sign-in page
            if "accounts.google.com" in safe_get_url(driver):
                save_step_screenshot(driver, f"{email}_login_loop_detected")
                emit_log(f"❌ Error: Redirected to sign-in page after 2FA for {email}. Potential session expiration or incorrect backup code.")
                raise Exception(f"Login Loop Detected for {email}")

            save_step_screenshot(driver, f"{email}_takeout_loaded")
            update_status(email, "Takeout page loaded", row_idx, sheet)

            # --- Handle /manage summary page (Google redirects here if exports already exist) ---
            curr_url_after_nav = safe_get_url(driver)
            if "/manage" in curr_url_after_nav or "takeout.google.com/manage" in curr_url_after_nav:
                emit_log(f"[{email}] Detected /manage summary page. Looking for 'Create new request' button...")
                try:
                    create_new_btn = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable(
                            (By.XPATH, "//*[contains(text(),'Create new request') or contains(text(),'Create new export')]"
                             " | //button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'create new')]")
                        )
                    )
                    click_element_js(driver, create_new_btn)
                    emit_log(f"[{email}] Clicked 'Create new request'. Waiting for export wizard...")
                    time.sleep(4)
                    save_step_screenshot(driver, f"{email}_create_new_request_clicked")
                except Exception:
                    # If button not found via WebDriverWait, try JS scan
                    btn_js = driver.execute_script("""
                        return [...document.querySelectorAll('button,a,div[role=button]')]
                               .find(el => /create new request|create new export/i.test(el.innerText || ''));
                    """)
                    if btn_js:
                        click_element_js(driver, btn_js)
                        emit_log(f"[{email}] Clicked 'Create new request' (JS fallback).")
                        time.sleep(4)
                    else:
                        emit_log(f"[{email}] \u26a0\ufe0f Could not find 'Create new request' on /manage page. Attempting direct navigation...")
                        driver.get("https://takeout.google.com/takeout/custom/gmail")
                        time.sleep(4)

            emit_log("Checking for Workspace notices or 'Unavailable Services' warnings...")
            driver.execute_script("window.scrollTo(0, 200);") # Scroll slightly to bypass floating headers
            time.sleep(1)
            driver.execute_script("window.scrollTo(0, 0);")

            for _ in range(8):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                next_btn = driver.execute_script("""
                    return [...document.querySelectorAll('button, div[role=button], span, a')]
                           .find(el => /Next step/i.test(el.innerText||'')); 
                """)
                if next_btn:
                    click_element_js(driver, next_btn)
                    save_step_screenshot(driver, f"{email}_clicked_next_step")
                    break

            set_export_size_50gb(driver, email.replace('@','_'))

            btn = None
            for _ in range(15):
                btn = driver.execute_script("""
                    return [...document.querySelectorAll('button, div[role=button], span, a')]
                           .find(el => /Create export|Create your export|Create archive|Create new export/i.test(el.innerText||'')); 
                """)
                if btn: break
                time.sleep(1)
                
            if btn:
                click_element_js(driver, btn)
                save_step_screenshot(driver, f"{email}_export_created")
                update_status(email, "✅ Export created successfully", row_idx, sheet)
                emit_log(f"✅ Export started for {email}")
            else:
                emit_log(f"[{email}] Create export button not found.")
                update_status(email, "❌ Failed", row_idx, sheet)

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
        status = get_val('create_status', get_val('download_status'))
        
        # LOGGING FOR DEBUGGING FIELD SWAPS
        masked_pwd = "*" * len(password) if password else "EMPTY"
        masked_codes = f"...{raw_backups[-8:]}" if len(raw_backups) > 8 else raw_backups
        emit_log(f"DEBUG: Processing row for {email} | Pwd Length: {len(password)} | Codes: {masked_codes}")

        if not email or not password:
            emit_log(f"Skipping row for {email or 'Unknown'}: Missing Email or Password")
            stats['skipped'] += 1
            return
            
        if "✅" in status or "Export" in status or "Success" in status or "Successful" in status:
            emit_log(f"Skipping {email} (already processed)")
            stats['skipped'] += 1
            return
            
        backup_list = [c.strip() for c in raw_backups.split(",")] if raw_backups else [""]
        if not backup_list: backup_list = [""]

        emit_log(f"=== Beginning backup creation for {email} ===")
        update_status(email, "Starting backup creation...", row_idx, sheet)
        
        success = False
        last_error = None
        
        current_codes = list(backup_list)
        
        for attempt in range(len(backup_list)):
            if not current_codes: break
            target_code = current_codes[0] # Always try the first available
            
            try:
                if len(current_codes) > 1:
                    emit_log(f"[{email}] Attempt {attempt+1} using 2FA code ending in ...{target_code[-4:] if len(target_code)>4 else target_code}")
                
                login_and_takeout(email, password, target_code, row_idx, sheet)
                
                # If we get here, it succeeded or didn't throw a 2FA error
                # Consume the code anyway since it was submitted
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
            stats['errors'].append(f"{email}: {last_error}")
            send_account_notification(email, "", last_error)
        elif success:
            stats['success'] += 1
            send_account_notification(email, "Export created successfully", "")
            
        emit_log(f"=== Completed session for {email} ===")
        time.sleep(2)

    try:
        check_cancel()
        if data_source == "csv":
            emit_log("Authenticating with local SQLite Database...")
            run_owner = settings.get('_run_owner', 'admin')
            conn = sqlite3.connect(DB_FILE)
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM target_accounts WHERE owner = ?", (run_owner,)).fetchall()
            conn.close()
            
            emit_log(f"✅ Connected to Database. Found {len(rows)} accounts.")

            for row in rows:
                try:
                    process_row(dict(row))
                except UserCancelledException:
                    raise
                except Exception as e:
                    traceback.print_exc()

        elif data_source == "agent":
            # Windows Agent passes the account data directly via kwargs
            agent_accounts = kwargs.get("agent_accounts", [])
            for row in agent_accounts:
                try:
                    process_row(row)
                except UserCancelledException:
                    raise
                except Exception as e:
                    traceback.print_exc()

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
            status_idx = find_idx(['create_status', 'createstatus', 'status', 'download_status', 'downloadstatus'])
            if status_idx != -1:
                state['status_col'] = status_idx + 1
            else:
                state['status_col'] = 4 # Default
            
            for idx, row in enumerate(sheet_rows[1:], start=2):
                row_dict = {
                    'email': row[email_idx].strip() if email_idx != -1 and len(row) > email_idx else "",
                    'password': row[pwd_idx].strip() if pwd_idx != -1 and len(row) > pwd_idx else "",
                    'backup_codes': row[code_idx].strip() if code_idx != -1 and len(row) > code_idx else "",
                    'create_status': row[status_idx].strip() if status_idx != -1 and len(row) > status_idx else ""
                }
                # If create_status wasn't found, try download_status for compatibility
                if status_idx == -1:
                    row_dict['create_status'] = row[3].strip() if len(row) > 3 else ""

                try:
                    process_row(row_dict, idx, sheet)
                except UserCancelledException:
                    raise
                except Exception as e:
                    traceback.print_exc()
        
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
