import re
import os

with open(r"c:\Users\skcha\.gemini\antigravity\scratch\takeout_web_app\takeout_downloader.py", "r", encoding="utf-8") as f:
    text = f.read()

# 1. Update TAKEOUT_URL
text = text.replace('TAKEOUT_URL = "https://takeout.google.com"', 'TAKEOUT_URL = "https://takeout.google.com/settings/takeout/downloads"')

# 2. Add dependencies
text = text.replace('import traceback\nimport gspread', 'import traceback\nimport gspread\nimport glob')

# 3. Add prefs to create_driver
driver_setup_code = """
        opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        opts.add_experimental_option("useAutomationExtension", False)
        temp_profile = tempfile.mkdtemp()
        opts.add_argument(f"--user-data-dir={temp_profile}")
        
        download_dir = os.path.abspath(os.path.join("data", "downloads"))
        os.makedirs(download_dir, exist_ok=True)
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "profile.default_content_setting_values.automatic_downloads": 1
        }
        opts.add_experimental_option("prefs", prefs)
        
        return webdriver.Chrome(options=opts)
"""
text = re.sub(r'opts\.add_experimental_option\("excludeSwitches", \["enable-automation", "enable-logging"\]\).*?return webdriver\.Chrome\(options=opts\)', driver_setup_code, text, flags=re.DOTALL)


# 4. Remove set_export_size_50gb
text = re.sub(r'    def set_export_size_50gb\(driver, email_tag\):.*?return False\n', '', text, flags=re.DOTALL)

# 5. Add wait_for_downloads
wait_dl_code = """
    def wait_for_downloads_to_complete(download_dir):
        emit_log("Waiting for downloads to complete...", bypass_cancel=True)
        while True:
            check_cancel()
            check_pause()
            crdownloads = glob.glob(os.path.join(download_dir, "*.crdownload"))
            if not crdownloads:
                break
            time.sleep(2)
        
"""
text = text.replace('    def set_export_size_50gb', wait_dl_code + '\n    def set_export_size_50gb')


# 6. Replace the execution part in `login_and_takeout`
new_logic = """
            wait.until(EC.url_contains("takeout.google.com"))
            
            # Go to downloads page specifically
            driver.get(TAKEOUT_URL)
            # if we get kicked back to authentication, it will handle it or wait
            
            save_step_screenshot(driver, f"{email}_downloads_loaded")
            update_sheet_status(sheet, row, "Downloads page loaded")

            download_dir = os.path.abspath(os.path.join("data", "downloads"))
            os.makedirs(download_dir, exist_ok=True)

            download_btns = []
            for _ in range(20):
                download_btns = driver.execute_script('''
                    return [...document.querySelectorAll('a, button, div[role=button]')]
                           .filter(el => /Download/i.test(el.innerText||''))
                ''')
                if download_btns: break
                time.sleep(1)

            if not download_btns:
                emit_log("No download buttons found. Is the export ready?")
                update_sheet_status(sheet, row, "❌ No downloads found")
            else:
                emit_log(f"Found {len(download_btns)} download button(s).")
                for index, btn in enumerate(download_btns):
                    emit_log(f"Clicking download {index + 1} of {len(download_btns)}...")
                    
                    # Refresh element reference in case of DOM changes
                    try:
                        current_btns = driver.execute_script('''
                            return [...document.querySelectorAll('a, button, div[role=button]')]
                                   .filter(el => /Download/i.test(el.innerText||''))
                        ''')
                        if index < len(current_btns):
                            click_element_js(driver, current_btns[index])
                        else:
                            click_element_js(driver, btn)
                    except:
                        click_element_js(driver, btn)
                    
                    time.sleep(4)
                    
                    # Check for Re-auth prompt
                    try:
                        el_pwd = driver.find_elements(By.NAME, "Passwd")
                        if el_pwd and el_pwd[0].is_displayed():
                            emit_log("Re-authentication required for download. Entering password...")
                            el_pwd[0].clear()
                            el_pwd[0].send_keys(password)
                            click_element_js(driver, driver.find_element(By.ID, "passwordNext"))
                            time.sleep(3)
                            # Handle 2FA if it appears again
                            try:
                                handle_2fa_flow(driver, backup_code, sheet, row, email)
                            except Exception as e:
                                emit_log(f"2FA re-auth check: {e}")
                                pass
                            
                            # Wait to return to downloads
                            WebDriverWait(driver, 30).until(EC.url_contains("downloads"))
                            # The download usually starts automatically after re-auth, or we might need to click again. Let's just wait to see.
                            time.sleep(5)
                    except Exception as e:
                        pass
                    
                    # Wait for download to finish before clicking the next one
                    emit_log(f"Waiting for download {index + 1} to finish...")
                    
                    while True:
                        check_cancel()
                        check_pause()
                        crdownloads = glob.glob(os.path.join(download_dir, "*.crdownload"))
                        crdownloadstmp = glob.glob(os.path.join(download_dir, "*.tmp"))
                        if not crdownloads and not crdownloadstmp:
                            break
                        time.sleep(2)
                        
                    time.sleep(2) # Give it an extra moment after .crdownload disappears
                    update_sheet_status(sheet, row, f"Downloaded {index + 1}/{len(download_btns)}")
                    save_step_screenshot(driver, f"{email}_download_{index+1}_finished")

                emit_log(f"✅ All {len(download_btns)} files downloaded for {email}")
                update_sheet_status(sheet, row, "✅ Download Complete")
"""

text = re.sub(r'            wait\.until\(EC\.url_contains\("takeout\.google\.com"\)\)\n.*?update_sheet_status\(sheet, row, "✅ Export created successfully"\)\n.*?emit_log\("Create export button not found\."\)\n.*?update_sheet_status\(sheet, row, "❌ Failed"\)', new_logic, text, flags=re.DOTALL)

with open(r"c:\Users\skcha\.gemini\antigravity\scratch\takeout_web_app\takeout_downloader.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Migration script completed.")
