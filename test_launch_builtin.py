import sys
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

def log(msg):
    print(msg)
    sys.stdout.flush()

log("Starting Selenium 4 minimal launch test...")

try:
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    # In Selenium 4.6+, we don't need a path if Chrome is in standard location
    
    log("Initializing Chrome browser via Selenium Manager (built-in)...")
    driver = webdriver.Chrome(options=opts)
    log("Browser initialized successfully!")
    
    log("Navigating to Google...")
    driver.get("https://www.google.com")
    log(f"Page title: {driver.title}")
    
    log("Waiting 3 seconds...")
    time.sleep(3)
    
    log("Closing browser...")
    driver.quit()
    log("Test completed successfully.")

except Exception as e:
    log(f"FATAL ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.stdout.flush()
