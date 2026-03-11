import sys
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

def log(msg):
    print(msg)
    sys.stdout.flush()

log("Starting minimal launch test...")

try:
    log("Installing ChromeDriver...")
    path = ChromeDriverManager().install()
    log(f"Driver installed at: {path}")

    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    # Uncomment next line to test headless if visible fails
    # opts.add_argument("--headless=new")

    log("Initializing Chrome browser (Visible)...")
    driver = webdriver.Chrome(service=Service(path), options=opts)
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
