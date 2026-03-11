# Cross-Platform Deployment Guide

This guide details the exact steps to transition and host the Google Takeout Automator directly into native macOS or Ubuntu ecosystems.

## Option A: Ubuntu (Desktop) Deployment
*Ubuntu natively executes Python and Selenium precisely like Windows, provided there is a physical GUI (X11/Wayland) window manager running to support the Ghost-Browser.*

### 1. Install Google Chrome Native
```bash
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo dpkg -i google-chrome-stable_current_amd64.deb
sudo apt --fix-broken install -y
```

### 2. Install Python & Fetch the Source
```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv git
git clone https://github.com/aisanjaychauhan/takeout-automation-dashboard.git
cd takeout-automation-dashboard
git checkout ghost-browser
```

### 3. Initialize the Virtual Environment
Securely inject your `.env` and `users.db` files from the Windows machine directly into the newly created `takeout-automation-dashboard` physical root folder.

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 4. Boot the Internal Server
```bash
python3 app.py
```

---

## Option B: macOS (Apple Silicon / Intel) Deployment
*macOS is completely Unix-compatible and runs headless Ghost-Browser architecture flawlessly, so no complex dependencies are required natively.*

### 1. Install Chrome & Python
*If you do not have Homebrew, install it first: `/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"`*

```bash
brew install python
brew install --cask google-chrome
```

### 2. Fetch the Source & Environment Setup
```bash
git clone https://github.com/aisanjaychauhan/takeout-automation-dashboard.git
cd takeout-automation-dashboard
git checkout ghost-browser

# Securely move your .env and users.db credentials into this folder before proceeding!
```

### 3. Initialize the Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 4. Boot the Internal Server
```bash
python3 app.py
```

## Special Note on the Ghost-Browser System
In both of these operating systems, the script boots Google Chrome perfectly off-screen at coordinate `-3000, 0`. When you command **"Show Browser"** from the web portal, Flask calls native `set_window_position(0, 0)` bindings to instantly trace the Chromium physical display footprint back straight onto your active Desktop window manager.
