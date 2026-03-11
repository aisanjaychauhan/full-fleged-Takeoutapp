import PyInstaller.__main__
import os
import shutil

# Ensure we're in the right directory
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)

# Clean previous builds
if os.path.exists("build"):
    shutil.rmtree("build")
if os.path.exists("dist"):
    shutil.rmtree("dist")

print("Building TakeoutAgent.exe...")

PyInstaller.__main__.run([
    'agent.py',
    '--name=TakeoutAgent',
    '--onefile',
    '--console',  # Keep console so user sees logs
    '--add-data=templates;templates',  # Include HTML templates
    '--add-data=takeout_runner.py;.',
    '--add-data=takeout_downloader.py;.',
    '--hidden-import=flask',
    '--hidden-import=playwright',
    '--hidden-import=requests',
    '--hidden-import=eventlet',
    '--hidden-import=selenium',
    '--hidden-import=selenium.webdriver.chrome.webdriver',
    '--hidden-import=selenium.webdriver.firefox.webdriver',
    '--hidden-import=selenium.webdriver.edge.webdriver',
    '--hidden-import=engineio.async_drivers.threading',
    '--noconfirm',
])

print("\nBuild complete! Executable is located at dist/TakeoutAgent.exe")
