# Local Deployment Instructions

Your beast of a machine is ready to host the Takeout Automator! Here is how to get it running for internal network access.

## Strategy: SSD for Performance, HDD for Storage
- **Application Files**: Located on your **1.5TB SSD** (current folder). This ensures the database and UI are extremely fast.
- **Backup Downloads**: To be stored on your **3TB HDD**.

---

## Option 1: Docker (Recommended)
This is the cleanest way to run the app. It packages all dependencies (Chrome, Python, etc.) inside a container.

1. **Install Docker Desktop**: If you haven't already, download and install it for Windows.
2. **Configure Storage**:
   - Open `docker-compose.yml`.
   - Uncomment the line `# - D:/TakeoutBackups:/app/backups` and change `D:/TakeoutBackups` to whatever folder on your **3TB HDD** you want to use.
3. **Start the App**:
   - Open PowerShell in this folder.
   - Run: `docker-compose up -d --build`
4. **Access**:
   - On this machine: `http://localhost:5000`
   - On other devices in your house: `http://<YOUR_IP_ADDRESS>:5000`

---

## Option 2: Direct Python (Waitress)
Use this if you don't want to use Docker.

1. **Install Waitress**:
   ```powershell
   pip install waitress
   ```
2. **Configure HDD path**:
   - Update your `.env` file or `app.py` settings to point the download directory to your **3TB HDD** drive letter.
3. **Run the Server**:
   ```powershell
   python run_prod.py
   ```
4. **Access**:
   - Same as above, using port **5000**.

---

## Finding your IP Address
To access the app from other devices on your network:
1. Open PowerShell and type `ipconfig`.
2. Look for `IPv4 Address` (usually something like `192.168.1.x`).
3. Use that address in your mobile browser or other computers.
