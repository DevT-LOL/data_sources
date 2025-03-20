import paramiko

# VPS Connection Details
VPS_HOST = "89.116.34.94"  # Your VPS IP
VPS_USERNAME = "root"       # Your VPS user
VPS_PASSWORD = "your-password"  # Avoid plaintext passwords, use SSH keys instead
REMOTE_PATH = "/root/my_project/myfile.py"  # File on VPS
LOCAL_PATH = "/Users/your-user/Downloads/myfile.py"  # Where to save it on your PC

def download_file():
    try:
        # Initialize SSH client
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect to VPS
        ssh.connect(VPS_HOST, username=VPS_USERNAME, password=VPS_PASSWORD)

        # Start SFTP session
        sftp = ssh.open_sftp()

        # Download file
        print(f"Downloading {REMOTE_PATH} from VPS to {LOCAL_PATH}...")
        sftp.get(REMOTE_PATH, LOCAL_PATH)

        # Close connections
        sftp.close()
        ssh.close()
        print("Download complete!")

    except Exception as e:
        print(f"Error: {e}")

# Run the function
download_file()
