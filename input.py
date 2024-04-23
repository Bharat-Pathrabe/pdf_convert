import paramiko
import os
import datetime
import sqlite3
import logging
import base64

# Define the path to the log file
log_file_path = 'logs/pdf_convert.log'

# Check if the log file exists, if not, create it
if not os.path.exists(log_file_path):
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    with open(log_file_path, 'w'):
        pass

# Set up logging
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to decode Base64 encoded variables
def decode_variable(encoded_variable):
    decoded_variable = base64.b64decode(encoded_variable.encode()).decode()
    return decoded_variable

# Fetch encoded environment variables for SSH connection and decode it
host = decode_variable(os.environ.get('SSH_HOST'))
username = decode_variable(os.environ.get('SSH_USERNAME'))
password = decode_variable(os.environ.get('SSH_PASSWORD'))
port = int(decode_variable(os.environ.get('SSH_PORT', '22')))


# Create SSH client
ssh_client = paramiko.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    ssh_client.connect(hostname=host, port=port, username=username, password=password)
    logging.info("SSH connection established.")
    
    # Open SFTP connection
    ftp = ssh_client.open_sftp()
    logging.info("SFTP connection established.")

    # Define the remote directory path
    remote_directory = "/home/trellissoft/temp_files/"

    # Get today's date in the format YYMMDD
    today_date_yymmdd = datetime.date.today().strftime("%y%m%d")

    # Get today's date in the format YYYY-MM-DD
    today_date_yyyymmdd = datetime.date.today().strftime("%Y-%m-%d")

    # Define default directories
    default_directories = ['input', 'processing', 'completed', 'failed', 'deleted', 'reports', 'logs']

    # Create directories if they don't exist
    for directory in default_directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logging.info(f"Directory '{directory}' created.")

    # Check if the same folder exists remotely
    remote_folder_path = os.path.join(remote_directory, today_date_yymmdd)
    try:
        folder_attributes = ftp.stat(remote_folder_path)
        logging.info(f"The folder '{remote_folder_path}' exists remotely.")

        # Connect to SQLite database
        conn = sqlite3.connect('conversion.db')
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute('''CREATE TABLE IF NOT EXISTS SourceFile 
                          (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                          source_file TEXT, 
                          local_file TEXT, 
                          file_size INTEGER, 
                          status TEXT, 
                          download_datetime TEXT,
                          updated_datetime TEXT)''')

        # Download .pdf files if the folder exists
        pdf_files = [file for file in ftp.listdir(remote_folder_path) if file.endswith('.pdf')]
        if pdf_files:
            for pdf_file in pdf_files:
                # Check if the file has been downloaded today
                cursor.execute('''SELECT * FROM SourceFile WHERE source_file=? AND download_datetime LIKE ?''', (pdf_file, f'{today_date_yyyymmdd}%'))
                existing_entry = cursor.fetchone()

                # If the file hasn't been downloaded today, download it
                if not existing_entry:
                    local_download_directory = os.path.join(os.getcwd(), 'input', today_date_yymmdd)
                    if not os.path.exists(local_download_directory):
                        os.makedirs(local_download_directory)
                        logging.info(f"Local directory '{local_download_directory}' created.")
                    
                    remote_file_path = os.path.join(remote_folder_path, pdf_file)
                    local_file_path = os.path.join(local_download_directory, pdf_file)
                    
                    try:
                        ftp.get(remote_file_path, local_file_path)
                        logging.info(f"Downloaded '{pdf_file}' from '{remote_file_path}' to '{local_file_path}'.")
                            
                        # Insert record into SQLite database
                        file_size = ftp.stat(remote_file_path).st_size
                        download_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        relative_file_path = os.path.relpath(local_file_path, os.getcwd())
                        cursor.execute('''INSERT INTO SourceFile 
                                          (source_file, local_file, file_size, status, download_datetime, updated_datetime) 
                                          VALUES (?, ?, ?, ?, ?, ?)''', 
                                          (pdf_file, relative_file_path, file_size, 'pending', download_datetime, download_datetime))
                        conn.commit()
                        logging.info(f"Inserted '{pdf_file}' into the database.")
                    except Exception as e:
                        logging.error(f"Error downloading '{pdf_file}': {str(e)}")
                else:
                    logging.info(f"Skipping '{pdf_file}' as it has already been downloaded today.")
        else:
            logging.info(f"No .pdf files found in '{remote_folder_path}'.")
    except FileNotFoundError:
        logging.error(f"The folder '{remote_folder_path}' does not exist remotely.")

except Exception as e:
    logging.error(f"An error occurred: {str(e)}")

finally:
    # Close connections
    if 'ftp' in locals():
        ftp.close()
    if 'ssh_client' in locals():
        ssh_client.close()
    if 'conn' in locals():
        conn.close()
