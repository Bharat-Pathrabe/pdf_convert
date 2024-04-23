import os
import shutil
import subprocess
import sqlite3
import logging
from datetime import datetime
from pdf2image import convert_from_path

# Set up logging
log_file = 'logs/pdf_convert.log'
os.makedirs(os.path.dirname(log_file), exist_ok=True)
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

input_folder = "input/"
processing_folder = "processing/"

def convert_pdf_to_images(input_file, output_folder):
    try:
        # Connect to SQLite database
        conn = sqlite3.connect('conversion.db')
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute('''CREATE TABLE IF NOT EXISTS ProcessedFile 
                          (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                          local_file TEXT, 
                          source_file TEXT, 
                          status TEXT, 
                          image_created_datetime TEXT,
                          updated_datetime TEXT)''')
        conn.commit()

        # Update database with processing status and start datetime
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file_name = os.path.basename(input_file)
        cursor.execute('''SELECT * FROM SourceFile WHERE source_file=?''', (file_name,))
        row = cursor.fetchone()
        if row:
            cursor.execute('''UPDATE SourceFile 
                              SET status=?, updated_datetime=? 
                              WHERE source_file=?''', 
                           ('processing', start_datetime, file_name))
        else:
            cursor.execute('''INSERT INTO SourceFile (source_file, status, updated_datetime) 
                              VALUES (?, ?, ?)''', (file_name, 'processing', start_datetime))
        conn.commit()

        # Create folder for the input file
        today_date = datetime.now().strftime("%y%m%d")
        file_name_no_extension = os.path.splitext(file_name)[0]
        file_folder = os.path.join(output_folder, today_date, file_name_no_extension)
        os.makedirs(file_folder, exist_ok=True)  # Create folder for original PDF file
        os.makedirs(os.path.join(file_folder, "converted"), exist_ok=True)  # Create folder for converted images

        # Create original folder and copy the original WAV file
        original_folder = os.path.join(file_folder, "original")
        os.makedirs(original_folder, exist_ok=True)
        original_file_destination = os.path.join(original_folder, file_name)
        shutil.copy(input_file, original_file_destination)

        # Check if images already exist
        existing_images = [f for f in os.listdir(os.path.join(file_folder, "converted")) if f.startswith(file_name_no_extension) and f.endswith('.jpg')]
        if existing_images:
            logging.info(f"Skipping conversion for '{file_name}', images already exist.")
            # Update status of the source file to 'Completed'
            end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute('''UPDATE SourceFile 
                              SET status=?, updated_datetime=? 
                              WHERE source_file=?''', 
                           ('Completed', end_datetime, file_name))
            conn.commit()
            conn.close()
            return

        # Convert PDF to images using pdf2image
        logging.info(f"Converting '{file_name}' to images.")
        images = convert_from_path(input_file, dpi=300, fmt='jpeg')

        # Save each page as a separate image
        for i, image in enumerate(images):
            image_path = os.path.join(file_folder, "converted", f"{file_name_no_extension}_{i + 1}.jpg")
            image.save(image_path, "JPEG")

            # Update ProcessedFile table
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute('''INSERT INTO ProcessedFile 
                              (local_file, source_file, status, image_created_datetime, updated_datetime) 
                              VALUES (?, ?, ?, ?, ?)''', 
                           (os.path.basename(image_path), file_name, 'Processing', now, now))
            conn.commit()

        # Update status of the source file to 'Completed'
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute('''UPDATE SourceFile 
                          SET status=?, updated_datetime=? 
                          WHERE source_file=?''', 
                       ('Completed', end_datetime, file_name))
        conn.commit()
        conn.close()
        
    except Exception as e:
        logging.error(f"Error processing file {input_file}: {str(e)}")
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute('''UPDATE SourceFile 
                          SET status=?, updated_datetime=? 
                          WHERE source_file=?''', 
                       ('Failed', end_datetime, file_name))
        conn.commit()

        try:
            error_file_folder = os.path.join(processing_folder, today_date, file_name_no_extension)
            failed_folder = os.path.join(os.getcwd(), "failed")
            if not os.path.exists(failed_folder):
                os.makedirs(failed_folder)
            if os.path.exists(error_file_folder):
                shutil.move(error_file_folder, failed_folder)
                logging.info(f"Error file '{file_name}' moved to 'failed' folder.")
        except Exception as e:
            logging.error(f"Error moving error file to 'Failed' folder: {str(e)}")


def process_pdf_files():
    try:
        today_date = datetime.now().strftime("%y%m%d")
        input_folder_today = os.path.join(input_folder, today_date)
        if not os.path.exists(input_folder_today):
            logging.info(f"No files found for today's date {today_date} in the input folder.")
            return
        
        files = os.listdir(input_folder_today)

        for file in files:
            if file.endswith(".pdf"):
                convert_pdf_to_images(os.path.join(input_folder_today, file), processing_folder)
    except Exception as e:
        logging.error(f"Error processing PDF files: {str(e)}")



# Call the function to process PDF files
process_pdf_files()
