import sys
import os

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.utils.google_drive import GoogleDriveConnector

def delete_folders():
    # Initialize Google Drive connector
    drive = GoogleDriveConnector()
    
    # Authenticate using the client secrets file
    credentials_path = os.path.join('src', 'credentials', 'client_secret_967176381819-31ir2mu652gpctml2ndf5ntt50f0u1mu.apps.googleusercontent.com.json')
    token_path = os.path.join('src', 'credentials', 'token.pickle')
    drive.authenticate(credentials_path=credentials_path, token_path=token_path)
    
    # Folders to delete
    folders_to_delete = ['models', 'csv_file', 'IDA', 'Arron']
    
    # List all files and folders
    files = drive.list_files()
    
    # Find and delete specified folders
    for file in files:
        if file['name'] in folders_to_delete:
            try:
                drive.service.files().delete(fileId=file['id']).execute()
                print(f"Successfully deleted {file['name']}")
            except Exception as e:
                print(f"Error deleting {file['name']}: {str(e)}")

if __name__ == "__main__":
    delete_folders()
