import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import pickle
from pathlib import Path

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive.file']

def get_google_drive_service():
    """Get or create Google Drive API service."""
    creds = None
    token_path = Path(__file__).parent.parent / 'credentials' / 'token.pickle'
    credentials_path = Path(__file__).parent.parent / 'credentials' / 'client_secret_967176381819-31ir2mu652gpctml2ndf5ntt50f0u1mu.apps.googleusercontent.com.json'

    # Load existing credentials if available
    if token_path.exists():
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)

    # If credentials are invalid or don't exist, get new ones
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                str(credentials_path), SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save credentials for future use
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds)

def find_or_create_folder(service, folder_name, parent_id=None):
    """Find or create a folder in Google Drive."""
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    
    results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    items = results.get('files', [])

    if items:
        return items[0]['id']
    else:
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_id:
            file_metadata['parents'] = [parent_id]
        
        folder = service.files().create(body=file_metadata, fields='id').execute()
        return folder['id']

def upload_file(service, file_path, folder_id):
    """Upload a file to Google Drive."""
    file_name = os.path.basename(file_path)
    
    # Check if file already exists
    query = f"name='{file_name}' and '{folder_id}' in parents"
    results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    items = results.get('files', [])

    file_metadata = {
        'name': file_name,
        'parents': [folder_id]
    }
    
    media = MediaFileUpload(
        file_path,
        mimetype='text/csv',
        resumable=True
    )

    if items:
        # Update existing file
        file_id = items[0]['id']
        file = service.files().update(
            fileId=file_id,
            body=file_metadata,
            media_body=media
        ).execute()
        print(f'Updated file: {file_name}')
    else:
        # Upload new file
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        print(f'Uploaded new file: {file_name}')

def main():
    try:
        # Get Google Drive service
        service = get_google_drive_service()
        
        # Find or create AI-DRL folder
        ai_drl_folder_id = find_or_create_folder(service, 'AI-DRL')
        
        # Find or create mt5_price_data folder inside AI-DRL
        price_data_folder_id = find_or_create_folder(service, 'mt5_price_data', ai_drl_folder_id)
        
        # Get all CSV files from data/raw directory
        raw_data_path = Path(__file__).parent.parent / 'data' / 'raw'
        csv_files = list(raw_data_path.glob('*.csv'))
        
        if not csv_files:
            print("No CSV files found in data/raw directory")
            return
        
        # Upload each CSV file
        for csv_file in csv_files:
            print(f'Processing {csv_file.name}...')
            upload_file(service, str(csv_file), price_data_folder_id)
            
        print("Upload completed successfully!")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == '__main__':
    main()
