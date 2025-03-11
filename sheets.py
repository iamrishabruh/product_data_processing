# sheets.py
import os
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

# Define the required scope for the Google Sheets API
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def get_sheets_service():
    creds = None
    token_pickle = 'token.pickle'
    if os.path.exists(token_pickle):
        with open(token_pickle, 'rb') as token:
            creds = pickle.load(token)
    # If no valid credentials, initiate login.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            credentials_path = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_pickle, 'wb') as token:
            pickle.dump(creds, token)
    service = build('sheets', 'v4', credentials=creds)
    return service

def upload_chunk_to_sheet(chunk_df, title):
    """
    Creates a new Google Sheet with the specified title and uploads the DataFrame chunk.
    """
    service = get_sheets_service()
    
    # Create a new spreadsheet with the given title
    spreadsheet = {
        'properties': {
            'title': title
        }
    }
    spreadsheet = service.spreadsheets().create(body=spreadsheet, fields='spreadsheetId').execute()
    spreadsheet_id = spreadsheet.get('spreadsheetId')
    
    # Prepare the data: header row plus data rows
    data = [chunk_df.columns.tolist()] + chunk_df.astype(str).values.tolist()
    
    body = {
        'values': data
    }
    # Write data starting at cell A1
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range='A1',
        valueInputOption='RAW',
        body=body
    ).execute()
    
    print(f"Uploaded chunk to Google Sheet with ID: {spreadsheet_id}")
    return spreadsheet_id
