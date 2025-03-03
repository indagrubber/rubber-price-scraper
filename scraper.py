import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
import json
import os

# Google Sheets setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Define sheet IDs, sheet names, and corresponding categories
SHEET_CONFIG = {
    'Latex(60%)': {'spreadsheet_id': '1--LQjv_7DMHDOyAqTorGo3hoN5UNiIWF', 'category': 'Latex(60%)'},
    'ISNR20': {'spreadsheet_id': '1-28VAH431gzMf6bEXLQenDkiY3gwTkfN', 'category': 'ISNR20'},
    'RSS4': {'spreadsheet_id': '1P-vAhLs1ieD2Qbv1lZeqpTmRKAHe14wQ', 'category': 'RSS4'},
    'RSS5': {'spreadsheet_id': '1-4AdM4au0a_-sZH0yl7i2kVpcCT1FFkh', 'category': 'RSS5'}
}

def get_sheets_service():
    # Load credentials from environment variable
    creds_json = os.getenv('GOOGLE_CREDENTIALS')
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS environment variable not set.")
    
    creds_dict = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    return service

def scrape_rubber_prices():
    url = "https://rubberboard.gov.in/public"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch data. HTTP Status Code: {response.status_code}")
        return

    soup = BeautifulSoup(response.content, "html.parser")
    tables = soup.find_all("table")
    if len(tables) < 5:
        print("Table 5 not found on the webpage.")
        return

    table = tables[4]
    rows = table.find_all("tr")
    data = []
    for row in rows[1:]:
        cols = row.find_all("td")
        cols = [col.text.strip() for col in cols]
        data.append(cols)

    df = pd.DataFrame(data, columns=["Category", "Price (INR)", "Price (USD)"])
    df["Date"] = datetime.now().strftime("%Y-%m-%d")
    update_google_sheets(df)

def update_google_sheets(df):
    service = get_sheets_service()
    sheet = service.spreadsheets()

    for sheet_name, config in SHEET_CONFIG.items():
        spreadsheet_id = config['spreadsheet_id']
        category = config['category']
        category_df = df[df["Category"] == category]

        if category_df.empty:
            print(f"No data found for category: {category}")
            continue

        # Check if sheet exists, create if not (optional, if sheets already exist)
        try:
            sheet.values().get(spreadsheetId=spreadsheet_id, range=f'{sheet_name}!A1').execute()
        except Exception as e:
            body = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': sheet_name
                        }
                    }
                }]
            }
            sheet.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

        # Clear existing data and append new data
        clear_range = f'{sheet_name}!A2:D'
        sheet.values().clear(spreadsheetId=spreadsheet_id, range=clear_range).execute()

        values = [category_df.columns.tolist()] + category_df.values.tolist()
        body = {'values': values}
        range_ = f'{sheet_name}!A2'
        sheet.values().append(spreadsheetId=spreadsheet_id, range=range_, valueInputOption='USER_ENTERED', body=body).execute()

    print("Data updated in Google Sheets successfully.")

if __name__ == "__main__":
    scrape_rubber_prices()
