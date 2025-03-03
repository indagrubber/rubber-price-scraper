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
    'Latex(60%)': {'spreadsheet_id': '1hHh1FMholQvVdxFJIY67Yvo5B-8hTfuILvr0BWhp8i4', 'category': 'Latex(60%)'},
    'ISNR20': {'spreadsheet_id': '1xL9wPZGUJoCqwtNlcqZAvQLUyXuASGg_FRyr_d1wKxE', 'category': 'ISNR20'},
    'RSS4': {'spreadsheet_id': '16L7Vz7oJMiamKbg4g-LkQ64wZdXlmNEMOO24MZhC0Bs', 'category': 'RSS4'},
    'RSS5': {'spreadsheet_id': '1OU3IaW5WHPja03CPQ2VjmTmGMA-YyPLPAyAIrwKqc8g', 'category': 'RSS5'}
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

        print(f"Processing sheet: {sheet_name}, Spreadsheet ID: {spreadsheet_id}")

        try:
            # Fetch existing data from the sheet
            result = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!A1:D'
            ).execute()
            existing_data = result.get('values', [])

            # Prepare headers and combine new and existing data
            headers = ["Category", "Price (INR)", "Price (USD)", "Date"]
            new_data = category_df.values.tolist()

            # Ensure headers are present
            if not existing_data or existing_data[0] != headers:
                combined_data = [headers] + new_data + existing_data[1:]
            else:
                combined_data = [existing_data[0]] + new_data + existing_data[1:]

            # Write combined data back to the sheet
            body = {'values': combined_data}
            sheet.values().update(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!A1',
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()

        except Exception as e:
            print(f"Error updating data for sheet '{sheet_name}' in spreadsheet '{spreadsheet_id}': {e}")

if __name__ == "__main__":
    scrape_rubber_prices()
