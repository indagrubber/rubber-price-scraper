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
    'SMR20': {'spreadsheet_id': '1hHh1FMholQvVdxFJIY67Yvo5B-8hTfuILvr0BWhp8i4', 'category': 'SMR20'},
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
    
    # Fetch Table 5 for all categories except SMR20
    tables = soup.find_all("table")
    if len(tables) < 8:  # Ensure there are at least 8 tables for SMR20
        print("Required tables not found on the webpage.")
        return

    # Process Table 5 for ISNR20, RSS4, RSS5
    table_5 = tables[4]
    rows_5 = table_5.find_all("tr")
    data_5 = []
    for row in rows_5[1:]:
        cols = row.find_all("td")
        cols = [col.text.strip() for col in cols]
        data_5.append(cols)

    df_5 = pd.DataFrame(data_5, columns=["Category", "Price (INR)", "Price (USD)"])
    
    # Process Table 8 for SMR20
    table_8 = tables[7]
    rows_8 = table_8.find_all("tr")
    data_8 = []
    for row in rows_8[1:]:
        cols = row.find_all("td")
        cols = [col.text.strip() for col in cols]
        data_8.append(cols)

    df_8 = pd.DataFrame(data_8, columns=["Category", "Price (INR)", "Price (USD)"])

    # Combine data from both tables into one DataFrame
    df_combined = pd.concat([df_5, df_8], ignore_index=True)
    
    # Add a date column to the combined DataFrame
    df_combined["Date"] = datetime.now().strftime("%Y-%m-%d")

    update_google_sheets(df_combined)

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

            # Ensure headers are present only once
            if existing_data and existing_data[0] == headers:
                combined_data = [existing_data[0]] + new_data + existing_data[1:]
            else:
                combined_data = [headers] + new_data + existing_data

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
