import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
import json
import os
import re
from pytz import timezone

# Constants
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
IST = timezone('Asia/Kolkata')
DUPLICATE_CHECK_RANGE = 10  # Check last 10 entries for duplicates

SHEET_CONFIG = {
    'SMR20': {'spreadsheet_id': '1hHh1FMholQvVdxFJIY67Yvo5B-8hTfuILvr0BWhp8i4', 'category': 'SMR20'},
    'ISNR20': {'spreadsheet_id': '1xL9wPZGUJoCqwtNlcqZAvQLUyXuASGg_FRyr_d1wKxE', 'category': 'ISNR20'},
    'RSS4': {'spreadsheet_id': '16L7Vz7oJMiamKbg4g-LkQ64wZdXlmNEMOO24MZhC0Bs', 'category': 'RSS4'},
    'RSS5': {'spreadsheet_id': '1OU3IaW5WHPja03CPQ2VjmTmGMA-YyPLPAyAIrwKqc8g', 'category': 'RSS5'}
}

def get_sheets_service():
    creds_json = os.getenv('GOOGLE_CREDENTIALS')
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS environment variable not set.")
    
    creds_dict = json.loads(creds_json)
    return build('sheets', 'v4', credentials=service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES))

def validate_price_data(price_str):
    """Convert price string to float and validate format"""
    try:
        # Remove currency symbols and commas
        clean_price = re.sub(r'[^\d.]', '', price_str)
        return float(clean_price)
    except (ValueError, TypeError):
        return None

def scrape_rubber_prices():
    url = "https://rubberboard.gov.in/public"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch data. HTTP Status Code: {response.status_code}")
        return

    soup = BeautifulSoup(response.content, "html.parser")
    tables = soup.find_all("table")
    
    if len(tables) < 8:
        print("Required tables not found on the webpage.")
        return

    # Data processing with validation
    def process_table(table, expected_columns=3):
        data = []
        for row in table.find_all("tr")[1:]:
            cols = [col.text.strip() for col in row.find_all("td")]
            if len(cols) != expected_columns:
                print(f"Skipping invalid row: {cols}")
                continue
            
            # Validate price columns
            inr_price = validate_price_data(cols[1])
            usd_price = validate_price_data(cols[2])
            
            if inr_price is None or usd_price is None:
                print(f"Skipping row with invalid prices: {cols}")
                continue
                
            data.append([cols[0], inr_price, usd_price])
        return data

    # Process tables with validation
    df_5 = pd.DataFrame(process_table(tables[4]), columns=["Category", "Price (INR)", "Price (USD)"])
    df_8 = pd.DataFrame(process_table(tables[7]), columns=["Category", "Price (INR)", "Price (USD)"])

    # Combine and add timestamp
    df_combined = pd.concat([df_5, df_8], ignore_index=True)
    df_combined["Date"] = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S%z")
    
    update_google_sheets(df_combined)

def update_google_sheets(df):
    service = get_sheets_service()
    sheet = service.spreadsheets()

    for sheet_name, config in SHEET_CONFIG.items():
        spreadsheet_id = config['spreadsheet_id']
        category = config['category']
        category_df = df[df["Category"] == category]

        if category_df.empty:
            print(f"No valid data found for category: {category}")
            continue

        try:
            headers = ["Category", "Price (INR)", "Price (USD)", "Date"]
            new_data = category_df.values.tolist()
            current_time = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

            # Header management
            header_range = f'{sheet_name}!A1:D1'
            existing_headers = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=header_range
            ).execute().get('values', [])

            if not existing_headers or existing_headers[0] != headers:
                sheet.values().update(
                    spreadsheetId=spreadsheet_id,
                    range=header_range,
                    valueInputOption='USER_ENTERED',
                    body={'values': [headers]}
                ).execute()

            # Duplicate check
            last_entries = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!A2:D{DUPLICATE_CHECK_RANGE + 1}'
            ).execute().get('values', [])

            duplicate_found = any(
                entry == new_data[0] for entry in last_entries
                if len(entry) == 4  # Ensure complete entry
            )

            if duplicate_found:
                print(f"Duplicate entry prevented for {category} at {current_time}")
                continue

            # Data append
            sheet.values().append(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!A:D',
                valueInputOption='USER_ENTERED',
                insertDataOption='INSERT_ROWS',
                body={'values': new_data}
            ).execute()
            print(f"Successfully updated {category} at {current_time}")

        except Exception as e:
            print(f"Error updating {category}: {str(e)}")

if __name__ == "__main__":
    scrape_rubber_prices()
