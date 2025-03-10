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
        if "(Market Holiday)" in str(price_str):
            return 0.0
        clean_price = re.sub(r'[^\d.]', '', str(price_str))
        return float(clean_price)
    except (ValueError, TypeError):
        return 0.0

def process_price_table(table):
    """Process any table with valid price data structure"""
    data = []
    for row in table.find_all("tr")[1:]:
        cols = [col.text.strip() for col in row.find_all("td")]
        if len(cols) != 3:
            print(f"Skipping invalid row: {cols}")
            continue
            
        category = cols[0].strip()
        inr_price = validate_price_data(cols[1])
        usd_price = validate_price_data(cols[2])
        
        data.append([category, inr_price, usd_price])
    
    return pd.DataFrame(data, columns=["Category", "Price (INR)", "Price (USD)"])

def scrape_rubber_prices():
    url = "https://rubberboard.gov.in/public"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch data. HTTP Status Code: {response.status_code}")
        return

    soup = BeautifulSoup(response.content, "html.parser")
    tables = soup.find_all("table")
    
    if len(tables) < 5:
        print(f"Not enough tables found for primary processing. Total tables: {len(tables)}")
        return

    # Process Table 5 (RSS4 and ISNR20 only)
    df_primary = process_price_table(tables[4])
    df_primary = df_primary[df_primary['Category'].isin(['RSS4', 'ISNR20'])]
    print("Table 5 contents:")
    print(df_primary)

    # Check for SMR20 in Table 9 first, then fallback to Table 8
    df_secondary = pd.DataFrame(columns=["Category", "Price (INR)", "Price (USD)"])  # Initialize empty DataFrame
    if len(tables) > 8:
        df_secondary = process_price_table(tables[8])
        df_secondary = df_secondary[df_secondary['Category'] == 'SMR20']
        if not df_secondary.empty:
            print("SMR20 found in Table 9.")
        else:
            print("SMR20 not found in Table 9. Checking Table 8...")
    
    if df_secondary.empty and len(tables) > 7:  # Fallback to Table 8
        df_secondary = process_price_table(tables[7])
        df_secondary = df_secondary[df_secondary['Category'] == 'SMR20']
        if not df_secondary.empty:
            print("SMR20 found in Table 8.")
        else:
            print("SMR20 not found in Table 8 either.")

    # Combine and add timestamp
    df_combined = pd.concat([df_primary, df_secondary], ignore_index=True)
    df_combined = df_combined.drop_duplicates()
    
    # Changed date format to MM/DD/YYYY
    df_combined["Date"] = datetime.now(IST).strftime("%m/%d/%Y")
    
    print("All categories found:", df_combined['Category'].tolist())
    
    update_google_sheets(df_combined)

def update_google_sheets(df):
    service = get_sheets_service()
    sheet = service.spreadsheets()

    for sheet_name, config in SHEET_CONFIG.items():
        spreadsheet_id = config['spreadsheet_id']
        category = config['category']
        
        print(f"Categories in scraped data: {df['Category'].unique()}")
        print(f"Looking for category: {category}")

        category_df = df[df["Category"].str.strip().str.upper() == category.strip().upper()]

        if category_df.empty:
            print(f"No valid data found for category: {category}")
            continue

        try:
            headers = ["Category", "Price (INR)", "Price (USD)", "Date"]
            new_data = category_df.values.tolist()

            existing_data_result = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!A2:D'
            ).execute()
            existing_data = existing_data_result.get('values', [])

            new_data_filtered = [
                row for row in new_data 
                if row not in existing_data
            ]

            if not new_data_filtered:
                print(f"No new rows to append for {category}. Data already exists.")
                continue

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

            sheet.values().append(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!A:D',
                valueInputOption='USER_ENTERED',
                insertDataOption='INSERT_ROWS',
                body={'values': new_data_filtered}
            ).execute()
            print(f"Successfully updated {category} with {len(new_data_filtered)} new rows.")

        except Exception as e:
            print(f"Error updating {category}: {str(e)}")

if __name__ == "__main__":
    scrape_rubber_prices()
