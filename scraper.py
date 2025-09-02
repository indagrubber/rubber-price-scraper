import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
import json
import os
import re
import time
from pytz import timezone

# Constants
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
IST = timezone('Asia/Kolkata')
SHEET_CONFIG = {
    'SMR20': {'spreadsheet_id': '1hHh1FMholQvVdxFJIY67Yvo5B-8hTfuILvr0BWhp8i4', 'category': 'SMR20'},
    'ISNR20': {'spreadsheet_id': '1xL9wPZGUJoCqwtNlcqZAvQLUyXuASGg_FRyr_d1wKxE', 'category': 'ISNR20'},
    'RSS4': {'spreadsheet_id': '16L7Vz7oJMiamKbg4g-LkQ64wZdXlmNEMOO24MZhC0Bs', 'category': 'RSS4'}
}

def get_sheets_service():
    creds_json = os.getenv('GOOGLE_CREDENTIALS')
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS environment variable not set.")
    creds_dict = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return build('sheets', 'v4', credentials=credentials)

def validate_price_data(price_str):
    try:
        if "(Market Holiday)" in str(price_str):
            return 0.0
        clean_price = re.sub(r'[^\d.]', '', str(price_str))
        return float(clean_price)
    except (ValueError, TypeError):
        return 0.0

def process_price_table(table):
    data = []
    for row in table.find_all("tr")[1:]:
        cols = [col.get_text(strip=True) for col in row.find_all("td")]
        if len(cols) != 3:
            continue
        category = cols[0].strip()
        inr_price = validate_price_data(cols[1])
        usd_price = validate_price_data(cols[2])
        data.append([category, inr_price, usd_price])
    return pd.DataFrame(data, columns=["Category", "Price (INR)", "Price (USD)"])

def find_category_table(soup, category):
    """Search all tables dynamically for RSS4 and ISNR20 category"""
    for table in soup.find_all("table"):
        df = process_price_table(table)
        if not df.empty and category in df['Category'].values:
            print(f"Found '{category}' in one of the tables.")
            return df[df["Category"] == category]
    print(f"Could not find '{category}' in any table.")
    return pd.DataFrame(columns=["Category", "Price (INR)", "Price (USD)"])

def scrape_rubber_prices():
    url = "https://rubberboard.gov.in/public"
    max_retries = 5
    for attempt in range(max_retries):
        try:
            headers = {
                'User-Agent': 'RubberPriceScraper/1.0 (github-actions@example.com)'
            }
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                break
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"Request failed: {e}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print("Failed to fetch data after multiple retries.")
                return

    soup = BeautifulSoup(response.content, "html.parser")
    tables = soup.find_all("table")
    if len(tables) < 5:
        print(f"Not enough tables found for processing. Total tables: {len(tables)}")
        return

    # Dynamic search for RSS4 and ISNR20
    df_rss4 = find_category_table(soup, 'RSS4')
    df_isnr20 = find_category_table(soup, 'ISNR20')

    # Use original logic for SMR20 from Table 9 then Table 8
    df_smr20 = pd.DataFrame(columns=["Category", "Price (INR)", "Price (USD)"])
    if len(tables) > 8:
        df_tmp = process_price_table(tables[8])
        df_smr20 = df_tmp[df_tmp['Category'] == 'SMR20']
        if not df_smr20.empty:
            print("SMR20 found in Table 9.")
        else:
            print("SMR20 not found in Table 9. Checking Table 8...")
    if df_smr20.empty and len(tables) > 7:
        df_tmp = process_price_table(tables[7])
        df_smr20 = df_tmp[df_tmp['Category'] == 'SMR20']
        if df_smr20.empty:
            print("SMR20 not found in Table 8 either.")
        else:
            print("SMR20 found in Table 8.")

    # Combine all dataframes and drop duplicates
    df_combined = pd.concat([df_rss4, df_isnr20, df_smr20], ignore_index=True)
    df_combined = df_combined.drop_duplicates()
    df_combined["Date"] = datetime.now(IST).strftime("%m/%d/%Y")

    print("All categories found:", df_combined['Category'].tolist())
    update_google_sheets(df_combined)

def retry_with_backoff(func, retries=5, backoff_factor=2):
    for attempt in range(retries):
        try:
            return func()
        except Exception as e:
            if attempt < retries - 1:
                wait_time = backoff_factor ** attempt
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise e

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

            existing_data_result = retry_with_backoff(lambda: sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!A2:D'
            ).execute())

            existing_data = existing_data_result.get('values', [])
            new_data_filtered = [row for row in new_data if row not in existing_data]

            if not new_data_filtered:
                print(f"No new rows to append for {category}. Data already exists.")
                continue

            header_range = f'{sheet_name}!A1:D1'
            existing_headers_result = retry_with_backoff(lambda: sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=header_range
            ).execute())

            existing_headers = existing_headers_result.get('values', [])
            if not existing_headers or existing_headers[0] != headers:
                retry_with_backoff(lambda: sheet.values().update(
                    spreadsheetId=spreadsheet_id,
                    range=header_range,
                    valueInputOption='USER_ENTERED',
                    body={'values': [headers]}
                ).execute())

            retry_with_backoff(lambda: sheet.values().append(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!A:D',
                valueInputOption='USER_ENTERED',
                insertDataOption='INSERT_ROWS',
                body={'values': new_data_filtered}
            ).execute())

            print(f"Successfully updated {category} with {len(new_data_filtered)} new rows.")
        except Exception as e:
            print(f"Error updating {category}: {str(e)}")

if __name__ == "__main__":
    scrape_rubber_prices()
