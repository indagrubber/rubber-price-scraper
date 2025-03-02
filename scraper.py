import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
from google.colab import drive

drive.mount('/content/drive')

drive_folder = '/content/drive/My Drive/Rubber_Prices'

if not os.path.exists(drive_folder):
    os.makedirs(drive_folder)

# Function to scrape Table 5 from the website
def scrape_rubber_prices():
    url = "https://rubberboard.gov.in/public"

    # Fetch the webpage
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch data. HTTP Status Code: {response.status_code}")
        return

    # Parse the HTML content
    soup = BeautifulSoup(response.content, "html.parser")

    # Locate Table 5 (adjust index if necessary)
    tables = soup.find_all("table")
    if len(tables) < 5:
        print("Table 5 not found on the webpage.")
        return

    table = tables[4]  # Table 5 is at index 4 (zero-based indexing)

    # Extract rows and columns from the table
    rows = table.find_all("tr")
    data = []
    for row in rows[1:]:  # Skip header row
        cols = row.find_all("td")
        cols = [col.text.strip() for col in cols]
        data.append(cols)

    # Convert to DataFrame
    df = pd.DataFrame(data, columns=["Category", "Price (INR)", "Price (USD)"])

    # Add today's date as a column
    df["Date"] = datetime.now().strftime("%Y-%m-%d")

    # Save each category to a separate Excel file in Google Drive
    save_to_drive(df)

# Function to save data into separate Excel files for each category in Google Drive
def save_to_drive(df):
    categories = df["Category"].unique()

    for category in categories:
        category_df = df[df["Category"] == category]
        file_name = os.path.join(drive_folder, f"{category}.xlsx")

        # Append data if file exists, otherwise create a new file
        if os.path.exists(file_name):
            existing_df = pd.read_excel(file_name)
            updated_df = pd.concat([existing_df, category_df], ignore_index=True)
            updated_df.to_excel(file_name, index=False)
        else:
            category_df.to_excel(file_name, index=False)

    print(f"Data saved successfully to Google Drive folder: {drive_folder}")

# Run the scraper function
scrape_rubber_prices()
