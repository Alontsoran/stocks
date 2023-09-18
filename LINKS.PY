from bs4 import BeautifulSoup
from selenium import webdriver
import time
import pandas as pd
import os

# Specify the full path to the Excel file including the desired location
output_file = 'C:/Users/alont/OneDrive/פרויקט עם אלון/new2/extracted_data.xlsx'

# Load the Excel file with the symbols
excel_file = "C:\\Users\\alont\\OneDrive\\פרויקט עם אלון\\New folder\\combined_file.xlsx"
df = pd.read_excel(excel_file, sheet_name='Combined')

symbols = df['company']

# Set up the Chrome WebDriver to run headless
driver = webdriver.Chrome()

# Initialize a list to store all data
all_data = []

for symbol in symbols:
    # Set the URL based on the symbol
    url = f"https://maya.tase.co.il/reports/finance?q=%7B%22FromYear%22:2018,%22ToYear%22:2023,%22Period%22:%226%22,%22EntityId%22:{symbol}%7D"

    # Navigate to the page
    driver.get(url)

    # Wait for the page to load
    time.sleep(10)

    # Get the page source
    html = driver.page_source

    # Parse the HTML with BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    # Initialize a list to store data for the current symbol
    data = []

    # Find each row on the page
    for row in soup.find_all('div', {'role': 'row'}):
        try:
            # Extract company name
            company_element = row.find('a', {'title': True})
            company_name = company_element['title'].replace('לדף חברה  ', '') if company_element else None

            # Extract report type
            report_element = row.find('div', {'class': 'tableCol col_3 ng-binding ng-scope'})
            report_type = report_element.text if report_element else None

            # Extract links
            link_elements = row.find_all('a', {'class': 'ng-isolate-scope'}, href=True)
            links = [a['href'] for a in link_elements if a['href'].endswith('/1/0')]

            # Append the data if all elements are present
            if company_name and report_type and links:
                data.append((company_name, report_type, links))

        except Exception as e:
            print(f"Skipping row due to error: {e}")

    # Append the data for the current symbol to the overall data list
    all_data.extend(data)

# Convert the list of data into a DataFrame
df_symbol = pd.DataFrame(all_data, columns=['Company Name', 'Report Type', 'Links'])

# Save the DataFrame as an Excel file (overwrite the existing file)
df_symbol.to_excel(output_file, sheet_name='Data', index=False)

# Close the WebDriver
driver.quit()
