import csv
import requests
import os

# Read the symbols from the uploaded file
symbols = []
with open('C:/Users/Administrator/Downloads/f.csv', 'r') as file:
    reader = csv.reader(file)
    for row in reader:
        symbols.append(row[0])  # Read symbols without .TA suffix

# Define the API endpoint and the API key
api_endpoint = "https://financialmodelingprep.com/api/v3/income-statement/{}?period=quarter&limit=400&apikey=728c1b4c2d8218add598fcfc7401f70b"

# Initialize the list to store the results
results = []

# Fetch the data for each symbol from the API
for symbol in symbols:
    response = requests.get(api_endpoint.format(symbol + ".TA"))  # Add .TA suffix only while making API request
    
    if response.status_code == 200:
        data = response.json()
        results.extend(data)
    else:
        print(f"Failed to fetch data for {symbol}.TA")

# Define the path where the final file should be saved
save_path = "C:\\Users\\Administrator\\Documents\\GitHub\\consolidated_financial_data.csv"

# Write the results to a new CSV file
with open(save_path, 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    
    # Write the header
    if results:
        header = results[0].keys()
        writer.writerow(header)
        
        # Write the rows
        for row in results:
            writer.writerow(row.values())
