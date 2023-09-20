import pyodbc
import schedule
import time
from datetime import datetime
import openpyxl
import os

# Load DB details from keys.txt
with open('C:/Users/zvi25/Desktop/StartUp/פרויקט עם אלון/Milestones/2/2.1/keys.txt', 'r', encoding='utf-8') as file:
    lines = file.readlines()
    db_details = {line.split('=')[0]: line.split('=')[1].strip() for line in lines}

# Create a new Excel workbook
wb = openpyxl.Workbook()
ws = wb.active

# Use the STOCKHISTORY function in cell B1 to get historical data from A1 (stock symbol)
ws['B1'] = '=STOCKHISTORY(A1, TODAY(),TODAY(),0,0,1)'

desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop')
file_path = os.path.join(desktop_path, 'stock_history.xlsx')
wb.save(file_path)

def update_stock_price(stock_name):
    # Open the existing Excel file
    wb = openpyxl.load_workbook(file_path)
    ws = wb.active

    # Insert the stock symbol into Excel
    ws['A1'] = f'"{stock_name}"'
    wb.save(file_path)
    
    # Assume Excel calculates the price automatically in cell B2 (or whatever cell the value is in)
    stock_price = ws['B2'].value  # Change this to the appropriate cell if it's not B2
    
    # Print the current value from Excel for verification
    print(f"Current value for {stock_name}: {stock_price}")

    # Ensure the value is float
    stock_price = float(stock_price)
    
    # Connect to the database and update the stock price
    conn_str = (
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"Server={db_details['server']};"
        f"Database={db_details['database']};"
        f"UID={db_details['username']};"
        f"PWD={db_details['password']};"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # Get the current date and time in the format "dd_mm_yyyy_hh_mm"
    current_datetime = datetime.now().strftime("%d_%m_%Y_%H_%M")
    
    # Add a new column without checking if it exists
    add_column_query = f"ALTER TABLE stocks_prices ADD [{current_datetime}] FLOAT"
    cursor.execute(add_column_query)

    # Update the stock price for the current datetime
    update_query = f"UPDATE stocks_prices SET [{current_datetime}] = ? WHERE Stock_name = ?"
    cursor.execute(update_query, (stock_price, stock_name))
    conn.commit()

    cursor.close()
    conn.close()
    wb.close()

def check_and_update_stock_prices():
    # Connect to the database and read stock names
    conn_str = (
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"Server={db_details['server']};"
        f"Database={db_details['database']};"
        f"UID={db_details['username']};"
        f"PWD={db_details['password']};"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT Stock_name FROM stocks_prices")
    
    stock_names = [row.Stock_name for row in cursor.fetchall()]

    for stock_name in stock_names:
        update_stock_price(stock_name)

    conn.close()

# Schedule the function to run every 30 minutes
schedule.every(0.1).minutes.do(check_and_update_stock_prices)

while True:
    schedule.run_pending()
    time.sleep(1)
