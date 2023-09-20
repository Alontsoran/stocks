import pyodbc
import schedule
import time
from datetime import datetime
import requests

# Function to get stock price using Alpha Vantage
def get_stock_price(stock_name):
    api_key = 'W6DIXEGJ3GSWLUON'
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={stock_name}&interval=5min&apikey={api_key}"
    response = requests.get(url)
    data = response.json()
    latest_close_price = data['Time Series (5min)'][list(data['Time Series (5min)'].keys())[0]]['4. close']
    return float(latest_close_price)

# Load DB details from keys.txt
with open('C:/Users/alont/OneDrive/פרויקט עם אלון/keys[2].txt', 'r') as file:
    lines = file.readlines()
    db_details = {line.split('=')[0]: line.split('=')[1].strip() for line in lines}

# Establish a single DB connection
conn_str = (
    f"Driver={{ODBC Driver 17 for SQL Server}};"
    f"Server={db_details['server']};"
    f"Database={db_details['database']};"
    f"UID={db_details['username']};"
    f"PWD={db_details['password']};"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

def update_stock_price(stock_name):
    # Get the stock price using Alpha Vantage
    stock_price = get_stock_price(stock_name)
    
    # Add a new column with the current time as the name
    current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    alter_table_query = f"ALTER TABLE stockp ADD [{current_time}] FLOAT"
    cursor.execute(alter_table_query)

    # Update the new column with the stock price
    update_query = f"UPDATE stockp SET [{current_time}] = ? WHERE names = ?"
    cursor.execute(update_query, (stock_price, stock_name))
    
    conn.commit()

def check_and_update_stock_prices():
    # Read stock names from the database
    cursor.execute("SELECT names FROM stockp")
    stock_names = [row.names for row in cursor.fetchall()]

    # Update the stock price for each stock
    for stock_name in stock_names:
        try:
            update_stock_price(stock_name)
        except Exception as e:
            print(f"Failed to update stock price for {stock_name}: {e}")

# Schedule the function to run every 30 minutes
schedule.every(30).minutes.do(check_and_update_stock_prices)

try:
    while True:
        schedule.run_pending()
        time.sleep(0.1)
except KeyboardInterrupt:
    print("Stopping the scheduler...")
finally:
    cursor.close() 
    conn.close()