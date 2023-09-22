import pyodbc
import schedule
import time
from datetime import datetime
import certifi
import json
import ssl
import re

try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen


def get_stock_price(stock_name):
    print(f"Getting stock price for {stock_name}")
    stock_name = re.sub(r'[^\w]', '', stock_name)
    
    api_key = "728c1b4c2d8218add598fcfc7401f70b"
    url = f"https://financialmodelingprep.com/api/v3/quote-short/{stock_name}?apikey={api_key}"
    
    context = ssl.create_default_context(cafile=certifi.where())
    
    try:
        response = urlopen(url, context=context)
        data = response.read().decode("utf-8")
        stock_data = json.loads(data)
        
        if stock_data:
            latest_close_price = float(stock_data[0]['price'])
            return latest_close_price
    except Exception as e:
        print(f"Failed to get stock price for {stock_name} due to {type(e).__name__}: {e}")
        return None


with open('C:/Users/alont/OneDrive/פרויקט עם אלון/keys.txt', 'r') as file:
    lines = file.readlines()
    db_details = {line.split('=')[0]: line.split('=')[1].strip() for line in lines}

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
    print(f"Updating stock price for {stock_name}")
    stock_price = get_stock_price(stock_name)
    
    if stock_price is not None:
        current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        alter_table_query = f"ALTER TABLE stockp ADD [{current_time}] FLOAT"
        cursor.execute(alter_table_query)
        
        update_query = f"UPDATE stockp SET [{current_time}] = ? WHERE names = ?"
        cursor.execute(update_query, (stock_price, stock_name))
        
        conn.commit()
        print(f"Database has been updated with the latest price for {stock_name}: {stock_price}")


def check_and_update_stock_prices():
    print("Checking and updating stock prices")
    cursor.execute("SELECT names FROM stockp")
    stock_names = [row.names for row in cursor.fetchall()]
    
    for stock_name in stock_names:
        try:
            stock_name = stock_name.strip() + ".TA"
            update_stock_price(stock_name)
        except Exception as e:
            print(f"Failed to update stock price for {stock_name}: {e}")

print("Script has started. Waiting for the first scheduled run...")
schedule.every(10).seconds.do(check_and_update_stock_prices)  # For testing purpose

try:
    while True:
        schedule.run_pending()
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopping the scheduler...")
finally:
    cursor.close()
    conn.close()
