   
import pyodbc
import schedule
import time
from datetime import datetime
import certifi
import json
import ssl
import concurrent.futures

try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

def get_stock_price(stock_name):
    print(f"Getting stock price for {stock_name}")
    url = f"https://financialmodelingprep.com/api/v3/quote-short/{stock_name}?apikey=728c1b4c2d8218add598fcfc7401f70b"
    
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

with open('YOUR_PATH_TO_KEYS/keys.txt', 'r') as file:
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

def update_stock_prices():
    global cursor, conn
    print("Updating stock prices")
    current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    column_name = f"price_{current_time}"
    
    alter_table_query = f"ALTER TABLE stockp ADD [{column_name}] FLOAT"
    print(f"Executing alter_table_query at {current_time}")
    cursor.execute(alter_table_query)
    
    cursor.execute("SELECT names FROM stockp")
    stock_names = [row.names for row in cursor.fetchall()]
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for stock_name in stock_names:
            stock_name_with_suffix = stock_name.strip() + ".TA"
            futures.append(executor.submit(get_stock_price, stock_name_with_suffix))
        
        for future, stock_name in zip(concurrent.futures.as_completed(futures), stock_names):
            try:
                stock_price = future.result()
                print(f"Stock price for {stock_name} is {stock_price}")
                if stock_price is not None:
                    update_query = f"UPDATE stockp SET [{column_name}] = ? WHERE names = ?"
                    print(f"Executing update_query for {stock_name} at {current_time}")
                    cursor.execute(update_query, (stock_price, stock_name))
                    conn.commit()
                    print(f"Database has been updated with the latest price for {stock_name}: {stock_price}")
            except Exception as e:
                print(f"Failed to update stock price for {stock_name}: {e}")

print("Script has started. Waiting for the first scheduled run...")
schedule.every(1).minutes.do(update_stock_prices)

try:
    while True:
        schedule.run_pending()
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopping the scheduler...")
finally:
    cursor.close()
    conn.close()
