import yfinance as yf
from googlefinance import getQuotes
import pyodbc
import schedule
import time
from datetime import datetime

def get_stock_price(stock_name):
    # Try to fetch stock price using your existing methods
    formats = [stock_name, f"{stock_name}.TA", f"{stock_name}:TLV", f"TLV:{stock_name}"]
    
    for stock_format in formats:
        try:
            stock = yf.Ticker(stock_format)
            # Get the latest closing price
            history = stock.history(period='1d')
            if not history.empty:  # Check if the history DataFrame is not empty
                latest_close_price = history['Close'].iloc[-1]
                return float(latest_close_price)
        except Exception as e:
            print(f"Failed to get stock price for {stock_format}: {e}")
    
    # If your existing methods fail, try to fetch using googlefinance as a fallback
    try:
        stock_quote = getQuotes(stock_name)
        if stock_quote:
            # Extract the 'LastTradePrice' from the quote
            last_trade_price = float(stock_quote[0]['LastTradePrice'])
            return last_trade_price
    except Exception as e:
        print(f"Failed to get stock price for {stock_name} with Google Finance: {e}")
    
    print(f"Failed to get stock price for {stock_name} with any format")
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
    stock_price = get_stock_price(stock_name)
    
    if stock_price is not None:
        current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        alter_table_query = f"ALTER TABLE stockp ADD [{current_time}] FLOAT"
        cursor.execute(alter_table_query)
        
        update_query = f"UPDATE stockp SET [{current_time}] = ? WHERE names = ?"
        cursor.execute(update_query, (stock_price, stock_name))
        
        conn.commit()

def check_and_update_stock_prices():
    cursor.execute("SELECT names FROM stockp")
    stock_names = [row.names for row in cursor.fetchall()]
    
    for stock_name in stock_names:
        try:
            update_stock_price(stock_name)
        except Exception as e:
            print(f"Failed to update stock price for {stock_name}: {e}")

# Schedule the function to run every 5 minutes
schedule.every(5).minutes.do(check_and_update_stock_prices)

try:
    while True:
        schedule.run_pending()
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopping the scheduler...")
finally:
    cursor.close()
    conn.close()
