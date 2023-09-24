

import pyodbc
from urllib.request import urlopen
import certifi
import json
from datetime import datetime, timedelta


def get_db_connection(db_details):
    conn_str = (
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"Server={db_details['server']};"
        f"Database={db_details['database']};"
        f"UID={db_details['username']};"
        f"PWD={db_details['password']};"
    )
    return pyodbc.connect(conn_str)


def get_stock_symbols(cursor):
    cursor.execute("SELECT names FROM stockp")
    return [row.names for row in cursor.fetchall()]


def check_and_create_column(cursor, column_name):
    cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'stockpyear' AND COLUMN_NAME = '{column_name}'")
    if not cursor.fetchone():
        cursor.execute(f"ALTER TABLE stockpyear ADD [{column_name}] FLOAT")
        print(f"Added column {column_name}")


def update_price(cursor, symbol, column_name, price):
    cursor.execute(f"UPDATE stockpyear SET [{column_name}] = {price} WHERE Symbol = '{symbol}'")
    if cursor.rowcount == 0:  # If no row exists for this symbol, insert a new one
        cursor.execute(f"INSERT INTO stockpyear (Symbol, [{column_name}]) VALUES ('{symbol}', {price})")
    print(f"Updated {symbol} {column_name} with {price}")


def get_jsonparsed_data(url):
    response = urlopen(url, cafile=certifi.where())
    data = response.read().decode("utf-8")
    return json.loads(data)


def process_stocks(cursor, stock_symbols, start_date=datetime(2018, 8, 21)):
    print("Processing stocks...")
    current_date = start_date
    
    while current_date <= datetime.now():
        column_name = current_date.strftime('%Y_%m_%d')
        check_and_create_column(cursor, column_name)
        
        for symbol in stock_symbols:
            symbol_with_suffix = f"{symbol.strip()}.TA"
            url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol_with_suffix}?apikey=728c1b4c2d8218add598fcfc7401f70b"
            try:
                data = get_jsonparsed_data(url)
                price_date_str = current_date.strftime('%Y-%m-%d')
                price = next((item.get('close') for item in data.get('historical', []) if item['date'] == price_date_str), None)
                if price is not None:
                    update_price(cursor, symbol, column_name, price)
                else:
                    print(f"No data found for {symbol} on {price_date_str}")
            except Exception as e:
                print(f"Error processing {symbol} on {price_date_str}: {e}")
        
        conn.commit()  # Commit the changes to the database after processing each date
        current_date += timedelta(days=1)


def main():
    with open('C:/Users/Administrator/Downloads/keys.txt', 'r') as file:
        lines = file.readlines()
        db_details = {line.split('=')[0]: line.split('=')[1].strip() for line in lines}
    
    global conn
    conn = get_db_connection(db_details)
    cursor = conn.cursor()
    stock_symbols = get_stock_symbols(cursor)
    
    print("Processing stocks starting from 2018-08-21...")
    process_stocks(cursor, stock_symbols)
    
    conn.close()


if __name__ == "__main__":
    main()
