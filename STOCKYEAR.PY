import pyodbc
import requests
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
    print(f"Updated {symbol} {column_name} with {price}")


def process_dates(cursor, stock_symbols, start_date_str='2018-03-12', end_date_str='2023-09-23'):
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    date = start_date
    
    while date <= end_date:
        column_name = date.strftime('%Y_%m_%d')
        check_and_create_column(cursor, column_name)
        for symbol in stock_symbols:
            symbol_with_suffix = f"{symbol}.TA"
            url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol_with_suffix}?from={start_date_str}&to={end_date_str}&apikey=728c1b4c2d8218add598fcfc7401f70b"
            response = requests.get(url)
            if response.status_code != 200:
                print(f"Failed to get data for {symbol} on {date}, status code: {response.status_code}")
                continue
            data = response.json()
            historical_data = data.get('historical', [])
            if not historical_data:
                print(f"No historical data found for {symbol} on {date}")
                continue
            price_date_str = date.strftime('%Y-%m-%d')
            price = next((item.get('close') for item in historical_data if item['date'] == price_date_str), None)
            if price is not None:
                update_price(cursor, symbol, column_name, price)
        date += timedelta(days=1)


def main():
    with open('C:/Users/Administrator/Downloads/keys.txt', 'r') as file:
        lines = file.readlines()
        db_details = {line.split('=')[0]: line.split('=')[1].strip() for line in lines}
    
    conn = get_db_connection(db_details)
    cursor = conn.cursor()
    stock_symbols = get_stock_symbols(cursor)
    
    print("Processing dates between 2018-03-12 and 2023-09-23...")
    process_dates(cursor, stock_symbols)
    
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
