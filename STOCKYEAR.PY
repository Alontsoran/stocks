
import pyodbc
import requests
import time
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


def process_date(date, cursor, stock_symbols):
    column_name = date.strftime('%Y_%m_%d')
    check_and_create_column(cursor, column_name)
    for symbol in stock_symbols:
        symbol_with_suffix = f"{symbol}.TA"
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol_with_suffix}?from={date.strftime('%Y-%m-%d')}&to={date.strftime('%Y-%m-%d')}&apikey=728c1b4c2d8218add598fcfc7401f70b"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to get data for {symbol} on {date}, status code: {response.status_code}")
            continue
        data = response.json()
        historical_data = data.get('historical', [])
        if not historical_data:
            print(f"No historical data found for {symbol} on {date}")
            continue
        price = historical_data[0].get('close')
        if price is not None:
            update_price(cursor, symbol, column_name, price)
            

def check_and_create_column(cursor, column_name):
    cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'stockpyear' AND COLUMN_NAME = '{column_name}'")
    if not cursor.fetchone():
        cursor.execute(f"ALTER TABLE stockpyear ADD [{column_name}] FLOAT")
        print(f"Added column {column_name}")


def update_price(cursor, symbol, column_name, price):
    cursor.execute(f"UPDATE stockpyear SET [{column_name}] = {price} WHERE Symbol = '{symbol}'")
    print(f"Updated {symbol} {column_name} with {price}")


def main():
    with open('C:/Users/Administrator/Downloads/keys.txt', 'r') as file:
        lines = file.readlines()
        db_details = {line.split('=')[0]: line.split('=')[1].strip() for line in lines}

    conn = get_db_connection(db_details)
    cursor = conn.cursor()
    
    start_date = datetime.strptime('2018-03-12', '%Y-%m-%d')
    end_date = datetime.now()
    stock_symbols = get_stock_symbols(cursor)

    date = start_date
    while date <= end_date:
        print(f"Processing {date}...")
        process_date(date, cursor, stock_symbols)
        date += timedelta(days=1)
        time.sleep(2)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
