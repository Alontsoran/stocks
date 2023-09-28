import pyodbc
import requests
from datetime import datetime, timedelta
import time

def update_minute_data(cursor, conn, stocks, table_name, api_endpoint, api_key):
    for stock in stocks:
        stock_ta = f"{stock}.TA"
        url = f"{api_endpoint}/{stock_ta}?apikey={api_key}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            for entry in data:
                datetime_str = entry['date']
                datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                closing_price = entry['close']
                
                cursor.execute(f"SELECT * FROM {table_name} WHERE Symbol = ? AND DateTime = ?", (stock, datetime_obj))
                existing_row = cursor.fetchone()
                
                if existing_row:
                    cursor.execute(f"UPDATE {table_name} SET ClosePrice = ? WHERE Symbol = ? AND DateTime = ?", (closing_price, stock, datetime_obj))
                else:
                    cursor.execute(f"INSERT INTO {table_name} (Symbol, DateTime, ClosePrice) VALUES (?, ?, ?)", (stock, datetime_obj, closing_price))
                
                conn.commit()
        else:
            print(f"Failed to fetch data for {stock_ta}")

def cleanup_old_data(cursor, conn, table_name):
    today = datetime.now().date()
    # Calculate the date one week ago
    one_week_ago = today - timedelta(days=7)
    
    # Delete records older than one week ago
    cursor.execute(f"DELETE FROM {table_name} WHERE DateTime < ?", (one_week_ago,))
    conn.commit()

def main():
    with open('C:/Users/Administrator/Downloads/keys.txt', 'r') as file:
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
    
    cursor.execute("SELECT Symbol FROM STOCKS")
    stocks = [row.Symbol for row in cursor.fetchall()]
    
    api_endpoint = "https://financialmodelingprep.com/api/v3/historical-chart/1min"
    api_key = "728c1b4c2d8218add598fcfc7401f70b"
    
    table_name = "MinuteStockPrices"
    cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{table_name}'")
    if not cursor.fetchone():
        create_table_query = f"""
        CREATE TABLE {table_name} (
            Symbol NVARCHAR(50),
            DateTime DATETIME,
            ClosePrice FLOAT
        )
        """
        cursor.execute(create_table_query)
        conn.commit()
    
    last_reset_time = datetime.now().date()
    
    while True:
        current_time = datetime.now()
        if current_time.date() > last_reset_time:
            cleanup_old_data(cursor, conn, table_name)
            last_reset_time = current_time.date()
        
        update_minute_data(cursor, conn, stocks, table_name, api_endpoint, api_key)
        
        time.sleep(60)
    
    conn.close()

if __name__ == "__main__":
    main()
