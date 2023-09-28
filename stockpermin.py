import pyodbc
import requests
from datetime import datetime, timedelta
import time

def update_minute_data(cursor, conn, stocks, table_name, api_endpoint, api_key):
    for stock in stocks:
        stock_ta = f"{stock}.TA"  # הוספת הסיומת לשליפת הנתונים מהאתר
        url = f"{api_endpoint}/{stock_ta}?apikey={api_key}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            for entry in data:
                datetime_str = entry['date']
                datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                closing_price = entry['close']
                
                # בדיקה אם הנתון כבר קיים בטבלה
                cursor.execute(f"SELECT * FROM {table_name} WHERE Symbol = ? AND DateTime = ?", (stock, datetime_obj))
                existing_row = cursor.fetchone()
                
                if existing_row:
                    # אם הנתון כבר קיים - עדכון
                    cursor.execute(f"UPDATE {table_name} SET ClosePrice = ? WHERE Symbol = ? AND DateTime = ?", (closing_price, stock, datetime_obj))
                else:
                    # אם הנתון לא קיים - הוספה
                    cursor.execute(f"INSERT INTO {table_name} (Symbol, DateTime, ClosePrice) VALUES (?, ?, ?)", (stock, datetime_obj, closing_price))
                
                conn.commit()
        else:
            print(f"Failed to fetch data for {stock_ta}")

def main():
    # קריאת פרטי החיבור למסד הנתונים
    with open('C:/Users/Administrator/Downloads/keys.txt', 'r') as file:
        lines = file.readlines()
        db_details = {line.split('=')[0]: line.split('=')[1].strip() for line in lines}
    
    # חיבור למסד הנתונים
    conn_str = (
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"Server={db_details['server']};"
        f"Database={db_details['database']};"
        f"UID={db_details['username']};"
        f"PWD={db_details['password']};"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # שליפת שמות המניות ממסד הנתונים מהטבלה STOCKS
    cursor.execute("SELECT Symbol FROM STOCKS")
    stocks = [row.Symbol for row in cursor.fetchall()]
    
    # הגדרת נתוני ה-API
    api_endpoint = "https://financialmodelingprep.com/api/v3/historical-chart/1min"
    api_key = "728c1b4c2d8218add598fcfc7401f70b"
    
    # בדיקה אם הטבלה כבר קיימת
    table_name = "MinuteStockPrices"
    cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{table_name}'")
    if not cursor.fetchone():
        # אם הטבלה איננה קיימת, יוצר אותה
        create_table_query = f"""
        CREATE TABLE {table_name} (
            Symbol NVARCHAR(50),
            DateTime DATETIME,
            ClosePrice FLOAT
        )
        """
        cursor.execute(create_table_query)
        conn.commit()
    
    last_reset_time = datetime.now()
    
    while True:
        current_time = datetime.now()
        if (current_time - last_reset_time).days >= 7:
            cursor.execute(f"DELETE FROM {table_name}")
            conn.commit()
            last_reset_time = current_time
        
        update_minute_data(cursor, conn, stocks, table_name, api_endpoint, api_key)
        
        time.sleep(60)  # השהייה לפני הרצה הבאה
    
    conn.close()

if __name__ == "__main__":
    main()
