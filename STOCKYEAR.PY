import pyodbc
import requests
from datetime import datetime

def update_stock_data():
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
    
    # שליפת שמות המניות ממסד הנתונים מהטבלה החדשה
    cursor.execute("SELECT Symbol FROM STOCKS")
    stocks = [row.Symbol for row in cursor.fetchall()]
    
    # הגדרת נתוני ה-API
    api_endpoint = "https://financialmodelingprep.com/api/v3/historical-price-full"
    api_key = "728c1b4c2d8218add598fcfc7401f70b"
    
    # טבלת הנתונים
    table_name = "HistoricalStockPrices1"
    
    # קבלת הנתונים ההיסטוריים מה-API
    for stock in stocks:
        stock_ta = f"{stock}.TA"  # הוספת הסיומת לשליפת הנתונים מהאתר
        url = f"{api_endpoint}/{stock_ta}?apikey={api_key}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            if 'historical' in data:
                for entry in data['historical']:
                    date = datetime.strptime(entry['date'], '%Y-%m-%d').date()
                    closing_price = entry['close']
                    
                    # בדיקה אם הנתון כבר קיים בטבלה
                    cursor.execute(f"SELECT * FROM {table_name} WHERE Symbol = ? AND Date = ?", (stock, date))
                    existing_row = cursor.fetchone()
                    
                    if existing_row:
                        # אם הנתון כבר קיים - עדכון
                        cursor.execute(f"UPDATE {table_name} SET ClosePrice = ? WHERE Symbol = ? AND Date = ?", (closing_price, stock, date))
                        print(f"Updated {stock} data for {date} with closing price {closing_price}")
                    else:
                        # אם הנתון לא קיים - הוספה
                        cursor.execute(f"INSERT INTO {table_name} (Symbol, Date, ClosePrice) VALUES (?, ?, ?)", (stock, date, closing_price))
                        print(f"Inserted {stock} data for {date} with closing price {closing_price}")
                    
                    conn.commit()
        else:
            print(f"Failed to fetch data for {stock_ta}")
    
    # סגירת החיבור למסד הנתונים
    conn.close()

if __name__ == "__main__":
    update_stock_data()
