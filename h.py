import pyodbc
import requests
import csv
from collections import defaultdict
from datetime import datetime

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

# שליפת שמות המניות ממסד הנתונים
cursor.execute("SELECT names FROM stockp")
stocks = [f"{row.names}.TA" for row in cursor.fetchall()]

# הגדרת נתוני ה-API
api_endpoint = "https://financialmodelingprep.com/api/v3/historical-price-full"
api_key = "728c1b4c2d8218add598fcfc7401f70b"

# הגדרת מילון לשמירת הנתונים
closing_prices = []

# קבלת הנתונים ההיסטוריים מה-API
for stock in stocks:
    url = f"{api_endpoint}/{stock}?apikey={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if 'historical' in data:
            for entry in data['historical']:
                date = datetime.strptime(entry['date'], '%Y-%m-%d').date()
                closing_price = entry['close']
                closing_prices.append((stock, date, closing_price))
    else:
        print(f"Failed to fetch data for {stock}")

# יצירת טבלה חדשה במסד הנתונים
table_name = "HistoricalStockPrices"
create_table_query = f"""
CREATE TABLE {table_name} (
    Symbol NVARCHAR(50),
    Date DATE,
    ClosePrice FLOAT
)
"""
cursor.execute(create_table_query)
conn.commit()

# הוספת הנתונים לטבלה
for stock, date, closing_price in closing_prices:
    insert_query = f"""
    INSERT INTO {table_name} (Symbol, Date, ClosePrice)
    VALUES ('{stock}', '{date}', {closing_price})
    """
    cursor.execute(insert_query)
conn.commit()

# סגירת החיבור למסד הנתונים
conn.close()
