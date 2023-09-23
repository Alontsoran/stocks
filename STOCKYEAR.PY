import pyodbc
import requests
from datetime import datetime, timedelta

# התחברות למסד הנתונים
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

# תאריכים
start_date = '2018-03-12'
end_date = datetime.now().strftime('%Y-%m-%d')

# שליפת שמות המניות מהטבלה 'stockp'
cursor.execute("SELECT names FROM stockp")
stock_symbols = [row.names for row in cursor.fetchall()]

for symbol in stock_symbols:
    print(f"Processing {symbol}...")
    symbol_with_suffix = f"{symbol}.TA"  # הוספת הסיומת
    # שליפת מידע מה-API
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol_with_suffix}?from={start_date}&to={end_date}&apikey=728c1b4c2d8218add598fcfc7401f70b"
    response = requests.get(url)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")
    if response.status_code == 200:
        data = response.json()
        historical_data = data.get('historical', [])
        if not historical_data:
            print(f"No historical data found for {symbol}")
            continue
        for price_info in historical_data:
            date = price_info.get('date')
            price = price_info.get('close')  # משיכת מחיר סגירה
            if date and price is not None:  # אם יש תאריך ומחיר
                column_name = datetime.strptime(date, "%Y-%m-%d").strftime('%Y_%m_%d')
                print(f"Date: {date}, Column: {column_name}, Close Price: {price}")
                
                # בדיקה האם העמודה עבור התאריך הנוכחי כבר קיימת
                cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'stockpyear' AND COLUMN_NAME = '{column_name}'")
                if not cursor.fetchone():  # אם העמודה איננה קיימת, צור אותה
                    cursor.execute(f"ALTER TABLE stockpyear ADD {column_name} FLOAT")
                    print(f"Added column {column_name}")
                
                # עדכון המחיר בעמודה המתאימה
                cursor.execute(f"UPDATE stockpyear SET {column_name} = {price} WHERE Symbol = '{symbol}'")
                print(f"Updated {symbol} {column_name} with {price}")
    else:
        print(f"Failed to get data for {symbol}, status code: {response.status_code}")

# שמירת השינויים במסד הנתונים
conn.commit()

# סגירת החיבור למסד הנתונים
conn.close()
