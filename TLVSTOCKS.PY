import pyodbc
import requests
from datetime import datetime
import time

def update_stock_data():
    with open('C:/Users/Administrator/Downloads/keys1.txt', 'r') as file:
        lines = file.readlines()
        db_details = {line.split('=')[0]: line.split('=')[1].strip() for line in lines}
    
    sleep_time = 5  
    
    while True:  
        try:
            conn_str = (
                f"Driver={{ODBC Driver 17 for SQL Server}};"
                f"Server={db_details['server']};"
                f"Database={db_details['database']};"
                f"UID={db_details['username']};"
                f"PWD={db_details['password']};"
            )
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            cursor.execute("SELECT Symbol FROM f")
            stocks = [row.Symbol for row in cursor.fetchall()]
            
            api_endpoint = "https://financialmodelingprep.com/api/v3/historical-price-full"
            api_key = "728c1b4c2d8218add598fcfc7401f70b"

            
            table_name = "HistoricalStockPrices1tlv"
            
            for stock in stocks:
                stock_ta = f"{stock}.TA"
                url = f"{api_endpoint}/{stock_ta}?apikey={api_key}"
                response = requests.get(url)
                
                if response.status_code == 200:
                    data = response.json()
                    if 'historical' in data:
                        for entry in data['historical']:
                            date = datetime.strptime(entry['date'], '%Y-%m-%d').date()
                            closing_price = entry['close']
                            cursor.execute(f"SELECT * FROM {table_name} WHERE Symbol = ? AND Date = ?", (stock, date))
                            existing_row = cursor.fetchone()
                            if existing_row:
                                cursor.execute(f"UPDATE {table_name} SET ClosePrice = ? WHERE Symbol = ? AND Date = ?", (closing_price, stock, date))
                                print(f"Updated {stock} data for {date} with closing price {closing_price}")
                            else:
                                cursor.execute(f"INSERT INTO {table_name} (Symbol, Date, ClosePrice) VALUES (?, ?, ?)", (stock, date, closing_price))
                                print(f"Inserted {stock} data for {date} with closing price {closing_price}")
                            conn.commit()
                else:
                    print(f"Failed to fetch data for {stock_ta} - Response Code: {response.status_code}")
            
            conn.close()
            break
        except pyodbc.OperationalError as e:
            print(f"Failed to connect due to OperationalError, retrying in {sleep_time} seconds. Error: {e}")
            time.sleep(sleep_time)
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch data due to RequestException, retrying in {sleep_time} seconds. Error: {e}")
            time.sleep(sleep_time)
        except Exception as e:
            print(f"An unexpected error occurred: {e}, retrying in {sleep_time} seconds.")
            time.sleep(sleep_time)

if __name__ == "__main__":
    update_stock_data()

