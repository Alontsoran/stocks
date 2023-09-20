import pandas as pd
import pyodbc

#פונקצת הבאת המפתחות להתחברות לשרת, כרגע זה דרך SSMS בהמשך נצתרך את זה דרך AWS ישירות
def get_connection_string_from_file(filename):
    # קריאה מהקובץ
    with open(filename, 'r') as file:
        credentials = {}
        lines = file.readlines()
        for line in lines:
            key, value = line.strip().split('=')
            credentials[key] = value
    
    # יצירת מחרוזת ההתחברות
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={credentials['server']};"
        f"DATABASE={credentials['database']};"
        f"UID={credentials['username']};"
        f"PWD={credentials['password']};"
    )
    
    return conn_str


## הבאת הנתונים
def fetch_data_for_stock(connection_string, stock_name):
    """
    פונקציה המקבלת מחרוזת התחברות ומספר מניה,
    מבצעת שאילתה לבסיס הנתונים לפי מספר המניה ומחזירה את התוצאות.
    """
    # יצירת השאילתה
    query = f"SELECT revenues, profits, Report_Type FROM [dbo].[122] WHERE Unnamed_23 = N'{stock_name}'"

    
    # יצירת החיבור לבסיס הנתונים
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    # ביצוע השאילתה
    cursor.execute(query)
    rows = cursor.fetchall()

    # סגירת החיבור
    cursor.close()
    conn.close()
    
    return rows





def evaluate_stock_recommendation(stock_data):
    # Check if the data is complete for both revenues and profits
    if len(stock_data) != 2:
        return "DATA INCOMPLETE"
    
    # Extracting data for the specific stock
    revenues = stock_data[stock_data["×§×˜×’×•×¨×™×”"] == "הכנסות"]
    profits = stock_data[stock_data["×§×˜×’×•×¨×™×”"] == "רווח  נקי"]
    
    # Assign values based on the provided code
    Q_N_sales = revenues.iloc[0, 1]
    Q_N_profit = profits.iloc[0, 1]
    Q_minus1_sales = revenues.iloc[0, 2]
    Q_minus1_profit = profits.iloc[0, 2]
    Q_minus2_sales = revenues.iloc[0, 3]
    Q_minus2_profit = profits.iloc[0, 3]
    Q_minus3_sales = revenues.iloc[0, 4]
    Q_minus3_profit = profits.iloc[0, 4]
    Q_minus4_sales = revenues.iloc[0, 5]
    Q_minus4_profit = profits.iloc[0, 5]
    
    # Provided decision parameters
    B_required_sales_versus_corresponding_quarter = 1.2
    B_required_sales_versus_average = 1.2
    B_required_profit_versus_corresponding_quarter = 1.2
    B_required_profit_versus_average = 1.2
    B_min_profit = 0.03

    S_required_sales_versus_corresponding_quarter = 0.8
    S_required_sales_versus_average = 0.8
    S_required_profit_versus_corresponding_quarter = 0.7
    S_required_profit_versus_average = 0.7
    S_min_profit = 0.03

    # Calculate the average sales and profit from the previous year
    average_sales_previous_year = (Q_minus1_sales + Q_minus2_sales + Q_minus3_sales + Q_minus4_sales) / 4
    average_profit_previous_year = (Q_minus1_profit + Q_minus2_profit + Q_minus3_profit + Q_minus4_profit) / 4

    # Adjust the buy conditions based on the formula provided
    buy_condition1 = Q_N_sales > B_required_sales_versus_corresponding_quarter * Q_minus4_sales
    buy_condition2 = Q_N_sales > B_required_sales_versus_average * average_sales_previous_year
    buy_condition3 = Q_N_profit > B_required_profit_versus_corresponding_quarter * Q_minus4_profit
    buy_condition4 = Q_N_profit > B_required_profit_versus_average * average_profit_previous_year
    buy_condition5 = Q_N_profit > B_min_profit

    # Calculate conditions for selling
    sell_condition1 = Q_N_sales < S_required_sales_versus_corresponding_quarter * Q_minus4_sales
    sell_condition2 = Q_N_sales < S_required_sales_versus_average * average_sales_previous_year
    sell_condition3 = Q_N_profit < S_required_profit_versus_corresponding_quarter * Q_minus4_profit
    sell_condition4 = Q_N_profit < S_required_profit_versus_average * average_profit_previous_year
    sell_condition5 = Q_N_profit < S_min_profit

    # Determine recommendation
    if buy_condition1 and buy_condition2 and buy_condition3 and buy_condition4 and buy_condition5:
        recommendation = "BUY"
    elif sell_condition1 and sell_condition2 and sell_condition3 and sell_condition4 and sell_condition5:
        recommendation = "SELL"
    else:
        recommendation = "NEUTRAL"
    
    return recommendation

### שימוש בפונקציה של המפתח, לכאן צריך להכניס את הנתיב של קובץ ההתחברות
connection_string = get_connection_string_from_file('C:/Users/zvi25/Desktop/StartUp/פרויקט עם אלון/Milestones/2/2.1/keys.txt')

# שימוש בפונקציה ליבוא הנתונים
stock_number = '''מנועי בית שמש אחזקות (1997) בע"מ'''  # לדוגמה
results = fetch_data_for_stock(connection_string, stock_number)

for row in results:
    print(row)
    print('\n')

#הפעלת התחזית
#print(evaluate_stock_recommendation(stock_data))
print('***The job is done***')
