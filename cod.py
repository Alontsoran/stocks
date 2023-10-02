import pyodbc
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import create_engine, text

# פונקציה לשליפת רשימת המניות ממסד הנתונים
def get_stock_symbols_from_database(connection_string):
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        query = text("SELECT DISTINCT [Symbol] FROM [Stocks].[dbo].[TLV]")
        result = connection.execute(query)
        stock_symbols = [row[0] for row in result.fetchall()]
    return stock_symbols


# פונקציה ליצירת מאגר נתונים
def Create_DB():
    pyodbc_conn_str, sqlalchemy_conn_str = get_connection_string_from_file('C:/Users/Administrator/Downloads/keys.txt')
    table_name = 'BasicAlgorithm_Results'
    stock_symbols = get_stock_symbols_from_database(sqlalchemy_conn_str)

    for symbol in stock_symbols:
        recommendations_df = generate_recommendations_for_quarters(pyodbc_conn_str, symbol)
        add_dataframe_to_sql(recommendations_df, table_name, sqlalchemy_conn_str)

    print("The job is done")


# פונקציה לשליפת מחרוזת התחברות מקובץ
def get_connection_string_from_file(filename):
    with open(filename, 'r') as file:
        credentials = {}
        lines = file.readlines()
        for line in lines:
            key, value = line.strip().split('=')
            credentials[key] = value
    
    # Connection strings
    pyodbc_conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={credentials['server']};"
        f"DATABASE={credentials['database']};"
        f"UID={credentials['username']};"
        f"PWD={credentials['password']};"
    )
    
    sqlalchemy_conn_str = (
        f"mssql+pyodbc://{credentials['username']}:{credentials['password']}@"
        f"{credentials['server']}/{credentials['database']}?driver=ODBC+Driver+17+for+SQL+Server"
    )
    
    return pyodbc_conn_str, sqlalchemy_conn_str


# פונקציה לשליפת נתונים ממסד הנתונים
def get_company_data(connection_string, stock_symbol):
    # יצירת השאילתה
    query = f"""
        SELECT [symbol], [reportedCurrency], [calendarYear], [period], [revenue], [netIncome] FROM [Stocks].[dbo].[TLV] WHERE [symbol] = '{stock_symbol}'
    """
    
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

# פונקציה לאותר המלצה למניה
def evaluate_stock_recommendation(revenues, profits,
                                  B_required_sales_versus_corresponding_quarter=1.2,
                                  B_required_sales_versus_average=1.2,
                                  B_required_profit_versus_corresponding_quarter=1.2,
                                  B_required_profit_versus_average=1.2,
                                  B_min_profit=0.03,
                                  S_required_sales_versus_corresponding_quarter=0.8,
                                  S_required_sales_versus_average=0.8,
                                  S_required_profit_versus_corresponding_quarter=0.7,
                                  S_required_profit_versus_average=0.7,
                                  S_min_profit=0.03):
  
    # Assign values based on the provided code
    Q_N_sales = revenues[4]
    Q_N_profit = profits[4]
    Q_minus1_sales = revenues[3]
    Q_minus1_profit = profits[3]
    Q_minus2_sales = revenues[2]
    Q_minus2_profit = profits[2]
    Q_minus3_sales = revenues[1]
    Q_minus3_profit = profits[1]
    Q_minus4_sales = revenues[0]
    Q_minus4_profit = profits[0]

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

# פונקציה ליצירת המלצות רבעוניות לכל מניה
def generate_recommendations_for_quarters(connection_string, stock_symbol, Update=False):
    # שליפת הנתונים מהפונקציה הראשונה
    data = get_company_data(connection_string, stock_symbol)
    
    # הפיכת הנתונים לרשימה של רשימות
    data_list = [list(item) for item in data]
    
    # יצירת ה-DataFrame
    df = pd.DataFrame(data_list, columns=['symbol', 'reportedCurrency', 'calendarYear', 'period', 'revenue', 'netIncome'])
    df = df.sort_values(by='calendarYear')

    # יצירת עידכון בלבד
    if Update:
        df = df.tail(5)  # שמירה רק של החמישה האחרונים

    # יצירת רשימה לשמירת התוצאות
    recommendations = []
    
    # לולאה למעבר על הרבעונים
    for i in range(len(df) - 4):
        # חילוק הנתונים לחמישה רבעונים
        sub_df = df.iloc[i:i+5]
        
        # בדיקה שיש לנו חמישה רבעונים רצופים
        if sub_df.iloc[4]['calendarYear'] - sub_df.iloc[0]['calendarYear'] <= 1:
            # חישוב ההמלצה
            recommendation = evaluate_stock_recommendation(list(sub_df['revenue']), list(sub_df['netIncome']))
            
            # הוספת ההמלצה לרשימה
            recommendations.append({
                'shelet_symbol': stock_symbol,
                'reportedCurrency': sub_df.iloc[4]['reportedCurrency'],
                'year': sub_df.iloc[4]['calendarYear'] + (1 if sub_df.iloc[4]['period'] == 'Q4' else 0),
                'period': 'Q1' if sub_df.iloc[4]['period'] == 'Q4' else f"Q{int(sub_df.iloc[4]['period'][1]) + 1}",
                'recommendation': recommendation
            })
    
    # המרה של הרשימה לDataFrame
    recommendations_df = pd.DataFrame(recommendations)
    
    return recommendations_df

# פונקציה להוספת DataFrame למסד הנתונים
def add_dataframe_to_sql(df, table_name, connection_string):
    engine = create_engine(connection_string)
    df.to_sql(table_name, engine, if_exists='append', index=False)

# הרצת הקוד המרכזי ליצירת מאגר
Create_DB()
