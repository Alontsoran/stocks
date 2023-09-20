import pyodbc
import re

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
def fetch_data_for_stock(connection_string, stock_number):
    """
    פונקציה המקבלת מחרוזת התחברות ומספר מניה,
    מבצעת שאילתה לבסיס הנתונים לפי מספר המניה ומחזירה את התוצאות.
    """
    # יצירת השאילתה
    query = f"SELECT revenues, profits, Report_Type FROM [dbo].[122] WHERE [KEY] = '{stock_number}'"
    
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

#סידור לפי שנים ורבעונים
def split_year_quarter_from_results(results):
    # יצירת רשימה חדשה לתוצאות המעודכנות
    updated_results = []
    
    # הולכים על כל שורה בתוצאות
    for row in results:
        # משתמשים בביטויים רגולריים לחלוקה לרבעון ושנה
        match = re.match(r"(רבעון (\d)|שנתי) (\d{4})", row[2])
        
        if match:
            if match.group(1) == "שנתי":
                quarter_eng = "Q4"
            else:
                quarter_eng = f"Q{match.group(2)}"
            year = int(match.group(3))
            
            new_row = (row[0], row[1], quarter_eng, year)
            updated_results.append(new_row)
        else:
            # אם הפורמט אינו תקפי, נשאיר את השורה כפי שהיא
            updated_results.append(row)
    
    return updated_results


def extract_revenues_profits_by_year(data, year):
    # רשימות להכנסות ולרווחים
    # איתחול הרשימות
    revenues = []
    profits = []

    # הבאת הנתונים הרלוונטיים
    relevant_data = [row for row in data if row[3] == year or (row[3] == year - 1 and row[2] == 'Q4')]

    # מיון הנתונים לפי הרבעון והשנה
    sorted_data = sorted(relevant_data, key=lambda x: (x[3], x[2]))

    # הוספת הנתונים לרשימות המתאימות
    for row in sorted_data:
        if row[2] == 'Q1' or row[2] == 'Q2' or row[2] == 'Q3' or row[2] == 'Q4':
            revenues.append([row[0], row[3], row[2]])
            profits.append([row[1], row[3], row[2]])

    return revenues, profits

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
    Q_N_sales = revenues[4][0]
    Q_N_profit = profits[4][0]
    Q_minus1_sales = revenues[3][0]
    Q_minus1_profit = profits[3][0]
    Q_minus2_sales = revenues[2][0]
    Q_minus2_profit = profits[2][0]
    Q_minus3_sales = revenues[1][0]
    Q_minus3_profit = profits[1][0]
    Q_minus4_sales = revenues[0][0]
    Q_minus4_profit = profits[0][0]


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


def Create_recommendation(stock_number,year=2022):
    ### שימוש בפונקציה של המפתח, לכאן צריך להכניס את הנתיב של קובץ ההתחברות
    #מפתח בררת מחדל
    connection_string = get_connection_string_from_file('C:/Users/zvi25/Desktop/StartUp/פרויקט עם אלון/Milestones/2/2.1/keys.txt')

    # שימוש בפונקציה ליבוא הנתונים
    results = fetch_data_for_stock(connection_string, stock_number)

    #פיצול המידע
    updated_results = split_year_quarter_from_results(results)

    # יצירת רשימות
    extracted_revenues, extracted_profits = extract_revenues_profits_by_year(updated_results, year)

    #הפעלת התחזית
    return evaluate_stock_recommendation(extracted_revenues, extracted_profits)
    


key = '514091680'  # לדוגמה
##שמירת נתונים לקובץ לצורך בדיקות
#with open('C:/Users/zvi25/Desktop/results.txt', 'w', encoding='utf-8') as file:
#    for row in updated_results:
#        file.write(str(row) + '\n')

##שמירת נתונים לקובץ לצורך בדיקות
#with open('C:/Users/zvi25/Desktop/results.txt', 'w', encoding='utf-8') as file:
#       file.write(str(extracted_revenues) + '\n')
#        file.write(str(extracted_profits) + '\n')

#קריאה לפונקציה הראשית
print(Create_recommendation(key))

print('***The job is done***')