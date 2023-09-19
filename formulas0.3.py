import pandas as pd


##מקבל את הקובץ של הנתונים ואת הקובץ של מספרי המניות יוצר המלצה ושומר עם שם המניה
# Load the data
data = pd.read_excel("C:/Users/zvi25/Downloads/microsoft.microsoftskydrive_8wekyb3d8bbwe!App/Downloads/נתונים מסודרים לבדיקות.xlsx")

# Load the securities market data with the correct columns and skipping the header rows
securities_data = pd.read_csv("C:/Users/zvi25/Desktop/StartUp/פרויקט עם אלון/data/securitiesmarketdata - Copy.csv", skiprows=2, usecols=[0, 2], names=["Name", "Stock Number"], dtype={"Stock Number": str})

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

# Extract unique stock numbers
unique_stock_numbers = data["Stock Number"].unique()

# Evaluate recommendation for each stock and save results to a list
results_list = []

for stock_number in unique_stock_numbers:
    stock_data = data[data["Stock Number"] == stock_number]
    recommendation = evaluate_stock_recommendation(stock_data)
    results_list.append({"Stock Number": stock_number, "Recommendation": recommendation})

results = pd.DataFrame(results_list)

# Convert "Stock Number" column to string type for merging
results["Stock Number"] = results["Stock Number"].astype(str)

# Merge the recommendations with the securities data to get the stock name
final_data = pd.merge(results, securities_data, on="Stock Number", how="left")

# Save the final results to an Excel file
final_data.to_excel("C:/Users/zvi25/Desktop/risult.xlsx", index=False)
print('***The job is done***')
