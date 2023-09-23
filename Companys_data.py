
import requests
import pandas as pd
from pathlib import Path
import datetime


# פונקציה לטעינת סמלי החברות מקובץ Excel
def load_symbols_from_excel(file_path):
    df = pd.read_excel(file_path)
    return df['Symbol'].tolist()

# פונקציה להבאת הדוחות הרבעוניים לפי שנים מסוימות
def get_quarterly_data_for_years(stock_name, api_key, years):
    all_data = []
    
    # עבור כל שנה ברשימה
    for year in years:
        url = f"https://financialmodelingprep.com/api/v3/income-statement/{stock_name}.TA?period=quarter&apikey={api_key}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            data_year = [item for item in data if str(year) in item['date']]
            for item in data_year:
                item['symbol'] = stock_name
            all_data.extend(data_year)
    
    return all_data

def main_updated():
    api_key = '728c1b4c2d8218add598fcfc7401f70b'  # Replace with your API key
    file_path = 'C:/Users/zvi25/Desktop/StartUp/פרויקט עם אלון/API/extracted_stocks_new.xlsx'  # Replace with the path to your Excel file
    companies = load_symbols_from_excel(file_path)
    
    # רשימה לשמירת הסטטוס של הבאת הנתונים לכל חברה
    status_list = []

    # שנים של הדוחות שאנו רוצים להביא
    years = list(range(2018, 2024))
    
    output_file = 'quarterly_data_all.xlsx'
    if Path(output_file).exists():
        all_data_df = pd.read_excel(output_file)
    else:
        all_data_df = pd.DataFrame()
    
    current_year = datetime.datetime.now().year
    counter=0
    for company in companies:
        data = get_quarterly_data_for_years(company, api_key, years)
        
        # הוספת הנתונים ל-DataFrame הקיים ושמירה
        all_data_df = pd.concat([all_data_df, pd.DataFrame(data)], ignore_index=True)
        all_data_df.to_excel(output_file, index=False)
        
        # בדיקת הסטטוס של הבאת הנתונים ושמירה ברשימה
        retrieved_quarters = len(data)
        if current_year in years:
            required_quarters = len(years) * 4 - (4 - (datetime.datetime.now().month - 1) // 3)
        else:
            required_quarters = len(years) * 4

        if retrieved_quarters == required_quarters:
            status = "All quarters retrieved"
        elif retrieved_quarters == 0:
            status = "No data retrieved"
        else:
            status = "Partial data retrieved"
        
        counter+=1
        print(f'{counter}: {company}- {status}')
        status_list.append({"Company": company, "Status": status})
    
    # שמירת הסטטוסים לקובץ Excel נפרד
    status_df = pd.DataFrame(status_list)
    status_df.to_excel('retrieval_status.xlsx', index=False)

if __name__ == "__main__":
    main_updated()
