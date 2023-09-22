try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen

import certifi
import json


def get_stock_price(stock_name):
    stock_name += ".TA"  # Appending '.TA' to the stock_name as per the new requirement
    api_key = "728c1b4c2d8218add598fcfc7401f70b"  # API key
    url = f"https://financialmodelingprep.com/api/v3/quote-short/{stock_name}?apikey={api_key}"  # Constructing the URL

    try:
        response = urlopen(url, cafile=certifi.where())  # Sending the request
        data = response.read().decode("utf-8")  # Reading and decoding the response
        stock_data = json.loads(data)  # Parsing the JSON response
        
        if stock_data:  # Checking if data is not empty
            latest_close_price = float(stock_data[0]['price'])  # Extracting the price
            return latest_close_price  # Returning the price as a float
    except Exception as e:  # Catching any exceptions that occur during the process
        print(f"Failed to get stock price for {stock_name}: {e}")
        return None  # Returning None in case of an exception


# You can call the function like this
stock_name = "AAPL"  # Example stock name
price = get_stock_price(stock_name)
if price:
    print(f"The stock price for {stock_name} is {price}")
else:
    print(f"Failed to get the stock price for {stock_name}")
