import quandl
import ipypb
import pandas as pd

quandl.ApiConfig.api_key = 'N/A'

CANDLE_COLUMNS = ['Open', 'High', 'Low', 'Close', 'Volume']

def get_candle_data(stock, date_range):
    
    new_columns = [stock+'.'+column for column in CANDLE_COLUMNS]
    
    stock_candle_data = quandl.get("EOD/{}".format(stock))
    stock_candle_data.index = pd.to_datetime(stock_candle_data.index)
    stock_candle_data = stock_candle_data[date_range[0]:date_range[-1]][CANDLE_COLUMNS]
    
    stock_candle_data = stock_candle_data.rename(columns=dict(zip(CANDLE_COLUMNS, new_columns)))
    
    return stock_candle_data
    

