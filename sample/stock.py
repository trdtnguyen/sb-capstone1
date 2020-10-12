"""
Read stock prices
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'


import pandas as pd

from yahoo_finance import Share

from googlefinance import getQuotes
import json

from pandas_datareader import data

## Google
#print (json.dumps(getQuotes('AAPL'), indent=2))

## Yahoo
#yahoo = Share('YHOO')
# print (yahoo.get_open())
# print (yahoo.get_price())
# print (yahoo.get_trade_datetime())

## pandas_datareader
tickers = ['AAPL', 'MSFT', '^GSPC']
start_date = '2020-01-01'
end_date = '2020-09-30'

panel_data = data.DataReader(tickers[0], 'yahoo', start_date, end_date)
print(panel_data.head())
print(panel_data.columns)
# for ticker in tickers:
#     panel_data = data.DataReader(ticker, 'yahoo', start_date, end_date)
#     print(type(panel_data))
#     print("ticker: ", ticker)
#     print(panel_data)
#panel_data.to_frame().head(9)