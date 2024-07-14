import yfinance as yf
import pandas as pd

# Function to print full DataFrame or Series without truncation
def print_full(x):
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    print(x)
    pd.reset_option('display.max_rows')
    pd.reset_option('display.max_columns')
    pd.reset_option('display.width')
    pd.reset_option('display.max_colwidth')

ticker = yf.Ticker("GOOGL")

# get all stock info
ticker.info
ticker_bs = ticker.quarterly_balance_sheet
print_full(ticker_bs)
