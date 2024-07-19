from flask import Flask, jsonify
import yfinance as yf
import pandas as pd
from collections import defaultdict

app = Flask(__name__)

def restructure_data(df):
    restructured_data = []
    
    for column in df.columns:
        date_str = column.strftime('%Y-%m-%d')
        period_data = {"date": date_str, "values": {}}
        for index, value in df[column].items():
            if pd.notna(value):  # Only include non-NaN values
                period_data["values"][index] = float(value)
        restructured_data.append(period_data)
    
    # Sort the array by date, most recent first
    restructured_data.sort(key=lambda x: x["date"], reverse=True)
    
    return restructured_data

@app.route('/stock_info/<ticker_symbol>')
def get_stock_info(ticker_symbol):
    ticker = yf.Ticker(ticker_symbol)
    return jsonify(ticker.info)

@app.route('/calender/<ticker_symbol>')
def get_calender(ticker_symbol):
    ticker = yf.Ticker(ticker_symbol)
    return jsonify(ticker.calendar)

@app.route('/annual_income_statement/<ticker_symbol>')
def get_annual_income_statement(ticker_symbol):
    ticker = yf.Ticker(ticker_symbol)
    income_statement = ticker.income_stmt
    restructured_data = restructure_data(income_statement)
    return jsonify(restructured_data)

@app.route('/annual_balance_sheet/<ticker_symbol>')
def get_annual_balance_sheet(ticker_symbol):
    ticker = yf.Ticker(ticker_symbol)
    balance_sheet = ticker.balance_sheet
    restructured_data = restructure_data(balance_sheet)
    return jsonify(restructured_data)

@app.route('/annual_cash_flow/<ticker_symbol>')
def get_annual_cash_flow(ticker_symbol):
    ticker = yf.Ticker(ticker_symbol)
    cash_flow = ticker.cash_flow
    restructured_data = restructure_data(cash_flow)
    return jsonify(restructured_data)

@app.route('/quarterly_income_statement/<ticker_symbol>')
def get_quarterly_income_statement(ticker_symbol):
    ticker = yf.Ticker(ticker_symbol)
    income_statement = ticker.quarterly_income_stmt
    restructured_data = restructure_data(income_statement)
    return jsonify(restructured_data)

@app.route('/quarterly_balance_sheet/<ticker_symbol>')
def get_quarterly_balance_sheet(ticker_symbol):
    ticker = yf.Ticker(ticker_symbol)
    balance_sheet = ticker.quarterly_balance_sheet
    restructured_data = restructure_data(balance_sheet)
    return jsonify(restructured_data)

@app.route('/quarterly_cash_flow/<ticker_symbol>')
def get_quarterly_cash_flow(ticker_symbol):
    ticker = yf.Ticker(ticker_symbol)
    cash_flow = ticker.quarterly_cash_flow
    restructured_data = restructure_data(cash_flow)
    return jsonify(restructured_data)

if __name__ == '__main__':
    app.run(debug=True)