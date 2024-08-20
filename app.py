from flask import Flask, jsonify
import yfinance as yf
import pandas as pd
from collections import defaultdict
import requests
from io import StringIO
from database import DatabaseHandler
from bs4 import BeautifulSoup
from datetime import datetime
from data_helper import *
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import date, timedelta
import random
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

@app.route("/test")
def test():
    return jsonify(random.randint(0,10000))

@app.route('/update_country_risk_premium')
def update_country_risk_premium():
    data_tuples, error = clean_crp_table()
    
    if error:
        return jsonify({"error": error}), 400

    last_update = getLastUpdate_crp("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Last updated:")

    # Create an instance of DatabaseHandler
    db_handler = DatabaseHandler()

    # Connect to the database
    db_handler.connect()
    db_last_update = db_handler.fetch_query("""SELECT last_update FROM data_last_update WHERE data_name = 'country_risk_premium' """)
    if db_last_update:
       db_last_update = db_last_update[0][0]
    else:
        return jsonify({"status": "Error getting last_update from table data_last_update"})
    if last_update > db_last_update:
        try:
            # Truncate the table
            truncate_query = "TRUNCATE TABLE country_risk_premium"
            db_handler.execute_query(truncate_query)

            # Define SQL insert query
            insert_query_crp = """
            INSERT INTO country_risk_premium VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            # Insert data into the database
            db_handler.execute_query_many(insert_query_crp, data_tuples)

            update_query_data_last_update = f"""
            UPDATE data_last_update
            SET last_update = '{last_update}'
            WHERE data_name = 'country_risk_premium'
            """
            db_handler.execute_query(update_query_data_last_update)

        except Exception as e:
            print(f"An error occurred: {e}")
            db_handler.rollback()
    else:
        # Close the connection
        db_handler.close()
        return jsonify({"status": "Data is the same"})


    # Close the connection
    db_handler.close()

    return jsonify({"status": "Data inserted successfully"})

@app.route('/update_effective_tax_rate')
def update_effective_tax_rate():
    data_tuples, error = clean_taxRate_table()
    if error:
        return jsonify({"error": error}), 400

    last_update = getLastUpdate("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/taxrate.html","Updated")

    # Create an instance of DatabaseHandler
    db_handler = DatabaseHandler()

    # Connect to the database
    db_handler.connect()
    db_last_update = db_handler.fetch_query("""SELECT last_update FROM data_last_update WHERE data_name = 'effective_tax_rate' """)
    if db_last_update:
       db_last_update = db_last_update[0][0]
    else:
        return jsonify({"status": "Error getting last_update from table data_last_update"})
    if last_update > db_last_update:
        try:
            # Truncate the table
            truncate_query = "TRUNCATE TABLE effective_tax_rate"
            db_handler.execute_query(truncate_query)

            # Define SQL insert query
            insert_query_crp = """
            INSERT INTO effective_tax_rate VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s)
            """
            # Insert data into the database
            db_handler.execute_query_many(insert_query_crp, data_tuples)

            update_query_data_last_update = f"""
            UPDATE data_last_update
            SET last_update = '{last_update}'
            WHERE data_name = 'effective_tax_rate'
            """
            db_handler.execute_query(update_query_data_last_update)

        except Exception as e:
            print(f"An error occurred: {e}")
            db_handler.rollback()
    else:
        # Close the connection
        db_handler.close()
        return jsonify({"status": "Data is the same"})


    # Close the connection
    db_handler.close()

    return jsonify({"status": "Data inserted successfully"})

@app.route('/update_sales_to_cap_us')
def update_sales_to_cap_us():
    data_tuples, error = clean_sales_to_cap_us()
    if error:
        return jsonify({"error": error}), 400

    last_update = getLastUpdate("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/capex.html","Last updated")
    print(last_update)
    # Create an instance of DatabaseHandler
    db_handler = DatabaseHandler()

    # Connect to the database
    db_handler.connect()
    db_last_update = db_handler.fetch_query("""SELECT last_update FROM data_last_update WHERE data_name = 'sales_to_cap_us' """)
    if db_last_update:
       db_last_update = db_last_update[0][0]
    else:
        return jsonify({"status": "Error getting last_update from table data_last_update"})
    if last_update > db_last_update:
        try:
            # Truncate the table
            truncate_query = "TRUNCATE TABLE sales_to_cap_us"
            db_handler.execute_query(truncate_query)

            # Define SQL insert query
            insert_query_sales_to_cap_us = """
            INSERT INTO sales_to_cap_us VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s)
            """
            # Insert data into the database
            db_handler.execute_query_many(insert_query_sales_to_cap_us, data_tuples)

            update_query_data_last_update = f"""
            UPDATE data_last_update
            SET last_update = '{last_update}'
            WHERE data_name = 'sales_to_cap_us'
            """
            db_handler.execute_query(update_query_data_last_update)

        except Exception as e:
            print(f"An error occurred: {e}")
            db_handler.rollback()
    else:
        # Close the connection
        db_handler.close()
        return jsonify({"status": "Data is the same"})


    # Close the connection
    db_handler.close()

    return jsonify({"status": "Data inserted successfully"})

@app.route('/update_beta_us')
def update_beta_us():
    data_tuples, error = clean_beta_us()

    if error:
        return jsonify({"error": error}), 400

    last_update = getLastUpdate("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/totalbeta.html","Last Updated in")
    print(last_update)
    # Create an instance of DatabaseHandler
    db_handler = DatabaseHandler()

    # Connect to the database
    db_handler.connect()
    db_last_update = db_handler.fetch_query("""SELECT last_update FROM data_last_update WHERE data_name = 'beta_us' """)
    if db_last_update:
       db_last_update = db_last_update[0][0]
    else:
        return jsonify({"status": "Error getting last_update from table data_last_update"})
    if last_update > db_last_update:
        try:
            # Truncate the table
            truncate_query = "TRUNCATE TABLE beta_us"
            db_handler.execute_query(truncate_query)

            # Define SQL insert query
            insert_query_beta_us= """
            INSERT INTO beta_us VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            # Insert data into the database
            db_handler.execute_query_many(insert_query_beta_us, data_tuples)

            update_query_data_last_update = f"""
            UPDATE data_last_update
            SET last_update = '{last_update}'
            WHERE data_name = 'beta_us'
            """
            db_handler.execute_query(update_query_data_last_update)

        except Exception as e:
            print(f"An error occurred: {e}")
            db_handler.rollback()
    else:
        # Close the connection
        db_handler.close()
        return jsonify({"status": "Data is the same"})


    # Close the connection
    db_handler.close()

    return jsonify({"status": "Data inserted successfully"})

@app.route('/update_pe_ratio_us')
def update_pe_ratio_us():
    data_tuples, error = clean_pe_ratio_us()

    if error:
        return jsonify({"error": error}), 400

    last_update = getLastUpdate("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/pedata.html","Last Updated in")

    # Create an instance of DatabaseHandler
    db_handler = DatabaseHandler()

    # Connect to the database
    db_handler.connect()
    db_last_update = db_handler.fetch_query("""SELECT last_update FROM data_last_update WHERE data_name = 'pe_ratio_us' """)
    if db_last_update:
       db_last_update = db_last_update[0][0]
    else:
        return jsonify({"status": "Error getting last_update from table data_last_update"})
    if last_update > db_last_update:
        try:
            # Truncate the table
            truncate_query = "TRUNCATE TABLE pe_ratio_us"
            db_handler.execute_query(truncate_query)

            # Define SQL insert query
            insert_query_pe_ratio_us= """
            INSERT INTO pe_ratio_us VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            # Insert data into the database
            db_handler.execute_query_many(insert_query_pe_ratio_us, data_tuples)

            update_query_data_last_update = f"""
            UPDATE data_last_update
            SET last_update = '{last_update}'
            WHERE data_name = 'pe_ratio_us'
            """
            db_handler.execute_query(update_query_data_last_update)

        except Exception as e:
            print(f"An error occurred: {e}")
            db_handler.rollback()
    else:
        # Close the connection
        db_handler.close()
        return jsonify({"status": "Data is the same"})


    # Close the connection
    db_handler.close()

    return jsonify({"status": "Data inserted successfully"})


@app.route('/update_rev_growth_rate')
def update_rev_growth_rate():
    data_tuples, error = clean_rev_growth_rate()

    if error:
        return jsonify({"error": error}), 400

    last_update = getLastUpdate("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/histgr.html","Last updated in")

    # Create an instance of DatabaseHandler
    db_handler = DatabaseHandler()

    # Connect to the database
    db_handler.connect()
    db_last_update = db_handler.fetch_query("""SELECT last_update FROM data_last_update WHERE data_name = 'rev_growth_rate' """)
    if db_last_update:
       db_last_update = db_last_update[0][0]
    else:
        return jsonify({"status": "Error getting last_update from table data_last_update"})
    if last_update > db_last_update:
        try:
            # Truncate the table
            truncate_query = "TRUNCATE TABLE rev_growth_rate"
            db_handler.execute_query(truncate_query)

            # Define SQL insert query
            insert_query_rev_growth_rate= """
            INSERT INTO rev_growth_rate VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            # Insert data into the database
            db_handler.execute_query_many(insert_query_rev_growth_rate, data_tuples)

            update_query_data_last_update = f"""
            UPDATE data_last_update
            SET last_update = '{last_update}'
            WHERE data_name = 'rev_growth_rate'
            """
            db_handler.execute_query(update_query_data_last_update)

        except Exception as e:
            print(f"An error occurred: {e}")
            db_handler.rollback()
    else:
        # Close the connection
        db_handler.close()
        return jsonify({"status": "Data is the same"})


    # Close the connection
    db_handler.close()

    return jsonify({"status": "Data inserted successfully"})

@app.route('/update_ebit_growth')
def update_ebit_growth():
    data_tuples, error = clean_ebit_growth()
    if error:
        return jsonify({"error": error}), 400

    last_update = getLastUpdate("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/fundgrEB.html","Last updated in")

    # Create an instance of DatabaseHandler
    db_handler = DatabaseHandler()

    # Connect to the database
    db_handler.connect()
    db_last_update = db_handler.fetch_query("""SELECT last_update FROM data_last_update WHERE data_name = 'ebit_growth' """)
    if db_last_update:
       db_last_update = db_last_update[0][0]
    else:
        return jsonify({"status": "Error getting last_update from table data_last_update"})
    if last_update > db_last_update:
        try:
            # Truncate the table
            truncate_query = "TRUNCATE TABLE ebit_growth"
            db_handler.execute_query(truncate_query)

            # Define SQL insert query
            insert_query_ebit_growth= """
            INSERT INTO ebit_growth VALUES (%s, %s, %s, %s, %s)
            """
            # Insert data into the database
            db_handler.execute_query_many(insert_query_ebit_growth, data_tuples)

            update_query_data_last_update = f"""
            UPDATE data_last_update
            SET last_update = '{last_update}'
            WHERE data_name = 'ebit_growth'
            """
            db_handler.execute_query(update_query_data_last_update)

        except Exception as e:
            print(f"An error occurred: {e}")
            db_handler.rollback()
    else:
        # Close the connection
        db_handler.close()
        return jsonify({"status": "Data is the same"})


    # Close the connection
    db_handler.close()

    return jsonify({"status": "Data inserted successfully"})


@app.route('/update_default_spread')
def update_default_spread():
    data_tuple_big_firm,data_tuple_small_firm, error = clean_default_spread()
    print(data_tuple_big_firm)
    if error:
        return jsonify({"error": error}), 400


    # Create an instance of DatabaseHandler
    db_handler = DatabaseHandler()

    # Connect to the database
    db_handler.connect()
    db_last_update = db_handler.fetch_query("""SELECT last_update FROM data_last_update WHERE data_name = 'default_spread' """)
    if db_last_update:
       db_last_update = db_last_update[0][0]
    else:
        return jsonify({"status": "Error getting last_update from table data_last_update"})


    current_date = date.today()
    if current_date - db_last_update > timedelta(days=30):
        try:
            # Truncate the table
            truncate_query_1 = "TRUNCATE TABLE default_spread_large_firm"
            db_handler.execute_query(truncate_query_1)
            truncate_query_2 = "TRUNCATE TABLE default_spread_small_firm"
            db_handler.execute_query(truncate_query_2)

            # Define SQL insert query
            insert_query_1= """
            INSERT INTO default_spread_large_firm VALUES (%s, %s, %s, %s)
            """
            # Insert data into the database
            db_handler.execute_query_many(insert_query_1, data_tuple_big_firm)

            # Define SQL insert query
            insert_query_2= """
            INSERT INTO default_spread_small_firm VALUES (%s, %s, %s, %s)
            """
            # Insert data into the database
            db_handler.execute_query_many(insert_query_2, data_tuple_small_firm)

            update_query_data_last_update = f"""
            UPDATE data_last_update
            SET last_update = '{current_date}'
            WHERE data_name = 'default_spread'
            """
            db_handler.execute_query(update_query_data_last_update)

        except Exception as e:
            print(f"An error occurred: {e}")
            db_handler.rollback()
    else:
        # Close the connection
        db_handler.close()
        return jsonify({"status": "Data is the same"})


    # Close the connection
    db_handler.close()

    return jsonify({"status": "Data inserted successfully"})




# # Scheduler setup
# scheduler = BackgroundScheduler()

# # Add each update function as a separate job
# scheduler.add_job(
#     func=update_country_risk_premium,
#     trigger=IntervalTrigger(seconds=60),
#     id='update_country_risk_premium',
#     name='Update country risk premium every day',
#     replace_existing=True
# )

# scheduler.add_job(
#     func=update_effective_tax_rate,
#     trigger=IntervalTrigger(seconds=60),
#     id='update_tax_rates_sector',
#     name='Update tax rates sector every day',
#     replace_existing=True
# )

# scheduler.add_job(
#     func=update_sales_to_cap_us,
#     trigger=IntervalTrigger(seconds=60),
#     id='update_sales_to_cap_us',
#     name='Update sales to cap US every day',
#     replace_existing=True
# )

# scheduler.add_job(
#     func=update_beta_us,
#     trigger=IntervalTrigger(seconds=60),
#     id='update_beta_us',
#     name='Update beta US every day',
#     replace_existing=True
# )

# scheduler.add_job(
#     func=update_pe_ratio_us,
#     trigger=IntervalTrigger(seconds=60),
#     id='update_pe_ratio_us',
#     name='Update PE ratio US every day',
#     replace_existing=True
# )

# scheduler.add_job(
#     func=update_rev_growth_rate,
#     trigger=IntervalTrigger(seconds=60),
#     id='update_rev_growth_rate',
#     name='Update revenue growth rate every day',
#     replace_existing=True
# )

# scheduler.add_job(
#     func=update_ebit_growth,
#     trigger=IntervalTrigger(seconds=60),
#     id='update_ebit_growth',
#     name='Update EBIT growth every day',
#     replace_existing=True
# )

# scheduler.add_job(
#     func=update_default_spread,
#     trigger=IntervalTrigger(seconds=60),
#     id='update_default_spread',
#     name='Update default spread every day',
#     replace_existing=True
# )

# # Start the scheduler
# scheduler.start()

# Ensure Flask app runs
if __name__ == '__main__':
    app.run(debug=True)