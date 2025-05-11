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

@app.route('/stock_info/tnx')
def get_stock_info_tnx():
    ticker = yf.Ticker("^TNX")
    return jsonify(ticker.info)

# deprecated
# @app.route('/calender/<ticker_symbol>')
# def get_calender(ticker_symbol):
#     ticker = yf.Ticker(ticker_symbol)
#     return jsonify(ticker.calendar)

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


#has issues with EPS both basic and diluted
@app.route('/ttm_income_statement/<ticker_symbol>')
def get_ttm_income_statement(ticker_symbol):
    ticker = yf.Ticker(ticker_symbol)
    income_statement = ticker.ttm_income_stmt
    print(income_statement)
    return income_statement.to_json()  

@app.route('/ttm_cash_flow/<ticker_symbol>')
def get_cash_flow(ticker_symbol):
    ticker = yf.Ticker(ticker_symbol)
    cash_flow = ticker.ttm_cashflow
    print(cash_flow)
    return cash_flow.to_json()  

@app.route('/currency_conversion/<source_currency>/<target_currency>/<start_date>/<end_date>')
def get_currency_conversion(source_currency, target_currency, start_date, end_date):
    # Format the currency pair for Yahoo Finance
    currency_pair = f"{source_currency}{target_currency}=X"
    
    try:
        # Get ticker for the currency pair
        ticker = yf.Ticker(currency_pair)
        
        # Fetch historical data
        historical_data = ticker.history(start=start_date, end=end_date)
        
        if historical_data.empty:
            return jsonify({"error": "No data found for this currency pair or date range"}), 404
        
        # Process the data into the same format as other endpoints
        restructured_data = []
        
        for date, row in historical_data.iterrows():
            date_str = date.strftime('%Y-%m-%d')
            
            # Create an object with date and values
            period_data = {"date": date_str, "values": {}}
            
            # Add non-NaN values to the values object
            for column, value in row.items():
                if pd.notna(value):  # Only include non-NaN values
                    period_data["values"][column] = float(value)
            
            restructured_data.append(period_data)
        
        # Sort the array by date, most recent first
        restructured_data.sort(key=lambda x: x["date"], reverse=True)
        
        return jsonify(restructured_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

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
            INSERT INTO country_risk_premium VALUES (%s, %s, %s, %s, %s, %s)
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
    if error:
        return jsonify({"error": error}), 400

    # Create structured tuples that match your database schema
    # Assuming you only need the first 4 elements from each tuple
    processed_big_firm = [(row[0], row[1], row[2], row[3]) for row in data_tuple_big_firm]
    processed_small_firm = [(row[0], row[1], row[2], row[3]) for row in data_tuple_small_firm]
    
    print("Processed big firm data:", processed_big_firm[:2])  # Print first 2 for verification
    print("Processed small firm data:", processed_small_firm[:2])

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
            db_handler.execute_query_many(insert_query_1, processed_big_firm) 

            # Define SQL insert query
            insert_query_2= """
            INSERT INTO default_spread_small_firm VALUES (%s, %s, %s, %s)
            """
            # Insert data into the database
            db_handler.execute_query_many(insert_query_2, processed_small_firm)  

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

@app.route('/update_roic')
def update_roic():
    data_tuples, error = clean_roic_table()
    if error:
        return jsonify({"error": error}), 400

    last_update = getLastUpdate("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/fundgrEB.html", "Last updated in")

    # Create an instance of DatabaseHandler
    db_handler = DatabaseHandler()

    # Connect to the database
    db_handler.connect()
    db_last_update = db_handler.fetch_query("""SELECT last_update FROM data_last_update WHERE data_name = 'roic' """)
    print(db_last_update)
    if db_last_update:
       db_last_update = db_last_update[0][0]
    else:
        return jsonify({"status": "Error getting last_update from table data_last_update"})
    
    if last_update > db_last_update:
        try:
            # Truncate the table
            truncate_query = "TRUNCATE TABLE roic"
            db_handler.execute_query(truncate_query)

            # Define SQL insert query
            insert_query_roic = """
            INSERT INTO roic VALUES (%s, %s, %s, %s, %s)
            """
            # Insert data into the database
            db_handler.execute_query_many(insert_query_roic, data_tuples)

            update_query_data_last_update = f"""
            UPDATE data_last_update
            SET last_update = '{last_update}'
            WHERE data_name = 'roic'
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

@app.route('/init/last-update')
def initialize_last_update():
    try:
        # Connect to the database
        db_handler = DatabaseHandler()
        db_handler.connect()
        
        # Define the tables that need tracking
        tables = [
            'beta_us',
            'country_risk_premium',
            'ebit_growth',
            'pe_ratio_us',
            'rev_growth_rate',
            'sales_to_cap_us',
            'effective_tax_rate',
            'default_spread_large_firm',
            'default_spread_small_firm',
            'input_stats',
            'roic',
            'default_spread'
        ]
        
        # Ensure data_last_update table exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS data_last_update (
            data_name varchar(255),
            last_update date,
            PRIMARY KEY (data_name)
        )
        """
        db_handler.execute_query(create_table_sql)
        
        # Set a default early date for all tables
        default_date = '2023-01-01'
        
        # Insert or update records for each table
        for table_name in tables:
            # Check if entry exists
            check_sql = "SELECT COUNT(*) FROM data_last_update WHERE data_name = %s"
            # Assuming execute_query returns a cursor or result directly
            result = db_handler.execute_query(check_sql, (table_name,))
            
            # Get the count value - adjust this based on how your DatabaseHandler returns results
            count = 0
            if result:
                # This might need adjustment based on how results are returned
                first_row = result[0] if isinstance(result, list) else next(result, None)
                if first_row:
                    count = first_row[0]
            
            if count > 0:
                # Update existing record
                update_sql = "UPDATE data_last_update SET last_update = %s WHERE data_name = %s"
                db_handler.execute_query(update_sql, (default_date, table_name))
            else:
                # Insert new record
                insert_sql = "INSERT INTO data_last_update (data_name, last_update) VALUES (%s, %s)"
                db_handler.execute_query(insert_sql, (table_name, default_date))
        
        # Close the connection
        db_handler.close()
        
        return jsonify({
            'status': 'success',
            'message': f'Successfully initialized last_update dates to {default_date} for all tables'
        }), 200
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500
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