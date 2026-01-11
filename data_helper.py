import pandas as pd
import requests
from io import StringIO
from bs4 import BeautifulSoup
from flask import jsonify
from datetime import datetime
import math
import time
import logging
from functools import wraps

# Configure logging
logger = logging.getLogger(__name__)

# Constants
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
REQUEST_TIMEOUT = 30  # seconds

def retry_on_failure(max_retries=MAX_RETRIES, delay=RETRY_DELAY):
    """
    Decorator to retry functions on failure with exponential backoff
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Failed after {max_retries} attempts in {func.__name__}: {str(e)}")
                        raise
                    wait_time = delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Attempt {attempt + 1} failed in {func.__name__}: {str(e)}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
            return None
        return wrapper
    return decorator

def fetch_url_with_retry(url, timeout=REQUEST_TIMEOUT):
    """
    Fetch URL content with retry logic and timeout
    """
    try:
        response = requests.get(url, verify=False, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.exceptions.Timeout:
        logger.error(f"Timeout while fetching {url}")
        raise Exception(f"Request timeout after {timeout} seconds")
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for {url}: {str(e)}")
        raise Exception(f"Failed to fetch URL: {str(e)}")

#returns the data in tuple
@retry_on_failure()
def clean_crp_table():
    try:
        # URL of the page
        url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html"
        logger.info(f"Fetching data from {url}")

        # Fetch the webpage content with retry
        response = fetch_url_with_retry(url)

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Wrap the HTML content in StringIO
        html_content = StringIO(response.text)

        # Use pandas to read the HTML content and extract tables
        tables = pd.read_html(html_content)
  
        
        # Assume the second table is the one we need
        df = tables[1]

        # Basic data validation
        if df.empty:
            return None, "DataFrame is empty"

        # Check for expected number of columns
        expected_columns = 8  # Updated: Website now has 8 columns (added Sovereign CDS and ERP based on sovereign CDS)



        print("DataFrame shape:", df.shape)
        print("DataFrame columns:", df.columns.tolist())
        print("DataFrame head with all columns:\n", df)
        if len(df.columns) != expected_columns:
            return None, f"Unexpected number of columns. Expected {expected_columns}, got {len(df.columns)}"
        
        # Apply the function to clean the country column
        df.iloc[:, 0] = df.iloc[:, 0].apply(clean_string)

        # Remove % signs and convert to float where applicable
        df = df.applymap(lambda x: x.replace('%', '').strip() if isinstance(x, str) else x)
        
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(x) for x in df.to_numpy()]

        # Remove first row
        data_tuples = data_tuples[1:]

        return data_tuples, None

    except Exception as e:
        return None, str(e)

#returns the data in tuple
@retry_on_failure()
def clean_taxRate_table():
    try:
        # URL of the page
        url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/taxrate.html"

        # Fetch the webpage content with retry
        logger.info(f"Fetching data from {url}")
        response = fetch_url_with_retry(url)

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Wrap the HTML content in StringIO
        html_content = StringIO(response.text)

        # Use pandas to read the HTML content and extract tables
        tables = pd.read_html(html_content)
        
        # Assume the first table is the one we need
        df = tables[0]

        # Basic data validation
        if df.empty:
            return None, "DataFrame is empty"

        # Check for expected number of columns
        expected_columns = 11  # Adjust this number based on your table structure
        if len(df.columns) != expected_columns:
            return None, f"Unexpected number of columns. Expected {expected_columns}, got {len(df.columns)}"
        
        # Apply the function to clean the industry column
        df.iloc[:, 0] = df.iloc[:, 0].apply(clean_string)

        # Remove % signs and convert to float where applicable
        df = df.applymap(lambda x: x.replace('%', '').strip() if isinstance(x, str) else x)
        
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(x) for x in df.to_numpy()]

        # Remove first row
        data_tuples = data_tuples[2:]
        
        return data_tuples, None

    except Exception as e:
        return None, str(e)
    
#returns the data in tuple
@retry_on_failure()
def clean_sales_to_cap_us():
    try:
        # URL of the page
        url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/capex.html"

        # Fetch the webpage content with retry
        logger.info(f"Fetching data from {url}")
        response = fetch_url_with_retry(url)

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Wrap the HTML content in StringIO
        html_content = StringIO(response.text)

        # Use pandas to read the HTML content and extract tables
        tables = pd.read_html(html_content)
        
        # Assume the first table is the one we need
        df = tables[0]

        # Basic data validation
        if df.empty:
            return None, "DataFrame is empty"

        # Check for expected number of columns
        expected_columns = 10  # Adjust this number based on your table structure
        if len(df.columns) != expected_columns:
            return None, f"Unexpected number of columns. Expected {expected_columns}, got {len(df.columns)}"
        
        # Apply the function to clean the industry column
        df.iloc[:, 0] = df.iloc[:, 0].apply(clean_string)

        # Remove % signs and convert to float where applicable
        df = df.applymap(lambda x: x.replace('%', '').strip() if isinstance(x, str) else x)
        
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(x) for x in df.to_numpy()]

        # Remove first row
        data_tuples = data_tuples[1:]
        
        return data_tuples, None

    except Exception as e:
        return None, str(e)


#returns the data in tuple
@retry_on_failure()
def clean_beta_us():
    try:
        # URL of the page
        url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/totalbeta.html"

        # Fetch the webpage content with retry
        logger.info(f"Fetching data from {url}")
        response = fetch_url_with_retry(url)

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Wrap the HTML content in StringIO
        html_content = StringIO(response.text)

        # Use pandas to read the HTML content and extract tables
        tables = pd.read_html(html_content)
        
        # Assume the first table is the one we need
        df = tables[0]

        # Basic data validation
        if df.empty:
            return None, "DataFrame is empty"

        # Check for expected number of columns
        expected_columns = 7  # Adjust this number based on your table structure
        if len(df.columns) != expected_columns:
            return None, f"Unexpected number of columns. Expected {expected_columns}, got {len(df.columns)}"
        
        # Apply the function to clean the industry column
        df.iloc[:, 0] = df.iloc[:, 0].apply(clean_string)

        # Remove % signs and convert to float where applicable
        df = df.applymap(lambda x: x.replace('%', '').strip() if isinstance(x, str) else x)
        
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(x) for x in df.to_numpy()]

        # Remove first row
        data_tuples = data_tuples[1:]
        
        return data_tuples, None

    except Exception as e:
        return None, str(e)
    
@retry_on_failure()
def getLastUpdate(url,textToFind):
    logger.info(f"Getting last update from {url}")
    response = fetch_url_with_retry(url)

    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract the "Last updated..." text
    last_updated_text = soup.find(text=lambda t: t and textToFind in t)
    if last_updated_text:
        last_updated_text = last_updated_text.strip()
        # Get the date part only
        date_part = last_updated_text.replace(f"{textToFind} ", "")
        # Convert to SQL date format
        date_obj = datetime.strptime(date_part, "%B %Y").date()
        return(date_obj)
        
    return None

#returns the data in tuple
@retry_on_failure()
def clean_pe_ratio_us():
    try:
        # URL of the page
        url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/pedata.html"

        # Fetch the webpage content with retry
        logger.info(f"Fetching data from {url}")
        response = fetch_url_with_retry(url)

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Wrap the HTML content in StringIO
        html_content = StringIO(response.text)

        # Use pandas to read the HTML content and extract tables
        tables = pd.read_html(html_content)
        
        # Assume the first table is the one we need
        df = tables[0]

        # Basic data validation
        if df.empty:
            return None, "DataFrame is empty"

        # Check for expected number of columns
        expected_columns = 10  # Adjust this number based on your table structure
        if len(df.columns) != expected_columns:
            return None, f"Unexpected number of columns. Expected {expected_columns}, got {len(df.columns)}"
        
        # Apply the function to clean the industry column
        df.iloc[:, 0] = df.iloc[:, 0].apply(clean_string)

        # Remove % signs and convert to float where applicable
        df = df.applymap(lambda x: x.replace('%', '').strip() if isinstance(x, str) else x)
        
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(x) for x in df.to_numpy()]

        # Remove first row
        data_tuples = data_tuples[1:]
        
        return data_tuples, None

    except Exception as e:
        return None, str(e)
    
#returns the data in tuple
@retry_on_failure()
def clean_rev_growth_rate():
    try:
        # URL of the page
        url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/histgr.html"

        # Fetch the webpage content with retry
        logger.info(f"Fetching data from {url}")
        response = fetch_url_with_retry(url)

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Wrap the HTML content in StringIO
        html_content = StringIO(response.text)

        # Use pandas to read the HTML content and extract tables
        tables = pd.read_html(html_content)
        
        # Assume the first table is the one we need
        df = tables[0]

        # Basic data validation
        if df.empty:
            return None, "DataFrame is empty"

        # Check for expected number of columns
        expected_columns = 7  # Adjust this number based on your table structure
        if len(df.columns) != expected_columns:
            return None, f"Unexpected number of columns. Expected {expected_columns}, got {len(df.columns)}"

        # Apply the function to clean the industry column
        df.iloc[:, 0] = df.iloc[:, 0].apply(clean_string)

        # Remove % signs and convert to float where applicable
        df = df.applymap(lambda x: x.replace('%', '').strip() if isinstance(x, str) else x)
        
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(x) for x in df.to_numpy()]

        # Remove first row
        data_tuples = data_tuples[1:]
        
        return data_tuples, None

    except Exception as e:
        return None, str(e)

#returns the data in tuple
@retry_on_failure()
def clean_ebit_growth():
    try:
        # URL of the page
        url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/fundgrEB.html"

        # Fetch the webpage content with retry
        logger.info(f"Fetching data from {url}")
        response = fetch_url_with_retry(url)

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Wrap the HTML content in StringIO
        html_content = StringIO(response.text)

        # Use pandas to read the HTML content and extract tables
        tables = pd.read_html(html_content)
        
        # Assume the first table is the one we need
        df = tables[0]

        # Basic data validation
        if df.empty:
            return None, "DataFrame is empty"

        # Check for expected number of columns
        expected_columns = 5  # Adjust this number based on your table structure
        if len(df.columns) != expected_columns:
            return None, f"Unexpected number of columns. Expected {expected_columns}, got {len(df.columns)}"

        # Apply the function to clean the industry column
        df.iloc[:, 0] = df.iloc[:, 0].apply(clean_string)

        # Remove % signs and convert to float where applicable
        df = df.applymap(lambda x: x.replace('%', '').strip() if isinstance(x, str) else x)
        
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(x) for x in df.to_numpy()]

        # Remove first row
        data_tuples = data_tuples[1:]
        
        return data_tuples, None

    except Exception as e:
        return None, str(e)

#returns the data in tuple
@retry_on_failure()
def clean_default_spread():
    try:
        # URL of the page
        url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ratings.html"

        # Fetch the webpage content with retry
        logger.info(f"Fetching data from {url}")
        response = fetch_url_with_retry(url)

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Wrap the HTML content in StringIO
        html_content = StringIO(response.text)

        # Use pandas to read the HTML content and extract tables
        tables = pd.read_html(html_content)
        # Assume the first table is the one we need
        df = tables[0]

        # Basic data validation
        if df.empty:
            return None, None, "DataFrame is empty"

        # Check for expected number of columns (9 columns: 4 for large firms, 1 separator, 4 for small firms)
        expected_columns = 9
        if len(df.columns) != expected_columns:
            return None, None, f"Unexpected number of columns. Expected {expected_columns}, got {len(df.columns)}"

        # Remove % signs and convert to float where applicable
        df = df.map(lambda x: x.replace('%', '').strip() if isinstance(x, str) else x)

        # Skip the first 4 header rows and extract data
        df_data = df.iloc[4:, :]

        # Extract large firms data (columns 0-3: coverage ratio >, coverage ratio <=, rating, spread)
        large_firms_df = df_data.iloc[:, [0, 1, 2, 3]]
        bigFirms = [tuple(x) for x in large_firms_df.to_numpy()]

        # Extract small firms data (columns 5-8: coverage ratio >, coverage ratio <=, rating, spread)
        # Skip column 4 which is the separator
        small_firms_df = df_data.iloc[:, [5, 6, 7, 8]]
        smallFirms = [tuple(x) for x in small_firms_df.to_numpy()]

        return bigFirms, smallFirms, None

    except Exception as e:
        logger.error(f"Error in clean_default_spread: {str(e)}", exc_info=True)
        return None, None, str(e)

# Function to clean the industry column strings
def clean_string(s):
    if isinstance(s, str):
        # Remove leading/trailing whitespaces and replace multiple spaces with a single space
        return ' '.join(s.split())
    return s


@retry_on_failure()
def getLastUpdate(url,textToFind):
    logger.info(f"Getting last update from {url}")
    response = fetch_url_with_retry(url)

    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract the "Last updated..." text
    last_updated_text = soup.find(text=lambda t: t and textToFind in t)
    print(last_updated_text)
    if last_updated_text:
        last_updated_text = last_updated_text.strip()
        # Get the date part only
        date_part = last_updated_text.replace(f"{textToFind} ", "")
        # Convert to SQL date format
        date_obj = datetime.strptime(date_part, "%B %Y").date()
        return(date_obj)
        
    return None


@retry_on_failure()
def getLastUpdate_crp(url,textToFind):
    logger.info(f"Getting last update from {url}")
    response = fetch_url_with_retry(url)

    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract the "Last updated..." text
    last_updated_text = soup.find(text=lambda t: t and textToFind in t)
    if last_updated_text:
        last_updated_text = last_updated_text.strip()
        # Get the date part only
        date_part = last_updated_text.replace(f"{textToFind} ", "")
        print(date_part)
        # Convert to SQL date format
        date_obj = datetime.strptime(date_part, "%B %d, %Y").date()
        return(date_obj)
    return None


@retry_on_failure()
def clean_roic_table():
    try:
        # URL of the page
        url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/fundgrEB.html"

        # Fetch the webpage content with retry
        logger.info(f"Fetching data from {url}")
        response = fetch_url_with_retry(url)

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Wrap the HTML content in StringIO
        html_content = StringIO(response.text)

        # Use pandas to read the HTML content and extract tables
        tables = pd.read_html(html_content)
        
        # Assume the first table is the one we need
        df = tables[0]

        # Basic data validation
        if df.empty:
            return None, "DataFrame is empty"

        # Check for expected number of columns
        expected_columns = 5  # Adjust this number based on your table structure
        if len(df.columns) != expected_columns:
            return None, f"Unexpected number of columns. Expected {expected_columns}, got {len(df.columns)}"

        # Apply the function to clean the industry column
        df.iloc[:, 0] = df.iloc[:, 0].apply(clean_string)

        # Remove % signs and convert to float where applicable
        df = df.applymap(lambda x: x.replace('%', '').strip() if isinstance(x, str) else x)
        
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(x) for x in df.to_numpy()]

        # Remove first row
        data_tuples = data_tuples[1:]
        print(data_tuples)
        
        return data_tuples, None

    except Exception as e:
        return None, str(e)