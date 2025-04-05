import pandas as pd
import requests
from io import StringIO
from bs4 import BeautifulSoup
from flask import jsonify
from datetime import datetime
import math

#returns the data in tuple
def clean_crp_table():
    try:
        # URL of the page
        url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html"

        # Fetch the webpage content
        response = requests.get(url, verify=False)
        response.raise_for_status()  # Ensure we notice bad responses

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
        expected_columns = 6  # Adjust this number based on your table structure
       

        
        print("DataFrame shape:", df.shape)
        print("DataFrame columns:", df.columns.tolist())
        print("DataFrame head with all columns:\n", df)
        print(df)
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
def clean_taxRate_table():
    try:
        # URL of the page
        url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/taxrate.html"

        # Fetch the webpage content
        response = requests.get(url, verify=False)
        response.raise_for_status()  # Ensure we notice bad responses

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
def clean_sales_to_cap_us():
    try:
        # URL of the page
        url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/capex.html"

        # Fetch the webpage content
        response = requests.get(url, verify=False)
        response.raise_for_status()  # Ensure we notice bad responses

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
def clean_beta_us():
    try:
        # URL of the page
        url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/totalbeta.html"

        # Fetch the webpage content
        response = requests.get(url, verify=False)
        response.raise_for_status()  # Ensure we notice bad responses

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

        print(df)
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
    
def getLastUpdate(url,textToFind):
    response = requests.get(url, verify=False)
    response.raise_for_status()  # Ensure we notice bad responses

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
def clean_pe_ratio_us():
    try:
        # URL of the page
        url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/pedata.html"

        # Fetch the webpage content
        response = requests.get(url, verify=False)
        response.raise_for_status()  # Ensure we notice bad responses

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
def clean_rev_growth_rate():
    try:
        # URL of the page
        url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/histgr.html"

        # Fetch the webpage content
        response = requests.get(url, verify=False)
        response.raise_for_status()  # Ensure we notice bad responses

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
def clean_ebit_growth():
    try:
        # URL of the page
        url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/fundgrEB.html"

        # Fetch the webpage content
        response = requests.get(url, verify=False)
        response.raise_for_status()  # Ensure we notice bad responses

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
def clean_default_spread():
    try:
        # URL of the page
        url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ratings.html"

        # Fetch the webpage content
        response = requests.get(url, verify=False)
        response.raise_for_status()  # Ensure we notice bad responses

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
        # expected_columns = 5  # Adjust this number based on your table structure
        # if len(df.columns) != expected_columns:
        #     return None, f"Unexpected number of columns. Expected {expected_columns}, got {len(df.columns)}"

        # Apply the function to clean the industry column
        df.iloc[:, 0] = df.iloc[:, 0].apply(clean_string)

        # Remove % signs and convert to float where applicable
        df = df.map(lambda x: x.replace('%', '').strip() if isinstance(x, str) else x)
        
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(x) for x in df.to_numpy()]


        # Remove first row
        data_tuples = data_tuples[3:]

        # Find the index of the (nan, nan, nan, nan) tuple
        split_index = next((i for i, t in enumerate(data_tuples) if all(isinstance(x, float) and math.isnan(x) for x in t)), None)

        # Split the list
        bigFirms = data_tuples[:split_index]
        smallFirms = data_tuples[split_index+1:]  # Skip the (nan, nan, nan, nan) tuple
        smallFirms =smallFirms [3:]

        
        return bigFirms,smallFirms, None

    except Exception as e:
        return None,None, str(e)

# Function to clean the industry column strings
def clean_string(s):
    if isinstance(s, str):
        # Remove leading/trailing whitespaces and replace multiple spaces with a single space
        return ' '.join(s.split())
    return s


def getLastUpdate(url,textToFind):
    response = requests.get(url, verify=False)
    response.raise_for_status()  # Ensure we notice bad responses

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

    
def getLastUpdate_crp(url,textToFind):
    response = requests.get(url, verify=False)
    response.raise_for_status()  # Ensure we notice bad responses

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
