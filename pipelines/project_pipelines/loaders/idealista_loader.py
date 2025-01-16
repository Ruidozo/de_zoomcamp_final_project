import requests
import pandas as pd
import os
from dotenv import load_dotenv
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# Load environment variables from the .env file
load_dotenv('/home/src/.env')

@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Load data from the Idealista API using a POST request.
    """
    # Step 1: Get OAuth Token
    def get_access_token():
        url = "https://api.idealista.com/oauth/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "client_credentials",
            "scope": "read",
            "client_id": os.getenv('CLIENT_ID'),  # Corrected syntax: Keys must be strings
            "client_secret": os.getenv('CLIENT_SECRET'),  # Corrected syntax
        }
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()  # Raise an error for bad responses
        return response.json()["access_token"]

    # Step 2: Make the API call
    def fetch_real_estate_data(token):
        url = "https://api.idealista.com/3.5/pt/search"
        headers = {"Authorization": f"Bearer {token}"}
        data = {
            "center": "41.55294951984219,-8.309814888638995",  # Coordinates (latitude, longitude)
            "propertyType": "homes",   # Type of property
            "distance": "15000",       # Radius in meters
            "operation": "sale",       # Operation type
        }
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()  # Raise an error for bad responses
        return response.json()

    try:
        # Authenticate and fetch data
        token = get_access_token()
        api_response = fetch_real_estate_data(token)

        # Extract the element list into a DataFrame
        element_list = api_response.get("elementList", [])
        df = pd.DataFrame(element_list)

        print("Fetched data:", df.head())
        return df
    except Exception as e:
        print(f"Error during API call: {e}")
        return pd.DataFrame()

@test
def test_output(output, *args) -> None:
    """
    Test the output of the data loader block.
    """
    assert output is not None, 'The output is undefined'
    assert not output.empty, 'The output DataFrame is empty'
    assert 'propertyCode' in output.columns, 'PropertyCode is missing in the output'
