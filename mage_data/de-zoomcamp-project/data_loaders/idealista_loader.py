import requests
import pandas as pd
import os
from shapely.geometry import Point, Polygon
from dotenv import load_dotenv
import time

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# Load environment variables from the .env file
load_dotenv()

@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Fetch all data from the Idealista API for Portugal by iterating over grid points in batches.
    Filter the listings to include only those inside Portugal's boundaries.
    """
    # Step 1: Get OAuth Token
    def get_access_token():
        url = "https://api.idealista.com/oauth/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "client_credentials",
            "scope": "read",
            "client_id": os.getenv('CLIENT_ID'),
            "client_secret": os.getenv('CLIENT_SECRET'),
        }
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        return response.json()["access_token"]

    # Step 2: Make the API call for a specific center point and page
    def fetch_real_estate_data(token, center, page):
        url = "https://api.idealista.com/3.5/pt/search"
        headers = {"Authorization": f"Bearer {token}"}
        data = {
            "country": "pt",
            "operation": "sale",
            "propertyType": "homes",
            "center": center,
            "distance": "10000",  # Larger radius to reduce grid density
            "maxItems": 50,       # Fetch 50 items per page
            "numPage": page,      # Specify the page number
        }

        retries = 5
        for attempt in range(retries):
            try:
                response = requests.post(url, headers=headers, data=data)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:  # Handle rate limit
                    print(f"Rate limit hit. Retrying in {2 ** attempt} seconds...")
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise e
        raise Exception("Failed to fetch data after multiple retries.")

    # Define Portugal's boundaries using a polygon
    portugal_polygon = Polygon([
        (-9.5268, 36.8385),  # Southwest corner
        (-6.189, 36.9966),   # Southeast corner
        (-6.3855, 42.1543),  # Northeast corner
        (-9.5143, 41.9787),  # Northwest corner
        (-9.5268, 36.8385),  # Closing the polygon
    ])

    # Helper function to check if a point is within Portugal
    def is_within_portugal(lat, lon):
        try:
            point = Point(lon, lat)  # Shapely expects (longitude, latitude)
            return portugal_polygon.contains(point)
        except ValueError:
            return False

    # Generate a grid of center points across Portugal (smaller batch)
    center_points = [
        "38.7169,-9.1390",  # Lisbon
        "41.1579,-8.6291",  # Porto
        "37.0179,-7.9304",  # Faro
        "39.8222,-7.4909",  # Castelo Branco
        "40.5373,-7.2670",  # Guarda
    ]

    all_data = pd.DataFrame()

    try:
        # Authenticate
        token = get_access_token()

        # Process the center points in batches
        batch_size = 2  # Adjust the batch size as needed
        for i in range(0, len(center_points), batch_size):
            batch = center_points[i:i + batch_size]
            print(f"Processing batch: {batch}")
            for center in batch:
                print(f"Fetching data for center: {center}")
                page = 1
                while True:
                    api_response = fetch_real_estate_data(token, center, page)
                    element_list = api_response.get("elementList", [])
                    if not element_list:  # If no more data, break the loop
                        break

                    df = pd.DataFrame(element_list)
                    if 'latitude' in df.columns and 'longitude' in df.columns:
                        # Filter listings within Portugal
                        df = df[df.apply(lambda x: is_within_portugal(x['latitude'], x['longitude']), axis=1)]
                        all_data = pd.concat([all_data, df], ignore_index=True)

                    print(f"Page {page} fetched for center: {center}")
                    page += 1
                    time.sleep(1)  # Small delay between pages

            # Delay between batches to avoid hitting the rate limit
            print("Waiting before processing the next batch...")
            time.sleep(10)  # Add a longer delay between batches

        # Print the total number of rows
        total_rows = all_data.shape[0]
        print(f"Total number of rows: {total_rows}")

        print("Filtered data inside Portugal:")
        print(all_data.head())
        return all_data
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
    assert 'latitude' in output.columns and 'longitude' in output.columns, 'Latitude and Longitude columns are missing'
