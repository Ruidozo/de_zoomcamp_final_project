import json
import requests
import pandas as pd
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the full path to the JSON file
json_file_path = os.path.join(script_dir, 'municipios.json')

# Load the list of municipalities from the JSON file
with open(json_file_path, 'r', encoding='utf-8') as file:
    municipios = json.load(file)

# Configuration for the OpenStreetMap API
base_url = "https://nominatim.openstreetmap.org/search"

# Create a list to store results
resultados = []

# Get the coordinates of each municipality
for municipio in municipios:
    params = {
        'q': municipio + ", Portugal",  # Search for the municipality in Portugal
        'format': 'json',
        'addressdetails': 1,
        'limit': 1
    }
    response = requests.get(base_url, params=params, headers={'User-Agent': 'YourAppName/1.0'})
    
    # Debugging: Print the request URL and parameters
    print(f"Request URL: {response.url}")
    print(f"Request Params: {params}")
    
    if response.status_code == 200:
        response_data = response.json()
        # Debugging: Print the response content
        print(f"Response Data: {response_data}")
        
        if response_data:
            data = response_data[0]
            latitude = data.get('lat')
            longitude = data.get('lon')
            resultados.append({'Município': municipio, 'Latitude': latitude, 'Longitude': longitude})
        else:
            print(f"Não foi possível encontrar geolocalização para: {municipio}")
    else:
        print(f"Erro na requisição para: {municipio}, Status Code: {response.status_code}")

# Create a DataFrame from the results
resultados_df = pd.DataFrame(resultados)

# Print the table to the console
print("\nTabela de Geolocalização:")
print(resultados_df)
