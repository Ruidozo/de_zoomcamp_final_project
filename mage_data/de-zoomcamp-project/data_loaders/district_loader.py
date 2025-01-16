import io
import pandas as pd
import geopandas as gpd
import requests
from shapely.geometry import shape

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Load district data from the public API and return it as a GeoPandas DataFrame.
    """
    # Fetch data from the API
    url = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/georef-portugal-distrito/exports/csv?lang=en&timezone=Europe%2FLondon&use_labels=true&delimiter=%3B"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")

    # Load data into a Pandas DataFrame
    df = pd.read_csv(io.StringIO(response.text), sep=';')

    # Parse the 'Geo Shape' column into geometry
    df['geometry'] = df['Geo Shape'].apply(lambda x: shape(eval(x)) if pd.notnull(x) else None)

    # Convert the DataFrame to a GeoPandas GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry='geometry')

    # Rename columns for easier usage
    gdf = gdf.rename(columns={
        'Official Name District': 'District Name',
        'Geo Point': 'Geo Coordinates'
    })

    # Select only necessary columns
    gdf = gdf[['District Name', 'Geo Coordinates', 'geometry']]

    # Debugging outputs
    print(f"GeoDataFrame shape: {gdf.shape}")
    print(f"GeoDataFrame columns: {gdf.columns}")
    print(f"GeoDataFrame preview:\n{gdf.head()}")

    # Redirect output to a temporary file to avoid directory issues
    valid_output_path = "/tmp/district_data_temp.geojson"
    gdf.to_file(valid_output_path, driver="GeoJSON")
    print(f"GeoDataFrame temporarily saved to {valid_output_path}")

    # Return GeoDataFrame to continue in-memory processing
    return gdf


@test
def test_output(output, *args) -> None:
    """
    Test the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert not output.empty, 'The output GeoDataFrame is empty'
    assert 'District Name' in output.columns, 'Expected column "District Name" not found in output'
    assert 'geometry' in output.columns, 'Expected column "geometry" not found in output'
