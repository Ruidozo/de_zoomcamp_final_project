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
    url = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/georef-portugal-distrito/exports/csv?lang=en&timezone=Europe%2FLondon&use_labels=true&delimiter=%3B"
    response = requests.get(url)

    # Check for successful response
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")

    # Convert response text to DataFrame
    df = pd.read_csv(io.StringIO(response.text), sep=';')

    # Parse Geo Shape column into geometry
    df['geometry'] = df['Geo Shape'].apply(lambda x: shape(eval(x)) if pd.notnull(x) else None)

    # Convert to GeoPandas DataFrame
    gdf = gpd.GeoDataFrame(df, geometry='geometry')

    # Rename relevant columns for clarity
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
