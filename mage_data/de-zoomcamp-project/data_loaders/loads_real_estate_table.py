from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_postgres(*args, **kwargs):
    """
    Load the overall PostgreSQL table into a DataFrame.
    Specify your configuration settings in 'io_config.yaml'.
    """
    # SQL query to fetch data from the overall table
    query = """
    SELECT *
    FROM public.real_estate_data  -- Replace with your actual table name and schema
    """
    
    # Path to the configuration file
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Connect to PostgreSQL and execute the query
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        print(f"Loading data from PostgreSQL table 'public.real_estate_data'...")
        return loader.load(query)


@test
def test_output(output, *args) -> None:
    """
    Test that the output DataFrame is not None or empty.
    """
    assert output is not None, 'The output is undefined'
    assert not output.empty, 'The output DataFrame is empty'
