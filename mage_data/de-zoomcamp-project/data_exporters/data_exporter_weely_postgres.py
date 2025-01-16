from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, *args, **kwargs) -> None:
    """
    Export the cleaned DataFrame from the transformer block to a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.
    """
    # Define the schema and table names
    schema_name = 'public'  # Adjust this if you have a custom schema
    table_name = 'real_estate_data_weekly'  # Name of the table for the exported data

    # Path to the database configuration file
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Validate the DataFrame
    if df is None or df.empty:
        raise ValueError("The DataFrame is empty or undefined. Cannot export to PostgreSQL.")

    print(f"Exporting DataFrame to PostgreSQL table '{schema_name}.{table_name}'...")
    print(f"DataFrame columns: {df.columns}")

    # Export the DataFrame to PostgreSQL
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name=schema_name,
            table_name=table_name,
            index=False,  # Do not include the DataFrame index in the exported table
            if_exists='replace',  # Replace the table if it already exists
        )
    print(f"Data exported successfully to '{schema_name}.{table_name}'.")
