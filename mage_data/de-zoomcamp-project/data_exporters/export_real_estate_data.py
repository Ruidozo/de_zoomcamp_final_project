from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Append the filtered data to the real_estate_data table in PostgreSQL.
    If the DataFrame is empty, log a message and exit gracefully.
    """
    schema_name = 'public'  # Adjust this if you have a custom schema
    table_name = 'real_estate_data'  # Name of the table for the exported data
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Check if the DataFrame is empty
    if df is None or df.empty:
        print("No new data to append. Exiting block.")
        return

    print(f"Appending data to PostgreSQL table '{schema_name}.{table_name}'...")
    print(f"DataFrame contains {len(df)} rows and {len(df.columns)} columns.")

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,  # Do not include the index in the exported table
            if_exists='append',  # Append data to the table
        )
    print(f"Data appended successfully to '{schema_name}.{table_name}'.")
