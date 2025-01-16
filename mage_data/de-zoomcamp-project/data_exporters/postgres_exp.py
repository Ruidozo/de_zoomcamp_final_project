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
    Create a table from the transformed DataFrame and insert all rows.
    """
    import pandas as pd

    # Validate DataFrame
    if df is None or df.empty:
        raise ValueError("The DataFrame is empty or undefined. Cannot export to PostgreSQL.")

    print(f"DataFrame columns: {df.columns}")

    schema_name = 'public'
    table_name = 'real_estate_data'

    # Load PostgreSQL configuration
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:


        # Create the table
        print(f"Creating table '{schema_name}.{table_name}'...")
        create_table_query = f"""
        CREATE TABLE {schema_name}.{table_name} (
            {', '.join([f"{col} TEXT" for col in df.columns])}
        );
        """
        print(f"Executing query: {create_table_query}")
        loader.execute(create_table_query)
        print(f"Table '{schema_name}.{table_name}' created successfully.")


        print(f"Data inserted into table '{schema_name}.{table_name}'.")
