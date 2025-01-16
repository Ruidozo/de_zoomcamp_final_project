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
    Export Kaggle dataset to a PostgreSQL database with proper table recreation and schema alignment.
    """
    import pandas as pd

    # Validate DataFrame
    if df is None or df.empty:
        raise ValueError("The DataFrame is empty or undefined. Cannot export to PostgreSQL.")

    print(f"Original DataFrame columns: {df.columns}")

    # Clean column names to match PostgreSQL requirements
    df.columns = (
        df.columns.str.strip()                 # Remove leading/trailing spaces
        .str.lower()                           # Convert to lowercase
        .str.replace(' ', '_')                # Replace spaces with underscores
        .str.replace(r'[^a-zA-Z0-9_]', '')    # Remove invalid characters
    )
    print(f"Cleaned DataFrame columns: {df.columns}")

    # Set schema and table name
    schema_name = 'public'  # Default PostgreSQL schema
    table_name = 'real_estate_data'

    # Load PostgreSQL configuration
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # Drop the table if it exists (explicitly)
        print(f"Dropping table '{schema_name}.{table_name}' if it exists...")
        loader.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE;")

        # Export the DataFrame to PostgreSQL
        loader.export(
            df,
            schema_name=schema_name,
            table_name=table_name,
            index=False,        # Do not export the index
            if_exists='replace' # Replace the table if it already exists
        )
    print(f"Data exported to PostgreSQL table '{schema_name}.{table_name}'.")
