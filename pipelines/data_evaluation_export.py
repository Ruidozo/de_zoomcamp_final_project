from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path
import pandas as pd
import numpy as np

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(stats: DataFrame, **kwargs) -> None:
    """
    Export the evaluation DataFrame (summary statistics) to a PostgreSQL table.
    """
    schema_name = 'public'  # Specify the schema
    table_name = 'data_evaluation_table'  # Specify the table name
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Debug input type and content
    print(f"Received input type: {type(stats)}")
    if not isinstance(stats, pd.DataFrame):
        raise ValueError(f"Expected a DataFrame, but got {type(stats)} instead.")

    # Debug DataFrame structure
    print(f"DataFrame contains {len(stats)} rows and {len(stats.columns)} columns.")
    print(f"DataFrame dtypes before conversion:\n{stats.dtypes}")
    print(f"DataFrame preview:\n{stats.head()}")

    # Step 1: Replace invalid values
    print("Cleaning DataFrame for PostgreSQL export...")
    stats = stats.replace({np.nan: None, "NaN": None, pd.NA: None})
    stats = stats.fillna("")  # Replace NaN with empty strings

    # Force all columns to string type
    stats = stats.applymap(str)  # Convert every cell to a string explicitly

    # Debug cleaned DataFrame
    print(f"DataFrame dtypes after conversion:\n{stats.dtypes}")
    print(f"Cleaned DataFrame preview:\n{stats.to_string()}")

    # Step 2: Export DataFrame to PostgreSQL
    try:
        with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
            loader.export(
                stats,
                schema_name=schema_name,
                table_name=table_name,
                index=False,  # Exclude the DataFrame index
                if_exists='replace',  # Replace the table if it already exists
            )
        print(f"Data exported successfully to '{schema_name}.{table_name}'.")
    except Exception as e:
        print(f"Error during export: {e}")
        raise
