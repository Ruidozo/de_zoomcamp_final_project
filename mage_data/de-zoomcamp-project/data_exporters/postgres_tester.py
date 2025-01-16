from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df, **kwargs) -> None:
    """
    Template for exporting data to a PostgreSQL database.
    """
    import pandas as pd

    # Convert to DataFrame if input is a list
    if isinstance(df, list):
        print("Input is a list. Converting to DataFrame.")
        df = pd.DataFrame(df)

    print(f"Input content: {df}")
    print(f"Input type: {type(df)}")


    # Validate DataFrame
    if df is None or df.empty:
        raise ValueError("The DataFrame is empty or undefined. Cannot export to PostgreSQL.")

    print(f"DataFrame shape: {df.shape}")
    print(f"DataFrame columns: {df.columns}")
    print(f"DataFrame preview:\n{df.head()}")

    schema_name = 'public'
    table_name = 'real_estate_data'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,
            if_exists='replace',
        )
    print(f"Data exported to PostgreSQL table '{schema_name}.{table_name}'.")
