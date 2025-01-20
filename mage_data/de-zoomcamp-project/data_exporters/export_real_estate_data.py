from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> DataFrame:
    """
    Append the filtered data to the real_estate_data table in PostgreSQL and return the DataFrame.
    """
    schema_name = 'public'
    table_name = 'real_estate_data'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    if df is None or df.empty:
        print("No new data to append. Exiting block.")
        return None

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,
            if_exists='append',
        )
    print(f"Data appended successfully to '{schema_name}.{table_name}'.")
    return df  # Return the DataFrame
