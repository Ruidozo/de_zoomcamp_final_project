if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_ai.io.postgres import Postgres
from mage_ai.io.config import ConfigFileLoader
from mage_ai.settings.repo import get_repo_path
from os import path
import pandas as pd


@custom
def transform_custom(*args, **kwargs):
    """
    Calculate detailed statistics for all columns in a PostgreSQL table.
    Return only the evaluation data for export.
    """
    # Load PostgreSQL configuration
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Specify the table name
    table_name = 'public.real_estate_data'

    def summary(df):
        """
        Calculate detailed statistics for a DataFrame and include column names.
        """
        print(f"Data shape: {df.shape}")

        # Start building the summary DataFrame
        summary_data = {
            "Column Name": df.columns,  # Add column names explicitly
            "Data Type": df.dtypes.astype(str),  # Convert dtypes to string
            "Missing#": df.isna().sum(),
            "Missing%": (df.isna().sum() / len(df)) if len(df) > 0 else 0,
            "Dups": [df.duplicated().sum()] * len(df.columns),  # Apply duplicated count to all rows
            "Uniques": df.nunique().values,
            "Count": df.count().values,
        }

        # Calculate numeric statistics (only for numeric columns)
        numeric_columns = df.select_dtypes(include=["number"])
        if not numeric_columns.empty:
            numeric_stats = numeric_columns.describe().transpose()
            summary_data["Min"] = numeric_stats["min"].reindex(df.columns, fill_value=None)
            summary_data["Max"] = numeric_stats["max"].reindex(df.columns, fill_value=None)
            summary_data["Average"] = numeric_stats["mean"].reindex(df.columns, fill_value=None)
            summary_data["Standard Deviation"] = numeric_stats["std"].reindex(df.columns, fill_value=None)
        else:
            # Add empty placeholders for numeric stats if there are no numeric columns
            summary_data["Min"] = [None] * len(df.columns)
            summary_data["Max"] = [None] * len(df.columns)
            summary_data["Average"] = [None] * len(df.columns)
            summary_data["Standard Deviation"] = [None] * len(df.columns)

        # Create the summary DataFrame
        summ = pd.DataFrame(summary_data)
        return summ


    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # Fetch all data from the table
        query_fetch_data = f"SELECT * FROM {table_name};"
        df = loader.load(query_fetch_data)

        print(f"Data fetched: {df.shape}")

        # Calculate summary statistics
        stats = summary(df)

        # Return only the evaluation data (summary statistics)
        print(f"Data_stats fetched: {stats.shape}")
        return stats


@test
def test_output(output, *args) -> None:
    """
    Test to ensure the evaluation DataFrame is valid.
    """
    assert output is not None, "The output is undefined"
    assert isinstance(output, pd.DataFrame), "The output is not a DataFrame"
    assert not output.empty, "The output DataFrame is empty"
    required_columns = [
        "Column Name", "Data Type", "Missing#", "Missing%", "Dups", "Uniques", "Count",
        "Min", "Max", "Average", "Standard Deviation"
    ]
    for col in required_columns:
        assert col in output.columns, f"Missing expected column: {col}"
