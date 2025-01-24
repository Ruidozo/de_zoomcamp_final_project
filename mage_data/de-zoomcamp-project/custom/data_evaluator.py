from mage_ai.io.postgres import Postgres
from mage_ai.io.config import ConfigFileLoader
from mage_ai.settings.repo import get_repo_path
from os import path
import pandas as pd
import time

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(*args, **kwargs):
    """
    Calculate detailed statistics for all columns in a PostgreSQL table.
    Return only the evaluation data for export, with a 1-minute delay before execution.
    """
    # Load PostgreSQL configuration
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Specify the table name
    table_name = 'public.real_estate_data_weekly'

    # Add a delay 
    delay_seconds = 30
    print(f"Delaying execution by {delay_seconds} seconds...")
    time.sleep(delay_seconds)
    print("Resuming execution after delay.")

    def summary(df):
        """
        Calculate detailed statistics for a DataFrame and include column names.
        """
        print(f"Data shape: {df.shape}")

        # Start building the summary DataFrame
        summary_data = {
            "Column Name": df.columns,  # Add column names explicitly
            "Data Type": df.dtypes.astype(str),  # Convert dtypes to string
            "Missing percent": (df.isna().sum() / len(df)) if len(df) > 0 else 0,
            "Missing quant": df.isna().sum(),
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

        # Cast columns to match PostgreSQL schema
        summ["Missing percent"] = summ["Missing percent"].astype(float)  # Cast to float
        summ["Missing quant"] = summ["Missing quant"].astype(int)  # Cast to integer

        # Replace NaN values with appropriate defaults
        for col in summ.columns:
            if summ[col].isna().any():
                print(f"Column '{col}' contains NaN values. Filling with appropriate value.")
                if summ[col].dtype in ['float64', 'int64']:  # Numeric columns
                    summ[col] = summ[col].fillna(0)  # Replace NaN with 0 for numeric
                else:  # Object columns
                    summ[col] = summ[col].fillna("")  # Replace NaN with empty string for objects

        # Drop rows where all numeric values are 0
        numeric_cols = summ.select_dtypes(include=["number"]).columns
        summ = summ[~(summ[numeric_cols].sum(axis=1) == 0)]

        return summ

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # Fetch all data from the table
        query_fetch_data = f"SELECT * FROM {table_name};"
        df = loader.load(query_fetch_data)

        # Debugging: Print DataFrame columns
        print("Original DataFrame Columns:", df.columns)

        # Align DataFrame columns to match PostgreSQL table
        expected_columns = [
            "unique_id", "price", "district", "city", "town", "_type",
            "energy_certificate", "gross_area", "total_area", "parking",
            "has_parking", "_floor", "construction_year", "energy_efficiency_level",
            "publish_date", "garage", "elevator", "electric_cars_charging",
            "total_rooms", "number_of_bedrooms", "number_of_w_c",
            "conservation_status", "living_area", "lot_size", "built_area",
            "number_of_bathrooms"
        ]
        df = df[expected_columns]

        print("Post-alignment DataFrame Columns:", df.columns)
        print(f"Data fetched: {df.shape}")

        # Calculate summary statistics
        stats = summary(df)

        # Return only the evaluation data (summary statistics)
        print(f"Data_stats fetched: {stats.shape}")
        return stats
