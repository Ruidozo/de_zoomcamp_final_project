from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from sqlalchemy import create_engine
import pandas as pd
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, *args, **kwargs) -> None:
    """
    Export the DataFrame `stats` to an existing PostgreSQL table using raw SQL.
    """
    schema_name = 'public'
    table_name = 'stats'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Load database configuration
    config = ConfigFileLoader(config_path, config_profile).config
    connection_string = f"postgresql://{config['POSTGRES_USER']}:{config['POSTGRES_PASSWORD']}@" \
                        f"{config['POSTGRES_HOST']}:{config['POSTGRES_PORT']}/{config['POSTGRES_DBNAME']}"

    print(f"Connecting to {connection_string}")

    # Align DataFrame columns with the table schema
    expected_columns = [
        "Column Name", "Data Type", "Missing#", "Missing%", "Dups", "Uniques",
        "Count", "Min", "Max", "Average", "Standard Deviation"
    ]
    df = df[expected_columns]

    # Replace NaN values with appropriate defaults
    for col in df.columns:
        if df[col].isna().any():
            print(f"Column '{col}' contains NaN values. Filling with appropriate value.")
            if df[col].dtype in ['float64', 'int64']:  # Numeric columns
                df[col] = df[col].fillna(pd.NA)  # Use pd.NA for numeric columns
            else:  # Object columns
                df[col] = df[col].fillna(None)  # Use None for object columns

    # Debugging: Check DataFrame alignment
    print("Aligned DataFrame:")
    print(df.info())

    # Convert DataFrame to tuples for insertion
    data = [tuple(row) for row in df.itertuples(index=False, name=None)]

    # Debugging: Validate data alignment
    print("Validating data alignment...")
    for i, row in enumerate(data):
        if len(row) != len(df.columns):
            print(f"Mismatch in row {i}: Expected {len(df.columns)} values, got {len(row)}")
            print(f"Row content: {row}")
            raise ValueError("Row length mismatch detected!")

    # Connect to PostgreSQL
    engine = create_engine(connection_string)

    print(f"Inserting data into {schema_name}.{table_name}...")
    with engine.connect() as conn:
        # Truncate the table if replacing
        conn.execute(f"TRUNCATE TABLE {schema_name}.{table_name};")

        # Create INSERT query
        columns = ', '.join(f'"{col}"' for col in df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({placeholders});"

        # Debugging: Print query and data sample
        print("Insert Query:")
        print(insert_query)
        print("Data Sample:")
        print(data[:5])

        # Execute the INSERT query
        with conn.connection.cursor() as cursor:
            cursor.executemany(insert_query, data)
            conn.connection.commit()

    print(f"Data successfully inserted into {schema_name}.{table_name}.")
