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
    Export Kaggle dataset to PostgreSQL, appending only unique rows using an interim table.
    Creates the final table if it does not exist.
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
    # Rename _type to type if exists (ensure it happens early)
    if '_type' in df.columns:
        print("Renaming '_type' to 'type'...")
        df.rename(columns={'_type': 'type'}, inplace=True)

    print(f"Cleaned DataFrame columns: {df.columns}")

    # Ensure '_type' is no longer in the DataFrame
    if '_type' in df.columns:
        raise ValueError("'_type' column still exists after renaming. Please check the column cleaning process.")

    # Create a unique identifier column
    def generate_unique_id(row):
        # Numbers: Use the full value
        price = str(row['price'])
        total_area = str(row['totalarea'])

        # Letters: Use the first letter of each word
        district = ''.join([word[0] for word in str(row['district']).split()])
        city = ''.join([word[0] for word in str(row['city']).split()])
        town = ''.join([word[0] for word in str(row['town']).split()])
        property_type = ''.join([word[0] for word in str(row['type']).split()])  # Using renamed 'type'

        # Combine all parts into a unique_id without hyphens
        return f"{price}{total_area}{district}{city}{town}{property_type}"

    # Generate unique_id
    df['unique_id'] = df.apply(generate_unique_id, axis=1)

    # Set schema and table names
    schema_name = 'public'
    table_name = 'real_estate_data'
    interim_table_name = f"{table_name}_temp"

    # Load PostgreSQL configuration
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # Check if the table exists
        print(f"Checking if table '{schema_name}.{table_name}' exists...")
        table_check_query = f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
        );
        """
        with loader.conn.cursor() as cursor:
            cursor.execute(table_check_query)
            result = cursor.fetchone()  # Fetch one row

        table_exists = result[0] if result else False  # Access the first column of the result

        # Create the final table if it does not exist
        if not table_exists:
            print(f"Table '{schema_name}.{table_name}' does not exist. Creating it...")
            create_table_query = f"""
            CREATE TABLE {schema_name}.{table_name} (
                {', '.join([f"{col} TEXT" for col in df.columns])},
                unique_id TEXT UNIQUE
            );
            """
            loader.execute(create_table_query)
            print(f"Table '{schema_name}.{table_name}' created.")

        # Drop interim table if it exists (to avoid schema mismatch)
        print(f"Dropping interim table '{schema_name}.{interim_table_name}' if it exists...")
        loader.execute(f"DROP TABLE IF EXISTS {schema_name}.{interim_table_name} CASCADE;")

        # Create interim table explicitly
        print(f"Creating interim table '{schema_name}.{interim_table_name}'...")
        columns_for_table = [f"{col} TEXT" for col in df.columns if col != 'unique_id']
        columns_for_table.append("unique_id TEXT")
        create_interim_table_query = f"""
        CREATE TABLE {schema_name}.{interim_table_name} (
            {', '.join(columns_for_table)}
        );
        """
        loader.execute(create_interim_table_query)

        # Insert data into interim table
        print(f"Inserting data into interim table '{schema_name}.{interim_table_name}'...")
        loader.export(
            df,
            schema_name=schema_name,
            table_name=interim_table_name,
            index=False,
            if_exists='append'  # Append data to the interim table
        )
        print(f"Interim table '{schema_name}.{interim_table_name}' created.")

        # Insert only unique rows from interim table into final table
        insert_query = f"""
        INSERT INTO {schema_name}.{table_name}
        SELECT *
        FROM {schema_name}.{interim_table_name}
        WHERE NOT EXISTS (
            SELECT 1
            FROM {schema_name}.{table_name} final
            WHERE final.unique_id = {schema_name}.{interim_table_name}.unique_id
        );
        """
        print(f"Inserting unique rows into '{schema_name}.{table_name}'...")
        loader.execute(insert_query)
        print(f"Unique rows inserted into '{schema_name}.{table_name}'.")

        # Drop the interim table
        print(f"Dropping interim table '{schema_name}.{interim_table_name}'...")
        loader.execute(f"DROP TABLE IF EXISTS {schema_name}.{interim_table_name} CASCADE;")
        print(f"Interim table '{schema_name}.{interim_table_name}' dropped.")
