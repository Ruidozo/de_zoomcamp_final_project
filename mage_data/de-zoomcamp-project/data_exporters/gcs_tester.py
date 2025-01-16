from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from mage_ai.io.config import ConfigFileLoader
from mage_ai.settings.repo import get_repo_path
from pandas import DataFrame
from os import path


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Export data to a Google Cloud Storage bucket, creating the bucket if it does not exist.
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'mage_test_setup'
    object_key = 'real_estate_data.csv'

    gcs = GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile))

    # Ensure the bucket exists
    def ensure_bucket_exists(bucket_name):
        client = gcs.client
        try:
            bucket = client.get_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' already exists.")
        except Exception:
            bucket = client.bucket(bucket_name)
            bucket = client.create_bucket(bucket, location="europe-west2")
            print(f"Bucket '{bucket_name}' created in location 'europe-west2'.")

    # Ensure the bucket exists before exporting
    ensure_bucket_exists(bucket_name)

    # Export the DataFrame to the GCS bucket
    gcs.export(
        df,
        bucket_name,
        object_key,
    )

    print(f"Data exported to GCS bucket '{bucket_name}' at '{object_key}'.")

    # List all buckets in the project
    print("\nListing all GCS buckets:")
    for bucket in gcs.client.list_buckets():
        print(f"- {bucket.name}")
