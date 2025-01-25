import os
import pandas as pd
import kagglehub
import gc  # Import garbage collection module

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data_from_kaggle(*args, **kwargs):
    """
    Load data from Kaggle using KaggleHub.
    Free memory before starting the pipeline.
    """
    try:
        # Free up memory before loading data
        gc.collect()
        print("Memory freed before loading data.")

        # Download the latest version of the dataset
        path = kagglehub.dataset_download("luvathoms/portugal-real-estate-2024")
        print("Path to dataset files:", path)

        # Print the contents of the directory for debugging
        print("Downloaded files:", os.listdir(path))

        # Look for a CSV file in the dataset directory
        dataset_file = None
        for file in os.listdir(path):
            if file.endswith(".csv"):
                dataset_file = os.path.join(path, file)
                break
        
        if not dataset_file:
            raise FileNotFoundError(f"No CSV file found in dataset directory: {path}")
        
        # Load the data into a pandas DataFrame
        df = pd.read_csv(dataset_file)

        # Display dataset summary
        print(f"Dataset loaded with {df.shape[0]} rows and {df.shape[1]} columns.")
        print(df.head())
        
        return df
    except Exception as e:
        print(f"Error during data loading: {e}")
        return pd.DataFrame()

@test
def test_output(output, *args) -> None:
    """
    Test the output of the Kaggle data loader block.
    """
    assert output is not None, 'The output is undefined'
    assert not output.empty, 'The output DataFrame is empty'
