from pandas import DataFrame
from mage_ai.data_preparation.repo_manager import get_repo_path
from mage_ai.data_preparation.variable_manager import VariableManager

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def remove_duplicates(weekly_df: DataFrame, overall_df: DataFrame, *args, **kwargs) -> DataFrame:
    """
    Compares weekly_df with overall_df and removes rows in weekly_df
    that have duplicate 'unique_id' values in overall_df.
    Also flags if there are new rows to process.
    """
    # Ensure the column for comparison exists in both DataFrames
    column_to_compare = 'unique_id'
    if column_to_compare not in weekly_df.columns:
        raise ValueError(f"Column '{column_to_compare}' is missing from weekly_df.")
    if column_to_compare not in overall_df.columns:
        raise ValueError(f"Column '{column_to_compare}' is missing from overall_df.")

    # Drop rows in weekly_df where 'unique_id' exists in overall_df
    print(f"Removing duplicates based on '{column_to_compare}'...")
    filtered_weekly_df = weekly_df[~weekly_df[column_to_compare].isin(overall_df[column_to_compare])]

    # Log the number of rows before and after filtering
    print(f"Rows in weekly_df before filtering: {len(weekly_df)}")
    print(f"Rows in weekly_df after filtering: {len(filtered_weekly_df)}")

    # Check if there are new rows and set a global variable
    has_new_rows = len(filtered_weekly_df) > 0

    # Initialize the VariableManager and add the variable
    variable_manager = VariableManager(get_repo_path())
    variable_manager.add_variable('default', 'has_new_rows', 'has_new_rows', has_new_rows)

    if has_new_rows:
        print("New rows found. Setting variable 'has_new_rows' to True.")
    else:
        print("No new rows found. Setting variable 'has_new_rows' to False.")

    return filtered_weekly_df


@test
def test_output(output, *args) -> None:
    """
    Test to ensure duplicates are removed correctly.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, DataFrame), 'The output is not a DataFrame'
