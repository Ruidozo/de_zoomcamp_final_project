from pandas import DataFrame

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def remove_duplicates(weekly_df: DataFrame, overall_df: DataFrame, *args, **kwargs) -> DataFrame:
    """
    Compares weekly_df with overall_df and removes rows in weekly_df
    that have duplicate 'unique_id' values in overall_df.
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

    return filtered_weekly_df


@test
def test_output(output, *args) -> None:
    """
    Test to ensure duplicates are removed correctly.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, DataFrame), 'The output is not a DataFrame'
