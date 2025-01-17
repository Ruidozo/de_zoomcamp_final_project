from pandas import DataFrame

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd


@custom
def summary(df):
    print(f"Data shape: {df.shape}")
    summ = pd.DataFrame(df.dtypes.astype(str), columns=["Data Type"])  # Convert dtypes to string
    summ["Missing#"] = df.isna().sum()
    summ["Missing%"] = (df.isna().sum()) / len(df) if len(df) > 0 else 0
    summ["Dups"] = df.duplicated().sum()
    summ["Uniques"] = df.nunique().values
    summ["Count"] = df.count().values

    # Handle numeric stats if DataFrame is not empty
    if not df.empty:
        desc = pd.DataFrame(df.describe(include="all").transpose())
        summ["Min"] = desc.get("min", None).values
        summ["Max"] = desc.get("max", None).values
        summ["Average"] = desc.get("mean", None).values
        summ["Standard Deviation"] = desc.get("std", None).values
        summ["First Value"] = df.iloc[0].values if len(df) > 0 else None
        summ["Second Value"] = df.iloc[1].values if len(df) > 1 else None
        summ["Third Value"] = df.iloc[2].values if len(df) > 2 else None
    else:
        summ["Min"] = None
        summ["Max"] = None
        summ["Average"] = None
        summ["Standard Deviation"] = None
        summ["First Value"] = None
        summ["Second Value"] = None
        summ["Third Value"] = None

    # Ensure all columns have consistent types
    summ = summ.astype(str)
    return summ



@test
def test_output(output, *args) -> None:
    """
    Test to ensure the summary function works correctly.
    """
    assert output is not None, "The output is undefined"
    assert isinstance(output, pd.DataFrame), "The output is not a DataFrame"
