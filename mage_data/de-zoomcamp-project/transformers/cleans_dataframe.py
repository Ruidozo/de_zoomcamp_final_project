from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
from pandas import DataFrame
from datetime import datetime

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def execute_transformer_action(df: DataFrame, *args, **kwargs) -> DataFrame:
    """
    Clean column names, add a 'unique_id' column, and move it to the first position.
    """
    # Step 1: Clean column names
    action = build_transformer_action(
        df,
        action_type=ActionType.CLEAN_COLUMN_NAME,
        arguments=df.columns,
        axis=Axis.COLUMN,
    )
    df = BaseAction(action).execute(df)

    # Step 2: Rename '_type' to 'type' if it exists
    if '_type' in df.columns:
        print("Renaming '_type' to 'type'...")
        df.rename(columns={'_type': 'type'}, inplace=True)

    # Step 3: Add 'unique_id' column if not already present
    if 'unique_id' not in df.columns:
        print("Adding 'unique_id' column...")
        def generate_unique_id(row):
            # Numbers: Use the full value
            total_area = str(row.get('totalarea', ''))

            # Letters: Use the first letter of each word
            floor = ''.join([word[0] for word in str(row.get('floor', '')).split()])
            district = ''.join([word[0] for word in str(row.get('district', '')).split()])
            city = ''.join([word[0] for word in str(row.get('city', '')).split()])
            town = ''.join([word[0] for word in str(row.get('town', '')).split()])
            property_type = ''.join([word[0] for word in str(row.get('type', '')).split()])

            # Combine all parts into a unique_id without hyphens
            return f"{floor}{total_area}{district}{city}{town}{property_type}"

        df['unique_id'] = df.apply(generate_unique_id, axis=1)

    # Step 4: Move 'unique_id' to the first column
    columns = ['unique_id'] + [col for col in df.columns if col != 'unique_id']
    df = df[columns]

    # Steop 4: Add a push data column
    df['push_date'] = datetime.utcnow().date()  # Add today's UTC date


    # Step 5: Return the cleaned DataFrame
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert output.columns[0] == 'unique_id', "'unique_id' column is not the first column in the output."
    assert 'type' in output.columns, "'type' column is missing from the output."
    assert '_type' not in output.columns, "'_type' column still exists in the output."