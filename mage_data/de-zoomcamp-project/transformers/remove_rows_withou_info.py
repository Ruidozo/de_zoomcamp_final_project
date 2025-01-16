from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
from pandas import DataFrame

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def execute_transformer_action(df: DataFrame, *args, **kwargs) -> DataFrame:
    """
    Execute Transformer Action: Remove empty rows and rows where 'province' does not exist.
    """

    # Remove rows where 'province' column does not exist or is empty
    if 'province' in df.columns:
        df = df[df['province'].notna() & (df['province'] != '')]

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert not output.empty, 'The output DataFrame is empty'
    assert 'province' in output.columns, 'Expected column "province" not found in output'
    assert output['province'].notna().all(), 'There are rows with missing "province" information'