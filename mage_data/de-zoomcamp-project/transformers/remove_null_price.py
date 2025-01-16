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
    Remove rows where the price is null or exceeds 250 million EUR.
    """
    # Define the maximum price limit
    max_price = 250_000_000

    df = df[
            df['price'].notnull() & 
            (df['price'] <= max_price) & 
            (df['district'] != 'Z - Fora de Portugal')
        ].reset_index(drop=True)

    return df


@test
def test_output(output, *args) -> None:
    """
    Test to ensure no rows have a null price or a price over 250 million EUR.
    """
    assert output is not None, 'The output is undefined'
    assert not output['price'].isnull().any(), 'There are rows with null prices in the output.'
    assert (output['price'] <= 250_000_000).all(), 'There are rows with a price exceeding 250 million EUR.'
