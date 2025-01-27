if 'condition' not in globals():
    from mage_ai.data_preparation.decorators import condition
from mage_ai.data_preparation.repo_manager import get_repo_path
from mage_ai.data_preparation.variable_manager import VariableManager


@condition
def evaluate_condition(*args, **kwargs) -> bool:
    """
    Evaluate whether the DBT block should run based on the presence of new rows.
    """
    # Access the global variable to check if there are new rows
    variable_manager = VariableManager(get_repo_path())
    has_new_rows = variable_manager.get_variable('default', 'has_new_rows')

    # Return True if there are new rows, False otherwise
    return has_new_rows
