if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import requests
import os
from dotenv import load_dotenv
from mage_ai.data_preparation.repo_manager import get_repo_path
from mage_ai.data_preparation.variable_manager import VariableManager

# Load .env file
load_dotenv()


def notify_slack(pipeline_name, status, db_data=None, dbt_status=None):
    """
    Sends a notification to Slack with the pipeline status and DBT execution information.
    """
    # Access the webhook URL from environment variables
    webhook_url = os.getenv('SLACK_WEBHOOK_URL')

    if not webhook_url:
        raise ValueError("Slack webhook URL is not set in the environment variables.")

    # Prepare the message
    if db_data and isinstance(db_data, int):
        message_text = (
            f"Pipeline `{pipeline_name}` has completed with status: {status}.\n"
            f"Number of rows exported to the database: {db_data}\n"
            f"DBT execution status: {'Executed' if dbt_status else 'Skipped'}."
        )
    else:
        message_text = (
            f"Pipeline `{pipeline_name}` has completed with status: {status}.\n"
            f"No data was exported.\n"
            f"DBT execution status: {'Executed' if dbt_status else 'Skipped'}."
        )

    message = {"text": message_text}

    # Send the Slack message
    response = requests.post(webhook_url, json=message)

    # Debug: Print the response
    print(f"Response status code: {response.status_code}")
    print(f"Response text: {response.text}")


@custom
def transform_custom(*args, **kwargs):
    """
    Sends a Slack notification with the number of rows exported and DBT status.
    """
    # Retrieve the DataFrame from the upstream block, if available
    df_data = args[0] if args else None

    # Calculate the number of rows exported
    num_rows_exported = len(df_data) if df_data is not None else 0

    # Debug: Print the row count
    print(f"Number of rows exported: {num_rows_exported}")

    # Check the global variable `has_new_rows` to determine if DBT was run
    variable_manager = VariableManager(get_repo_path())
    try:
        has_new_rows = variable_manager.get_variable('default', 'has_new_rows', 'has_new_rows')
    except Exception as e:
        print(f"Error fetching 'has_new_rows': {e}")
        has_new_rows = False

    # Notify Slack
    notify_slack(
        pipeline_name="data_real_estate",
        status="Success",
        db_data=num_rows_exported,
        dbt_status=has_new_rows
    )

    return num_rows_exported


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
