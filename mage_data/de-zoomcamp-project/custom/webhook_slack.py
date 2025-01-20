if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import requests

def notify_slack(pipeline_name, status, db_data=None):
    """
    Sends a notification to Slack with the pipeline status and the number of rows exported.
    """
    webhook_url = "https://hooks.slack.com/services/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"

    # Prepare the message
    if db_data and isinstance(db_data, int):
        message_text = (
            f"Pipeline {pipeline_name} has completed with status: {status}.\n"
            f"Number of rows exported to the database: {db_data}"
        )
    else:
        message_text = f"Pipeline {pipeline_name} has completed with status: {status}.\nNo data was exported."

    message = {"text": message_text}

    # Send the Slack message
    requests.post(webhook_url, json=message)


@custom
def transform_custom(*args, **kwargs):
    """
    Sends a Slack notification with the number of rows exported.
    """
    # Retrieve the DataFrame from the upstream block
    df_data = args[0] if args else None

    # Calculate the number of rows exported
    num_rows_exported = len(df_data) if df_data is not None else 0

    # Debug: Print the row count
    print(f"Number of rows exported: {num_rows_exported}")

    # Notify Slack
    notify_slack(
        pipeline_name="data_real_estate",
        status="Success",
        db_data=num_rows_exported
    )

    return num_rows_exported






@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
