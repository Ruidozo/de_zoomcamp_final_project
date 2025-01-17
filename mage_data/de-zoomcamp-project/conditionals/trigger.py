if 'condition' not in globals():
    from mage_ai.data_preparation.decorators import condition


@condition
def evaluate_condition(*args, **kwargs) -> bool:
    """
    Check if the 'export_real_estate_data' block has been executed successfully.
    """
    # Retrieve the pipeline context from kwargs
    pipeline = kwargs.get('pipeline')
    if pipeline is None:
        print("Pipeline context is not available.")
        return False

    # Get the specific block by name
    export_block = pipeline.get_block('export_real_estate_data')
    if export_block is None:
        print("Block 'export_real_estate_data' not found.")
        return False

    # Check the execution status of the block
    if export_block.status == 'completed':  # Replace with the correct status check if different
        print("Block 'export_real_estate_data' has been executed. Proceeding...")
        return True
    else:
        print("Block 'export_real_estate_data' has not been executed. Skipping downstream blocks.")
        return False
