if 'condition' not in globals():
    from mage_ai.data_preparation.decorators import condition
from mage_ai.data_preparation.models.block import Block

@condition
def evaluate_condition(*args, **kwargs) -> bool:
    """
    Conditional check to evaluate if 'export_real_estate_data' has been executed.
    """
    # Access the pipeline instance
    pipeline = kwargs.get('pipeline')
    
    # Get the block by its name
    export_block = pipeline.get_block('export_real_estate_data')
    if export_block is None:
        raise ValueError("Block 'export_real_estate_data' not found in the pipeline.")

    # Check the status of the block
    if export_block.status == Block.Status.COMPLETED:
        print("Block 'export_real_estate_data' has been executed. Proceeding...")
        return True
    else:
        print("Block 'export_real_estate_data' has not been executed. Skipping downstream blocks.")
        return False
