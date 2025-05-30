import logging
from utils.cli import cli

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run the CLI
    try:
        cli() 
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise e