import json
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)

def read_dag_data(data_file: Path) -> Dict:
    """Read DAG data from CSV file.
    
    Args:
        data_file (Path): Path to the CSV data file
        
    Returns:
        dict: DAG data structure with nodes and their relationships
        
    Raises:
        FileNotFoundError: If the data file doesn't exist
        pd.errors.EmptyDataError: If the file is empty
    """
    try:
        logger.info(f"Reading DAG data from {data_file}")
        # Read CSV file
        df = pd.read_csv(data_file)
        logger.info(f"Successfully read {len(df)} rows from CSV file")
        
        # Create DAG data structure
        dag_data = {}
        
        # Process each row
        for _, row in df.iterrows():
            work_item_id = str(row['WORK_ITEM_ID'])
            relationship_id = str(row['WORK_ITEM_RELATIONSHIP_ID'])
            relationship_type = row['WORK_ITEM_RELATIONSHIP_TYPE']
            
            # Initialize node if not exists
            if work_item_id not in dag_data:
                dag_data[work_item_id] = {
                    'predecessors': [],
                    'successors': [],
                    'type': row['WORK_ITEM_TYPE_NAME'],
                    'state': row['WORK_ITEM_RELATIONSHIP_STATE_NAME']
                }
            
            # Add relationship
            if relationship_type == 'Predecessor':
                dag_data[work_item_id]['predecessors'].append(relationship_id)
            elif relationship_type == 'Successor':
                dag_data[work_item_id]['successors'].append(relationship_id)
        
        logger.info(f"Processed {len(dag_data)} unique work items")
        logger.debug(f"Found {sum(len(d['predecessors']) for d in dag_data.values())} predecessor relationships")
        logger.debug(f"Found {sum(len(d['successors']) for d in dag_data.values())} successor relationships")
        
        return dag_data
    except Exception as e:
        logger.error(f"Error reading data file: {e}")
        raise

def get_data_file_path() -> Path:
    """Get the path to the default DAG data file.
    
    Returns:
        Path: Path to the data file
    """
    path = Path(__file__).parent.parent.parent / 'data' / 'working.csv'
    logger.debug(f"Resolved data file path: {path}")
    return path 