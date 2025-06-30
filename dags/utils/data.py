import json
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class NodeTemporalInfo:
    """Class to store temporal information for a node."""
    def __init__(self, start_date: Optional[str], target_date: Optional[str], closed_date: Optional[str], opportunity: Optional[str]):
        self.start_date = self._parse_date(start_date)
        self.target_date = self._parse_date(target_date)
        self.closed_date = self._parse_date(closed_date)
        self.in_degree = None
        self.out_degree = None
        self.opportunity = opportunity
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse date string to datetime object."""
        if not date_str or pd.isna(date_str):
            return None
        try:
            return pd.to_datetime(date_str)
        except Exception as e:
            logger.warning(f"Failed to parse date '{date_str}': {e}")
            return None
    
    def __str__(self) -> str:
        return (f"Start: {self.start_date}, "
                f"Target: {self.target_date}, "
                f"Closed: {self.closed_date}, "
                f"In-Degree: {self.in_degree}, "
                f"Out-Degree: {self.out_degree}"
                f"Opportunity: {self.opportunity}")

def read_dag_data(data_file: Path) -> Tuple[Dict, Dict[str, NodeTemporalInfo]]:
    """Read DAG data from CSV file.
    
    Args:
        data_file (Path): Path to the CSV data file
        
    Returns:
        tuple: (dag_data, temporal_data)
            dag_data: DAG data structure with nodes and their relationships
            temporal_data: Dictionary mapping node IDs to their temporal information
        
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
        temporal_data = {}
        
        # Process each row
        for _, row in df.iterrows():
            work_item_id = str(row['WORK_ITEM_ID'])
            relationship_id = str(row['WORK_ITEM_RELATED_ID'])
            relationship_type = row['WORK_ITEM_RELATIONSHIP_TYPE']
            
            # Initialize node if not exists
            if work_item_id not in dag_data:
                dag_data[work_item_id] = {
                    'predecessors': [],
                    'successors': [],
                    'type': row['WORK_ITEM_TYPE_NAME'],
                    'state': row['WORK_ITEM_RELATIONSHIP_STATE_NAME']
                }
                
            if relationship_id not in temporal_data:
                # Store temporal information
                temporal_data[relationship_id] = NodeTemporalInfo(
                    start_date=row['START_DATETIME'],
                    target_date=row['TARGET_DATETIME'],
                    closed_date=row['CLOSED_DATETIME'],
                    opportunity=row['OPPORTUNITY_NAME']
                )
            
            # Add relationship
            if relationship_type == 'Predecessor':
                dag_data[work_item_id]['predecessors'].append(relationship_id)
            elif relationship_type == 'Successor':
                dag_data[work_item_id]['successors'].append(relationship_id)
        
        logger.info(f"Processed {len(dag_data)} unique work items")
        logger.debug(f"Found {sum(len(d['predecessors']) for d in dag_data.values())} predecessor relationships")
        logger.debug(f"Found {sum(len(d['successors']) for d in dag_data.values())} successor relationships")
        
        # Log temporal information summary
        temporal_summary = {
            'with_start_date': sum(1 for t in temporal_data.values() if t.start_date is not None),
            'with_target_date': sum(1 for t in temporal_data.values() if t.target_date is not None),
            'with_closed_date': sum(1 for t in temporal_data.values() if t.closed_date is not None)
        }
        logger.info(f"Temporal data summary: {temporal_summary}")
        
        return dag_data, temporal_data
    except Exception as e:
        logger.error(f"Error reading data file: {e}")
        raise

def get_data_file_path() -> Path:
    """Get the path to the default DAG data file.
    
    Returns:
        Path: Path to the data file
    """
    path = Path(__file__).parent.parent.parent / 'data' / 'working.csv'
    #path = Path(__file__).parent.parent.parent / 'data' / 'sample.csv'
    logger.debug(f"Resolved data file path: {path}")
    return path 
