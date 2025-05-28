import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

def setup_logging(log_name='dag_analysis'):
    """Set up logging with rotating file handler.
    
    Args:
        log_name (str): Base name for the log file
        
    Returns:
        logging.Logger: Configured logger instance
    """
    log_dir = Path(__file__).parent.parent.parent
    log_file = log_dir / f'{log_name}.log'
    
    # Create rotating file handler
    handler = RotatingFileHandler(
        log_file,
        maxBytes=1024 * 1024,  # 1MB
        backupCount=5
    )
    
    # Set up logging format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    
    # Configure module loggers
    for module in ['dags.utils.data', 'dags.analysis.paths']:
        module_logger = logging.getLogger(module)
        module_logger.setLevel(logging.INFO)
        module_logger.addHandler(handler)
    
    return logger 