import logging
import os
import sys
from datetime import datetime

def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
        
    logger = logging.getLogger("omniprice_bronze")
    logger.critical("CRITICAL FAILURE DETECTED", exc_info=(exc_type, exc_value, exc_traceback))

def get_logger(name="omniprice_bronze"):
    # 1. Get current time
    now = datetime.now()
    
    # 2. Build the hierarchical path: logs/omniprice_bronze/2026/10/10/
    log_dir = os.path.join("logs", name, now.strftime("%Y"), now.strftime("%m"), now.strftime("%d"))
    
    # 3. Automatically create those folders if they don't exist
    os.makedirs(log_dir, exist_ok=True)
    
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
        
        # 4. Save the log file inside the deep directory with exact Hours/Mins/Secs
        log_file = os.path.join(log_dir, f"run_{now.strftime('%H%M%S')}.log")
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        
        sys.excepthook = handle_exception
        
    return logger
