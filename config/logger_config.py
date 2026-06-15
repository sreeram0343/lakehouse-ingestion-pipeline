import os
import logging
from logging.handlers import RotatingFileHandler

def get_logger(name="platform"):
    """
    Creates and returns a reusable, configured logger instance with both console 
    and rotating file handlers.
    """
    # Automatically determine project root and ensure the logs directory exists
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    logs_dir = os.path.join(base_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    log_file = os.path.join(logs_dir, "platform.log")

    logger = logging.getLogger(name)
    
    # Avoid adding duplicate handlers if the logger is already initialized
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        # Standardized log format with timestamp, level, name, line number, and message
        formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s [%(name)s:%(lineno)d] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # 1. Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # 2. File handler (Rotating to manage disk usage - 10MB limit, keep 5 backups)
        file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
