import logging
import sys

class OverwriteStdoutHandler(logging.StreamHandler):
    def emit(self, record):
        sys.stdout.write("\033[F\033[K")  # Move cursor up & clear the line
        super().emit(record)  # Print new log message

def setup_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    logger = logging.getLogger(name)
    logger.handlers = []  # Remove existing handlers to prevent duplicates

    handler = OverwriteStdoutHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    
    logger.addHandler(handler)

    logging.getLogger("tqdm").setLevel(logging.WARNING)
    logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

    return logger

