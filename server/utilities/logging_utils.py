import logging

def setup_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    )
    logger = logging.getLogger(name)
    logging.getLogger("tqdm").setLevel(logging.WARNING)
    return logger
