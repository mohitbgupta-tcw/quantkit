import logging
import sys

logger = logging.getLogger()
logger.level = logging.DEBUG
stream_handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def log(message) -> None:
    """
    log message into console
    format:
    yyyy-mm-dd hh-mm-ss WORKING ON: message

    Parameters
    ----------
    message: str
        message to be logged to console
    """
    logging.warning("WORKING ON: %s", message)
