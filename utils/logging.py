import logging


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

    FORMAT = "%(asctime)s  %(message)s"
    logging.basicConfig(format=FORMAT)
    logger = logging.getLogger("tcpserver")
    logger.warning("WORKING ON: %s", message)
