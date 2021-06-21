import logging
import logging.handlers


def initialize_logs(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    return logger
