# Copyright 2022 Hewlett Packard Enterprise Development LP

import logging
from utils.vars import *

def install_logger_file_init(log_file_level=LOG_DEFAULT_FILE_LEVEL, verbose=False):
    """
    Create the config for the log file. This should only be call once.
    """

    install_logger = logging.getLogger(LOG_DEFAULT_NAME)
    install_logger.setLevel(LOG_DEFAULT_FILE_LEVEL)
    
    log_file_handler = logging.FileHandler(LOG_DEFAULT_FILENAME)
    log_file_handler.setLevel(log_file_level)
    if verbose:
        log_file_handler.setFormatter(logging.Formatter(LOG_DEFAULT_FILE_FORMAT_VERBOSE))
    else:
        log_file_handler.setFormatter(logging.Formatter(LOG_DEFAULT_FILE_FORMAT))

    install_logger.addHandler(log_file_handler)

    return install_logger

def install_logger_stream_init(log_console_level=LOG_DEFAULT_CONSOLE_LEVEL, verbose=False):
    """
    Create the config for the log console stream. This should only be call once.
    """
    install_logger = logging.getLogger(LOG_DEFAULT_NAME)

    log_stream_handler = logging.StreamHandler()
    log_stream_handler.setLevel(log_console_level)
    if verbose:
        log_stream_handler.setFormatter(logging.Formatter(LOG_DEFAULT_CONSOLE_FORMAT_VERBOSE))
    else:
        log_stream_handler.setFormatter(logging.Formatter(LOG_DEFAULT_CONSOLE_FORMAT))
    
    install_logger.addHandler(log_stream_handler)

    return install_logger

def get_install_logger(module=None):
    """
    Call this to get an instance of the logger.
    If this is being call outside of the __main__ module,
    Set the argument module=__name__ when calling.
    """
    if module == None:
        return logging.getLogger(LOG_DEFAULT_NAME)
    else:
        return logging.getLogger("{}.{}".format(LOG_DEFAULT_NAME, module))
