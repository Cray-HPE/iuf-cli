# Copyright 2022 Hewlett Packard Enterprise Development LP

import datetime
import logging
import os

from lib.vars import *


def addLoggingLevel(levelName, levelNum, methodName=None):
    """
    Comprehensively adds a new logging level to the `logging` module and the
    currently configured logging class.

    `levelName` becomes an attribute of the `logging` module with the value
    `levelNum`. `methodName` becomes a convenience method for both `logging`
    itself and the class returned by `logging.getLoggerClass()` (usually just
    `logging.Logger`). If `methodName` is not specified, `levelName.lower()` is
    used.

    To avoid accidental clobberings of existing attributes, this method will
    raise an `AttributeError` if the level name is already an attribute of the
    `logging` module or if the method name is already present

    Example
    -------
    >>> addLoggingLevel('TRACE', logging.DEBUG - 5)
    >>> logging.getLogger(__name__).setLevel("TRACE")
    >>> logging.getLogger(__name__).trace('that worked')
    >>> logging.trace('so did this')
    >>> logging.TRACE
    5

    """
    if not methodName:
        methodName = levelName.lower()

    if hasattr(logging, levelName):
       raise AttributeError('{} already defined in logging module'.format(levelName))
    if hasattr(logging, methodName):
       raise AttributeError('{} already defined in logging module'.format(methodName))
    if hasattr(logging.getLoggerClass(), methodName):
       raise AttributeError('{} already defined in logger class'.format(methodName))

    # This method was inspired by the answers to Stack Overflow post
    # http://stackoverflow.com/q/2183233/2988730, especially
    # http://stackoverflow.com/a/13638084/2988730
    def logForLevel(self, message, *args, **kwargs):
        if self.isEnabledFor(levelNum):
            self._log(levelNum, message, args, **kwargs)
    def logToRoot(message, *args, **kwargs):
        logging.log(levelNum, message, *args, **kwargs)

    logging.addLevelName(levelNum, levelName)
    setattr(logging, levelName, levelNum)
    setattr(logging.getLoggerClass(), methodName, logForLevel)
    setattr(logging, methodName, logToRoot)

def install_logger_file_init(config):
    """
    Create the config for the log file. This should only be called once.
    """

    log_file_level = config.args["level"]
    verbose = config.args["verbose"]

    install_logger = logging.getLogger(LOG_DEFAULT_NAME)
    install_logger.setLevel(LOG_DEFAULT_FILE_LEVEL)

    log_dir_base = config.args["log_dir"]
    log_dir = os.path.join(log_dir_base, config.timestamp)

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_filename = os.path.join(log_dir, LOG_DEFAULT_FILENAME)
    log_file_handler = logging.FileHandler(log_filename)

     #if os.path.exists(LOG_DEFAULT_FILENAME):
     #   os.rename(LOG_DEFAULT_FILENAME, "{}.{}".format(LOG_DEFAULT_FILENAME, os.getpid()))

    symlink_target = os.path.join(log_dir_base, LOG_DEFAULT_FILENAME)
    if os.path.islink(symlink_target):
        os.remove(symlink_target)

    os.symlink(log_filename, symlink_target)

    if verbose:
        log_file_handler.setFormatter(logging.Formatter(LOG_DEFAULT_FILE_FORMAT_VERBOSE))
    else:
        log_file_handler.setFormatter(logging.Formatter(LOG_DEFAULT_FILE_FORMAT))

    install_logger.addHandler(log_file_handler)

    return install_logger

def install_logger_stream_init(log_console_level=LOG_DEFAULT_CONSOLE_LEVEL, verbose=False):
    """
    Create the config for the log console stream. This should only be called once.
    """
    install_logger = logging.getLogger(LOG_DEFAULT_NAME)

    logging.Formatter.formatTime = (lambda self, record,
                                    datefmt = None: datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"))
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
    If this is being called outside of the __main__ module,
    Set the argument module=__name__ when calling.
    """
    if module == None:
        return logging.getLogger(LOG_DEFAULT_NAME)
    else:
        return logging.getLogger("{}.{}".format(LOG_DEFAULT_NAME, module))

def get_log_filename(config, name):
    """
    Take a base name and return a filename in the log_dir
    """

    log_dir_base = config.args["log_dir"]
    log_dir = os.path.join(log_dir_base, config.timestamp)

    log_filename = os.path.join(log_dir, "{}.log".format(name))

    # for now require the logs be unique, might need to revisit this
    if os.path.exists(log_filename):
        raise LoggingError("{} already exists.".format(log_filename))

    return log_filename
