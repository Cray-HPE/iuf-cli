# Copyright 2022 Hewlett Packard Enterprise Development LP

import datetime
import os
import logging

class VMConnectionException(Exception):
    """A pass-through class."""

class InstallError(Exception):
    """A wrapper for raising an InstallError exception."""
    pass

class TimeOut(Exception):
    """A wrapper for raising a TimeOut exception."""
    pass

class COSProblem(Exception):
    """A wrapper for raising a COSProblem exception."""
    pass

class UnexpectedState(Exception):
    """A wrapper for raising an UnexpectedState exception."""
    pass

class TestFailure(Exception):
    """A wrapper for raising a TestFailure exception."""
    pass

class SyntaxError(Exception):
    """A wrapper for raising yaml or json syntax errors."""
    pass

class NCNPersonalization(Exception):
    """A wrapper for raising an NCNPersonalization error"""
    pass

class GitError(Exception):
    """A wrapper for raising an NCNPersonalization error"""
    pass

LOCATION_DICT = "location_dict.yaml"

BOOT_POLL_SECS = 60
BOOT_TIMOUT_SECS = 3600

BOS_INFO_FILENAME = "bos-info.json"
IMAGE_INFO = "image_info.yaml"
BOS_SESSIONTEMPLATE_FILENAME = "bos_sessiontemplate.json"

NCNP_VARS = "ncnp-vars.yaml"

LOG_DEFAULT_CONSOLE_LEVEL = logging.INFO
LOG_DEFAULT_DIR = os.path.join(os.getcwd(), "log")
LOG_DEFAULT_FILE_LEVEL = logging.DEBUG
LOG_DEFAULT_FILENAME = "cos_install.log"
LOG_SESSION_FILENAME = "cos_install.{}.log".format(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
LOG_DEFAULT_FILE_FORMAT = '%(asctime)s %(levelname)s %(message)s'
LOG_DEFAULT_CONSOLE_FORMAT = '%(levelname)s %(message)s'
LOG_DEFAULT_FILE_FORMAT_VERBOSE = '%(asctime)s %(levelname)s %(module)s.%(funcName)s:%(lineno)d %(message)s'
LOG_DEFAULT_CONSOLE_FORMAT_VERBOSE = '%(levelname)s %(module)s.%(funcName)s:%(lineno)d %(message)s'
LOG_DEFAULT_NAME = "cos_install"
LOG_LEVELS = {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "DRYRUN": logging.INFO + 1,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG
}
