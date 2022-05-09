# Copyright 2022 Hewlett Packard Enterprise Development LP

import datetime
import os
import logging

class VMConnectionException(Exception):
    """A pass-through class."""

class InstallError(Exception):
    """A wrapper for raising an InstallError exception."""
    pass

class RunException(Exception):
    """A wrapper for raising an RunException exception."""
    def __init__(self, message, cmd, args, returncode, stdout, stderr):
        super().__init__(message)

        self.cmd = cmd
        self.args = args
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode

class RunTimeoutError(Exception):
    """A wrapper for raising an RunTimeoutError exception."""
    def __init__(self, message, cmd, args, returncode, stdout, stderr):
        super().__init__(message)

        self.cmd = cmd
        self.args = args
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode

class TimeOut(Exception):
    """A wrapper for raising a TimeOut exception."""
    pass

class COSProblem(Exception):
    """A wrapper for raising a COSProblem exception."""
    pass

class UnexpectedState(Exception):
    """A wrapper for raising an UnexpectedState exception."""
    pass
class PodProblem(Exception):
    """A wrapper for raising problems with pods."""

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
CFS_CONFIG_FILENAME = "cfs-config.json"
IMAGE_INFO = "image_info.yaml"
BOS_SESSIONTEMPLATE_FILENAME = "bos_sessiontemplate.json"

STAGE_HIST_FILENAME = "stage_hist.yaml"
NCNP_VARS = "ncnp-vars.yaml"

SAT_BOOTPREP_CFG = "bootprep-config.yaml"

LOG_DEFAULT_CONSOLE_LEVEL = logging.INFO
LOG_DEFAULT_DIR = os.path.join(os.getcwd(), "log")
LOG_DEFAULT_FILE_LEVEL = logging.DEBUG
LOG_DEFAULT_FILENAME = "install.log"
SESSION_TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
LOG_DEFAULT_FILE_FORMAT = '%(asctime)s %(levelname)s %(message)s'
LOG_DEFAULT_CONSOLE_FORMAT = '%(levelname)s %(message)s'
LOG_DEFAULT_FILE_FORMAT_VERBOSE = '%(asctime)s %(levelname)s %(module)s.%(funcName)s:%(lineno)d %(message)s'
LOG_DEFAULT_CONSOLE_FORMAT_VERBOSE = '%(levelname)s %(module)s.%(funcName)s:%(lineno)d %(message)s'
LOG_DEFAULT_NAME = "install"
LOG_LEVELS = {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "DRYRUN": logging.INFO + 1,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG
}
