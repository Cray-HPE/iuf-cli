# Copyright 2022 Hewlett Packard Enterprise Development LP

import datetime
import os
import logging


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

try:
    CRAY_RELEASE_DIR
except NameError:
    CRAY_RELEASE_DIR =  "/admin/cray/release"

try:
    REMOTE_PROJECT_DIR
except NameError:
    REMOTE_PROJECT_DIR = os.path.join(CRAY_RELEASE_DIR, 'project')
try:
    REMOTE_PROJECT_OLDJOBS_DIR
except NameError:
    REMOTE_PROJECT_OLDJOBS_DIR = os.path.join(REMOTE_PROJECT_DIR, 'oldjobs')

BUILD_ID = datetime.datetime.today().strftime("%Y%m%d-%H%M%S")
BUILD_DIR = os.path.join(REMOTE_PROJECT_DIR, BUILD_ID)
CI_DATE = BUILD_ID

LOCAL_WORKSPACE_TMP = os.path.join(CRAY_RELEASE_DIR, BUILD_ID, "tmp")
BOS_INFO_FILE = os.path.join(LOCAL_WORKSPACE_TMP, 'bos-info.json')

BOOT_POLL_SECS = 60
BOOT_TIMOUT_SECS = 3600

LOG_DEFAULT_CONSOLE_LEVEL = logging.INFO
LOG_DEFAULT_FILE_LEVEL = logging.DEBUG
LOG_DEFAULT_FILENAME = "cos_install.log"
LOG_DEFAULT_FILE_FORMAT = '%(asctime)s %(levelname)s %(message)s'
LOG_DEFAULT_CONSOLE_FORMAT = '%(levelname)s %(message)s'
LOG_DEFAULT_NAME = "cos_install"
LOG_LEVELS = {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG
}
