# Copyright 2022 Hewlett Packard Enterprise Development LP

import datetime
import os
import logging
import platform
import json
import subprocess

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

# FIXME The constants below this should not be constants; we should find a
# better way to deal with them.
ANALYTICS_BRANCH = "cray/analytics/1.1.24"

host = None
host_shortname = None

system_json = subprocess.run("sat showrev --system --format json".split(), stdout=subprocess.PIPE).stdout
showrev = json.loads(system_json)["System Revision Information"]

for component in showrev:
    if component["component"] == "System name":
        host_shortname = component["data"].lower()
        break

if host_shortname is not None:
    host = "{}-{}".format(host_shortname, platform.node())
