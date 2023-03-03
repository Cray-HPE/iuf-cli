#
# MIT License
#
# (C) Copyright 2022-2023 Hewlett Packard Enterprise Development LP
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
from collections import OrderedDict
import logging

import os

import threading

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

class SyntaxProblem(Exception):
    """A wrapper for raising yaml or json syntax errors."""
    pass

class NCNPersonalization(Exception):
    """A wrapper for raising an NCNPersonalization error"""
    pass

class GitError(Exception):
    """A wrapper for raising an GitError error"""
    pass

class LoggingError(Exception):
    """A wrapper for raising an LoggingError error"""
    pass


STAGE_DICT = OrderedDict({
    "process-media": {
        "description" : "Inventory and extract products in the media directory for use in subsequent stages"
        },
    "pre-install-check":  {
        "description" : "Perform pre-install readiness checks"
        },
    "deliver-product": {
        "description" : "Upload product content onto the system"
        },
    "update-vcs-config": {
        "description" : "Merge working branches and perform automated VCS configuration"
        },
    "update-cfs-config": {
        "description" : "Update CFS configuration utilizing sat bootprep"
        },
    "prepare-images": {
        "description" : "Build and configure management node and/or managed node images utilizing sat bootprep"
        },
    "management-nodes-rollout": {
        "description" : "Rolling rebuild of management nodes"
        },
    "deploy-product": {
        "description" : "Deploy services to system"
        },
    "post-install-service-check": {
        "description" : "Perform post-install checks of deployed product services"
        },
    "managed-nodes-rollout": {
        "description" : "Rolling reboot of managed nodes"
        },
    "post-install-check": {
        "description" : "Perform post-install checks"
        }
})

NOABORT_STAGES = [
]

ACTIVITY_DICT = "activity_dict.yaml"

CFS_CONFIG_FILENAME = "cfs-config.json"

STAGE_HIST_FILENAME = "stage_hist.yaml"
NCNP_VARS = "ncnp-vars.yaml"

# `sat bootprep`-related defaults.
RECIPE_VARS="product_vars.yaml"
MEDIA_VERSIONS = "media_versions.yaml"
BP_CONFIG_MANAGED = "compute-and-uan-bootprep.yaml"
BP_CONFIG_MANAGEMENT = "management-bootprep.yaml"

RLOCK = threading.RLock()

# RBD/media/state/activity dir defaults
RBD_BASE_DIR = "/etc/cray/upgrade/csm"
IUF_BASE_DIR = os.path.join(RBD_BASE_DIR, "iuf")
ACTIVITY_BASE_DIR = os.path.join(IUF_BASE_DIR, "activities")
MEDIA_BASE_DIR = "/opt/cray/iuf"

LOG_DEFAULT_CONSOLE_LEVEL = logging.INFO
LOG_DEFAULT_FILE_LEVEL = logging.DEBUG
LOG_DEFAULT_FILENAME = "install.log"
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
        "DEBUG": logging.DEBUG,
        "TRACE": logging.DEBUG - 1
}
