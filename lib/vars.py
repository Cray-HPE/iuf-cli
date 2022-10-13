# Copyright 2022 Hewlett Packard Enterprise Development LP

from collections import OrderedDict
import datetime
import logging

import textwrap
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
    """A wrapper for raising an GitError error"""
    pass

class LoggingError(Exception):
    """A wrapper for raising an LoggingError error"""
    pass

# Note this isn't imported from InstallerUtils to avoid circular dependencies.
def _formatted(text):
    """Format a text string for a standard 80-line terminal."""
    wrapper = textwrap.TextWrapper(width=78)
    raw = textwrap.dedent(text).strip()
    msg = wrapper.fill(text=raw)
    return msg

STAGE_DICT = OrderedDict({
    "process_media": {
        "func" : "get_prods",
        "description" : "Inventory and extract products in the media directory for use in subsequent stages"
        },
    "pre_install_check":  {
        "func" : "stub_pre_install_check",
        "description" : "Perform pre-install readyness checks"
        },
    "deliver_product": {
        "func" : "stub_deliver_product",
        "description" : "Upload product content onto the system"
        },
    "update_vcs_config": {
        "func" : "stub_update_config",
        "description" : "Merge working branches and perform, automated VCS configuration"
        },
    "update_cfs_config": {
        "func" : "stub_update_config",
        "description" : "Update CFS configuration (sat bootprep run --config)"
        },
    "deploy_product": {
        "func" : "stub_deploy_product",
        "description" : "Deploy services to system"
        },
    "prepare_images": {
        "func" : "stub_prepare_images",
        "description" : "Build and configure management node and/or managed node images (sat bootprep run --images)"
        },
    "management_nodes_rollout": {
        "func" : "stub_ncn_rollout",
        "description" : "Rolling reboot or liveupdate of management nodes"
        },
    "post_install_service_check": {
        "func" : "stub_post_install_service_check",
        "description" : "Perform post-install checks of processed services"
        },
    "managed_nodes_rollout": {
        "func" : "stub_cn_rollout",
        "description" : "Rolling reboot or liveupdate of managed nodes nodes"
        },
    "post_install_check": {
        "func" : "stub_post_install_compute_check",
        "description" : "Perform post-install checks"
        }
})

NOABORT_STAGES = [
]

LOCATION_DICT = "location_dict.yaml"

BOOT_POLL_SECS = 60
BOOT_TIMOUT_SECS = 3600

BOS_INFO_FILENAME = "bos-info.json"
CFS_CONFIG_FILENAME = "cfs-config.json"
IMAGE_INFO = "image_info.yaml"
BOS_SESSIONTEMPLATE_FILENAME = "bos_sessiontemplate.json"

STAGE_HIST_FILENAME = "stage_hist.yaml"
NCNP_VARS = "ncnp-vars.yaml"

SAT_BOOTPREP_CFG_CN = "bootprep-config-cn.yaml"

LOG_DEFAULT_CONSOLE_LEVEL = logging.INFO
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
