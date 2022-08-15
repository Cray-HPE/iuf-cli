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
    "process_product_media": {
        "func" : "get_prods",
        "description" : "Inventory and extract products in the media directory for use in subsequent stages."
        },
    "validate_products":  {
        "func" : "validate_products",
        "description" : "Perform product sanity checks."
        },
    "install_products": {
        "func" : "install",
        "description" : "Install products identified in the process_product_media stage."
        },
    "deploy_products": {
        "func" : "install",
        "description" : "Deploy products"
        },
    "verify_product_import": {
        "func" : "verify_product_import",
        "description" : "Verify all product import PODS and Jobs have completed."
        },
    "verify_product_install": {
        "func" : "verify_product_install",
        "description" : "Verify product installation by running product validations"
        },
    "update_working_branches": {
        "func" : "update_working_branches",
        "description" : _formatted("""
            Update config managment branches identified by
            --working-branch with the product release branch content for each product
            being installed that contains a config management repo.  See help for
            --working-branch for details on setting the working branch.""")
        },
    "create_bootprep_config": {
        "func" : "create_bootprep_config",
        "description" : "Generate the `sat bootprep` input config."
        },
    "sat_bootprep_cn": {
        "func" : "sat_bootprep_cn",
        "description" : "Run `sat bootprep` for compute nodes."
        },
    "sat_bootprep_ncn": {
        "func" : "sat_bootprep_ncn",
        "description" : "Run `sat bootprep` for non-compute nodes."
        },
    "update_ncn_config": {
        "func" : "update_ncn_config",
        "description" : _formatted("""
            Update the NCN personalization configuration.  Defaults to 'ncn-personalization',
            use --ncn-personalization to over-ride.""")
        },
    "worker_health_check": {
        "func":  "worker_health_check",
        "description": "Check the health of the workers prior to beginning NCN Personalization."
        },
    "ncn_personalization": {
        "func" : "ncn_personalization",
        "description" : "Perform NCN personalization."
        },
    "unload_dvs_and_lnet": {
        "func" : "unload_dvs_and_lnet",
        "description" : "Perform a rolling unload, upgrade and reload of DVS and LNET on worker nodes."
        },
    "check_services": {
        "func" : "check_services",
        "description" : "Check CPS, DVS, LNET. and NMD services."
        },
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
