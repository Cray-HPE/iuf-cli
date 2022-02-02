# Copyright 2022 Hewlett Packard Enterprise Development LP

import datetime
import os

from utils.InstallerUtils import getenv

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
