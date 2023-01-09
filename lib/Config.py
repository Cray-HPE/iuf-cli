"""
Copyright 2022 Hewlett Packard Enterprise Development LP
"""

import datetime
import os
import sys

from lib.vars import ACTIVITY_DICT, IUF_BASE_DIR, MEDIA_BASE_DIR
from lib.Connection import CmdMgr
import lib.Activity
from lib.InstallLogger import get_install_logger

class Config:
    _activity_session = None
    _activity_dict_file = None
    _args = None
    _connection = None
    _logger = None
    timestamp = None
    all_product_data = None

    def __init__(self):
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    def __repr__(self):
        return repr(self.__dict__)

    def _error(self, message):
        if self._logger:
            self._logger.error(message)
        else:
            print(f"ERROR: {message}")

    @property
    def logdir(self):
        return os.path.join(self._args["log_dir"], self.timestamp)

    @property
    def activity(self):
        if self._activity_session is None:
            session = self.args.get("activity_session", None)
            self._activity_session = lib.Activity.Activity(name=session, filename=self.activity_dict_file, dryrun=self.dryrun)

        return self._activity_session

    @activity.setter
    def activity(self, value):
        self._activity_session = value

    @property
    def connection(self):
        if self._connection is None:
            self._connection = CmdMgr.get_cmd_interface()
            self._connection.dryrun = self.dryrun

        return self._connection

    @connection.setter
    def connection(self, value):
        self._connection = value

    @property
    def activity_dict_file(self):
        if self._activity_dict_file is None and self._args is not None:
            state_dir = self._args.get("state_dir")
            if state_dir:
                self._activity_dict_file = os.path.join(state_dir, ACTIVITY_DICT)

        return self._activity_dict_file

    @property
    def dryrun(self):
        if self._args:
            return self._args.get("dryrun", False)

        return False

    @property
    def logger(self):
        return get_install_logger()
        #return self._logger

    @logger.setter
    def logger(self, value):
        self._logger = value

    @property
    def args(self):
        return self._args

    @args.setter
    def args(self, value):
        self._args = value.copy()
        validated = True

        # sanity test the directories
        activity = self._args.get("activity_session", None)
        if not activity:
            self._error("No activity session specified")
            sys.exit(1)

        default_base_dir = os.path.join(IUF_BASE_DIR, activity)
        base_dir = self._args.get("base_dir", None)
        if base_dir is None:
            base_dir = default_base_dir

        if not self._args.get("media_dir", None):
            self._args["media_dir"] = os.path.join(MEDIA_BASE_DIR, activity)

        media_dir_ok = False
        media_dir = self._args.get("media_dir")

        for dir in [media_dir, os.path.abspath(media_dir), os.path.join(MEDIA_BASE_DIR, media_dir)]:
            if dir.startswith(MEDIA_BASE_DIR) and os.path.exists(dir):
                media_dir_ok = True
                self._args["media_dir"] = dir
                break

        if not media_dir_ok:
            self._error("Media directory must exist and be under {}".format(MEDIA_BASE_DIR))
            validated = False

        for adir in ["state", "log"]:
            if not self._args.get(f"{adir}_dir", None):
                dir_default = os.path.join(base_dir, adir)
                self._args[f"{adir}_dir"] = dir_default
                if not os.path.exists(dir_default):
                    try:
                        os.makedirs(dir_default, exist_ok=True)
                    except Exception as e:
                        self._error(f"Unable to create {dir_default}")
                        self._error(e)
                        validated = False

            dpath = self._args.get(f"{adir}_dir")
            if not os.path.exists(dpath):
                self._error(f"{adir.capitalize()} directory {dpath} does not exist.")
                validated = False

        if not validated:
            sys.exit(1)

