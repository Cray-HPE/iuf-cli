"""
Copyright 2022 Hewlett Packard Enterprise Development LP
"""

import datetime
import os
from utils.ShastaUpdate import validate_products
import sys

from utils.vars import LOCATION_DICT
from utils.Connection import CmdMgr
import hpe.Products

class Config:
    _location_dict = None
    _location_dict_file = None
    _args = None
    _connection = None
    _logger = None
    all_product_data = None
    timestamp = None

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
    def location_dict(self):
        if self._location_dict is None:
            self._location_dict = hpe.Products.Products(self.location_dict_file)

        return self._location_dict

    @location_dict.setter
    def location_dict(self, value):
        self._location_dict = value
        self._location_dict.location_dict = self.location_dict_file
        self._location_dict.dryrun = self.dryrun
        self._location_dict.write_location_dict()

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
    def location_dict_file(self):
        if self._location_dict_file is None and self._args is not None:
            state_dir = self._args.get("state_dir")
            if state_dir:
                self._location_dict_file = os.path.join(state_dir, LOCATION_DICT)

        return self._location_dict_file

    @property
    def dryrun(self):
        if self._args:
            return self._args.get("dryrun", False)

        return False

    @property
    def logger(self):
        return self._logger

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
        for dir in ["media", "state", "log"]:
            if not self._args.get(f"{dir}_dir", None):
                base_dir = self._args.get("base_dir", os.getcwd())
                if not os.path.isdir(base_dir):
                    self._error(f"{base_dir} is not a valid directory")
                    validated = False
                    break
                dir_default = os.path.join(base_dir, dir)
                self._args[f"{dir}_dir"] = dir_default
                if not os.path.exists(dir_default):
                    try:
                        os.mkdir(dir_default)
                    except Exception as e:
                        self._error(f"Unable to create {dir_default}")
                        self._error(e)
                        validated = False

            dpath = self._args.get(f"{dir}_dir")
            if not os.path.exists(dpath):
                self._error(f"{dir.capitalize()} directory {dpath} does not exist.")
                validated = False

        if not validated:
            sys.exit(1)

