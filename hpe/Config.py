import datetime
import inspect
import os

from utils.vars import LOCATION_DICT
from utils.Connection import CmdMgr
import hpe.Products as p

class Config:
    _location_dict = None
    _location_dict_file = None
    _args = None
    _connection = None
    all_product_data = None
    timestamp = None

    def __init__(self):
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    def __repr__(self):
        return repr(self.__dict__)

    @property
    def location_dict(self):
        if self._location_dict is None:
            print(self.location_dict_file)
            self._location_dict = p.Products(self.location_dict_file)

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
    def args(self):
        return self._args

    @args.setter
    def args(self, value):
        self._args = value.copy()

        if self._args["media_dir"] is None:
            media_default = os.path.join(os.getcwd(), "media")
            self._args["media_dir"] = media_default
            if not os.path.exists(media_default):
                os.mkdir(media_default)

        if self._args["state_dir"] is None:
            state_default = os.path.join(os.getcwd(), "state")
            self._args["state_dir"] = state_default
            if not os.path.exists(state_default):
                os.mkdir(state_default)

