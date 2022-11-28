"""
Copyright 2022 Hewlett Packard Enterprise Development LP
"""

from xml.sax.handler import property_declaration_handler
import yaml
import os
import datetime
from dateutil import parser
from prettytable import PrettyTable
import re
import sys
import itertools
import time

import lib.ApiInterface

class StateError(Exception):
    """A wrapper for raising a StateError exception."""
    pass

class ActivityError(Exception):
    """A wrapper for raising an ActivityError exception."""
    pass


ACTIVITY_NEW_STATE = { 
    'state': None,
    'sessionid': None,
    'status': None,
    'comment': None
}

ACTIVITY_VALID_STATES = [
    'in_progress',
    'waiting_admin',
    'paused',
    'debug',
    'blocked',
    'finished'
]

ACTIVITY_VALID_STATUS = [
    'Succeeded',
    'Failed',
    'Running',
    'n/a'
]

class Activity():
    config = None
    states = None
    start = None
    name = None
    initialized = False
    dryrun = False
    filename = None
    auth = None
    api = None
    api_initialized = False

    def __init__(self, filename=None, name=None, dryrun=False):
        self.states = dict()

        self.dryrun = dryrun
        self.filename = filename

        if os.path.exists(filename):
            self.load_activity_dict(filename)

            if self.name != name:
                raise ActivityError(f"{filename} contains the activity session {self.name}, but {name} was specified.")
        else:
            self.name = name
            self.start = self.get_time()

            self.initialized = True
            self.write_activity_dict()

        self.api = lib.ApiInterface.ApiInterface()

    def __repr__(self):
        return repr(self.__dict__)

    def __str__(self):
        """Print the activity in a nice table."""
        table = PrettyTable()
        table.field_names = ["start", "activity state", "IUF sessionid", "Status", "Duration", "Comment"]
        states = self.states
        ordered_states = sorted(states.keys())
        length = range(len(ordered_states))
        for x in length:
            start = ordered_states[x]
            state = states[start]
            if x+1 in length:
                end = ordered_states[x+1]
            else:
                end = self.get_time()

            duration = self.get_duration(start, end)
            table.add_row([
                start,
                state['state'],
                state['sessionid'],
                state['status'],
                duration,
                state['comment']
            ])
        table.align = "l"

        tstring = table.get_string()
        table_width = len(tstring.split("\n")[0])

        activity_name = self.name
        if not self.api.activity_exists(activity_name):
            activity_name += " [local only]"

        retstring = "+" + "-" * (table_width - 2) + "+\n"
        retstring += f"| Activity: {activity_name:<{table_width - 14}} |\n"
        retstring += tstring

        return retstring

    def yaml(self):
        return yaml.dump(self.get_dict())

    def get_dict(self):
        curdict = dict()
        curdict[self.name] = dict()
        curdict[self.name]["states"] = self.states
        curdict[self.name]["start"] = self.start

        return curdict

    def load_activity_dict(self, filename=None, encoding='UTF-8'):
        if not filename or not os.path.exists(filename):
            return

        with open(filename, "r", encoding=encoding) as f:
            masterdict = yaml.full_load(f)
            if len(masterdict) > 1:
                raise ActivityError(f"{filename} contains multiple activities.  Remove any manual modifications, or remove the file, and try again.")

            aname = list(masterdict)[0]
            activity = masterdict[aname]

            self.name = aname
            self.start = self.get_time(activity["start"])
            self.filename = filename
            self.states = dict()
            if "states" in activity:
                for rtime in activity["states"].keys():
                    state = activity["states"][rtime]
                    # reprocess the time in case the formats changed or something
                    stime = self.get_time(rtime)
                    self.states[stime] = dict()
                    for attr in ACTIVITY_NEW_STATE:
                        if attr in state:
                            self.states[stime][attr] = state[attr]

        self.initialized = True
            
    def get_time(self, time=None):
        if time:
            if type(time) is datetime.datetime:
                dt = time
            else:
                dt = parser.parse(time)
        else:
            dt = datetime.datetime.now()
        
        return dt.strftime("%Y-%m-%dt%H:%M:%S")

    def get_duration(self, start, end):
        st = parser.parse(start)
        en = parser.parse(end)

        return en - st

    def write_activity_dict(self, encoding='UTF-8'):
        if self.dryrun or not self.filename:
            return
        data = self.yaml()
        with open(self.filename, "w", encoding=encoding) as f:
            f.write(data)

    def state(self, timestamp=None, sessionid=None, state=None, status="n/a", comment=None, create=False):
        if timestamp:
            try:
                stime = self.get_time(timestamp)
            except:
                raise StateError(f"Unable to parse date string: {timestamp}")

            if stime not in self.states and not create:
                raise StateError(f"No state exists at {stime}.  If you wish to create a new state back in time, use --create")
        else:
            stime = self.get_time()
        
        nstate = self.states.get(stime, ACTIVITY_NEW_STATE.copy())

        if sessionid:
            nstate['sessionid'] = sessionid
        
        if status:
            if status in ACTIVITY_VALID_STATUS:
                nstate['status'] = status
            else:
                raise StateError(f"Invalid status: {status}")
        
        if comment:
            nstate['comment'] = comment
        
        if state:
            if state in ACTIVITY_VALID_STATES:
                nstate['state'] = state
            else:
                raise StateError(f"Invalid state: {state}")
        
        self.states[stime] = nstate
        self.write_activity_dict()

        return stime

    def create_activity(self, config):
        payload = {
            "input_parameters": {},
            "name": self.name
        }

        if not self.api.activity_exists(self.name):
            try:
                self.api.post_activity(payload)
            except Exception as e:
                config.logger.error(f"Unable to create activity: {e}")
                sys.exit(1)

    def monitor_session(self, config, sessionid, stime):
        config.logger.debug(f"Monitoring session {sessionid} at {stime}")
        completed = False

        self.state(timestamp=stime, status="Running")

        #spinner = itertools.cycle(['-', '/', '|', '\\'])
        # probably want some sort of duration abort here
        while not completed:
            #sys.stdout.write(next(spinner))
            #sys.stdout.flush()
            try:
                session = self.api.get_activity_session(self.name, sessionid)
            except Exception as e:
                config.logger.error(f"Unable to get session {sessionid}: {e}")
                sys.exit(1)

            status = session.json()['current_state']
            if status and status != 'in_progress':
                completed = True
            else:
                time.sleep(1)
            #sys.stdout.write('\b')
        
        if status == "completed":
            stat = "Succeeded"
        else:
            stat = "Failed"

        self.state(timestamp=stime, status=stat)
        
        config.logger.debug(f"Finished monitoring session {sessionid}")

        return stat

    def run_stage(self, config, stage):
        if not self.api.activity_exists(self.name):
            raise ActivityError(f"The activity {self.name} does not exist.")

        force = config.args.get("force", False)
        payload = {
            "input_parameters": {
                "force": force,
                "media_dir": config.args.get("media_dir"),
                "stages": [stage]
            }
        }

        bp_config_management = config.args.get("bootprep_config_management", None)
        if bp_config_management:
            payload["input_parameters"]["bootprep_config_management"] = bp_config_management

        bp_config_managed = config.args.get("bootprep_config_managed", None)
        if bp_config_managed:
            payload["input_parameters"]["bootprep_config_managed"] = bp_config_managed

        site_params = config.args.get("site_parameters", None)
        if site_params:
            payload["input_parameters"]["site_parameters"] = site_params

        try:
            api_result = self.api.post_activity_history_run(self.name, payload)
        except Exception as e:
            config.logger.error(f"Unable to execute stage: {e}")
            raise

        session = api_result.json()
        sessionid = session['name']
        stime = self.state(state="in_progress", sessionid=session["name"], comment=f"Run {stage}")
        config.logger.info(f"     SESSION ID: {sessionid}")
        status = self.monitor_session(config, session["name"], stime)
        config.logger.info(f"         RESULT: {status}")

        return status

def valid_activity_name(aname):
    if re.match('^[0-9A-Za-z\.-]+$', aname):
        return True
    return False