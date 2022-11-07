"""
Copyright 2022 Hewlett Packard Enterprise Development LP
"""

from xml.sax.handler import property_declaration_handler
import yaml
import os
import datetime
from dateutil import parser
from prettytable import PrettyTable
import uuid
import re

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

        retstring = "+" + "-" * (table_width - 2) + "+\n"
        retstring += f"| Activity: {self.name:<{table_width - 14}} |\n"
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

def fake_sessionid():
    """ Temporary function to return something random for sessionids until the api is functional"""
    return(uuid.uuid4())

def valid_activity_name(aname):
    if re.match('^[0-9A-Za-z\.-]+$', aname):
        return True
    return False