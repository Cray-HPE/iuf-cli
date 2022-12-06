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
import time
from lib.vars import MEDIA_BASE_DIR
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
    workflows = None
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
        self.workflows = []

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
    
    def get_next_workflow(self, config, sessionid):
        retflow = None
        found = False

        while not found:
            try:
                rsession = self.api.get_activity_session(self.name, sessionid)
            except Exception as e:
                config.logger.error(f"Unable to get session {sessionid}: {e}")
                sys.exit(1)
            
            session = rsession.json()
            status = session['current_state']
            if status and status != 'in_progress':
                found = True
                break
            
            if "workflows" in session:
                try:
                    for workflow in session['workflows']:
                        if workflow['id'] not in self.workflows:
                            retflow = workflow["id"]
                            self.workflows.append(retflow)
                            found = True
                except:
                    pass

            if not found:
                time.sleep(1)
        
        return retflow
    
    def sort_phases(self, workflow, nodes):
        """ depth first search """
        def dfs(data, start):
            path = []
            q = [start]
            while q:
                v = q.pop(0)
                if v not in path:
                    path = path + [v]
                    q = data[v] + q

            return path

        slist = dict()
        for name in nodes:
            node = nodes[name]
            if "children" in node and node["children"]:
                slist[name] = node["children"]
            else:
                slist[name] = []

        """ some stages start so slowly we call argo before they've fully started"""
        try:
            dpath = dfs(slist, workflow)
        except:
            """ if dfs fails it's because the workflow is still starting up, just punt until the next 1 second check"""
            dpath = []

        return dpath

    def monitor_workflow(self, config, workflow):
        config.logger.debug(f"Monitoring workflow {workflow}")
        finished = False
        rstatus = 'Unknown'
        phases = dict()
        newphase = {
            "finishedAt": None,
            "startedAt": None,
            "status": None,
            "id": None,
            "log": None
            }


        while not finished:
            try:
                wflow = self.get_workflow(config, workflow)
            except Exception as e:
                config.logger.error(f"Unable to get workflow {workflow}: {e}")
                sys.exit(1)
            
            """ TODO: Need to figure out how to tell if the workflow has failed in some bad way """
            try:
                completed = wflow['metadata']['labels']['workflows.argoproj.io/completed']
                if completed and completed == 'true':
                    rstatus = wflow['metadata']['labels']['workflows.argoproj.io/phase']
                    finished = True
            except:
                pass

            nodes = []
            try:
                rnodes = wflow["status"]["nodes"]
                if rnodes and type(rnodes) is dict:
                    nodes = rnodes
            except:
                pass

            npath = self.sort_phases(workflow, nodes)

            for name in npath:
                if name not in nodes:
                    continue
                node = nodes[name]
                if "displayName" in node:
                    dname = node["displayName"]
                    if name not in phases:
                        phases[name] = newphase.copy()
                    display = True
                    if dname == workflow:
                        display = False

                    if "startedAt" in node:
                        if not phases[name]["startedAt"]:
                            if display:
                                config.logger.info(f"        BEGIN PHASE: {dname}")
                        phases[name]["startedAt"] = node["startedAt"]

                    if "phase" in node:
                        phases[name]["status"] = node["phase"]

                    if "finishedAt" in node and node["finishedAt"]:
                        if not phases[name]["finishedAt"]:
                            if display:
                                status = phases[name]["status"]
                                config.logger.info(f"     FINISHED PHASE: {dname} [{status}]")
                        phases[name]["finishedAt"] = node["finishedAt"]
                        try:
                            for artifact in node["outputs"]["artifacts"]:
                                if artifact["name"] == "main-logs":
                                    s3 = artifact["s3"]["key"]
                                    phases[name]["log"] = s3
                                    config.logger.debug(f"           LOG FILE FOR {dname}: {s3}")
                        except:
                            pass

            if not finished:
                time.sleep(1)
        
        return rstatus

    def monitor_session(self, config, sessionid, stime):
        config.logger.debug(f"Monitoring session {sessionid} at {stime}")
        completed = False

        self.state(timestamp=stime, status="Running")

        while not completed:
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
        
        if status == "completed":
            stat = "Succeeded"
        else:
            stat = "Failed"

        self.state(timestamp=stime, status=stat)
        
        config.logger.debug(f"Finished monitoring session {sessionid}")

        return stat
    
    def get_workflow(self, config, workflow):
        try:
            wf = config.connection.run("argo -n argo get {workflow} -o yaml".format(workflow=workflow))
        except Exception as e:
            config.logger.error(f"Unable to get workflow {workflow}: {e}")
            sys.exit(1)
        
        return yaml.safe_load(wf.stdout)

    def run_stages(self, config):
        if not self.api.activity_exists(self.name):
            raise ActivityError(f"The activity {self.name} does not exist.")
        
        config.stages.set_summary("activity_session", config.args.get("activity_session"))
        config.stages.set_summary("media_dir", config.args.get("media_dir"))
        config.stages.set_summary("log_dir", config.args.get("log_dir"))       

        force = config.args.get("force", False)
        stages = config.stages.stages
        # API backend wants relative paths only.  We make sure media dir is under base dir
        # in Config, so we can just strip the base dir off the front of the media dir
        media_dir = config.args.get("media_dir")[len(MEDIA_BASE_DIR):]
        payload = {
            "input_parameters": {
                "force": force,
                "media_dir": media_dir
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

        limit_management = config.args.get("limit_management_nodes", None)
        if limit_management:
            payload["input_parameters"]["limit_management_nodes"] = limit_management

        limit_managed = config.args.get("limit_managed_nodes", None)
        if limit_managed:
            payload["input_parameters"]["limit_managed_nodes"] = limit_managed
        
        sessions = []
        """ Run process-media on its own first if we're doing it """
        if stages[0] == "process-media":
            stages.pop(0)
            payload["input_parameters"]["stages"] = ["process-media"]
            sid = self.run_stage(config, payload)
            sessions.append(sid)

        """ TODO: Get product list from API """
        """ TODO: Generate site_parameters and add to payload """
        
        """ Run any remaining stages """
        if stages:
            payload["input_parameters"]["stages"] = stages
            sid = self.run_stage(config, payload)
            sessions.append(sid)

        return sessions

    def run_stage(self, config, payload):
        try:
            api_result = self.api.post_activity_history_run(self.name, payload)
        except Exception as e:
            config.logger.error(f"Unable to execute stage: {e}")
            raise

        session = api_result.json()
        sessionid = session['name']
        config.logger.info("MONITORING SESSION: {}".format(sessionid))
        status = self.monitor_session(config, session["name"], stime)
        stage =  payload["input_parameters"]["stages"][0]
        if stage == "process-media":
            ret_code = self.api.get_activity_session(session['name'], sessionid)
            result = ret_code.json()
            products = result.get('products', None)
            session_vars = {}
            if products:
                for product in products:
                    name = product.get('name', None)
                    version = product.get('version', None)
                    session_vars[name] = {'version': version }
                config.args["session_vars"] = session_vars


        have_wf = True
        while have_wf:
            wfid = self.get_next_workflow(config, sessionid)
            if not wfid:
                break
            config.logger.debug("Next workflow {}".format(wfid))
            wf = self.get_workflow(config, wfid)
            stage = wf['metadata']['labels']['stage']
            self.state(state="in_progress", sessionid=wfid, comment=f"Run {stage}")
            config.stages.exec_stage(config, wfid, stage)

        return sessionid

def valid_activity_name(aname):
    if re.match('^[0-9A-Za-z\.-]+$', aname):
        return True
    return False
