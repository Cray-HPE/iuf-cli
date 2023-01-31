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

from xml.sax.handler import property_declaration_handler
import yaml
import os
import datetime
from dateutil import parser
from prettytable import PrettyTable
import re
import sys
import time
import lib.ApiInterface
import base64
import gzip

from lib.PodLogs import PodLogs
from lib.SiteConfig import SiteConfig

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
    'blocked'
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
        self.site_conf = None
        if os.path.exists(filename):
            self.load_activity_dict(filename)

            if self.name != name:
                raise ActivityError(f"{filename} contains the activity {self.name}, but {name} was specified.")
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
        table.field_names = ["Start", "Category", "Argo Workflow", "Status", "Duration", "Comment"]
        states = self.states
        ordered_states = sorted(states.keys())
        length = range(len(ordered_states))
        summary = dict()
        for x in length:
            start = ordered_states[x]
            state = states[start]
            if x+1 in length:
                end = ordered_states[x+1]
            else:
                end = start

            duration = self.get_duration(start, end)
            table.add_row([
                start,
                state['state'],
                state['sessionid'],
                state['status'],
                duration,
                state['comment']
            ])
            if state['state'] not in summary:
                summary[state['state']] = duration
            else:
                summary[state['state']] += duration

        table.align = "l"

        tstring = table.get_string()
        table_width = len(tstring.split("\n")[0])

        activity_name = self.name
        if not self.api.activity_exists(activity_name):
            activity_name += " [local only]"

        retstring = "+" + "-" * (table_width - 2) + "+\n"
        retstring += f"| Activity: {activity_name:<{table_width - 14}} |\n"
        retstring += tstring

        if summary:
            retstring += "\n\nSummary:\n"
            retstring += "  Start time: " + self.start + "\n"
            retstring += "    End time: " + ordered_states[-1] + "\n\n"
            for astate in ACTIVITY_VALID_STATES:
                if astate in summary:
                    retstring += f"{astate:>14}: {summary[astate]}\n"

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
        count = 0

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
                count += 1
                if (count % 600) == 0:
                    # it's been 10 minutes, give up if the admin hasn't
                    config.logger.error(f"Giving up after 10 minutes.  Check to ensure the ARGO backend is functional then try again.")
                    sys.exit(1)
                if (count % 15) == 0:
                    config.logger.warning(f"Still waiting for workflow startup after {count} seconds.")
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


        # Launch threads for the pod logs and continue on.  Gather the
        # threads after the while loop.
        podlogs = PodLogs(config, workflow)
        podlogs.follow_pod_logs(config) # threaded at top-level, no waiting.
        printed_s3 = {}
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
                # if the workflow gets too big it gets compressed
                if "compressedNodes" in wflow["status"]:
                    compressedNodes = wflow["status"]["compressedNodes"]
                    gzipNodes = base64.b64decode(compressedNodes)
                    uncompressedNodes = gzip.decompress(gzipNodes)
                    rnodes = yaml.safe_load(uncompressedNodes)
                else:
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
                                config.logger.info(f"             BEGIN: {dname}")
                        phases[name]["startedAt"] = node["startedAt"]

                    if "phase" in node:
                        phases[name]["status"] = node["phase"]

                    if "finishedAt" in node and node["finishedAt"]:
                        if not phases[name]["finishedAt"]:
                            if display:
                                status = phases[name]["status"]
                                if status == "Failed" or status == "Error":
                                    config.logger.error(f"         FINISHED: {dname} [{status}]")
                                if status == "Omitted":
                                    config.logger.warning(f"       FINISHED: {dname} [{status}]")
                                else:
                                    config.logger.info(f"          FINISHED: {dname} [{status}]")
                        phases[name]["finishedAt"] = node["finishedAt"]
                        try:
                            for artifact in node["outputs"]["artifacts"]:
                                if artifact["name"] == "main-logs":
                                    s3 = artifact["s3"]["key"]
                                    phases[name]["log"] = s3
                                    dname_s3 = f"{dname}: {s3}"
                                    if dname_s3 not in printed_s3:
                                        config.logger.debug(f"           LOG FILE FOR {dname_s3}")
                                        printed_s3[dname_s3] = True
                        except:
                            pass

            if not finished:
                time.sleep(1)

        podlogs.collect_threads()
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
            wf = config.connection.run("kubectl -n argo get Workflow/{workflow} -o yaml".format(workflow=workflow))
        except Exception as e:
            config.logger.error(f"Unable to get workflow {workflow}: {e}")
            sys.exit(1)

        return yaml.safe_load(wf.stdout)

    def abort_activity(self, config):
        """Abort an activity."""

        payload = {
            "input_parameters": {},
            "name": self.name,
            "comment": " ".join(config.args.get("comment", "")),
            "force": config.args.get("force"),
        }
        try:
            api_result = self.api.abort_activity(self.name, payload)
        except Exception as ex:
            config.logger.error(f"Unable to abort activity {self.name}: {ex}")
        return self.name

    def run_stages(self, config):
        if not self.api.activity_exists(self.name):
            raise ActivityError(f"The activity {self.name} does not exist.")

        config.stages.set_summary("activity", config.args.get("activity"))
        config.stages.set_summary("media_dir", config.args.get("media_dir"))
        config.stages.set_summary("log_dir", config.args.get("log_dir"))

        force = config.args.get("force", False)
        stages = config.stages.stages
        # API backend wants relative paths only.  We make sure media dir is under base dir
        # in Config, so we can just strip the base dir off the front of the media dir
        media_dir = config.args.get("media_dir")[len(config.media_base_dir):]

        media_host = config.args.get("media_host", "ncn-m001")
        concurrency = config.args.get("concurrency", None)
        if concurrency != None:
            concurrency = int(concurrency)

        payload = {
            "input_parameters": {
                "concurrency": concurrency,
                "force": force,
                "media_dir": media_dir,
                "media_host": media_host,
                "stages": [],
            }
        }
        self.site_conf = SiteConfig(config)
        self.site_conf.organize_merge()

        bp_config_management = config.args.get("relative_bootprep_config_management", None)
        if bp_config_management:
            payload["input_parameters"]["bootprep_config_management"] = bp_config_management

        bp_config_managed = config.args.get("relative_bootprep_config_managed", None)
        if bp_config_managed:
            payload["input_parameters"]["bootprep_config_managed"] = bp_config_managed

        bp_config_dir = config.args.get("relative_bootprep_config_dir", None)
        if bp_config_dir:
            payload["input_parameters"]["bootprep_config_dir"] = bp_config_dir

        limit_management = config.args.get("limit_management_rollout", None)
        if limit_management:
            payload["input_parameters"]["limit_management_nodes"] = limit_management

        limit_managed = config.args.get("limit_managed_rollout", None)
        if limit_managed:
            payload["input_parameters"]["limit_managed_nodes"] = limit_managed

        managed_rollout_strat = config.args.get("managed_rollout_strategy", None)
        if managed_rollout_strat:
            payload["input_parameters"]["managed_rollout_strategy"] = managed_rollout_strat

        rollout_percentage = config.args.get("concurrent_management_rollout_percentage")
        if rollout_percentage:
           payload["input_parameters"]["concurrent_management_rollout_percentage"] = rollout_percentage

        sessions = []

        # Run process-media on its own first if we're doing it.
        session_vars = {}
        if stages[0] == "process-media":
            stages.pop(0)
            payload["input_parameters"]["stages"] = ["process-media"]
            sid = self.run_stage(config, payload)
            sessions.append(sid)

        ret_code = self.api.get_activity(self.name)
        result = ret_code.json()
        products = result.get('products', {})
        session_vars = {}

        if not products:
            full_media_dir = config.media_base_dir + media_dir
            if "process-media" in payload["input_parameters"]["stages"]:
                config.logger.error(f"No IUF installable products were found in {full_media_dir}.")
            else:
                config.logger.error(f"No products were found in {self.name}.  Either the process-media stage has never been executed, or no IUF installable products were found in {full_media_dir}")
            return sessions

        for product in products:
            name = product.get('name', None)
            version = product.get('version', None)
            session_vars[name] = {'version': version }

        if session_vars:
            # session_vars should always exist if products exist; i.e, any
            # stage after process-media.
            self.site_conf.manage_session_vars(session_vars, write=True)

        # Generate site_parameters and patch the activity.
        patched_payload = payload
        patched_payload["site_parameters"] = self.site_conf.site_params
        self.api.patch_activity(self.name, patched_payload)

        # Run any remaining stages.
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
        config.logger.info("IUF SESSION: {}".format(sessionid))

        have_wf = True
        while have_wf:
            wfid = self.get_next_workflow(config, sessionid)
            if not wfid:
                break
            config.logger.debug("Next workflow {}".format(wfid))
            wf = self.get_workflow(config, wfid)
            stage = wf['metadata']['labels']['stage']
            self.site_conf.update_dict_stack(stage)
            config.stages.exec_stage(config, wfid, stage)

        return sessionid

def valid_activity_name(aname):
    if re.match('^[0-9A-Za-z\.-]+$', aname):
        return True
    return False
