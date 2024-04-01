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

import base64
import copy
import datetime
from dateutil import parser
import gzip
import json
import os
from prettytable import PrettyTable
import re
import requests
import shutil
import sys
import multiprocessing
import tarfile
import textwrap
import time
import yaml

import lib.ApiInterface
from lib.PodLogs import PodLogs
from lib.SiteConfig import SiteConfig
from lib.InstallerUtils import formatted, format_column
from lib.vars import ARG_DEFAULTS

class StateError(Exception):
    """A wrapper for raising a StateError exception."""
    pass

class ActivityError(Exception):
    """A wrapper for raising an ActivityError exception."""
    pass


ACTIVITY_NEW_STATE = {
    'state': None,
    'session': None,
    'workflow_id': None,
    'status': None,
    'comment': None,
    'command': None,
    "args": None,
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
    'n/a',
    # Functional statuses.
    "restart",
    "resume",
    "abort",
]

def list_activity():
    """
    List an activity.  This is done outside the activity class so
    a user can list activities without having to specify an activity.
    """
    act_list = []
    api = lib.ApiInterface.ApiInterface()
    if api is not None:
        activities = api.get_activities().json()
        if activities is not None:
            act_list = sorted([act["name"] for act in activities])
   
    return "\n".join(act_list)


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
    st_event = None
    running_procs = None

    def __init__(self, filename=None, name=None, dryrun=False, config=None):
        self.states = dict()

        self.dryrun = dryrun
        self.filename = filename
        self.site_conf = None
        self.config = config
        self._media_dir = None
        self.sessions_dir = os.path.join(self.config.args["state_dir"], "sessions")
        if os.path.exists(filename):
            try:
                self.load_activity_dict(filename)
            except (IndexError, TypeError):

                for index in range(1000):
                    backup_file = f"{filename}.{index}"
                    if not os.path.exists(backup_file):
                        break
                self.config.logger.debug(f"Activity file {filename} seems corrupt.\n"
                                         f"Backing it up to {backup_file} and creating a new one.")
                shutil.copyfile(filename, backup_file)
                self.write_activity_dict()

            if self.name != name:
                raise ActivityError(f"{filename} contains the activity {self.name}, but {name} was specified.")
        else:
            self.name = name

            self.initialized = True
            self.write_activity_dict()

        self.podlogs = PodLogs(config, self.name)

        self.api = lib.ApiInterface.ApiInterface()
        self.workflows = []

    def __repr__(self):
        return repr(self.__dict__)

    def __str__(self):
        """Print the activity in a nice table."""
        table = PrettyTable()
        table.field_names = ["Start / Session", "Category", "Command / Argo Workflow", "Status", "Duration", "Comment"]
        states = self.states
        ordered_states = sorted(states.keys())
        length = range(len(ordered_states))
        summary = dict()
        session = None
        session_times = {}
        for x in length:
            start = ordered_states[x]
            state = states[start]
            if x+1 in length:
                end = ordered_states[x+1]
            else:
                end = start
            duration = self.get_duration(start, end)

            for key in ACTIVITY_NEW_STATE:
                if key not in state:
                    state[key] = ACTIVITY_NEW_STATE[key]
            if session != state['session'] and state['session'] != None:
                # This is a new session.
                session = state['session']
                session_times[session] = {"timestamp": start, "duration": duration}
                if x != 0:
                    table.add_row(["-------------------", "-----", "-----", "-----", "-----", "-----"])
                command = "command: {}".format(state["command"]) if "command" in state else ""
                command = " \\ \n".join(textwrap.wrap(command, width=45, break_long_words=False, break_on_hyphens=False))
                table.add_row([f"session: {session}","", command, "", "", ""])
            elif session:
                # This is the current session.  session_times[session]["duration"] will be
                # over-written until the session changes.
                session_times[session]["duration"] = self.get_duration(session_times[session]["timestamp"], start)

            table.add_row([
                start,
                state['state'],
                state['workflow_id'],
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
        retstring = []
        retstring.append("+" + "-" * (table_width - 2) + "+")
        retstring.append(f"| Activity: {activity_name:<{table_width - 14}} |")
        retstring.append(tstring)

        if summary:
            retstring.append("Summary:")
            retstring.append("  Start time: " + self.start)
            retstring.append("  End time:   " + ordered_states[-1] + "\n")
            retstring.append("  Time spent in sessions:")
            for sess in session_times:
                retstring.append("    {}: {}".format(sess, session_times[sess]["duration"]))
            retstring.append("")
            stage_durations = {}
            longest_stagename = 0
            for stage in self.config.stages.get_stage_status(self.name):
                if stage["duration"]:
                    stage_durations[stage["stage"]] = stage["duration"]
                    stagename_len = len(stage["stage"])
                    if stagename_len > longest_stagename:
                        longest_stagename = stagename_len

            if stage_durations:
                retstring.append("  Stage Durations:")
                for stage in stage_durations:
                    retstring.append("    {0:>{longest_stagename}}: {1}".format(stage, stage_durations[stage], longest_stagename=longest_stagename))
            retstring.append("")

            retstring.append("  Time spent in states:")
            longest_state = max(ACTIVITY_VALID_STATES, key=len)
            state_len = max(map(len, ACTIVITY_VALID_STATES))
            for astate in ACTIVITY_VALID_STATES:
                if astate in summary:
                    retstring.append("{0:>{state_len}}: {1}".format(astate, summary[astate], state_len=state_len))

            retstring.append("")
            total_time = self.get_duration(self.start, ordered_states[-1])
            retstring.append(f"  Total time: {total_time}")
            if "paused" in summary:
                active_time = total_time - summary['paused']
                retstring.append(f"  Unpaused time: {active_time}")
        return "\n".join(retstring)

    def yaml(self):
        return yaml.dump(self.get_dict())

    def get_dict(self):
        curdict = dict()
        curdict[self.name] = dict()
        curdict[self.name]["states"] = self.states
        curdict[self.name]["start"] = self.start

        return curdict

    @property
    def media_dir(self):
        arg_media_dir = self.config.args.get("media_dir", None)
        if not self._media_dir:
            if arg_media_dir:
                if os.path.exists(arg_media_dir):
                    self._media_dir = arg_media_dir
                else:
                    for path in [os.path.abspath(arg_media_dir), os.path.join(self.config.media_base_dir, arg_media_dir)]:
                        if os.path.exists(path):
                            self._media_dir = path
                            break
            elif self.states:
                ordered_states = sorted(self.states.keys())
                last_state = self.states[ordered_states[-1]]
                if "args" in last_state and "media_dir" in last_state["args"]:
                    self._media_dir = last_state["args"]["media_dir"]

            # If self._media_dir still isn't set, check if the activity name
            # exists under the media base directory.
            if not self._media_dir and os.path.exists(os.path.join(self.config.media_base_dir, self.name)):
                self._media_dir  = os.path.join(self.config.media_base_dir, self.name)

        # if _media_dir still isn't set, raise an error.
        if not self._media_dir:
            if arg_media_dir:
                if not os.path.exists(arg_media_dir):
                    self.config.logger.error(f"Could not determine media dir from argument '--media-dir {arg_media_dir}'.  Does the directory exist?")
                    sys.exit(1)
            else:
                err_msg = f"""
                    Could not determine the media_dir.  Ensure the directory exists under
                    {self.config.media_base_dir} and use the '--media-dir <directory>' option."""
                self.config.logger.error(formatted(err_msg))
                sys.exit(1)

        return self._media_dir


    @media_dir.setter
    def media_dir(self, val):
        self._media_dir = val
        self.config.args["media_dir"] = self._media_dir

    def load_activity_dict(self, filename=None, encoding='UTF-8'):
        if not filename or not os.path.exists(filename):
            return
        with open(filename, "r", encoding=encoding) as f:
            masterdict = yaml.full_load(f)
            if len(masterdict) > 1:
                raise ActivityError(f"{filename} contains multiple activities.  Remove any manual modifications, or remove the file, and try again.")

            aname = list(masterdict)[0]
            activity = masterdict[aname]
            last_finished = None

            self.name = aname
            self.filename = filename
            self.states = dict()
            if "states" in activity:
                for rtime in sorted(activity["states"].keys()):
                    state = activity["states"][rtime]

                    # "sessionid" was renamed "workflow_id" between the 1.4 and 1.5
                    # IUF/CSM releases. The "sessionid" check could probably be
                    # removed in some later release.
                    if "sessionid" in state.keys():
                        state["workflow_id"] = state.pop("sessionid")

                    # reprocess the time in case the formats changed or something
                    stime = self.get_time(rtime)
                    self.states[stime] = dict()
                    for attr in ACTIVITY_NEW_STATE:
                        if attr in state:
                            # Update any sessions that don't have a finished state
                            if attr == "status" and state[attr] not in ["Succeeded","Failed", "restart", "resume", "abort"]:
                                if state.get("workflow_id", None):
                                    state['status'], last_finished = self.get_workflow_status(state["workflow_id"])
                            self.states[stime][attr] = state[attr]

            # now see if the client was killed before putting the activity into debug or waiting_admin
            ordered_states = sorted(self.states.keys())
            last_state = self.states[ordered_states[-1]]
            workflow_id = last_state.get("workflow_id", None)

            if workflow_id:
                # ok, so the last state has a workflow_id so it isn't a debug/waiting
                last_status, last_finished = self.get_workflow_status(workflow_id)

                if last_status == "Succeeded":
                    self.state({"timestamp": last_finished, "state": 'waiting_admin', "create": True})
                elif last_status == "Failed":
                    self.state({"timestamp": last_finished, "state": 'debug', "create": True})
                #else it's still running and we don't care for now

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

    @property
    def start(self):
        ordered_states = sorted(self.states.keys())
        if ordered_states:
            return ordered_states[0]
        else:
            return self.get_time()

    def state(self, kwargs):

        defaults = {
            "timestamp": None,
            "workflow_id": None,
            "session": None,
            "state": None,
            "status": "n/a",
            "comment": None,
            "create": False,
            "command": None,
        }

        timestamp = kwargs["timestamp"] if "timestamp" in kwargs else defaults["timestamp"]
        if timestamp:
            try:
                stime = self.get_time(timestamp)
            except:
                raise StateError(f"Unable to parse date string: {timestamp}")

            if stime not in self.states and not kwargs.get("create", False):
                raise StateError(f"No state exists at {stime}.  If you wish to create a new state back in time, use --create")
        else:
            stime = self.get_time()

        nstate = self.states.get(stime, ACTIVITY_NEW_STATE.copy())

        for key in defaults:
            if key in kwargs:
                nstate[key] = kwargs[key]

        if "status" in nstate and nstate["status"] not in ACTIVITY_VALID_STATUS + [None]:
            raise StateError("Invalid status: {}".format(nstate["status"]))

        if "state" in nstate and nstate["state"] not in ACTIVITY_VALID_STATES + [None]:
            raise StateError(f"Invalid state: {nstate['state']}")

        nstate["command"] = " ".join(sys.argv)
        nstate["args"] = copy.deepcopy(self.config.args)

        self.states[stime] = nstate
        self.write_activity_dict()

        return stime

    def create_activity(self):
        payload = {
            "input_parameters": {},
            "name": self.name
        }

        if not self.api.activity_exists(self.name):
            try:
                self.api.post_activity(payload)
            except Exception as e:
                self.config.logger.error(f"Unable to create activity: {e}")
                sys.exit(1)

    def get_next_workflow(self, workflow_id):
        retflow = None
        found = False
        count = 0

        while not found:
            try:
                rsession = self.api.get_activity_session(self.name, workflow_id)
            except Exception as e:
                self.config.logger.error(f"Unable to get session {workflow_id}: {e}")
                sys.exit(1)


            # FIXME:
            # We sometimes abort on this line because rsession returns a list of
            # dicts rather than a dict.
            tmp_session = rsession.json()
            if type(tmp_session) is list:
                if len(tmp_session) > 1:
                    self.config.logger.warning("multiple sessions found.  Taking the first one...")
                session = tmp_session[0]
            else:
                session = tmp_session

            status = session['current_state']
            if status and status not in ["in_progress", "transitioning"]:
                self.config.logger.debug(f"Session {workflow_id} is not in progress.  Status: {status}")
                found = True
                break

            if status and "workflows" in session:
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
                    self.config.logger.error("Giving up after 10 minutes.  Check to ensure the ARGO backend is functional then try again.")
                    sys.exit(1)
                if (count % 15) == 0:
                    self.config.logger.warning(f"Still waiting for workflow startup after {count} seconds.")
                time.sleep(1)

        return retflow

    def sort_phases(self, workflow, nodes):

        def dfs(data, start):
            """ depth first search """
            path = []
            q = [start]
            while q:
                v = q.pop(0)
                if v not in path:
                    path = path + [v]
                    if v in data:
                        q = data[v] + q

            return path

        slist = dict()
        for name in nodes:
            node = nodes[name]
            if "children" in node and node["children"]:
                slist[name] = node["children"]
            else:
                slist[name] = []

        # Some stages start so slowly we call argo before they've fully started.
        try:
            dpath = dfs(slist, workflow)
        except:
            # If dfs fails it's because the workflow is still starting up, just punt until the next 1 second check.
            dpath = []

        return dpath

    def generate_user_readable(self, stage, name, dname):
        readable = name
        try:
            parts = name.split(".",2)
            if stage == "process-media":
                if parts[2].startswith("extract-tar-file"):
                    readable = parts[2]
                else:
                    readable = parts[1]
            elif stage in ["deliver-product","update-vcs-config","deploy-product","post-install-service-check","post-install-check"]:
                readable = "-".join(parts[1].split("-")[:-1])
            elif stage in ["management-nodes-rollout","managed-nodes-rollout"]:
                readable = parts[2].rstrip("()0123456789")
            elif stage == "pre-install-check":
                if parts[1].endswith("]"):
                    readable = "-".join(parts[1].split("-")[:-1])
                else:
                    readable = parts[1].rstrip("[]0123456789")
            else:
                readable = parts[1].rstrip("[]0123456789")
        except:
            readable = dname

        retval = format_column(readable)

        return retval

    def get_workflow_status(self, workflow):
        rstatus = "Unknown"
        rfinished = None

        try:
            wflow = self.get_workflow(workflow)
            rstatus = wflow['metadata']['labels']['workflows.argoproj.io/phase']
            rfinished = wflow['status']['finishedAt']
        except:
            pass

        return rstatus, rfinished

    def collect_procs(self):
        if self.st_event:
            # self.st_event may not be set if (for example) abort is called
            # from a different terminal than the `run` command was called
            # from.
            self.st_event.set()

        if self.running_procs:
            for proc in self.running_procs:
                proc.join()
            self.running_procs = []

    def initialize_multiprocessing(self):
        self.st_event = multiprocessing.Event()
        self.running_procs = []

    def monitor_workflow(self, workflow):
        self.config.logger.debug(f"Monitoring workflow {workflow}")
        finished = False
        rstatus = 'Unknown'
        phases = {}
        newphase = {
            "finishedAt": None,
            "startedAt": None,
            "status": None,
            "id": None,
            "log": None
            }

        # Launch subprocesses for the pod logs and continue on.  Gather the
        # processes after the while loop.
        printed_s3 = {}
        followed_pods = list()
        self.initialize_multiprocessing()
        while not finished:
            try:
                wflow = self.get_workflow(workflow)
            except Exception as e:
                self.config.logger.debug(f"Unable to get workflow {workflow}: {e}")

            """ TODO: Need to figure out how to tell if the workflow has failed in some bad way """

            try:
                completed = wflow['metadata']['labels']['workflows.argoproj.io/completed']
                if completed and completed == 'true':
                    rstatus = wflow['metadata']['labels']['workflows.argoproj.io/phase']
                    finished = True
            except:
                pass

            stage = "unknown"
            try:
                stage = wflow['metadata']['labels']['stage']
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

                dname = node.get("displayName", "unknown")
                step_name = node.get("name", "unknown")

                log_prefix = "unknown"
                if "name" in node:
                    try:
                        log_prefix = self.generate_user_readable(stage, step_name, dname)
                    except:
                        pass

                if "type" in node and node["type"] == "Pod":
                    node_id = node["id"]
                    podname = ""
                    try:
                          template = ""
                          node_id_splits = node_id.rsplit('-',1)
                          if "templateName" in node.keys():
                              template = node["templateName"]
                          elif "templateRef" in node.keys() and "template" in node["templateRef"].keys():
                              template =  node["templateRef"]["template"]
                          ## no need for an else statement, the pod name will have two '--' if the keys are not present
                          podname = node_id_splits[0] + "-" + template + "-" + node_id_splits[1]
                    except:
                        pass
                    if podname not in followed_pods:
                        followed_pods.append(podname)
                        for container in ["init", "wait", "main"]:
                            proc = multiprocessing.Process(target=self.podlogs.follow_pod_log, args=(podname, container, log_prefix, self.st_event))
                            proc.start()
                            self.running_procs.append(proc)
                if "displayName" in node:
                    if name not in phases:
                        phases[name] = newphase.copy()
                    display = True
                    if dname == workflow:
                        display = False
                    if "type" in node and node["type"] == "StepGroup":
                        display = False

                    if "startedAt" in node:
                        if not phases[name]["startedAt"]:
                            if display:
                                self.config.logger.info(f"{log_prefix} BEG {dname}")
                        phases[name]["startedAt"] = node["startedAt"]

                    if "phase" in node:
                        phases[name]["status"] = node["phase"]

                    if "finishedAt" in node and node["finishedAt"]:
                        if not phases[name]["finishedAt"]:
                            if display:
                                status = phases[name]["status"]
                                if status == "Failed" or status == "Error":
                                    self.config.logger.error(f"{log_prefix} END {dname} [{status}]")
                                if status == "Omitted":
                                    self.config.logger.warning(f"{log_prefix} END {dname} [{status}]")
                                else:
                                    self.config.logger.info(f"{log_prefix} END {dname} [{status}]")
                        phases[name]["finishedAt"] = node["finishedAt"]
                        try:
                            for artifact in node["outputs"]["artifacts"]:
                                if artifact["name"] == "main-logs":
                                    s3 = artifact["s3"]["key"]
                                    phases[name]["log"] = s3
                                    dname_s3 = f"{dname}: {s3}"
                                    timestamp= self.config.timestamp
                                    logdir=self.config.args.get("log_dir")
                                    file_name = s3.split("/")[-2]
                                    if dname_s3 not in printed_s3:
                                        self.config.logger.debug(f"{log_prefix} LOG FILE FOR {dname}: {logdir}/{timestamp}/argo_logs/{file_name}-main.txt")
                                        printed_s3[dname_s3] = True
                        except:
                            pass

            if not finished:
                time.sleep(1)
        # now we're finished
        self.collect_procs()
        return rstatus

    def monitor_session(self, workflow_id, stime):
        self.config.logger.debug(f"Monitoring workflow {workflow_id} at {stime}")
        completed = False

        self.state({"timestamp": stime, "status": "Running"})

        while not completed:
            try:
                session = self.api.get_activity_session(self.name, workflow_id)
            except Exception as e:
                self.config.logger.error(f"Unable to get session {workflow_id}: {e}")
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

        self.state({"timestamp": stime, "status": stat})

        self.config.logger.debug(f"Finished monitoring workflow {workflow_id}")

        return stat

    def get_workflow(self, workflow):
        try:
            wf = self.config.connection.run(f"kubectl -n argo get Workflow/{workflow} -o yaml")
        except Exception as e:
            self.config.logger.debug(f"Unable to get workflow {workflow}: {e}")
            return None

        return yaml.safe_load(wf.stdout)

    def abort_activity(self, background_only=False):
        """Abort an activity."""

        if background_only:
            # If podlogs aren't initialized yet, skip collecting processes,
            # since there will not be any.
            if self.podlogs:
                self.collect_procs()
            return

        comment_arg = self.config.args.get("comment", "")
        if type(comment_arg) is list:
            comment = " ".join(comment_arg)
        else:
            comment = comment_arg

        payload = {
            "input_parameters": {},
            "name": self.name,
            "comment": comment,
            "force": self.config.args.get("force"),
        }
        try:
            self.config.logger.debug(f"sending an abort, background_only={background_only}, payload={payload}")
            self.api.abort_activity(self.name, payload)
            if self.podlogs:
                self.collect_procs()
        except requests.ReadTimeout:
            self.config.logger.warning("Timed out sending an abort request.")
            self.config.logger.warning(f"Ensure the argo workflow for {self.name} is not running.")
        except Exception as ex:
            self.config.logger.error(f"Unable to abort activity {self.name}: {ex}")
            raise

        # Add an entry to the activity log.
        self.state({"comment": comment, "status": "abort"})

        return self.name

    def restart(self):
        self.config.logger.info(f"Attempting to restart activity {self.name}")
        if not self.api.activity_exists(self.name):
            self.config.logger.error(f"Activity {self.name} does not exist.")
            return

        payload = {
            "input_parameters": {},
            "comment": " ".join(self.config.args.get("comment", "")),
            "activity_name": self.config.args.get("activity"),
            "force": self.config.args.get("force", False),
        }
        try:
            api_results = self.api.post_restart(self.name, payload)
        except Exception as ex:
            self.config.logger.error(f"Unable to restart activity {self.name}: {ex}")
            raise

        api_json = api_results.json()
        workflow = api_json["name"]
        if "input_parameters" in api_json and "stages" in api_json["input_parameters"]:
            stages = api_json["input_parameters"]["stages"]
            self.config.stages.set_stages(stages)

        self.site_conf = SiteConfig(self.config)
        self.site_conf.organize_merge()
        self.watch_next_wf(workflow)

    def run_stages(self, resume=False):
        self.config.logger.info(f"Inside run stages.")
        if not self.api.activity_exists(self.name):
            raise ActivityError(f"The activity {self.name} does not exist.")

        self.config.stages.set_summary("activity", self.config.args.get("activity"))
        self.config.stages.set_summary("media_dir", self.config.args.get("media_dir"))
        self.config.stages.set_summary("state_dir", self.config.args.get("state_dir"))
        logdir = os.path.join(self.config.args.get("log_dir"), self.config.timestamp)
        self.config.stages.set_summary("log_dir", logdir)

        self.config.logger.info(f"{self.config}")

        try:
        force = self.config.args.get("force", False)
        stages = self.config.stages.stages

        if not resume:
            with open(os.path.join(self.config.args.get("state_dir"), f"{self.name}_stages.yaml"), "w") as fhandle:
                yaml.dump(stages, fhandle)

        # API backend wants relative paths only.  We make sure media dir is under base dir
        # in Config, so we can just strip the base dir off the front of the media dir
        media_dir = self.media_dir[len(self.config.media_base_dir):]

        media_host = self.config.args.get("media_host", "ncn-m001")
        concurrency = self.config.args.get("concurrency", None)
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
        try:
            self.site_conf = SiteConfig(self.config)
        except Exception as e:
            self.config.logger.info(f"{e}")
            
        self.site_conf.organize_merge()
        self.config.logger.info(f"{self.site_conf}")
        self.config.stages.set_summary("site_vars", self.site_conf.sv_path)

        bp_config_management = self.config.args.get("relative_bootprep_config_management", None)
        if bp_config_management:
            payload["input_parameters"]["bootprep_config_management"] = bp_config_management

        bp_config_managed = self.config.args.get("relative_bootprep_config_managed", None)
        if bp_config_managed:
            payload["input_parameters"]["bootprep_config_managed"] = bp_config_managed

        bp_config_dir = self.config.args.get("relative_bootprep_config_dir", None)
        if bp_config_dir:
            payload["input_parameters"]["bootprep_config_dir"] = bp_config_dir

        limit_management = self.config.args.get("limit_management_rollout", None)
        if limit_management:
            payload["input_parameters"]["limit_management_nodes"] = limit_management

        limit_managed = self.config.args.get("limit_managed_rollout", None)
        if limit_managed:
            payload["input_parameters"]["limit_managed_nodes"] = limit_managed

        managed_rollout_strat = self.config.args.get("managed_rollout_strategy", None)
        if managed_rollout_strat:
            payload["input_parameters"]["managed_rollout_strategy"] = managed_rollout_strat

        rollout_percentage = self.config.args.get("concurrent_management_rollout_percentage")
        if rollout_percentage:
           payload["input_parameters"]["concurrent_management_rollout_percentage"] = rollout_percentage

        sessions = []

        # Run process-media on its own first if we're doing it.
        session_vars = {}
        if stages[0] == "process-media":
            stages.pop(0)
            payload["input_parameters"]["stages"] = ["process-media"]
            sid = self.run_stage(payload)
            sessions.append(sid)
        self.config.logger.info(f"2nd time get activity.")
        ret_code = self.api.get_activity(self.name)
        result = ret_code.json()
        products = result.get('products', {})
        session_vars = {}
        self.config.logger.info(f"products : {products}.")
        if not products:

            full_media_dir = self.config.media_base_dir + media_dir
            if "process-media" in payload["input_parameters"]["stages"]:
                self.config.logger.error(f"No IUF installable products were found in {full_media_dir}.")
            else:
                self.config.logger.error(f"No products were found in {self.name}.  Either the process-media stage has never been executed, or no IUF installable products were found in {full_media_dir}")
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
        patched_payload = copy.deepcopy(payload)
        patched_payload["site_parameters"] = self.site_conf.site_params

        # Remove the "force" key from input_parameters for the patched
        # activity.
        patched_payload["input_parameters"].pop("force", None)
        self.config.logger.info(f"calling patch.")
        self.api.patch_activity(self.name, patched_payload)

        # Run any remaining stages.
        if stages:
            payload["input_parameters"]["stages"] = stages
            sid = self.run_stage(payload)
            sessions.append(sid)

        return sessions

    @property
    def bootprep_commands(self):
        if self.site_conf and self.site_conf.bootprep_commands:
            return textwrap.indent("\n".join(self.site_conf.bootprep_commands),
                                   " " * 4)
        else:
            return ""

    def get_wf_dict(self, workflow_arg=None):
        filenames = os.listdir(self.sessions_dir)
        workflows = {}
        stages = self.config.stages.get(list_fmt=True)
        for fname in filenames:
            for stage in stages:
                stage_str = f"-{stage}.yaml"
                if fname.endswith(stage_str):
                    if not workflow_arg:
                        workflows[fname .replace(stage_str, "")] = os.path.join(self.sessions_dir, fname)
                    else:
                        for workflow in workflow_arg:
                            if fname.startswith(workflow):
                                workflows[fname .replace(stage_str, "")] = os.path.join(self.sessions_dir, fname)

        return workflows

    def workflow_info(self):
        return_text = []
        return_states = []
        workflows = self.config.args["workflows"]
        wf_dict = self.get_wf_dict(workflows)
        debug_mode = self.config.args.get("debug", False)
        if not workflows:
            return_text = list(wf_dict.keys())
            return "\n".join(return_text)

        script_stdout = None
        def get_nested_dict(dictarg, keyname):
            """Get a value for a particular key in a nested dictionary"""
            nonlocal script_stdout
            for key in dictarg:
                if type(dictarg[key]) is dict:
                    get_nested_dict(dictarg[key], keyname)
                elif key == keyname:
                    # The eval below is to remove a quoting issue.
                    script_stdout = dictarg[key]

        #### We're still here.   There are workflows to process.
        state_dict = {}
        for workflow in wf_dict:
            wf_info = None
            with open(wf_dict[workflow], "r") as fh:
                wf_info = yaml.load(fh, yaml.SafeLoader)

            all_states = [self.states[s] for s in self.states if "workflow_id" in self.states[s] and self.states[s]["workflow_id"] == workflow]
            if wf_info:
                get_nested_dict(wf_info, "script_stdout")

            for state in all_states:
                state_dict = {}
                for arg in  [ "workflow_id", "session", "command", "media_dir", "status"]:
                    if arg in state:
                        state_dict[arg] = state[arg]
                state_dict["args"] = {}
                if script_stdout:
                    # Remove the double-quotes.
                    state_dict["script_stdout"] = eval(str(script_stdout))
                if "args" in state:
                    for arg in state["args"]:
                        if arg in ARG_DEFAULTS and ARG_DEFAULTS[arg] == state["args"][arg] and not debug_mode:
                            pass
                        elif debug_mode:
                            state_dict["args"][arg] = state["args"][arg]
                        elif state["args"][arg]:
                            state_dict["args"][arg] = state["args"][arg]
            if state_dict:
                return_states.append(state_dict)

            # Convert return_states to text.
            for state in return_states:
                return_text.append(yaml.dump(state, default_flow_style=False, sort_keys=False))
        if not wf_dict:
            self.config.logger.warning("Could not find a sessions for workflows: {}.  Was the right workflow entered?".format(", ".join(workflows)))
        elif not return_states:
            self.config.logger.warning("Could not parse the activity dict.  Was it generated in a previous incompatible release?")

        return "\n".join(return_text)

    def watch_next_wf(self, sessionid):
        while True:
            wfid = self.get_next_workflow(sessionid)
            if not wfid:
                self.config.logger.debug(f"No more workflows found for session {sessionid}.")
                break
            self.config.logger.debug("Next workflow {}".format(wfid))
            wf = self.get_workflow(wfid)
            if not wf:
                break
            stage = wf['metadata']['labels']['stage']
            self.site_conf.update_dict_stack(stage)

            # Dump the session/workflow information for debugging purposes.
            sessions_dir = self.sessions_dir
            if not os.path.exists(sessions_dir):
                os.mkdir(sessions_dir)
            cfgmaps = json.loads(self.config.connection.sudo("kubectl get configmaps -n argo  {} -o jsonpath='{{.data.iuf_activity}}'".format(self.name)).stdout)
            cfgmaps["sessionid"] = sessionid
            outfile = os.path.join(sessions_dir, f"{wfid}-{stage}.yaml")
            with open(outfile, "w", encoding='UTF-8') as fhandle:
                yaml.dump(cfgmaps, fhandle)
            files = [file for file in os.listdir(self.media_dir) if file.endswith(".tar.gz")]
            try:
                products = cfgmaps["operation_outputs"]["stage_params"]["process-media"]["products"]
                cfgmap_locs = [products[prod]["parent_directory"] for prod in products]
            except TypeError:
                # This happens during a new activity on process-media.
                cfgmap_locs = {}

            # Check the integrity of the tarballs in media_dir.
            for product in files:
                prodpath = os.path.join(self.media_dir, product)
                tar_paths = []
                try:
                    with tarfile.open(prodpath, "r:gz") as tarf:
                        counter = 0
                        for entry in tarf:
                            tar_paths.append(entry.name)
                            counter += 1
                            if counter > 10:
                                break
                except tarfile.ReadError as readerr:
                    self.config.logger.debug(f"Problems extracting {prodpath}:")
                    self.config.logger.debug(f"\t{readerr}")
                    self.config.logger.debug("Ignoring it!")
                    continue
                tar_path = None
                for path in tar_paths:
                    if path.startswith('.'):
                        continue
                    tar_path = os.path.join(self.media_dir, path.split("/")[0])
                    break
                if not tar_path:
                    # This should only be hit with a corrupt or empty tarball.
                    self.config.logger.debug(f"Couldn't find a directory for the {prodpath} tar file")
                    continue

                if tar_path and os.path.exists(tar_path):
                    # The path exists and process_media has been run.  Note
                    # the directory will not exist before process-media is ran.
                    dir_contents = os.listdir(tar_path)
                    if "iuf-product-manifest.yaml" in dir_contents:
                        matches = [cp for cp in cfgmap_locs if cp == tar_path]
                        if len(matches) < 1 and stage != "process-media":
                            # iuf-product-manifrest.yaml exists, so this
                            # product should appear in in the cfgmap.
                            # It is NOT in the cfgmap, so send a warning.
                            new_tar_msg = formatted(f"""
                                Tar file {product} found in media directory
                                but is not present in the list of products
                                IUF is working with, you will need re-run
                                starting from the process-media stage to
                                include this product.""")
                            self.config.logger.warning(new_tar_msg)
                elif tar_path:
                    # The tarfile exists within the media directory, but it hasn't been extracted.
                    if stage != "process-media":
                        new_tar_msg = formatted(f"""
                            {product} was found in the media directory, but it
                            hasn't been extracted.  Does process-media need
                            to be re-ran?""")
                        self.config.logger.debug(new_tar_msg)
            # Run the stage.
            self.config.stages.exec_stage(self.config, wfid, sessionid, stage)

    def run_stage(self, payload):
        try:
            api_result = self.api.post_activity_history_run(self.name, payload)
        except Exception as e:
            self.config.logger.error(f"Unable to execute stage: {e}")
            raise

        session = api_result.json()
        sessionid = session['name']
        prefix = format_column(f"IUF SESSION: {sessionid}")
        self.config.logger.info(f"{prefix} BEG Started at {datetime.datetime.now()}")

        self.watch_next_wf(sessionid)

        self.config.logger.info(f"{prefix} END Completed at {datetime.datetime.now()}")

        return sessionid

    def resume(self):
        """Resume an activity."""
        self.config.logger.info(f"Attempting to resume activity {self.name}")
        if not self.api.activity_exists(self.name):
            self.config.logger.error(f"Activity {self.name} does not exist.")
            return

        last_activity = {}
        last_workflow_id = None
        self.site_conf = SiteConfig(self.config)
        self.site_conf.organize_merge()

        skeys = sorted(self.states.keys(), reverse=True)
        for key in skeys:
            if "workflow_id" in self.states[key] and self.states[key]["workflow_id"]:
                last_activity = self.states[key]
                last_workflow_id = last_activity["workflow_id"]
                break

        # process-media is a unique stage, because it is sent separately to
        # the backend because it returns data needed by the API.  After
        # process-media succeeds, the remaining stages are sent to the API.
        # Cases to consider:
        # 1.    The activity is still running.
        #       -- a.   The activity disconnected in process media -- No other
        #               stages can be obtained from the backend.
        #       -- b.   The activity made it beyond process media. -- In this
        #               case, we can get the stages from the backend.
        # 2.    The activity is not still running.  In this case, I think we
        #       send a 'resume' to the backend, and monitor it. Still need to pay attention
        # to whether or not process-media is the first stage.
        try:
            result = self.api.get_activity(self.name)
            backend_data = result.json()
        except:
            self.config.logger.error(f"Unable to get activity {self.name} from backend.")
            return

        if last_activity["state"] == 'in_progress' and last_activity["status"].lower() not in ["succeeded", "failed"]:
            # 1. The activity is still running.
            if 'input_parameters' in backend_data and 'stages' in backend_data['input_parameters']:
                backend_stages = backend_data['input_parameters']['stages']
                last_stages = []
                stages_file = os.path.join(self.config.args.get("state_dir"), f"{self.name}_stages.yaml")
                if os.path.exists(stages_file):
                    with open(stages_file, "r", encoding="UTF-8") as fhandle:
                        last_stages = yaml.load(fhandle, yaml.SafeLoader)
                if last_stages and last_stages[0] == "process-media":
                    last_stages.pop(0)
                if backend_stages[0] == "process-media":
                    # 1a -- disconnected in process media.  -- will stop after.
                    self.monitor_workflow(last_workflow_id)
                    if last_stages:
                        self.config.stages.set_stages(last_stages)
                        self.run_stages(resume=True)
                else:
                    # 1b -- Disconnected after process-media.  The backend
                    # should have all the necessary stage information.  Just re-attach.

                    result = self.api.get_activity_sessions(self.name)
                    sessions = result.json()
                    sess_param = last_workflow_id
                    for sess in sessions:
                        if "input_parameters" in sess and "stages" in sess["input_parameters"]:
                            if sess["name"] in last_workflow_id:
                                sess_param = sess["name"]

                    # Watch the running workflow.
                    self.monitor_workflow(last_workflow_id)

                    bad_stage = True
                    while bad_stage:
                        try:
                            self.watch_next_wf(sess_param)
                            bad_stage = False
                        except:
                            if len(last_stages) > 1:
                                last_stages.pop(0)
                                self.config.stages.set_stages(last_stages)
                            else:
                                self.config.logger.warning("Giving up on executing stages.")
                                bad_stage = False
        else:
            # The session is no longer running.  Call resume on the backend.
            payload = {
                "input_parameters": {},
                "activity_name": self.name,
                "comment": "comment goes here..."
            }
            try:
                response = self.api.post_resume(self.name, payload)
                json_response = response.json()
                sess_name = json_response["name"]
            except Exception as e:
                # ok, so we got an error.   the server just tosses a 500 if the activity is not
                # resumable withou any additional info.  Try to invent some context.
                if backend_data.get("activity_state", "") == "wait_for_admin":
                    self.config.logger.warning(f"{self.name} cannot be resumed because it is either Completed or Aborted.")
                else:
                    self.config.logger.error(f"Unable to resume activity: {e}")
                return

            self.watch_next_wf(sess_name)

def valid_activity_name(aname):
    if re.match('^[0-9a-z\.-]+$', aname):
        return True
    return False
