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
import datetime
import os
import re
import requests
import threading
import time

from urllib3.exceptions import ReadTimeoutError as ReadTimeoutError

from kubernetes import client, watch
from kubernetes import config as kubeconfig

from lib.InstallLogger import get_install_logger
install_logger = get_install_logger(__name__)

MAX_RECORDS = 10000

# After the processes are finished, wait for 5 minutes for any lingering pods.
WAIT_FOR_PODS = 5 * 60 # 5 minutes

# Poll the logs for 20 minutes.  It can take a while for the pods to be
# scheduled.
POLL_LOGS = 20 * 60 # 20 minutes

def generate_query(pod, container="", namespace=""):
    print(f"(generate_query)pod={pod}")
    parts = pod.rpartition("-")
    print(f"(generate_query)parts={parts}")
    pod_body, _, pod_tail = pod.rpartition("-")
    match_phrase = pod_body
    query_strings = []
    if pod_tail:
        query_strings.append(f"{pod_tail}*")
    if namespace:
        query_strings.append(f"*_{namespace}_*")
    if container:
        container_pieces = container.split("-")
        query_strings.append(f"*_{container_pieces[0]}")
        for piece in container_pieces[1:]:
            query_strings.append(piece)

    query = {"query":{"bool":{"must":[{"match_phrase": {"hostname":match_phrase}}]}},"sort":[{"timereported":{"order":"asc","unmapped_type":"boolean"}}],"size": MAX_RECORDS}
    for query_string in query_strings:
        query["query"]["bool"]["must"].append({"query_string":{"fields":["hostname"],"query":f"{query_string}"}})
    return query


class PodLogs():
    def __init__(self, config_param, wfid):
        log_dir = config_param.args.get("log_dir")
        self._log_dir = os.path.join(log_dir, config_param.timestamp, "argo_logs")
        os.makedirs(self._log_dir, exist_ok=True)
        self._running_subthreads = []
        self._running_mainthread = None
        self.mt_event = None
        self._logs = []
        self.wfid = wfid
        kubeconfig.load_kube_config()
        self.core = client.CoreV1Api()
        try:
            service = self.core.read_namespaced_service(name="elasticsearch", namespace="sma")
            cluster_ip = service.spec.cluster_ip

            self.endpoint = f"http://{cluster_ip}:9200/_search"
        except:
            config_param.logger.debug("Unable to find elasticsearch service.")
            pass

    @property
    def logs(self):
        return self._logs

    def not_follow_pod_log(self, pod):

        search_after=None
        query = generate_query(pod, container=pod, namespace="argo")
        search_after = None
        log_name = os.path.join(self._log_dir, f"{pod}.txt")
        fhandle = open(log_name, "w", encoding="UTF-8")
        while True:
            if search_after:
                query["search_after"] = search_after
            try:
                data = requests.get(self.endpoint, json=query)
            except Exception:
                break
            hits = data.json().get("hits", {}).get("hits", [])
            print(f"(follow_pod_log)pod={pod}, hits={hits}")
            for record in data.json().get("hits", {}).get("hits", []):
                print(f'{record["_source"]["timereported"]} {record["_source"]["message"]}', file=fhandle)
            if len(hits) != MAX_RECORDS:
                time.sleep(1)
            if hits:
                search_after = hits[-1].get("sort")
            else:
                break

        fhandle.close()


    def follow_pod_log(self, pod, st_event, container):
        """Follow the log for a particular pod."""
        def parse_str(instr):
            """Parse a block of text which is at least one line from the
            pod logs.  Return a list of 3-tuples, which are formatted as
            (log level, stdout line, line to be logged)"""

            outlines = []
            lines = instr.split("\n")
            time = ""
            msg = ""
            level = ""
            level_re = re.compile(r"level=(\w+)", re.IGNORECASE)
            msg_re = re.compile(r'msg="(.+?)"') # non-greedy re
            generic_re = re.compile("(.*Z) ([A-Z]+):* (.*)")
            time_re = re.compile(r'(^\d{4}\-\d{2}\-\d{2}.*Z) ')

            for line in lines:
                generic_match = generic_re.search(line)
                if generic_match:
                    # The most obvious INFO line.  Example:
                    # 2023-01-09T16:16:45.243438802Z INFO s3 is operational.
                    message = generic_match.group(3)
                    severity = generic_match.group(2)
                    timestamp = generic_match.group(1)
                    log_msg = f"{timestamp} {severity} {message}"
                    stdout_msg = f"{message}"
                    outlines.append((severity, stdout_msg, log_msg))
                    #outlines.append(f"{info_match.group(1)}\n")
                    continue

                # If we haven't continued, the line is probably similar to
                # the following example:
                #
                # 2023-01-09T16:16:37.455645007Z time="2023-01-09T16:16:37.455Z" level=info msg="Starting deadline monitor"
                #
                # Consider these lines debug unless otherwise indicated.

                time_match = time_re.search(line)
                if time_match:
                    time = time_match.group(1)

                level_match = level_re.search(line)
                if level_match:
                    # If we match this expression, it's actually a debug
                    # message.  k8s outputs all messages with 'level=info'.
                    level_match = level_match.group(1).upper()
                    level = 'DEBUG' if level_match == "INFO" else level_match
                else:
                    # Assume debug
                    level = 'DEBUG'

                msg_match = msg_re.search(line)
                if msg_match:
                    msg = msg_match.group(1)
                    outlines.append((level, f"{msg}", f"{time} {level} {msg}"))
                else:
                    outlines.append((level, f"{line}", f"{line}"))
            return outlines

        log_name = os.path.join(self._log_dir, f"{pod}-{container}.txt")
        fhandle = open(log_name, 'w', encoding='UTF-8')
        start_poll = datetime.datetime.now()

        while True:
            try:
                watcher = watch.Watch()
                watch_kwargs = {
                    "container": container,
                    "follow": True,
                    "timestamps": True,
                    "pretty": True,
                    "_request_timeout": 5
                }
                for event in watcher.stream(self.core.read_namespaced_pod_log,
                                            name=pod, namespace='argo', **watch_kwargs):
                    for level, stdoutline, logline in parse_str(event):
                        print(f"{logline}", file=fhandle, flush=True)
                        # at some point we need to revisit this, INFO should map to DEBUG but
                        # not everyone has updated their logging for that distinction
                        if level == 'NOTICE' or level == 'INFO':
                            install_logger.info(f"            {stdoutline}")
                        elif level == 'WARNING':
                            install_logger.warning(f"            {stdoutline}")
                        elif level == 'ERROR':
                            install_logger.error(f"            {stdoutline}")
                        else:
                            install_logger.debug(stdoutline)
                        if st_event.is_set():
                            watcher.stop()
                            return
                watcher.stop()
                break

            except (client.rest.ApiException, client.exceptions.ApiException):
                # Catch this exception NTRIES times, then give up
                # waiting. This exception gets hit when the container isn't ready yet.
                total_polled_time = datetime.datetime.now() - start_poll
                polled_seconds = int(total_polled_time.seconds)
                if polled_seconds >= POLL_LOGS:
                    install_logger.warning(f"Giving up following the log for pod {pod}!")
                    return
                time.sleep(.5)
            except ReadTimeoutError:
                # Timed out reading in the watcher.stream(...).  The
                # timeout is low, so just continue.
                continue

            if st_event.is_set():
                # The threads are being collected.
                return
            elif not self.is_running(pod):
                # Return, since the pod is not running.
                return
        fhandle.close()


    def is_running(self, pod):
        """Check if a pod is running."""
        # We might want to look at the phases, which would look something like this:
            # job_pod_names = [p for p in pods.items if pod == p.metadata.name]
            # if not job_pod_names:
            #   return False
            # last_pod = job_pod_names[-1]
            # phase = last_pod.status.phase
        # Then use phase to determine whether or not the pod should continue running
        # The phases that are running are ["pending", "running"].  The reason this wasn't 
        # implemented here is because it seemed that pods could go from 'Success' back into
        # 'Running' or 'Pending'.  So we're just doing a list of the pods, and then return
        # true if the pod is in the list.

        pods = self.core.list_namespaced_pod('argo')
        running_phases = ["pending", "running"]
        job_pod_names = [p for p in pods.items if pod == p.metadata.name]
        if not job_pod_names:
            return False
        else:
            return True


    def follow_all_pods(self):
        """Watch for pods that match self.wfid.  The matching pods
        correspond to a pod launched this run.  A thread is launched
        to watch each pod to prevent the overall process from stalling
        while watching the pods."""

        # This job starts when the activity is initialized.
        following_pods = []
        got_pod = False
        st_event = threading.Event()
        while True:
            # FIXME: It might be better to use `--selector=...`.  All the
            # pods have a label on them.
            pods = self.core.list_namespaced_pod('argo')
            pod_id = re.sub('\W+', '-', self.wfid)
            job_pod_names = [pod.metadata.name for pod in pods.items if pod_id in pod.metadata.name]

            # Check that we've got a pod in addition to job_pod_names.
            # It's possible that pods aren't being spun up when this thread
            # is initially started.
            if not job_pod_names and got_pod:
                break
            elif job_pod_names:
                got_pod = True

            for jpn in job_pod_names:
                if jpn not in following_pods:
                    following_pods.append(jpn)
                    for container in ["init", "wait", "main"]:
                        thread = threading.Thread(target=self.follow_pod_log, args=(jpn, st_event, container))
                        thread.start()
                        self._running_subthreads.append(thread)
                        got_pod = True

            is_set =  self.mt_event.is_set()
            if is_set:
                st_event.set()
                for thread in self._running_subthreads:
                    thread.join()
                break

            time.sleep(1)


    def follow_pod_logs(self):
        """Launch a thread to follow the pod logs."""
        self.mt_event = threading.Event()
        thread = threading.Thread(target=self.follow_all_pods)
        thread.start()
        self._running_mainthread = thread

    def collect_threads(self):
        """Collect the threads once the stages are completed."""
        try:
            self.mt_event.set()
        except AttributeError:
            # The threads haven't been launched yet.
            pass

        if self._running_mainthread:
            self._running_mainthread.join()
