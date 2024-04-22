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
import multiprocessing
from multiprocessing import Process
import os
import re
import requests
import time
import sys
from urllib3.exceptions import ReadTimeoutError
from urllib3.exceptions import MaxRetryError

from kubernetes import client, watch
from kubernetes import config as kubeconfig

from lib.InstallerUtils import elapsed_time
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
        self._running_subprocs = {}
        self._running_mainproc = None
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


    def follow_pod_log(self, pod, container, log_prefix, st_event):
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
            erroreq_re = re.compile('error="(.*)"')
            loftsman_re = re.compile("(.*?Z) (.*?Z) ([A-Z]{3})\s+(.*)")
            ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

            for rawline in lines:
                # loftsman and helm have colors in their log output.  Strip
                line = ansi_escape.sub('', rawline)

                loftsman_match = loftsman_re.search(line)
                if loftsman_match:
                    # if we match this expression it's an error from loftsman/helm
                    time = loftsman_match.group(1)
                    msg = loftsman_match.group(4)
                    severity = loftsman_match.group(3)

                    if severity == "ERR":
                        level = "ERROR"
                        # some loftsman errors are "ERR error=<real message>", get rid of the redundant "error="
                        quotederr = erroreq_re.search(msg)
                        if quotederr:
                            msg = quotederr.group(1)
                    elif severity == "INF":
                        level = "DEBUG"
                    else:
                        # obviously redundant, but some day INF will become INFO
                        level = "DEBUG"

                    outlines.append((level, f"{msg}", f"{time} {level} {msg}"))
                    continue

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

        os.nice(20)
        log_name = os.path.join(self._log_dir, f"{pod}-{container}.txt")
        fhandle = open(log_name, 'w', encoding='UTF-8')
        start_poll = datetime.datetime.now()
        last_read = None
        while True:
            if "prom-metrics" in pod:
                sys.exit()
                try:
                    pass
                    #install_logger.warning("Inside BrokenPipeline")
                    #sys.stdout.close()
                    #sys.stdin.close()
                    #sys.exit()
                    install_logger.warning("No BrokenPipeline")
                except Exception as err:
                    install_logger.warning("Excuted BrokenPipeline")
            try:                
                watcher = watch.Watch()
                watch_kwargs = {
                    "container": container,
                    "follow": True,
                    "timestamps": True,
                    "pretty": True,
                    "_request_timeout": 30
                }
                if last_read:
                    seconds_back = elapsed_time(last_read, to_str=False)
                    # Dont set watch_kwargs["seconds_since"] to 0.  It causes
                    # an exception on the backend, and will always be 0.
                    if seconds_back > 0:
                        watch_kwargs["since_seconds"] = seconds_back
                    else:
                        watch_kwargs["since_seconds"] = 1
                for event in watcher.stream(self.core.read_namespaced_pod_log,
                                            name=pod, namespace='argo', **watch_kwargs):
                    for level, stdoutline, logline in parse_str(event):
                        print(f"{logline}", file=fhandle, flush=True)
                        # at some point we need to revisit this, INFO should map to DEBUG but
                        # not everyone has updated their logging for that distinction
                        if level == 'NOTICE' or level == 'INFO':
                            install_logger.info(f"{log_prefix}       {stdoutline}")
                        elif level == 'WARNING':
                            install_logger.warning(f"{log_prefix}       {stdoutline}")
                        elif level == 'ERROR':
                            install_logger.error(f"{log_prefix}       {stdoutline}")
                        else:
                            install_logger.debug(f"{log_prefix}       {stdoutline}")
                        if st_event.is_set():
                            watcher.stop()
                            fhandle.close()
                            return
                last_read = datetime.datetime.now()
                watcher.stop()

            except (client.rest.ApiException, client.exceptions.ApiException, MaxRetryError):
                # Catch this exception NTRIES times, then give up
                # waiting. These exceptions get hit when a container isn't ready yet
                # or when migrating pods.
                total_polled_time = datetime.datetime.now() - start_poll
                polled_seconds = int(total_polled_time.seconds)
                if polled_seconds >= POLL_LOGS:
                    install_logger.debug(f"Giving up following the log for pod {pod}!")
                    return
                last_read = datetime.datetime.now()
                time.sleep(.5)
            except ReadTimeoutError:
                # Timed out reading in the watcher.stream(...).  The
                # timeout is low, so just continue.
                last_read = datetime.datetime.now()
            except Exception as exc:
                install_logger.debug("Caught unhandled exception in PodLogs:")
                install_logger.debug(f"\tname={exc.__class__.__name__}")
                install_logger.debug(f"\tException={exc}")
                last_read = datetime.datetime.now()

            if st_event.is_set():
                # The processes are being collected.  Return from this thread.
                fhandle.close()
                return
            elif not self.is_running(pod):
                # Return since the pod is not running.
                fhandle.close()
                return
        fhandle.close()

    def list_namespaced_pod(self, pod):
        """A wrapper around list_namespaced_pod from the kubernetes api."""
        ntries = 0
        maxtries = 100
        keepgoing = True
        pods = None
        while keepgoing:
            try:
                # We throw exceptions here occasionally, so wrap it in an
                # except and retry if necessary.
                pods = self.core.list_namespaced_pod('argo')
                keepgoing = False
            except Exception as ex:
                pid = os.getpid()
                if ntries >= maxtries:
                    # Issue a warning and give up on following this pod.
                    install_logger.debug(f"Could not list pods.  Giving up on pod {pod}.")
                    keepgoing = False
                time.sleep(.1)
            finally:
                ntries += 1
        return pods

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
        pods = self.list_namespaced_pod(pod)
        if pods:
            job_pod_names = [p for p in pods.items if pod == p.metadata.name]
        else:
            job_pod_names = None
        if not job_pod_names:
            if pod in self._running_subprocs:
                del self._running_subprocs[pod]
            return False
        else:
            return True
