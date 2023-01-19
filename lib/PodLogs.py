
import datetime
import os
import re
import requests
import sys # DEBUG -- not actually needed.
import threading
import time

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
        self._log_dir = os.path.join(log_dir, config_param.timestamp, "pod_logs")
        os.makedirs(self._log_dir, exist_ok=True)
        self._running = []
        self._logs = []
        self.wfid = wfid

        kubeconfig.load_kube_config()
        self.core = client.CoreV1Api()
        service = self.core.read_namespaced_service(name="elasticsearch", namespace="sma")
        cluster_ip = service.spec.cluster_ip

        self.endpoint = f"http://{cluster_ip}:9200/_search"
        self._finished = False


    @property
    def finished(self):
        return self._finished

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
            except Exception as ex:
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


    def follow_pod_log(self, config, pod):
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
            info_re = re.compile("(.*Z) INFO (.*)")
            time_re = re.compile(r'(^\d{4}\-\d{2}\-\d{2}.*Z) ')

            for line in lines:
                info_match = info_re.search(line)
                if info_match:
                    # The most obvious INFO line.  Example:
                    # 2023-01-09T16:16:45.243438802Z INFO s3 is operational.
                    info_msg = info_match.group(2)
                    info_timestamp = info_match.group(1)
                    log_msg = f"{info_timestamp} INFO {info_msg}"
                    stdout_msg = f"{info_msg}"
                    outlines.append(("INFO", stdout_msg, log_msg))
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

        log_name = os.path.join(self._log_dir, f"{pod}.txt")
        fhandle = open(log_name, 'w', encoding='UTF-8')
        for container in ['init', 'wait', 'main']:
            print(f"[{container}]", file=fhandle, flush=True)
            start_poll = datetime.datetime.now()

            while True:
                try:
                    watcher = watch.Watch()
                    for event in watcher.stream(self.core.read_namespaced_pod_log,
                        name=pod, namespace='argo', container=container,
                        follow=True, timestamps=True, pretty=True):
                        for level, stdoutline, logline in parse_str(event):
                            print(f"{logline}", file=fhandle, flush=True)
                            if level == 'INFO':
                                install_logger.info(f"            {stdoutline}")
                            else:
                                install_logger.debug(stdoutline)
                    watcher.stop()
                    break
                except (client.rest.ApiException, client.exceptions.ApiException):
                    # Catch this exception NTRIES times, then give up
                    # waiting. This exception gets hit when the container isn't ready yet.
                    total_polled_time = datetime.datetime.now() - start_poll
                    polled_seconds = int(total_polled_time.seconds)
                    if polled_seconds >= POLL_LOGS:
                        install_logger.warning(f"Giving up following the log for pod {pod}!")
                        break
                    time.sleep(.5)
        fhandle.close()


    def follow_all_pods(self, config):
        """Watch for pods that match self.wfid.  The matching pods
        correspond to a pod launched this run.  A thread is launched
        to watch each pod to prevent the overall process from stalling
        while watching the pods."""
        following_pods = []
        while True:
            pods = self.core.list_namespaced_pod('argo')
            job_pod_names = [pod.metadata.name for pod in pods.items if self.wfid in pod.metadata.name]
            if not job_pod_names:
                break
            for jpn in job_pod_names:
                if jpn not in following_pods:
                    following_pods.append(jpn)
                    thread = threading.Thread(target=self.follow_pod_log, args=(config, jpn,))
                    thread.start()
                    self._running.append(thread)
            time.sleep(1)
        self._finished = True


    def follow_pod_logs(self, config):
        """Launch a thread to follow the pod logs."""
        thread = threading.Thread(target=self.follow_all_pods, args=(config,))
        thread.start()
        self._running.append(thread)

    def collect_threads(self):
        """Collect the threads once the stages are completed."""

        start_wait = datetime.datetime.now()

        # Wait for up to 5 minutes for the logging to finish.  It should
        # only take a few seconds.
        while not self.finished:
            time.sleep(5)
            waited = datetime.datetime.now() - start_wait
            seconds_waited = int(waited.total_seconds())
            if seconds_waited > WAIT_FOR_PODS:
                install_logger.warning("Giving up waiting on pods!")
                break

        for thread in self._running:
            thread.join()
