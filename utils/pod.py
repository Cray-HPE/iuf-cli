
import datetime
import json
import time

from utils.InstallerUtils import CmdMgr
from utils.InstallLogger import get_install_logger
from utils.vars import *

install_logger = get_install_logger(__name__)


class PodDetails:
    """
    This class holds the pod json data returned from 'kubectl get pods' type commands.
    """

    def __init__(self, pod_name, pod_namespace, pod_data=None):
        """
        pod_name and pod_namespace are strings that will be directly passed to kubectl,
        and pod_data holds the json info from kubectl that's be converted to a dict.
        """

        self.pod_name = pod_name
        self.pod_namespace = pod_namespace
        self.pod_data = pod_data

    def get_containers(self):
        """ Returns a list of the containers found in this pod """
        if "status" in self.pod_data:
            return self.pod_data["status"].get("containerStatuses", [])

    def _get_container_state(self, container):
        """ Looks at the container dict from the pod_data to figure out the current state.
            Returns the contianer name and container state dict key as a pair.
            Appends the reason the pod is terminated, if the pod is in that state.
            States can be "waiting", "running" or "terminated".
        """
        if container == None:
            return None,None
        container_name = container.get("name", "")
        try:
            container_state = list(container["state"].keys())[0]
            if container_state == "terminated":
                container_state = "{} ({})".format(container_state, container["state"][container_state]["reason"])
        except:
            container_state = "unknown"
        return container_name, container_state
        
    def get_container_states(self):
        """ Return a str with all the containers names, and their states. """
        return ",".join([" {}".format(": ".join(self._get_container_state(c))) for c in self.get_containers()])

    def get_phase(self):
        """ Return the phase name, can be "pending", "running", "succeeded", "failed", or "unknown" """
        if "status" in self.pod_data:
            return self.pod_data["status"].get("phase", "unknown").lower()
        return "unknown"

    def update_details(self):
        """ Update the pod_data with new json data"""
        cmd = "kubectl get pods -n {} -o json {}".format(self.pod_namespace, self.pod_name)
        connection = CmdMgr.get_cmd_interface()
        self.pod_data = json.loads(connection.sudo(cmd).stdout)

    def __str__(self):
        """
        Example output:
            pod_name=cfs-2017e827-4a1e-478f-b357-71fb249a0efe-9jmwp, namespace=services, phase=pending,
            containers=[ansible-0: waiting, inventory: waiting, istio-proxy: waiting]
        """
        return "\tpod_name={}, namespace={}, phase={}\n\tcontainers=[{}]".format(self.pod_name, self.pod_namespace,
                     self.get_phase(), self.get_container_states())

    def __ne__(self, o):
        return not self.__eq__(o)

    def __eq__(self, o):
        """ Compare the str representation """
        return str(self) == str(o)


    @staticmethod
    def find_pod_namespace(pod_name):
        """
        Returns the namespace of the pod_name input.
        If the namespace can't be determined, return None.
       
        """
        namespace = None
        try:
            connection = CmdMgr.get_cmd_interface()
            all_pods = connection.sudo("kubectl get pods -Ao wide").stdout.splitlines()
            for pod_line in all_pods:
                install_logger.debug("checking {} against {}".format(pod_name, pod_line))
                pod_list = pod_line.split()
                if pod_name == pod_list[1]:
                    namespace = pod_line.split()[0]
        except:
            install_logger.debug("Failed to find pod {} namespace".format(pod_name))

        return namespace

    @staticmethod
    def create_pod_details(pod_name):
        """
        Creates and returns an instance of PodDetails given the pod_name input.
        Determines the namespace of the pod, and updates the pod_data.
        Returns None if the namespace lookup fails, or data can't be updated.
        """
        try:
            namespace = PodDetails.find_pod_namespace(pod_name)
            if namespace == None:
                return None
            pod_details = PodDetails(pod_name, namespace)
            pod_details.update_details()
        except:
            install_logger.debug("Failed to find pod details: {}".format(pod_name))
            pod_details = None
        return pod_details


def wait_for_pod(connection, pod_name, timeout=1200, delete=False):
    """ Wait for a pod to be either created or deleted.

        If the pod is being deleted,
         - Considered succeeded when the pod can't be found.

        If the pod is NOT being deleted,
         - the wait has succeeded when the pod reaches the "succeeded" phase.
         - Raise exception if the pod can't be found.

        In both cases:
         - Raise exception if the phase is "failed"
         - Raise exception if the timeout has exceeded.
    """

    seconds_waited = 0
    sleep_seconds = 10
    loop_count = 0
    alert_freq = 6

    action_str = "delete" if delete else "complete"
    pod_details = None
    start_time = datetime.datetime.now()
    while True:
        new_pod_details = PodDetails.create_pod_details(pod_name)
        seconds_waited = int((datetime.datetime.now() - start_time).total_seconds())
        if loop_count % alert_freq == 0:
            install_logger.info("Waiting for pod {} to {}, {} of {} second(s) elapsed.".format(pod_name, action_str, seconds_waited, timeout))
        if new_pod_details != None and pod_details != new_pod_details:
            install_logger.info(new_pod_details)

        pod_details = new_pod_details

        if pod_details == None and delete == False:
            raise PodProblem("Can't find Pod {}".format(pod_name))
        elif pod_details == None and delete == True:
            install_logger.info("Pod {} has been deleted, {} second(s) elapsed".format(pod_name, seconds_waited))
            return
        else:
            install_logger.debug("Pod {}, phase={}".format(pod_name, pod_details.get_phase()))
            if pod_details.get_phase() == "succeeded" and delete == False:
                install_logger.info("Pod {} has succeeded, {} second(s) elapsed".format(pod_name, seconds_waited))
                return
            elif pod_details.get_phase() == "failed" :
               raise PodProblem("Pod {} failed, {} second(s) elapsed.".format(pod_name, seconds_waited))
            elif pod_details.get_phase() in ["pending", "running"]:
                install_logger.debug("Pod {} is {}".format(pod_name, pod_details.get_phase()))
            else:
                install_logger.warning("Pod {} is in an unknown state.")

        if seconds_waited >= timeout:
            raise PodProblem("Timed out waiting {} seconds for pod {} to {}".format(seconds_waited, pod_name, action_str))
        time.sleep(sleep_seconds)
        loop_count += 1
