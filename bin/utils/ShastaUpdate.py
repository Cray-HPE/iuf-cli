#!/usr/bin/env python3

# Copyright 2022 Hewlett Packard Enterprise Development LP

"""
This is a Jenkins job fab file. These job fab files are downloaded by the
jenkins server and executed in order to test certain functionality, or to
perform integration tests against actual Cray hardware.

Copyright 2021 Hewlett Packard Enterprise Development LP
"""

import argparse
import datetime
import json
import os
from packaging import version as vers_mod
import sys
import textwrap
import time
import re

from pprint import pprint
from utils.InstallLogger import get_install_logger

import yaml #pylint: disable=import-error

from utils.InstallLogger import get_install_logger

install_logger = get_install_logger(__name__)

CWD = os.getcwd()

from utils.vars import *
import utils.InstallerUtils as utils #pylint: disable=wrong-import-position,import-error
from utils.InstallerUtils import getenv,flushprint #pylint: disable=wrong-import-position,import-error

# pylint: disable=consider-using-f-string

host = "lemondrop-ncn-m001"
connection = utils.CmdInterface(host)
install_logger = get_install_logger(__name__)

def get_binaries(args):
    """Get the cos, sle, and slingshot-host packages."""

    product = args.product
    prod_version = {"cos": getenv("COS_VERSION"),
                    "sle": getenv("SLE_VERSION"),
                    "slingshot_host": getenv("SLINGSHOT_HOST_VERSION")}
    dist = None if product != "sle" else getenv("RELEASE_DIST")
    prod_url = getenv("%s_URL" % product.upper())
    install_logger.debug("Working here: {}".format(os.getcwd()))

    if product == "sle":
        onepackage = False
    else:
        onepackage = True

    packages = utils.download_artifacts(connection, prod_url,
                                        prod_version[product], dist,
                                        whereto=BUILD_DIR,
                                        onepackage=onepackage)
    packages_dict = {"packages": packages}
    outfile = os.path.join(get_dirs(args, "state"),
                           "{}-packages.yaml".format(product))

    install_logger.debug("dumping {} to {}".format(packages_dict, outfile))

    with open(outfile, 'w', encoding='UTF-8') as fhandle:
        yaml.dump(packages_dict, fhandle)


def get_dirs(args,which=None):
        media_dir = args.get("media_dir", os.getcwd())
        state_dir = args.get("state_dir", os.getcwd())
        if which == "state":
            return state_dir
        elif which == "media":
            return media_dir
        else:
            return media_dir, state_dir


def get_prods(args):
    """A passthrough function to InstallerUtils.get_products."""

    media_dir, state_dir = get_dirs(args)

    location_dict = utils.get_products(media_dir)
    filepath = os.path.join(state_dir, "location_dict.yaml")
    with open(filepath, "w", encoding="UTF-8") as fhandle:
        yaml.dump(location_dict, fhandle)


def install(args):
    """Install COS, Slingshot-host, or SLE"""

    media_dir, state_dir = get_dirs(args)

    with open(os.path.join(state_dir, LOCATION_DICT), 'r',
              encoding='UTF-8') as fhandle:
        location_dict = yaml.full_load(fhandle)

    print("location_dict=")
    pprint(location_dict)

    for prod in location_dict:
        loc = location_dict[prod]["work_dir"]
        connection.sudo("./install.sh", cwd=loc)


def is_ready(ready):
    """
    Check the output of a pod to see if its ready.  The ready variable
    should be something like '1/1' or '1/2'.  If the numerator doesn't
    match the denominator they aren't ready.
    """
    numerator, denominator = ready.split('/')
    if numerator != denominator:
        return False
    else:
        return True


def check_pods(args): #pylint: disable=unused-argument
    """
    Check the status of the pods for a given product.  Assume COS for now.

    Note in S-8000_RevA (step 7 on page 83) they check the image recipes as
    well.  That is not implemented here.  I don't think it would have purpose
    in this context.
    """

    keep_waiting = True
    time_to_wait = 60 * 20 # Wait 20 minutes.
    total_time = 0
    sleep_time = 10

    while keep_waiting:

        jobs_list = connection.sudo("kubectl --kubeconfig=/etc/kubernetes/admin.conf get jobs -A").stdout.splitlines()
        jobs_list = [job for job in jobs_list if 'cos' in job]

        pods_list = connection.sudo("kubectl --kubeconfig=/etc/kubernetes/admin.conf get pods -A").stdout.splitlines()
        pods_list = [pod for pod in pods_list if 'cos' in pod]

        if not (jobs_list or pods_list):
            keep_waiting = False

        not_running = []

        for job in jobs_list:
            fields = job.split()
            job_name = fields[1]
            if 'cos-config' in job_name or 'cos-image' in job_name:
                completions = fields[2]
                if not is_ready(completions):
                    install_logger.warning("found the following job --not-- running:{}".format(fields))
                    not_running.append(job_name)

        for pod in pods_list:
            fields = pod.split()
            pod_name = fields[1]
            if 'cos-config' in pod_name or 'cos-image' in pod_name:
                if not 'completed' in fields[3].lower():
                    install_logger.warning("found the following pod --not-- running:{}".format(fields))
                    not_running.append(pod_name)

        if not_running:
            install_logger.debug("not_running={} ==> sleep".format(not_running))
            total_time += sleep_time
            time.sleep(sleep_time)
        else:
            keep_waiting = False

        if total_time > time_to_wait:
            msg = textwrap.dedent("""
                WARNING: the following job/pods have not completed booting: {}
                """.format(','.join(not_running)))
            raise TimeOut(msg)

        sys.stdout.flush()


def check_services(args): #pylint: disable=unused-argument
    """Check the cps and nmd services.  Also check dvs and lnet"""
    services = connection.sudo('kubectl  --kubeconfig=/etc/kubernetes/admin.conf get pods -A').stdout.splitlines()
    services = [s for s in services if 'nmd' in s or 'cray-cps' in s]

    for service in services:
        service_list = service.split()
        name, status = service_list[1], service_list[3]
        if not status.lower() in ["running", "completed"]:
            install_logger.warning("WARNING: service {} is not ready.  It's status is:\n\t{}".format(name, service))

    w_ncn_tuples = utils.get_hosts(connection, 'w0')
    w_ncns = [w[1] for w in w_ncn_tuples]

    for node in w_ncns:
        all_lines = connection.sudo("ssh {} lsmod".format(node)).stdout.splitlines()

        modules = [line.split()[0].strip() for line in all_lines]

        # Check the DVS and LNET services
        found_dvs = False
        found_lnet = False
        for module in modules:
            if module == "dvs":
                found_dvs = True
            elif module == "lnet":
                found_lnet = True
            if found_dvs and found_lnet:
                break

        if not found_dvs:
            install_logger.warning("WARNING: dvs not found in kernel modules on node {}!".format(node))

        if not found_lnet:
            install_logger.warning("WARNING: lnet not found in kernel modules on node {}!".format(node))


def sync_ci_tools():
    """Sync any tools the CI process might need to a temp dir on the NCN"""

    tools = [
            "src/tools/get_vcspw.sh"
            ]

    for tool in tools:
        tool_basename = os.path.basename(tool)
        connection.put(tool,"{}/{}".format(REMOTE_PROJECT_DIR, tool_basename))

def setup_git_config():
    """Setup a .gitconfig that can be used by the rest of the CI"""
    host_shortname = getenv('NCN_NAME').split("-")[0]

    connection.sudo("{}/get_vcspw.sh > {}/.vcspass".format(REMOTE_PROJECT_DIR, BUILD_DIR))
    connection.sudo("echo cat {}/.vcspass > {}/get_local_vcspw.sh".format(BUILD_DIR,BUILD_DIR))
    connection.sudo("chmod +x {}/get_local_vcspw.sh".format(BUILD_DIR))

    connection.sudo("HOME={} git config --global core.askPass {}/get_local_vcspw.sh".format(BUILD_DIR,BUILD_DIR))
    connection.sudo("HOME={} git config --global credential.https://api-gw-service-nmn.local.username crayvcs".format(BUILD_DIR))
    connection.sudo("HOME={} git config --global url.https://api-gw-service-nmn.local.insteadof https://vcs.{}.dev.cray.com".format(BUILD_DIR, host_shortname))


def backup_config_repos():
    """Backup the configuration repositories in git"""
    host_shortname = host.split("-")[0]

    backup_dir = BUILD_DIR + "/" + CI_DATE

    connection.sudo("mkdir -p {}".format(backup_dir))

    config_repos = connection.sudo("kubectl --kubeconfig=/etc/kubernetes/admin.conf get cm -n services cray-product-catalog -o custom-columns=DATA:.data | grep clone_url | cut -d: -f2- | sort -u").stdout.splitlines()

    for repo in config_repos:
        # yeah, yeah
        repodir = re.search(r"((git|ssh|http(s)?)|(git@[\w\.]+))(:(//)?)([\w\.@\:/\-~]+)(\.git)(/)?",repo).group(7).split("/")[-1]
        clonedir = backup_dir + "/" + repodir
        connection.sudo("HOME={} git clone {} {}".format(BUILD_DIR,repo,clonedir))
        remotes = connection.sudo("HOME={} git -C {} branch -a".format(
            BUILD_DIR,clonedir)).stdout.splitlines()
        for raw in remotes:
            if not raw.strip().startswith("remotes"):
                continue
            if "HEAD" in raw:
                continue

            rxp = re.search("(remotes/[^/]+)/(.*)$",raw)
            branch = rxp.group(2)

            if branch == "master":
                continue

            connection.sudo("HOME={} git -C {} branch --track {} {}".format(
                            BUILD_DIR,clonedir,branch,raw))

        connection.sudo("HOME={} git -C {} fetch --all".format(BUILD_DIR,clonedir))
        connection.sudo("HOME={} git -C {} pull --all".format(BUILD_DIR,clonedir))

        # now set up the backed up repo to work locally
        connection.sudo("git -C {} config core.askPass {}/get_vcspw.sh".format(clonedir,REMOTE_PROJECT_DIR))
        connection.sudo("git -C {} config credential.https://api-gw-service-nmn.local.username crayvcs".format(clonedir))
        connection.sudo("git -C {} config url.https://api-gw-service-nmn.local.insteadof https://vcs.{}.dev.cray.com".format(clonedir,host_shortname))


def merge_cos_integration(args):
    """Merge the product git branch to the working config"""

    # first things first, get a copy of all the config repos
    install_logger.debug("Cloning all of the configuration repositories...")
    git_checkout_dir = get_dirs(args, "state")

    cos_checkout_dir = git_checkout_dir + "/cos-config-management"

    # second, see what branches we want to work with
    _, integration_branch = curr_cos_branch(args)
    import_branch = None

    cos_version = getenv("COS_VERSION")
    import_branch_cmd = "kubectl --kubeconfig=/etc/kubernetes/admin.conf get cm -n services cray-product-catalog -o custom-columns=DATA:.data.cos | grep {} | grep import_branch | tail -1 | cut -d: -f2-".format(cos_version)
    import_branch_result = connection.sudo(import_branch_cmd).stdout.splitlines()

    for line in import_branch_result:
        import_branch = line.strip()
        break

    if import_branch is None:
        install_logger.error("Error: Unable to retrieve import branch")
        install_logger.debug("  Command executed: {}".format(import_branch_cmd))
        return False
    else:
        install_logger.debug("Importing configuration from {}".format(import_branch))

    # third, checkout (or create) the integration branch
    install_logger.debug("Checking to see if {} already exists".format(integration_branch))
    found_integration = False
    branch_list = connection.sudo("git -C {} branch".format(cos_checkout_dir)).stdout.splitlines()

    for line in branch_list:
        if line.strip("* ") == integration_branch:
            found_integration = True
            break

    checkout_ok = False
    if found_integration:
        # it exists, check it out
        install_logger.debug("Using existing integration branch")
        checkout_ok = connection.sudo("git -C {} checkout {}".format(
            cos_checkout_dir,integration_branch)).ok
    else:
        # doesn't exist, create it based on the import branch
        install_logger.debug("Unable to locate integration branch, creating it based on {}".format(import_branch))
        result = connection.sudo("git -C {} checkout {}".format(cos_checkout_dir,import_branch))
        if result.ok:
            result = connection.sudo("git -C {} checkout -b {}".format(
                cos_checkout_dir,integration_branch))
        if result.ok:
            checkout_ok = connection.sudo("git -C {} push --set-upstream origin {}".format(
                cos_checkout_dir,integration_branch)).ok

    # fourth, merge the import branch to the integration branch
    if checkout_ok:
        install_logger.debug("Merge branch {} into {}".format(import_branch,integration_branch))
        merge_ok = connection.sudo("git -C {} merge -m \"Merge branch '{}' into {}\" {}".format(
            cos_checkout_dir,import_branch,integration_branch,import_branch)).ok
        if merge_ok:
            push_ok = connection.sudo("git -C {} push".format(cos_checkout_dir)).ok
            if not push_ok:
                install_logger.debug("Unable to push merged changes back to origin")
        else:
            install_logger.error("Merge failed!")
            return False
    else:
        install_logger.error("Checkout of {} failed!".format(integration_branch))
        return False


def ncn_personalization(args): #pylint: disable=unused-argument
    """Do the NCN personalization as described in HPE Cray EX System
    Installation and Configuration Guide (1.4.2_S-8000 RevA)"""

    repos = {
        "cos": "cos-config-management",
        "csm": "csm-config-management",
        "sat":"sat-config-management"
    }

    pzation_base_file = "ncn-personalization.{}.{}.json".format(host, get_cos_version(args))

    # Get a list of all worker and manaagement ncns. We need to skip the
    # ncn-s00* nodes for now.  So use the m_ncn_tuples + w_ncn_tuples and
    # consider that to be all the ncns.
    m_ncn_tuples = utils.get_hosts(connection, "m0")
    w_ncn_tuples = utils.get_hosts(connection, "w0")
    all_ncn_tuples = w_ncn_tuples + m_ncn_tuples

    install_logger.debug("ncn_personalization, working here: {}".format(os.getcwd()))
    with open(os.path.join('../src/templates', pzation_base_file), 'r', encoding='UTF-8') as fhandle:
        pzation_template = json.load(fhandle)
    layers = pzation_template["layers"]

    def find_substr(substr):
        """Return the index of the element containing the substring.  This
        is a slow linear search, but there are only a few elements."""
        for i, _ in enumerate(layers):
            if substr in layers[i]['name']:
                return i

    # Get the commits from the repos to forumulate the
    # ncn-personalization.json.  Then write it to the ncn.
    for repo in repos:
        branches = utils.ls_remote(connection, repos[repo]).splitlines()
        branch = [b for b in branches if 'integration' in b][0]
        commit, _ = branch.split()
        layer_i = find_substr(repo)
        pzation_template["layers"][layer_i]["commit"] = commit

    pzation_file = os.path.join(get_dirs(args, "state"), pzation_base_file)
    remote_pzation_file = os.path.join('/root', pzation_base_file)
    with open(pzation_file, 'w', encoding='UTF-8') as fhandle:
        json.dump(pzation_template, fhandle)
    connection.put(pzation_file, remote_pzation_file)

    ncn_list_xnames = [n[0] for n in all_ncn_tuples]

    # Disable cfs.
    for ncn in ncn_list_xnames:
        connection.sudo("cray cfs components update {} --enabled false".format(ncn))
    # Upload the file to CFS.
    connection.sudo("cray cfs configurations update ncn-personalization --file {} --format json".format(remote_pzation_file))

    # Update the CFS component for all ncns.  Skip the ncn-s* nodes for now;
    # so this is basically the worker and management nodes.
    for ncn in ncn_list_xnames:
        connection.sudo("cray cfs components update --desired-config ncn-personalization --enabled true --format json {}".format(ncn))

    utils.wait_for_ncn_personalization(connection, ncn_list_xnames)


def curr_cos_branch(args):
    """Find the integration branch corresponding to the current COS version"""

    maj_min_v = get_cos_version(args)
    branches = utils.ls_remote(connection, "cos-config-management").splitlines()
    found_branch = None

    # Convention dictates to name the branch
    # <COS MAJOR VERSION>.<COS MINOR VERSION>-integration.  This may be
    # overly zealous.  If not, maybe there is a better way than the 3 'for'
    # loops.
    for branch in branches:
        # It's a bit hoaky, but I don't think we want to pick up the rocm branches for now.
        if maj_min_v in branch and 'integration' in branch.lower() and 'rocm' not in branch:
            found_branch = branch
            break
    if not found_branch:
        for branch in branches:
            if 'integration' in branch:
                found_branch = branch
                break

    if not found_branch:
        for branch in branches:
            if maj_min_v in branch:
                found_branch = branch

    commit, ref = found_branch.split()
    ref = ref.replace('remotes/origin', '').replace('refs/heads/', '')

    if found_branch:
        return commit.strip(), ref.strip()

    return None, None


def get_cos_version(args, short=True):
    """Get the COS version."""

    # Use static variables so the yaml doesn't need to be loaded every time.
    if hasattr(get_cos_version, "full_version") and hasattr(get_cos_version, "short_version"):
        if short:
            return get_cos_version.short_version
        else:
            return get_cos_version.full_version

    # If we haven't returned, full_version and short_version do not exist.
    # read the yaml and set them.
    state_dir = get_dirs(args, "state")
    with open(os.path.join(state_dir, LOCATION_DICT), "r",
              encoding='UTF-8') as fhandle:
        locs_dict = yaml.load(fhandle, yaml.SafeLoader)

    cos_versions = [ld.replace('cos-', '') for ld in locs_dict.keys() if 'cos' in ld]
    sorted_vers = sorted(cos_versions, key=vers_mod.Version)
    highest_vers = sorted_vers[-1]

    version_list = highest_vers.split('.')
    short_vers = "{}.{}".format(version_list[0], version_list[1])

    get_cos_version.short_version = short_vers
    get_cos_version.full_version = highest_vers
    if short:
        return short_vers
    else:
        return highest_vers


def wait_for_pod(job_id):
    """Wait for the COS after creating an image"""

    out = connection.sudo("kubectl --kubeconfig=/etc/kubernetes/admin.conf -n ims describe job {}".format(job_id)).stdout.splitlines()
    created_line = None
    for line in out:
        if 'created pod' in line.lower():
            created_line = line
            break
    if created_line:
        fields = created_line.split()
        event_type, event_reason = fields[0], fields[1]
        pod_name = fields[-1]
        install_logger.debug("type = {}, event = {}, pod_name = {}".format(event_type,
            event_reason, pod_name))
        utils.wait_for_pod(connection, pod_name)
    else:
        install_logger.warning("WARNING: Unable to get pod for job id {}".format(job_id))
        return None, None, None

    # Get the image id and etag
    job_hex = re.sub(r"cray-ims-([0-9abcdef\-]+)-create.*", r"\1", job_id)
    job_info = json.loads(connection.sudo("cray ims jobs describe {} --format json".format(job_hex)).stdout)
    resultant_image_id = job_info['resultant_image_id']

    # 'cray ims images describe <resultant_image_id>' would work as well.
    artifacts = json.loads(connection.sudo("cray artifacts describe boot-images {}/manifest.json --format json".format(resultant_image_id)).stdout)
    etag = artifacts['artifact']['ETag']

    return pod_name, resultant_image_id, etag


def customize_cos_compute_image(args, image_info):
    """Customize a COS compute image."""

    cos_version = get_cos_version(args)
    date = datetime.datetime.today().strftime("%Y%m%d")
    #configuration_name = "cos-config-{}-ci-{}".format(cos_version, date))
    existing_sessions = connection.sudo("cray cfs sessions list --format json |jq '.[].name'").stdout.splitlines()
    existing_sessions = [es.replace('"', '') for es in existing_sessions]

    # Find a session name that doesn't already exist.  We shouldn't need to
    # create more than 100 in a day.
    for i in range(100):
        istr = "%02d" % i
        session_name = "cos-config-{}-integration-{}-{}".format(cos_version, date, istr)
        if session_name not in existing_sessions:
            break

    configuration_name = "cos-{}-nogpu-integration".format(cos_version)
    install_logger.debug("(customize_cos_compute_image)session_name={}, cos_version={}, configuration_name={}".format(
        session_name, cos_version, configuration_name))
    cmd = "cray cfs sessions create --name {} --configuration-name {} \
           --target-definition image --target-group Compute {} --format json".format(
           session_name, configuration_name, image_info['resultant_image_id'])
    connection.sudo(cmd)

    # Now we need to poll the results and wait for it to finish.
    keep_going = True
    status_cmd = "cray cfs sessions describe {} --format json | jq -r .status.session.status".format(session_name)
    succeeded_cmd = "cray cfs sessions describe {} --format json | jq -r .status.session.succeeded".format(session_name)

    timeout = 1200
    start = datetime.datetime.now()

    finished = False
    while keep_going:
        status = connection.sudo(status_cmd).stdout.strip().lower()
        succeeded = connection.sudo(succeeded_cmd).stdout.strip().lower()
        if status == "complete" and succeeded == "true":
            keep_going = False
            finished = True
            install_logger.debug("Finished waiting for {}".format(session_name))
        elif status == "complete" and succeeded == "false":
            raise COSProblem("The image customization failed!")
        else:
            install_logger.debug("Still waiting for {}.  status={}, succeeded={}".format(
                       session_name, status, succeeded))

        tdiff = datetime.datetime.now() - start
        seconds_waited = tdiff.total_seconds()
        if seconds_waited > timeout:
            install_logger.warning("WARNING: timed out waiting for {} to succeed; cannot customize the COS image".format(session_name))
            keep_going = False
        time.sleep(10)

    if finished:
        image_id = connection.sudo("cray cfs sessions describe {} --format json | jq -r .status.artifacts[].result_id".format(session_name)).stdout.strip()
        install_logger.debug('artifacts cmd="cray artifacts describe boot-images {}/manifest.json --format json"'.format(image_id))
        artifacts = json.loads(connection.sudo("cray artifacts describe boot-images {}/manifest.json --format json".format(image_id)).stdout)
        etag = artifacts['artifact']['ETag'].replace("\\\"", "")
        bos_info = {
            "image_id": image_id,
            "etag": etag,
            "configuration": session_name
        }
        with open(BOS_INFO_FILE, 'w', encoding='UTF-8') as bos_info_fh:
            json.dump(bos_info, bos_info_fh)
    elif os.path.exists(BOS_INFO_FILE):
        os.remove(BOS_INFO_FILE)


def build_cos_compute_image(args): #pylint: disable=unused-argument
    """
    Create a Configuration Framework Service (CFS) session configuration
    for COS.
    """

    commit, name = curr_cos_branch(args)

    if commit is None:
        raise COSProblem("WARNING: Could not determine COS branch, so cannot build a compute image")

    # Update the configuration.

    cos_version = get_cos_version(args)
    config_file = "cos-config-{}-nogpu-integration.json".format(cos_version)
    local_config_path = os.path.join(get_dirs(args, "state"), config_file)

    # Retrieve And Modify An Existing Configuration For COS.
    errout = connection.sudo("cray cfs configurations describe {} --format json".format(name),
                             warn=True)
    install_logger.debug("out={}, err={}".format(errout.stdout, errout.stderr))
    curr_config = json.loads(errout.stdout)

    if 'name' in curr_config.keys():
        curr_config.pop('name')
    if 'lastUpdated' in curr_config.keys():
        curr_config.pop('lastUpdated')

    for layer in curr_config["layers"]:
        if layer["name"] == name:
            layer["commit"] = commit

    with open(local_config_path, 'w', encoding='UTF-8') as fhandle:
        json.dump(curr_config, fhandle)
    connection.put(local_config_path, '/root')

    # Update Configuration Framework Service (CFS) Session With New COS Configuration.
    connection.sudo("cray cfs configurations update cos-config-{}-nogpu-integration --file /root/{} --format json".format(cos_version, config_file))

    cos_recipe_name = getenv("COS_RECIPE_NAME")
    recipe_list = json.loads(connection.sudo("cray ims recipes list --format json").stdout)
    recipes = [r for r in recipe_list if r['name'] == cos_recipe_name]
    if not recipes:
        msg = textwrap.dedent("""
            WARNING: Could not find recipe {}.  Skipping image building.
            """.format(cos_recipe_name))
        raise COSProblem(msg)
    elif len(recipes) != 1:
        install_logger.warning("WARNING: multiple recipes found for {}.  recipes = {}.  Taking the first one".format(cos_recipe_name, recipes))

    ims_recipe_id = recipes[0]['id']

    created_public_keys = json.loads(connection.sudo("cray ims public-keys list --format json").stdout)
    ci_public_key_list = [k for k in created_public_keys if k["name"] == "ci_public_key"]
    if len(ci_public_key_list) <= 0:
        pkey_dict = json.loads(connection.sudo('cray ims public-keys create --name "ci_public_key" --format json --public-key  ~/.ssh/id_rsa.pub').stdout)
        public_key = pkey_dict["public_key"]
    else:
        public_key = ci_public_key_list[0]
    ims_public_key_id = public_key['id']

    # Now create the image.
    dist = getenv("RELEASE_DIST")
    cmd = "cray ims jobs create --job-type create --image-root-archive-name \
            {}-recipe-ci-image --artifact-id {} --public-key-id {} \
            --enable-debug False --format json".format(
                dist.lower(), ims_recipe_id, ims_public_key_id)
    image_result = connection.sudo(cmd)
    install_logger.debug("result of image create: out={}, err={}".format(image_result.stdout, image_result.stderr))

    # Load the resulting json just so that if there is any kind of error,
    # it's caught now (because the json won't load right).
    image_info = json.loads(image_result.stdout)
    job_id = image_info['kubernetes_job']

    # Sleep 30 seconds to give enough time for the fields from the ims command to be populated
    time.sleep(30)
    pod_name, resultant_image_id, etag = wait_for_pod(job_id)
    if any( x is None for x in [pod_name, resultant_image_id, etag]):
        raise COSProblem("WARNING: Cannot create the modified cos image, pod_name={}".format(pod_name))

    install_logger.debug("Built COS image with a pod named {}, resultant_image_id={}, etag={}".format(
        pod_name, resultant_image_id, etag))

    image_info['resultant_image_id'] = resultant_image_id
    image_info['etag'] = etag
    image_info['pod_name'] = pod_name
    customize_cos_compute_image(args, image_info)


def check_analytics_mount(node):
    """Check the analytics mount.  It occassionally takes a few tries."""
    keep_waiting = True
    timeout = 60
    sleep_time = 10
    waited = 0
    while keep_waiting:
        # Sometimes it takes multiple tries for forceleanup, so only warn if
        # it fails.
        connection.sudo("ssh {} 'sh /tmp/forcecleanup.sh'".format(node), warn=True)
        mounts = connection.sudo("ssh {} 'mount -t dvs'".format(node)).stdout.splitlines()
        an_mounts = [m for m in mounts if 'analytics' in m.lower()]
        if an_mounts:
            time.sleep(sleep_time)
            waited += sleep_time
        else:
            keep_waiting = False

        if waited >= timeout:
            install_logger.warning("WARNING: Could not unmount the dvs analytics mounts on {}".format(node))
            keep_waiting = False


def unload_dvs_and_lnet(args):
    """Unload the DVS and LNET modules."""

    worker_tuples = utils.get_hosts(connection, "w00")

    connection.sudo("scp ncn-w001:/opt/cray/dvs/default/sbin/dvs_reload_ncn /tmp")

    install_logger.debug("get all pods ...")
    all_pods = connection.sudo("kubectl --kubeconfig=/etc/kubernetes/admin.conf get pods -Ao wide").stdout.splitlines()

    # Clone the analytics repo.  It will be used to unmount analytics on the worker.
    analytics_dir = os.path.join(BUILD_DIR, 'analytics-config-management')
    utils.git_clone(connection, 'analytics-config-management', BUILD_DIR)
    k8s_job_line = None
    for w_xname, w_node in worker_tuples:
        # Disable cfs.
        install_logger.debug("disable cfs on {}".format(w_node))
        connection.sudo("cray cfs components update {} --enabled false".format(w_xname))
        try:
            if host == "lemondrop-ncn-m001":
                install_logger.debug("lemondrop has no lustre mounts ==> skip configure_fs_unload.yml play")
            else:
                install_logger.debug("call dvs_reload_ncn...configure_fs_unload.yaml...")
                k8s_job_line = connection.sudo("/tmp/dvs_reload_ncn -c ncn-personalization -p playbooks/configure_fs_unload.yml {}".format(w_xname),
                    timeout=120).stdout.splitlines()
        except Exception as ex:
            install_logger.warning("WARNING (unload_dvs_and_lnet): Caught exception {}, name={}".format(ex, ex.__class__.__name__))
            install_logger.warning("dvs_reload_ncn...playbooks/configure_fs_unload.yml timed out on node {}".format(w_node))

        if k8s_job_line:
            k8s_job = k8s_job_line.split()[1].strip()
            install_logger.debug("k8sjob={}  wait for the pod...".format(k8s_job))
            utils.wait_for_pod(connection, k8s_job)
        else:
            install_logger.debug("WARNING: Unable to get the K8S job name.")

        # I think we still need to run dvs_reload_ncn to unmount the DVS mounts.

        # Unmount PE and Analytics on the worker.
        # Check if cps-cm-pm pod is running on the worker.  Delete it if so.
        cps_cm_pm_pods = [cps for cps in all_pods if 'cray-cps-cm-pm' in cps and w_node in cps]
        install_logger.debug("(unload_dvs_and_lnet)cps_cm_pods={}".format(cps_cm_pm_pods))
        if  cps_cm_pm_pods:
            connection.sudo("cray cps deployment delete --nodes {}".format(w_node))
            pod_line = cps_cm_pm_pods[0]
            fields = pod_line.split()
            pod_name = fields[1]
            utils.wait_for_pod(connection, pod_name, delete=True)

        # Check to see if any UAIs are running on the worker.  Migrate them if so.
        uais = [ p for p in all_pods if 'uai' in p and w_node in p]
        for uai in uais:
            fields = uai.split()
            uai_name = fields[1]
            connection.sudo("kubectl --kubeconfig=/etc/kubernetes/admin.conf delete pod -n user {}".format(uai_name))

        # Unmount PE
        # Run the following sudo commands with 'warn=True' incase PE isn't installed.
        install_logger.debug("Unmount PE ...")
        connection.sudo("ssh {} '/etc/cray-pe.d/pe_overlay.sh cleanup'".format(w_node), warn=True)
        connection.sudo("ssh {} '/var/opt/cray/pe/pe_images -maxdepth 1 -exec umount -f {{}}\;'".format(w_node), warn=True)
        connection.sudo("ssh {} '/var/opt/cray/pe -maxdepth 1 -exec umount -f {{}} \;'".format(w_node), warn=True)

        # Unmount Analytics contents on the worker.
        connection.sudo("bash -c 'cd {} ; git checkout {} ; git pull'".format(
            analytics_dir, getenv('ANALYTICS_BRANCH')))
        connection.sudo("bash -c 'cd {} && scp roles/analyticsdeploy/files/forcecleanup.sh {}:/tmp'".format(analytics_dir, w_node))

        check_analytics_mount(w_node)

        # Add cps back to the worker
        connection.sudo("cray cps deployment update --nodes {}".format(w_node))

        # Make sure the reference count for dvs is 0.
        lsmods = connection.sudo("ssh {} 'lsmod'".format(w_node)).stdout.splitlines()
        skip_reload = False
        try:
            dvs_mod = [m for m in lsmods if m.startswith('dvs ')][0]
        except IndexError:
            skip_reload = True

        if not skip_reload:
            fields = dvs_mod.split()
            if fields[2] != '0':
                warning_str = textwrap.dedent(
                """WARNING: The DVS module ({}) didn't unload properly from {}.
                 Because of this, the COS Software cannot complete on this
                 worker.  fields[2]={}""".format(fields[0], w_node, fields[2]))
                install_logger.warning(warning_str)
                skip_reload = True

        # Unload previous COS release’s DVS and LNet services.
        try:
            install_logger.debug("call dvs_reload_ncn...cray_dvs_unload.yml")
            output = connection.sudo("/tmp/dvs_reload_ncn -D -c ncn-personalization -p playbooks/cray_dvs_unload.yml {}".format(w_xname),
                                     timeout=120).stdout.splitlines()
            k8s_job_line = [line for line in output if line.lower().startswith('services')][0]
        except Exception as ex:
            install_logger.warning("Caught exception {}, name={}".format(ex, ex.__class__.__name__))
            install_logger.warning("WARNING (unload_dvs_and_lnet): dvs_reload_ncn...cray_dvs_unload.yml timed out on node {}".format(w_node))
            k8s_job_line = None

        if k8s_job_line:
            k8s_job = k8s_job_line.split()[1].strip()
            install_logger.debug("k8sjob={}.  wait for the pod...".format(k8s_job))
            utils.wait_for_pod(connection, k8s_job)
        else:
            install_logger.warning("WARNING: Unable to get the K8S job name.")

        # TODO: We should probably continue here, but we need to make sure
        # of that, and that the check is valid.
        #if skip_reload:
        #    continue

        # Note the 'for' loop below is only for record-keeping.  The dvs,
        # lustre, and craytrace rpms need to be uninstalled in a specific
        # order because of dependencies.
        rpms = connection.sudo("ssh {} 'rpm -qa'".format(w_node)).stdout.splitlines()
        old_rpms = []
        for rpm in rpms:
            if any(name in rpm for name in ['dvs', 'craytrace', 'lustre']):
                old_rpms.append(rpm)

        # Package removal order.  Note regular expressions are needed because
        # (for example) 'cray-lustre-client' matches both 'cray-lustre-client'
        # and 'cray-lustre-client-kmp-default'.
        pkg_order_res = [r"cray-dvs-service-\d+",
                         r"cray-dvs-kmp-default-\d+",
                         r"cray-lustre-client-\d+",
                         r"cray-lustre-client-kmp-default-\d+",
                         r"cray-lustre-client-devel-\d+",
                         r"cray-craytrace-kmp-default-\d+"]

        for pkg in pkg_order_res:
            rpm_names = [r for r in old_rpms if re.match(pkg, r)]
            if len(rpm_names) > 1:
                install_logger.debug("NOTE: removing multiple rpms for package {}:".format(pkg))
                install_logger.debug("{}".format(rpm_names))
            for rpm in rpm_names:
                install_logger.debug("remove {} from {}".format(rpm, w_node))
                connection.sudo("ssh {} 'rpm -e {}'".format(w_node, rpm))

        # Enable and run NCN personalization on the worker.
        connection.sudo(" cray cfs components update --enabled true --state '[]' \
            --error-count 0 {} --format json".format(w_xname))

        # wait for ncn-personalization to finish.
        utils.wait_for_ncn_personalization(connection, [w_xname])

        rpms = connection.sudo("ssh {} 'rpm -qa'".format(w_node)).stdout.splitlines()
        new_rpms = []
        for rpm in rpms:
            if any(name in rpm for name in ['dvs', 'craytrace', 'lustre']):
                new_rpms.append(rpm)

        old_rpms = sorted(old_rpms)
        new_rpms = sorted(new_rpms)
        install_logger.debug("Old dvs, lustre, and craytrace rpms on {}: {}".format(w_node, ','.join(old_rpms)))
        install_logger.debug("New dvs, lustre, and craytrace rpms on {}: {}".format(w_node, ','.join(new_rpms)))

    # Add the UAIs back to the worker.  Note this is done earlier in the guide
    # (at https://stash.us.cray.com/projects/SHASTA-OS/repos/cos-docs/browse/portal/developer-portal/install/Upgrade_and_Configure_COS.md)
    # But since it's not done for each particular worker, it needs to be done out of order.
    connection.sudo("kubectl --kubeconfig=/etc/kubernetes/admin.conf label node ncn-m001 uas-")


def session_templates_sort(element):
    """A function to pass to sort."""
    return element["name"]


def boot_cos(args):
    """Boot a COS image"""

    if not os.path.exists(BOS_INFO_FILE) and os.environ["BUILD_COS_IMAGE"] == "true":
        msg = textwrap.dedent("""
        WARNING: the bos information file {} does not exist.  Cannot boot COS.
        """.format(BOS_INFO_FILE))
        raise COSProblem(msg)
    elif os.environ["BUILD_COS_IMAGE"] == "false":
        # If the templates aren't generated, use the last sessiontemplate generated by the CI
        session_templates = [st for st in json.loads(connection.sudo("cray bos sessiontemplate list --format json").stdout) if "cos-sessiontemplate-" in st["name"]]
        session_templates.sort(key=session_templates_sort, reverse=True)
        working_template = session_templates[0]
        sessiontemplate_name = working_template["name"]
        install_logger.debug("Using Previous session template: {}".format(sessiontemplate_name))
    else:
        # Get the current template and update the etag, image id, and configuration name.
        with open(BOS_INFO_FILE, 'r', encoding='UTF-8') as bos_fh:
            bos_info = json.load(bos_fh)

        bos_sessiontemplate = getenv("BOS_SESSIONTEMPLATE")
        working_template = json.loads(connection.sudo("cray bos sessiontemplate describe {} --format json".format(bos_sessiontemplate)).stdout)

        working_template.pop('name')
        working_template["boot_sets"]["compute"]["etag"] = bos_info["etag"]
        working_template["boot_sets"]["compute"]["path"] = "s3://boot-images/{}/manifest.json".format(bos_info["image_id"])
        working_template["cfs"]["configuration"] = bos_info["configuration"]
        local_bos_file = os.path.join(get_dirs(args, "state"), "bos_sessiontemplate.json")
        remote_bos_file = os.path.join(BUILD_DIR, "bos_sessiontemplate.json")
        with open(local_bos_file, 'w', encoding='UTF-8') as bos_fh:
            json.dump(working_template, bos_fh)
        connection.put(local_bos_file, remote_bos_file)

        date = datetime.datetime.today().strftime("%Y%m%d")
        sessiontemplate_name = "cos-sessiontemplate-{}-{}".format(get_cos_version(args), date)

        connection.sudo("cray bos sessiontemplate create --file {} --name {} ".format(
            remote_bos_file, sessiontemplate_name))

    boot_start_time = datetime.datetime.now()
    # Now reboot the compute nodes
    boot_session_job_id = connection.sudo("cray bos session create --template-uuid {} --operation reboot --format json | jq -r '.job'".format(sessiontemplate_name)).stdout.replace("\n", "")
    install_logger.debug("Boot session jobid {} created".format(boot_session_job_id))

    # Wait for the BOS session to"complete" or the BOS pod to be in a "Completed" state
    while True:
        in_progress, complete = json.loads(connection.sudo("cray bos session describe {} --format json | jq -r '[.in_progress, .complete]'".format(boot_session_job_id[4:])).stdout)
        pod_status = connection.sudo("kubectl --kubeconfig=/etc/kubernetes/admin.conf get pods -n services | grep {} | awk '{{print $3}}'".format(boot_session_job_id)).stdout.replace("\n", "")
        elapsed = (datetime.datetime.now() - boot_start_time).seconds
        install_logger.debug("Waited {} of {} seconds".format(elapsed, BOOT_TIMOUT_SECS))
        if (complete and not in_progress) or "Completed" in pod_status:
            break
        elif elapsed > BOOT_TIMOUT_SECS:
            raise TimeOut("Timed Out: stopped waiting for session {}".format(boot_session_job_id))
        time.sleep(BOOT_POLL_SECS)

    install_logger.info("BOS Session {} finished, complete: {}, in_progress: {}, pod status: {}, elapsed time: {}".format(
        boot_session_job_id, in_progress, complete, pod_status, elapsed))

def run_hello_world(args):
    """Run a "Hello World" test on a compute node."""
    sat_nodes = json.loads(connection.sudo("sat status --format json").stdout)
    node_lst = [ node for node in sat_nodes if "Compute" in node["Role"] and "Ready" in node["State"]]

    if len(node_lst) <= 0:
        raise UnexpectedState("There are no nodes in a 'Ready' state")

    head_compute_xname = node_lst[0]['xname']
    cmd = "ssh {} sinfo --Node --noheader | awk '{{print $1,$4}}'".format(head_compute_xname)
    sinfo_output = connection.sudo(cmd).stdout.split("\n")

    slurm_idle_node_lst = [n.split(" ")[0] for n in sinfo_output if len(n) > 0 and "idle" in n]
    slurm_down_node_lst = [n.split(" ")[0] for n in sinfo_output if len(n) > 0 and "idle" not in n]

    if len(slurm_idle_node_lst) <= 0:
        raise UnexpectedState("There are no slurm nodes in an 'idle' state")

    install_logger.debug("Slurm 'idle' nodes: {}".format(slurm_idle_node_lst))
    install_logger.debug("Slurm nodes not 'idle': {}".format(slurm_down_node_lst))

    if len(slurm_down_node_lst) > 0:
        install_logger.debug("{} of {} slurm node are not in 'idle' state".format(
            len(slurm_down_node_lst),
            len(slurm_down_node_lst)+len(slurm_idle_node_lst)))

    if len(node_lst) != len(slurm_idle_node_lst):
        install_logger.warning("WARNING: Not all 'Ready'({}) nodes are in 'idle'({}) state".format(
            len(node_lst), len(slurm_idle_node_lst)))

    install_logger.info("Running srun on {} nodes".format(len(slurm_idle_node_lst)))

    srun_node_list =  ",".join(slurm_idle_node_lst)
    cmd = "ssh {} srun -w {} /bin/hostname".format(head_compute_xname,srun_node_list)
    srun_output = [s for s in connection.sudo(cmd).stdout.split("\n") if len(s) > 0]

    slurm_idle_node_lst.sort()
    srun_output.sort()

    for out, rdy in zip(srun_output,slurm_idle_node_lst):
        if out != rdy:
            raise TestFailure("unexpected srun output: {} of {}".format(
                srun_output,slurm_idle_node_lst))

    install_logger.info("srun hello_world test succeded")

def setup(args): # pylint: disable=unused-argument
    """ Set up directories.  Remove the old job(s) from REMOTE_PROJECT_OLDJOBS_DIR"""
    connection.sudo("rm -rf {}/*".format(REMOTE_PROJECT_OLDJOBS_DIR))
    connection.sudo("mkdir -p {}".format(BUILD_DIR))

    sync_ci_tools()
    setup_git_config()
    backup_config_repos()


def cleanup(args): # pylint: disable=unused-argument
    """Clean things up after a run."""

    # remove files containing passwords
    connection.sudo("rm -f {}/.vcspass {}/get_local_vcspw.sh".format(BUILD_DIR,BUILD_DIR))

    # archive the build temp dir
    connection.sudo("mkdir -p {}".format(REMOTE_PROJECT_OLDJOBS_DIR))
    connection.sudo("mv {} {}".format(BUILD_DIR,REMOTE_PROJECT_OLDJOBS_DIR))


def hello(args):
    print("hello")
    allout = connection.sudo("echo hello")
    install_logger.debug("sudo result: stdout={}, stderr={}".format(allout.stdout, allout.stderr))

def main():
    """main entry point"""

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="subcommands")

    setup_sp = subparsers.add_parser("setup")
    setup_sp.set_defaults(func=setup)

    get_binaries_sp = subparsers.add_parser("get_binaries")
    get_binaries_sp.add_argument("product", action="store",
        help="product to fetch; one of cos, sle, or slingshot_host")
    get_binaries_sp.set_defaults(func=get_binaries)

    get_prods_sp = subparsers.add_parser("get_prods")
    get_prods_sp.add_argument("args")
    get_prods_sp.set_defaults(func=get_prods)

    cleanup_sp = subparsers.add_parser("cleanup")
    cleanup_sp.set_defaults(func=cleanup)

    install_sp = subparsers.add_parser("install")
    install_sp.add_argument("product", action="store",
        help="product to install; one of cos, sle, or slingshot_host")
    install_sp.set_defaults(func=install)

    check_pods_sp = subparsers.add_parser("check_pods")
    check_pods_sp.add_argument("product", action="store",
        help="product pods to check; one of cos, sle, or slingshot host")
    check_pods_sp.set_defaults(func=check_pods)

    check_services_sp = subparsers.add_parser("check_services")
    check_services_sp.add_argument("product", action="store",
        help="Check nmd, cray-cps, and dvs services.")
    check_services_sp.set_defaults(func=check_services)

    ncn_personalization_sp = subparsers.add_parser("ncn_personalization")
    ncn_personalization_sp.set_defaults(func=ncn_personalization)

    merge_cos_integration_sp = subparsers.add_parser("merge_cos_integration")
    merge_cos_integration_sp.set_defaults(func=merge_cos_integration)

    build_cos_compute_image_sp = subparsers.add_parser("build_cos_compute_image")
    build_cos_compute_image_sp.set_defaults(func=build_cos_compute_image)

    unload_cos_and_lnet_sp = subparsers.add_parser("unload_dvs_and_lnet")
    unload_cos_and_lnet_sp.set_defaults(func=unload_dvs_and_lnet)

    boot_cos_sp = subparsers.add_parser("boot_cos")
    boot_cos_sp.set_defaults(func=boot_cos)

    run_hello_world_sp = subparsers.add_parser("run_hello_world")
    run_hello_world_sp.set_defaults(func=run_hello_world)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
