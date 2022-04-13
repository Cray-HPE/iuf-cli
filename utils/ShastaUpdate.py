#!/usr/bin/env python3

"""
Copyright 2022 Hewlett Packard Enterprise Development LP
"""

import copy
import datetime
import json
import os
import platform
import re
import stat
import sys
import time

from pprint import pformat

from distutils.version import LooseVersion

import yaml #pylint: disable=import-error

from utils.InstallLogger import get_install_logger
from utils.InstallerUtils import CmdMgr

install_logger = get_install_logger(__name__)

CWD = os.getcwd()

from utils.vars import *
import utils.InstallerUtils as utils #pylint: disable=wrong-import-position,import-error
from utils.InstallerUtils import getenv #pylint: disable=wrong-import-position,import-error

# pylint: disable=consider-using-f-string

connection = utils.CmdMgr.get_cmd_interface()
install_logger = get_install_logger(__name__)


def get_dirs(args,which=None):
    media_dir = args.get("media_dir")
    state_dir = args.get("state_dir")
    if which == "state":
        return state_dir
    elif which == "media":
        return media_dir
    else:
        return media_dir, state_dir


def load_prods(args):
    """
    load the product dict
    """
    statedir = get_dirs(args, "state")

    with open(os.path.join(statedir, LOCATION_DICT), 'r',
              encoding='UTF-8') as fhandle:
        location_dict = yaml.full_load(fhandle)

    return location_dict


def update_prods(args, location_dict):
    """
    update location_dict with updated info
    """
    media_dir, statedir = get_dirs(args)
    install_logger.debug('updating location_dict')
    filepath = os.path.join(statedir, "location_dict.yaml")
    with open(filepath, "w", encoding="UTF-8") as fhandle:
        yaml.dump(location_dict, fhandle)


def get_prods(args):
    """A passthrough function to InstallerUtils.get_products."""

    media_dir, statedir = get_dirs(args)

    extract_archives = (args.get("dryrun", False) == False)
    location_dict = utils.get_products(connection, media_dir, extract_archives=extract_archives)
    filepath = os.path.join(statedir, "location_dict.yaml")

    with open(filepath, "w", encoding="UTF-8") as fhandle:
        yaml.dump(location_dict, fhandle)

    # print summary of products
    installable_products = []
    uninstallable_products = []
    for product in location_dict:
        if location_dict[product]['product'] and location_dict[product]['work_dir']:
            installable_products.append(product)
        else:
            uninstallable_products.append(product)
    install_logger.info("  Installable products:")
    for item in installable_products:
        install_logger.info("    {}".format(item))
    if uninstallable_products:
        install_logger.info("  Ignoring:")
        for item in uninstallable_products:
            install_logger.info("    {}".format(item))


def install(args):
    """Install COS, Slingshot-host, or SLE"""

    # load previously discovered produts
    location_dict = load_prods(args)

    unsuccessful_products = []
    for prod in location_dict:
        # only look at entries that are identified as products
        if location_dict[prod]['product']:
            # work_dir will not be set for invalid products
            if location_dict[prod]['work_dir'] and not location_dict[prod]['installed']:
                install_logger.info('  installing {}'.format(prod))
                loc = location_dict[prod]['work_dir']
                product = location_dict[prod]['product']

                # workaround LINUX-3213
                if product == 'sles':
                    problems = [
                        '[[ -t 1 ]] || return',
                        '[[ -n "$ncolors" && $ncolors -ge 8 ]] || return'
                        ]
                    color_file = os.path.join(loc, 'lib', 'color.sh')
                    try:
                        with open(color_file, "r") as color_fh:
                            origial_color_data = color_fh.read()
                        color_data = origial_color_data
                        for pattern in problems:
                            color_data = color_data.replace(pattern + '\n', pattern + ' 0\n')
                        with open(color_file, "w") as color_fh:
                            color_fh.write(color_data)
                        if color_data != origial_color_data:
                            install_logger.info('    patched extracted install for LINUX-3213')
                    except Exception as err:
                        install_logger.debug('failed to patch LINUX-3213 due to {}, perhaps obsolete?'.format(err))

                cmd = './install.sh'
                try:
                    result = connection.sudo(cmd, cwd=loc, timeout=900)
                    install_logger.debug(result)
                    install_logger.info('    OK')
                    location_dict[prod]['installed'] = True
                    update_prods(args, location_dict)
                except Exception as err:
                    install_logger.error('    Failed')
                    err_summary = {
                        'product': prod,
                        'stderr': err.stderr.splitlines()[-5:]
                    }
                    unsuccessful_products.append(err_summary)
                    location_dict[prod]['installed'] = False
                    update_prods(args, location_dict)

            else:
                install_logger.info('  {} already installed'.format(prod))

    valid_products = 0
    for prod in location_dict:
        # only look at entries that are identified as products
        if location_dict[prod]['product']:
            # work_dir will not be set for invalid products
            if location_dict[prod]['work_dir']:
                valid_products += 1

    if unsuccessful_products:
        install_logger.error('The following products failed to install:')
        for problem in unsuccessful_products:
            install_logger.error('  {} ==========================='.format(problem['product']))
            for line in problem['stderr']:
                if len(line) > 75:
                    fmt_line = line[:75] + '[..]'
                else:
                    fmt_line = line
                # log the full line, but print a short line to the screen
                install_logger.debug('    stderr> {}'.format(line))
                print('ERROR    stderr> {}'.format(fmt_line))

    # we shouldn't continue if we tried to install something and failed
    if unsuccessful_products:
        raise InstallError('Product installation failure')

    # if there are no valid products you'll fail at subsequent stages
    if not valid_products:
        raise InstallError('No valid products')


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

    time_to_wait = 60 * 20 # Wait 20 minutes.
    total_time = 0
    sleep_time = 10
    alert_time = 60

    get_jobs_cmd = "kubectl --kubeconfig=/etc/kubernetes/admin.conf get jobs -A"
    get_pods_cmd = "kubectl --kubeconfig=/etc/kubernetes/admin.conf get pods -A"

    while True:

        jobs_list = connection.sudo(get_jobs_cmd).stdout.splitlines()
        jobs_list = [job for job in jobs_list if 'import' in job]

        pods_list = connection.sudo(get_pods_cmd).stdout.splitlines()
        pods_list = [pod for pod in pods_list if 'import' in pod]

        if args['dryrun']:
            return

        if not (jobs_list or pods_list):
            msg = "Cannot find any related pods or jobs to monitor."
            raise Exception(msg)

        jobs_not_running = []
        pods_not_running = []

        for job in jobs_list:
            fields = job.split()
            job_name = fields[1]
            if 'config-import' in job_name or 'recipe-import' in job_name:
                completions = fields[2]
                if not is_ready(completions):
                    install_logger.debug("found the following job --not-- running:{}".format(fields))
                    jobs_not_running.append(job_name)

        for pod in pods_list:
            fields = pod.split()
            pod_name = fields[1]
            if 'config-import' in pod_name or 'recipe-import' in pod_name:
                if not 'completed' in fields[3].lower():
                    install_logger.debug("found the following pod --not-- running:{}".format(fields))
                    pods_not_running.append(pod_name)

        if jobs_not_running or pods_not_running:
            install_logger.debug("jobs_not_running={} ==> sleep".format(jobs_not_running))
            install_logger.debug("pods_not_running={} ==> sleep".format(pods_not_running))
            if total_time % alert_time == 0:
                install_logger.info("  Waiting for {} outstanding import jobs out of {} and {} outstanding import pods out of {}".format(
                                     len(jobs_not_running), len(jobs_list),
                                     len(pods_not_running), len(pods_list)))
            total_time+=sleep_time
            time.sleep(sleep_time)
        else:
            install_logger.info("  Finished waiting for {} import jobs and {} import pods".format(
                                     len(jobs_list),
                                     len(pods_list)))
            break

        if total_time > time_to_wait:
            msg = utils.formatted("""
                WARNING: the following job/pods have not completed booting: {}/{}
                """.format(','.join(jobs_not_running), ','.join(pods_not_running)))
            raise TimeOut(msg)


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
            install_logger.warning("dvs not found in kernel modules on node {}!".format(node))

        if not found_lnet:
            install_logger.warning("lnet not found in kernel modules on node {}!".format(node))


def get_mergeable_repos(args):

    # load previously discovered produts
    location_dict = utils.get_product_catalog(connection, load_prods(args))

    repos = {}

    # only products with an import_branch are mergable
    for product in location_dict:
        if location_dict[product]['import_branch']:
            repo = os.path.basename(location_dict[product]['clone_url']).replace('.git', '')
            product = location_dict[product]['product']
            repos[product] = repo

    install_logger.debug('found mergeable_repos {}'.format(repos))

    return repos


def update_working_branches(args):
    """Merge the product git branch to the working config"""

    # first things first, get a copy of all the config repos
    install_logger.debug("Cloning all of the configuration repositories...")

    # load previously discovered produts
    location_dict = utils.get_product_catalog(connection, load_prods(args))

    # get dict of mergeable repos
    repos = get_mergeable_repos(args)
    git = utils.git(args, connection)

    for product in location_dict:
        if location_dict[product]['import_branch']:

            import_branch = location_dict[product]['import_branch']
            product_name = location_dict[product]['product']
            if location_dict[product]['import_version']:
                prod_version = location_dict[product]['import_version']
            else:
                prod_version = location_dict[product]['product_version']
            repo = repos[product_name]
            cos_checkout_dir = git.clone(repo)

            # second, see what branches we want to work with
            _, integration_branch = current_repo_branch(args, repo, prod_version)

            if integration_branch is None:
                raise UnexpectedState("ERROR: Could not determine {} branch".format(product))

            # check out a local copy of the import_branch (release version)
            git.checkout(repo, import_branch)

            # check out a copy of the working branch, creating it if necessary
            git.checkout(repo, integration_branch, create=True)

            # fourth, merge the import branch to the integration branch
            install_logger.info("  Merging branch {} into {}".format(import_branch,integration_branch))
            git.merge(repo, import_branch)
            git.push(repo)

            # then clean up
            git.cleanup(repo)

            install_logger.info("    OK")

    # add git config and write out state file
    update_prods(args, utils.get_product_catalog(connection, location_dict))


def get_cos_recipe_name(args):
    # if cos_recipe_name was supplied on the command line, just return it
    if "cos_recipe_name" in args:
        if args["cos_recipe_name"]:
            install_logger.debug("recipe name found in args {}".format(repr(args['cos_recipe_name'])))
            return args['cos_recipe_name']

    cos_version = get_prod_version(args, 'cos', False)
    product = "cos-" + cos_version
    install_logger.debug("using product {}".format(product))

    # if not, lets see if we can find it
    # load previously discovered produts
    location_dict = utils.get_product_catalog(connection, load_prods(args))
    if product not in location_dict:
        # no cos at all in the location_dict, give up
        install_logger.debug("{} not found in the location_dict.".format(product))
        return None

    if "recipe" not in location_dict[product]:
        # no recipe was found by get_product_catalog, give up
        install_logger.debug("'recipe' keyword not found in the location_dict.")
        return None

    # return what we've got
    install_logger.debug("recipe found in product catalog {}".format(location_dict[product]["recipe"]))
    return location_dict[product]["recipe"]


def update_cfs_commits(args, cfs_template_arg):
    """Update the the commits in a CFS config."""
    cfs_template = copy.deepcopy(cfs_template_arg)

    del cfs_template["name"]
    del cfs_template["lastUpdated"]

    def find_substr(substr):
        """Return the index of the element containing the substring.  This
        is a slow linear search, but there are only a few elements."""
        indices = []
        for i, _ in enumerate(cfs_template["layers"]):
            if substr in cfs_template["layers"][i]["cloneUrl"]:
                indices.append(i)
        return indices

    # Get the commits from the repos to forumulate the
    # ncn-personalization.json.  Then write it out for the
    # `cray cfs configurations update ...`.
    repos = get_mergeable_repos(args)
    for product, repo in repos.items():
        install_logger.debug("processing {} {}".format(product, repo))
        prod_version = get_prod_version(args, product)
        install_logger.debug("prod_ver {}".format(prod_version))
        commit, branch = current_repo_branch(args, repo, prod_version)
        install_logger.debug("commit {} branch {}".format(commit,branch))
        indices = find_substr(repo)
        for layer_i in indices:
            cfs_template["layers"][layer_i]["commit"] = commit
            install_logger.info("  Updating {} layer of {} with commit {}".format(
                product,
                cfs_template_arg["name"],
                commit
                ))

    return cfs_template


def update_ncnp_config(args):
    """Update the config used for NCN Personalization."""

    repos = get_mergeable_repos(args)
    if 'cos' in repos:
        prod_version = get_prod_version(args, 'cos')
    else:
        prod = repos.keys()[0]
        prod_version = get_prod_version(args, prod)

    base_file = "cfs-config.{}-{}.json".format(
        prod_version,
        datetime.datetime.today().strftime("%Y%m%d-%H%M%S"))

    # ncn_personalization should always be found in args since it has a
    # default value.
    template_name = args.get("ncn_personalization")
    cfs_template = None
    configs = json.loads(connection.sudo("cray cfs configurations list --format json").stdout)
    for i, _ in enumerate(configs):
        if configs[i]["name"] == template_name:
            cfs_template = configs[i]
            break

    if not cfs_template:
        err_msg = utils.formatted("""
        Unable to find a cfs configuration named {}.  Was one
        specified via the commandline?""".format(template_name))
        raise NCNPersonalization(err_msg)

    cfs_template = update_cfs_commits(args, cfs_template)

    file_location = os.path.join(get_dirs(args, "state"), base_file)
    with open(file_location, 'w', encoding='UTF-8') as fhandle:
        json.dump(cfs_template, fhandle)

    outdict = {"template_name": template_name, "file_location": file_location}
    with open(os.path.join(get_dirs(args, "state"), NCNP_VARS), "w") as fhandle:
        yaml.dump(outdict, fhandle)

    return template_name, file_location


def ncn_personalization(args): #pylint: disable=unused-argument
    """Do the NCN personalization as described in HPE Cray EX System
    Installation and Configuration Guide (1.4.2_S-8000 RevA)"""

    # Get a list of all worker and manaagement ncns. We need to skip the
    # ncn-s00* nodes for now.  So use the m_ncn_tuples + w_ncn_tuples and
    # consider that to be all the ncns.
    all_ncn_tuples = utils.get_ncn_tuples(connection, args)

    ncn_list_xnames = [n[0] for n in all_ncn_tuples]

    ncn_p_file = os.path.join(get_dirs(args, "state"),NCNP_VARS)
    with open(os.path.join(get_dirs(args, "state"),NCNP_VARS), "r") as fhandle:
        ncnp_dict = yaml.load(fhandle, yaml.SafeLoader)

    template_name = ncnp_dict["template_name"]
    pzation_file = ncnp_dict["file_location"]

    # Disable cfs.
    for ncn in ncn_list_xnames:
        connection.sudo("cray cfs components update {} --enabled false".format(ncn))

    # Upload the file to CFS.
    connection.sudo("cray cfs configurations update {} --file {} --format json".format(template_name, pzation_file))

    # Update the CFS component for all ncns.  Skip the ncn-s* nodes for now;
    # so this is basically the worker and management nodes.
    for ncn in ncn_list_xnames:
        connection.sudo("cray cfs components update --desired-config {} --enabled true --format json --error-count 0 --state [] {}".format(template_name, ncn))

    bad_nodes = utils.wait_for_ncn_personalization(connection, ncn_list_xnames, timeout=3600)

    if bad_nodes:
        nodes_str = ", ".join(bad_nodes)
        err_msg = utils.formatted("""
            The following nodes failed NCN Personalization:
            {}""".format(nodes_str))
        raise(NCNPersonalization(err_msg))


def current_repo_branch(args, repo, version):
    """Find the integration branch corresponding to the current COS version"""

    version_list = version.split('.')
    maj_min_v = version_list[0] + '.' + version_list[1]
    git = utils.git(args, connection)
    branches = git.ls_remote(repo)
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


def get_prod_version(args, product, short=True):
    """Get the COS version."""

    # Use static variables so the yaml doesn't need to be loaded every time.
    if hasattr(get_prod_version, "products") and get_prod_version.products.has(product):
        if short:
            return get_prod_version.products.versions[product].short_version
        else:
            return get_prod_version.products.versions[product].full_version

    # If we haven't returned, full_version and short_version do not exist.
    # read the yaml and set them.
    statedir = get_dirs(args, "state")
    install_logger.debug("location_dict path = {}".format(os.path.join(statedir, LOCATION_DICT)))
    with open(os.path.join(statedir, LOCATION_DICT), "r",
              encoding='UTF-8') as fhandle:
        locs_dict = yaml.load(fhandle, yaml.SafeLoader)

    repos = get_mergeable_repos(args)

    # The version field is obtained by simply splitting the product name
    # into a name and version section. So use product_version if possible,
    # because it leverages the "official" version once the product has been
    # installed.
    prod_versions = []
    for key in locs_dict:
        if product in key and locs_dict[key]['work_dir']:
            if 'product_version' in locs_dict[key].keys():
                prod_versions.append(locs_dict[key]['product_version'])
            else:
                prod_versions.append(locs_dict[key]['version'])

    prod_versions = [locs_dict[key]['version'] for key in locs_dict if product in key and locs_dict[key]['work_dir']]
    sorted_vers = sorted(prod_versions, key=LooseVersion)
    install_logger.debug('sorted prod_versions are {}'.format(sorted_vers))
    if sorted_vers:
        highest_vers = sorted_vers[-1]
        version_list = highest_vers.split('.')
        short_vers = "{}.{}".format(version_list[0], version_list[1])
    else:
        highest_vers = ''
        short_vers = ''

    install_logger.debug("locs_dict=\n{}\n".format(pformat(locs_dict)))
    install_logger.debug('highest_vers {}'.format(highest_vers))
    install_logger.debug('short_vers {}'.format(short_vers))

    get_prod_version.products = utils.productVersions()
    get_prod_version.products.set(product, short_vers, highest_vers)

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


def customize_cos_compute_image(args):
    """Customize a COS compute image."""

    cos_version = get_prod_version(args, 'cos')
    date = datetime.datetime.today().strftime("%Y%m%d")

    image_info_location = os.path.join(get_dirs(args, "state"), IMAGE_INFO)
    if not os.path.exists(image_info_location):
        errmsg = utils.formatted("""
            Could not find {}, which is generated in the
            build_cos_compute_image stage.  Was this stage ran, or has the
            state directory (specified with the '-s/--stage-dir' option)
            changed?""".format(image_info_location))

        raise COSProblem(errmsg)

    with open(image_info_location, 'r') as fhandle:
        image_info = yaml.load(fhandle, yaml.SafeLoader)
    # Find a session name that doesn't already exist.  We shouldn't need to
    # create more than 100 in a day.
    cfs_sessions = json.loads(connection.sudo("cray cfs sessions list --format json").stdout)
    existing_sessions = [cfss["name"] for cfss in cfs_sessions]

    for i in range(100):
        istr = "%02d" % i
        session_name = "cos-config-{}-integration-{}-{}".format(cos_version, date, istr)
        if session_name not in existing_sessions:
            break

    configuration_name = "cos-{}-nogpu-integration".format(cos_version)
    install_logger.debug("(customize_cos_compute_image)session_name={}, cos_version={}, configuration_name={}".format(
        session_name, cos_version, configuration_name))
    install_logger.info("  Customizing image with CFS configuration {}".format(configuration_name))
    cmd = "cray cfs sessions create --name {} --configuration-name {} \
           --target-definition image --target-group Compute {} --format json".format(
           session_name, configuration_name, image_info['resultant_image_id'])
    connection.sudo(cmd)

    # Now we need to poll the results and wait for it to finish.
    keep_going = True
    cfs_cmd = "cray cfs sessions describe {} --format json".format(session_name)

    timeout = 1200
    start = datetime.datetime.now()

    finished = False
    while keep_going:
        cfs_desc = json.loads(connection.sudo(cfs_cmd).stdout)
        status = cfs_desc["status"]["session"]["status"].strip().lower()
        succeeded = cfs_desc["status"]["session"]["succeeded"].strip().lower()
        if status == "complete" and succeeded == "true":
            keep_going = False
            finished = True
            install_logger.debug("Finished waiting for {}".format(session_name))
        elif status == "complete" and succeeded == "false":
            raise COSProblem("  The image customization failed!")
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
        cfs_desc = json.loads(connection.sudo("cray cfs sessions describe {} --format json".format(session_name)).stdout)
        image_id = cfs_desc["status"]["artifacts"][0]["result_id"]
        artifacts = json.loads(connection.sudo("cray artifacts describe boot-images {}/manifest.json --format json".format(image_id)).stdout)
        etag = artifacts['artifact']['ETag'].replace("\\\"", "")
        bos_info = {
            "image_id": image_id,
            "etag": etag,
            "configuration": configuration_name
        }
        bos_file = os.path.join(get_dirs(args, "state"), BOS_INFO_FILENAME)
        with open(bos_file, 'w', encoding='UTF-8') as bos_info_fh:
            json.dump(bos_info, bos_info_fh)
        install_logger.info("  Image info saved in {}".format(bos_file))
        install_logger.info("  Resulting customized image id {}".format(image_id))
    elif os.path.exists(bos_file):
        os.remove(bos_file)


def build_cos_compute_image(args): #pylint: disable=unused-argument
    """
    Create a Configuration Framework Service (CFS) session configuration
    for COS.
    """

    cos_version = get_prod_version(args, 'cos')
    cos_recipe_name = get_cos_recipe_name(args)

    if not cos_recipe_name:
        raise COSProblem("A recipe name is needed to build the COS compute image.")

    _, intbranch = current_repo_branch(args, 'cos-config-management', cos_version)

    if intbranch is None:
        raise COSProblem("WARNING: Could not determine COS branch, so cannot build a compute image")

    # Update the configuration.
    config_file = "cos-config-{}-nogpu-integration.json".format(cos_version)
    local_config_path = os.path.join(get_dirs(args, "state"), config_file)

    # Retrieve And Modify An Existing Configuration For COS.
    errout = connection.sudo("cray cfs configurations describe {} --format json".format(intbranch))
    install_logger.debug("out={}, err={}".format(errout.stdout, errout.stderr))
    curr_config = json.loads(errout.stdout)
    curr_config = update_cfs_commits(args, curr_config)

    with open(local_config_path, 'w', encoding='UTF-8') as fhandle:
        json.dump(curr_config, fhandle)
    connection.put(local_config_path, "/root{}".format(config_file))

    # Update Configuration Framework Service (CFS) Session With New COS Configuration.
    connection.sudo("cray cfs configurations update cos-config-{}-nogpu-integration --file /root/{} --format json".format(cos_version, config_file))

    recipe_list = json.loads(connection.sudo("cray ims recipes list --format json").stdout)
    recipes = [r for r in recipe_list if r['name'] == cos_recipe_name]
    if not recipes:
        msg = utils.formatted("""
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

    install_logger.info("  Creating image from recipe {} id {}".format(cos_recipe_name, ims_recipe_id))

    # Now create the image.
    datestr =  datetime.datetime.today().strftime("%Y%m%d-%H%M%S")
    cmd = "cray ims jobs create --job-type create --image-root-archive-name \
            {}-recipe-ci-image --artifact-id {} --public-key-id {} \
            --enable-debug False --format json".format(
                datestr, ims_recipe_id, ims_public_key_id)
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

    image_info_path = os.path.join(get_dirs(args, "state"), IMAGE_INFO)

    # Dump the image information so it can be used when making the
    # customized compute image.
    with open(image_info_path, 'w') as fhandle:
        yaml.dump(image_info, fhandle)

    install_logger.info("  Image info saved in {}".format(image_info_path))
    install_logger.info("  Resulting image id {}".format(resultant_image_id))


def check_analytics_mount(args, node):
    """Check the analytics mount.  It occassionally takes a few tries."""

    if not hasattr(check_analytics_mount, "cloned_dir"):
        install_logger.debug("Cloning the analytics dir for node {}".format(node))
        product_catalog =utils.get_product_catalog(connection)
        try:
           analytics_data = yaml.safe_load(product_catalog["analytics"])
        except KeyError:
            errmsg = utils.formatted("""
                Could not find analytics import_branch in the product catalog.
                Analytics will --NOT-- be unmounted.""")
            return

        versions = analytics_data.keys()
        highest_version = sorted(versions, key=LooseVersion)[-1]
        try:
            analytics_branch = analytics_data[highest_version]["configuration"]["import_branch"]
        except KeyError:
            errmsg = utils.formatted("""
                Unable to get the analytics data from the product catalog.
                There should be something like the following lines in the product catalog:

                analytics: 2.14
                    configuration:
                        import_branch: feature/2.14
                        ...

                As a result of this, analytics will --NOT-- be umounted.
                """)
            install_logger.warning(errmsg)
            # Return early if the analytics data isn't found.
            return
        git = utils.git(args, connection)
        repo = "analytics-config-management"
        check_analytics_mount.cloned_dir = git.clone(repo)
        git.checkout(repo, analytics_branch)
        git.pull(repo, quiet=True)

    keep_waiting = True
    timeout = 60
    sleep_time = 10
    waited = 0

    # Unmount Analytics contents on the worker.
    connection.sudo("scp roles/analyticsdeploy/files/forcecleanup.sh {}:/tmp".format(node), cwd=check_analytics_mount.cloned_dir)

    while keep_waiting:
        # Sometimes it takes multiple tries for forceleanup, so only warn if
        # it fails.
        connection.sudo("ssh {} /bin/bash /tmp/forcecleanup.sh".format(node))
        mounts = connection.sudo("ssh {} mount -t dvs".format(node)).stdout.splitlines()
        an_mounts = [m for m in mounts if 'analytics' in m.lower()]
        if an_mounts:
            time.sleep(sleep_time)
            waited += sleep_time
        else:
            keep_waiting = False

        if waited >= timeout:
            install_logger.warning("    Could not unmount the Analytics mounts on {}".format(node))
            keep_waiting = False


def has_lustre_fs(node):
    mounts = connection.sudo("ssh {} mount -t lustre".format(node)).stdout
    return "lustre" in mounts


def worker_health_check(args):
    """
    Ensure the worker nodes are in a good state prior to starting
    NCN Personalization
    """

    worker_tuples = utils.get_ncn_tuples(connection, args)
    worker_nodes = [wt[1] for wt in worker_tuples]

    # CPS deployment check
    install_logger.info("  Checking CPS Deployments")
    cps_deployment_data = json.loads(connection.sudo("cray cps deployment list --format json").stdout)

    bad_cps_nodes = []
    good_cps_nodes = []
    for pod in cps_deployment_data:
        node = pod["node"]
        if pod["podname"] == "NA":
            podname = None
        else:
            podname = pod["podname"]
        install_logger.info("    Node {}: CPS podname: {}".format(node, podname))
        if not podname:
            bad_cps_nodes.append(node)
        else:
            good_cps_nodes.append(node)

    # Configuration Check
    install_logger.info("  Checking CFS Component Status")

    bad_status_nodes = []
    for worker in worker_tuples:
        w_xname, w_hname = worker
        cmd = "cray cfs components describe --format json {}".format(w_xname)
        component = json.loads(connection.sudo(cmd).stdout)
        w_status = component["configurationStatus"]
        w_err = component["errorCount"]
        w_cfg = component["desiredConfig"]
        if w_status != "configured":
            bad_status_nodes.append(w_hname)

        install_logger.info("    Node {} ({}): {} with {}".format(
            w_hname,
            w_xname,
            w_status,
            w_cfg
            ))

    if bad_cps_nodes:
        install_logger.warning("Check nodes {}".format(bad_cps_nodes))

    if len(good_cps_nodes) - len(bad_cps_nodes) <3:
        install_logger.error("Need at least three CPS deployments")
        raise NCNPersonalization("Too few CPS deployments")

    if bad_status_nodes:
        install_logger.error("Fix CFS component configuration errors before proceeding")
        raise NCNPersonalization("NCN nodes not starting in configured state")


def unload_dvs_and_lnet(args):
    """Unload the DVS and LNET modules."""

    worker_tuples = utils.get_ncn_tuples(connection, args, just_workers=True)

    # This stage is only ran on the worker nodes.  Make sure no other node
    # type was specified.
    bad_nodes = [wt[1] for wt in worker_tuples if "ncn-w" not in wt[1]]
    if bad_nodes:
        raise NCNPersonalization(utils.formatted("""
            DVS can only be reloaded on worker nodes.  The following nodes are not
            worker nodes:
            {}""".format(",".join(bad_nodes))))

    connection.sudo("scp ncn-w001:/opt/cray/dvs/default/sbin/dvs_reload_ncn /tmp")

    install_logger.debug("get all pods ...")
    all_pods = connection.sudo("kubectl --kubeconfig=/etc/kubernetes/admin.conf get pods -Ao wide").stdout.splitlines()

    statedir = get_dirs(args, "state")

    for w_xname, w_node in worker_tuples:

        install_logger.info("  Unloading DVS and LNET on node: {}".format(w_node))

        # Unmount PE and Analytics on the worker.
        # Check if cps-cm-pm pod is running on the worker.  Delete it if so.
        cps_cm_pm_pods = [cps for cps in all_pods if 'cray-cps-cm-pm' in cps and w_node in cps]
        install_logger.debug("(unload_dvs_and_lnet)cps_cm_pods={}".format(cps_cm_pm_pods))
        if  cps_cm_pm_pods:
            install_logger.info("    Deleting CPS deployment")
            connection.sudo("cray cps deployment delete --nodes {}".format(w_node))
            pod_line = cps_cm_pm_pods[0]
            fields = pod_line.split()
            pod_name = fields[1]
            utils.wait_for_pod(connection, pod_name, delete=True)

        # Check to see if any UAIs are running on the worker.  Migrate the UAIs and wait for each to finish.
        uais = [ p for p in all_pods if 'uai' in p and w_node in p]
        connection.sudo("kubectl --kubeconfig=/etc/kubernetes/admin.conf label node {} --overwrite uas=False".format(w_node))
        for uai in uais:
            fields = uai.split()
            uai_name = fields[1]
            install_logger.info("    Migrating UAI {} off the worker {}".format(uai_name, w_node))
            connection.sudo("kubectl --kubeconfig=/etc/kubernetes/admin.conf delete pod -n user {}".format(uai_name))
            install_logger.debug("Waiting for UAI {} to migrate".format(uai_name))
            utils.wait_for_pod(connection, uai_name)

        # enable cfs.  while the health check verifies a node needs to be configured
        # before you get here, if someone is trying to fix a configuration they may
        # well be re-running this stage and in that case they may have gotten to the
        # point where we disable cfs and that will mean cfs won't schedule anything
        # that uses cfs, ie the unload scripts
        install_logger.debug("enabling cfs on {}".format(w_node))
        connection.sudo("cray cfs components update {} --enabled true --error-count 0".format(w_xname))

        # check for conflicting (outstanding) cfs session
        try:
            conflicting_session = connection.sudo("cray cfs sessions describe configure-fs-unload-yml --format json")
            raise UnexpectedState("Found cfs session 'configure-fs-unload-yml' already exists")
        except Exception as err:
            pass

        install_logger.info("    Running fs_unload")
        k8s_job_line = None
        output = connection.sudo("/tmp/dvs_reload_ncn -c ncn-personalization -p configure_fs_unload.yml {}".format(w_xname),
            timeout=120).stdout.splitlines()

        # I don't think the k8s jobline not being found should be fatal.
        try:
            k8s_job_line = [line for line in output if line.lower().startswith('services')][0]
        except Exception as err:
            k8s_job_line = None

        if k8s_job_line:
            k8s_job = k8s_job_line.split()[1].strip()
            install_logger.debug("k8sjob={}  wait for the pod...".format(k8s_job))
            utils.wait_for_pod(connection, k8s_job)
        else:
            install_logger.debug("WARNING: Unable to get the K8S job name.")

        # Sleep for a minute before unmounting PE.
        install_logger.debug("Let the system settle prior to unmounting Analytics")
        time.sleep(60)

        # check_analytics will run forcecleanup.sh until dvs unmounts cleanly
        install_logger.info("    Unmounting Analytics")
        check_analytics_mount(args, w_node)

        # Unmount PE
        install_logger.info("    Unmounting PE")
        connection.sudo("scp tools/unmount_pe.sh {}:/tmp/unmount_pe.sh".format(w_node))
        connection.sudo("ssh {} /tmp/unmount_pe.sh".format(w_node))

        # Make sure the reference count for dvs is 0.
        lsmods = connection.sudo("ssh {} lsmod".format(w_node)).stdout.splitlines()
        try:
            dvs_mod = [m for m in lsmods if m.startswith('dvs ')][0]
            fields = dvs_mod.split()
            if fields[2] != '0':
                error_str = utils.formatted("""
                    The DVS module ({}) didn't unload properly from {}.
                    Because of this, the COS Software cannot complete on this
                    worker.  the reference count is {} (should be 0)""".format(fields[0], w_node, fields[2]))
                raise InstallError(error_str)
            else:
                install_logger.info("    DVS module reference count is 0 (OK)")
        except Exception as err:
            # there were no dvs modules loaded
            install_logger.info("    No DVS modules loaded (OK)")

        # Unload previous COS release’s DVS and LNet services.
        install_logger.info("    Running dvs_unload")
        output = connection.sudo("/tmp/dvs_reload_ncn -D -c ncn-personalization -p cray_dvs_unload.yml {}".format(w_xname)
                                     ).stdout.splitlines()

        # I don't think the k8s jobline not being found should be fatal.
        try:
            k8s_job_line = [line for line in output if line.lower().startswith('services')][0]
        except Exception as err:
            k8s_job_line = None

        if k8s_job_line:
            k8s_job = k8s_job_line.split()[1].strip()
            install_logger.debug("k8sjob={}.  wait for the pod...".format(k8s_job))
            utils.wait_for_pod(connection, k8s_job)
        else:
            install_logger.warning("    (unload_dvs_and_lnet): Unable to get the K8S job name.")

        # make sure dvs and lnet unload succeeded
        lsmods = connection.sudo("ssh {} lsmod".format(w_node)).stdout.splitlines()
        try:
            dvs_mods = None
            dvs_mods = [m for m in lsmods if m.startswith('dvs ')]
            raise UnexpectedState("DVS modules still loaded after running cray_dvs_unload.yml")
        except Exception as err:
            pass
        try:
            lnet_mods = None
            lnet_mods = [m for m in lsmods if m.startswith('lnet ')]
            raise UnexpectedState("LNET modules still loaded after running cray_dvs_unload.yml")
        except Exception as err:
            pass

        # move the lnet config aside, if exists
        try:
            connection.sudo("ssh {} mv /etc/lnet.conf /etc/lnet.conf.previous")
        except Exception as err:
            pass

        # Note the 'for' loop below is only for record-keeping.  The dvs,
        # lustre, and craytrace rpms need to be uninstalled in a specific
        # order because of dependencies.
        rpms = connection.sudo("ssh {} rpm -qa".format(w_node)).stdout.splitlines()
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

        install_logger.info("    Removing DVS, Lustre and Craytrace RPMs")
        for pkg in pkg_order_res:
            rpm_names = [r for r in old_rpms if re.match(pkg, r)]
            if len(rpm_names) > 1:
                install_logger.debug("NOTE: removing multiple rpms for package {}:".format(pkg))
                install_logger.debug("{}".format(rpm_names))
            for rpm in rpm_names:
                install_logger.debug("remove {} from {}".format(rpm, w_node))
                connection.sudo("ssh {} rpm -e {}".format(w_node, rpm))

        # Enable and run NCN personalization on the worker.
        install_logger.info("    Running NCN personalization")
        connection.sudo("cray cfs components update --enabled true --state '[]' \
            --error-count 0 {} --format json".format(w_xname))

        # wait for ncn-personalization to finish.
        bad_nodes = utils.wait_for_ncn_personalization(connection, [w_xname])
        if bad_nodes:
            raise NCNPersonalization("NCN Personalization failed for {}".format(w_xname))

        rpms = connection.sudo("ssh {} rpm -qa".format(w_node)).stdout.splitlines()
        new_rpms = []
        for rpm in rpms:
            if any(name in rpm for name in ['dvs', 'craytrace', 'lustre']):
                new_rpms.append(rpm)

        old_rpms = sorted(old_rpms)
        new_rpms = sorted(new_rpms)
        install_logger.debug("Previous RPMs {}: {}".format(w_node, ','.join(old_rpms)))
        install_logger.debug("Current RPMs {}: {}".format(w_node, ','.join(new_rpms)))

        # Add cps back to the worker if they were deleted
        install_logger.info("    Adding CPS deployment")
        connection.sudo("cray cps deployment update --nodes {}".format(w_node))

        # Add the UAIs back to the worker.  Note this is done earlier in the guide
        # (at https://stash.us.cray.com/projects/SHASTA-OS/repos/cos-docs/browse/portal/developer-portal/install/Upgrade_and_Configure_COS.md)
        # But since it's not done for each particular worker, it needs to be done out of order.
        install_logger.info("    Adding UAS label")
        connection.sudo("kubectl --kubeconfig=/etc/kubernetes/admin.conf label node {} uas-".format(w_node))

        install_logger.info("    {} OK".format(w_node))

    install_logger.info("  OK")


def create_bos_session_template(args):
    # load the image id information
    bos_file = os.path.join(get_dirs(args, "state"), BOS_INFO_FILENAME)
    if not os.path.exists(bos_file):
        msg = utils.formatted("""
        WARNING: the bos information file {} does not exist.  Cannot boot COS.
        """.format(bos_file))
        raise COSProblem(msg)
    
    with open(bos_file, 'r', encoding='UTF-8') as bos_fh:
        bos_info = json.load(bos_fh)

    if "source_bos_sessiontemplate" not in args:
        msg = utils.formatted("""
            When booting the compute nodes, a bos sessiontemplate
            (-t or --source-bos-sessiontemplate) is required""")
        raise COSProblem(msg)
    
    install_logger.debug("found source_bos_sessiontemplate in args")
    source_template_name = args["source_bos_sessiontemplate"]

    install_logger.info("  Using BOS sessiontemplate {} as a template".format(source_template_name))
    working_template = json.loads(connection.sudo("cray bos sessiontemplate describe {} --format json".format(source_template_name)).stdout)

    working_template.pop('name')
    working_template["boot_sets"]["compute"]["etag"] = bos_info["etag"]
    working_template["boot_sets"]["compute"]["path"] = "s3://boot-images/{}/manifest.json".format(bos_info["image_id"])

    bos_session_file = os.path.join(get_dirs(args, "state"), BOS_SESSIONTEMPLATE_FILENAME)

    date = datetime.datetime.today().strftime("%Y%m%d")
    sessiontemplate_name = "cos-sessiontemplate-{}-{}".format(get_prod_version(args, 'cos'), date)

    with open(bos_session_file, 'w', encoding='UTF-8') as fhandle:
        json.dump(working_template, fhandle)

    install_logger.info("  Generated BOS sessiontemplate saved as {}".format(bos_session_file))

    connection.sudo("cray bos sessiontemplate create --file {} --name {} ".format(
        bos_session_file, sessiontemplate_name))

    install_logger.info("  Created BOS sessiontemplate {}".format(sessiontemplate_name))

def boot_cos(args):
    """Boot a COS image"""

    #TODO figure out what sessiontemplate to boot with
    # sessiontemplate_name = ???

    boot_start_time = datetime.datetime.now()
    # Now reboot the compute nodes
    output = json.loads(connection.sudo("cray bos session create --template-uuid {} --operation reboot --format json".format(sessiontemplate_name)).stdout)
    boot_session_job_id = output["job"]
    install_logger.info("  Boot session jobid {} created".format(boot_session_job_id))

    # Wait for the BOS session to "complete" or the BOS pod to be in a "Completed" state
    while True:
        session_desc = json.loads(connection.sudo("cray bos session describe {} --format json".format(boot_session_job_id[4:])).stdout)
        in_progress = session_desc["in_progress"]
        complete = session_desc["complete"]
        all_pods =  connection.sudo("kubectl --kubeconfig=/etc/kubernetes/admin.conf get pods -n services").stdout.splitlines()
        boot_pod = [aps for aps in all_pods if boot_session_job_id in aps]
        pod_status = None
        if boot_pod:
            pod_status = boot_pod[0].split()[2]

        elapsed = (datetime.datetime.now() - boot_start_time).seconds
        install_logger.info("    Waited {} of {} seconds".format(elapsed, BOOT_TIMOUT_SECS))
        if (complete and not in_progress) or "Completed" in pod_status:
            break
        elif elapsed > BOOT_TIMOUT_SECS:
            raise TimeOut("    Timed Out: stopped waiting for session {}".format(boot_session_job_id))
        time.sleep(BOOT_POLL_SECS)

    install_logger.info("  BOS Session {} finished, complete: {}, in_progress: {}, pod status: {}, elapsed time: {}".format(
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


def cleanup(args): # pylint: disable=unused-argument
    """Clean things up after a run."""

    # remove files containing passwords
    statedir = get_dirs(args, "state")
    connection.sudo("rm -f {}/.vcspass {}/get_local_vcspw.sh".format(statedir, statedir))


def hello(args):
    print("hello")
    allout = connection.sudo("echo hello")
    install_logger.debug("sudo result: stdout={}, stderr={}".format(allout.stdout, allout.stderr))


def validate_products(args):
    """
    verify media prior to installation
    """

    # load previously discovered produts
    location_dict = load_prods(args)

    valid_products = {'cos': {}, 'slingshot-host-software': {}}

    # build a list of valid cos and shs products
    for prod in location_dict:
        # only look at entries that are identified as products
        if location_dict[prod]['product']:
            # work_dir will not be set for invalid products
            if location_dict[prod]['work_dir'] and not location_dict[prod]['installed']:
                product_name = location_dict[prod]['product']
                # products we care about from a validation perspective
                if product_name in ['cos']:
                    valid_products[product_name][prod] = location_dict[prod]

    num_cos_products = len(valid_products['cos'])
    # num_shs_products = len(valid_products['slingshot-host-software'])

    if num_cos_products == 1:

        cos_products = valid_products['cos']
        cos_key = list(cos_products.keys())[0]
        cos_product = valid_products['cos'][cos_key]
        cos_workdir = cos_product['work_dir']

        install_logger.info('  Performing compatibility check for COS {} on running NCN'.format(cos_key))

        # get the os release version (ie, 15-sp2)
        os_release = utils.get_os().lower()

        # get the running kernel version and format it to match the way it is
        # represented in the COS rpms
        cmd = 'uname -r'
        running_kernel = connection.sudo(cmd, dryrun=False).stdout.split('-default')[0].replace('-', '_')

        # find a representive rpm from the cos media
        query_package = 'cray-dvs-kmp-default'
        cmd = "find {} -path '*{}-ncn*{}*' -print".format(cos_workdir, os_release, query_package)
        rpms = connection.sudo(cmd, dryrun=False).stdout.splitlines()

        # see if our sample rpm matches the running NCN kernel
        if len(rpms) == 1:
            if running_kernel in rpms[0]:
                install_logger.info('    COS NCN content matches running NCN kernel {}'.format(running_kernel))
                install_logger.debug(rpms)
            else:
                install_logger.debug(rpms)
                install_logger.error('    COS NCN content does NOT match running NCN kernel {}'.format(running_kernel))
                install_logger.error('    It is not recommended to proceed as packages such as DVS which have')
                install_logger.error('    a kernel module will not work correctly on your NCN worker nodes.')
                raise InstallError('COS NCN content does NOT match running NCN kernel {}'.format(running_kernel))
        else:
            install_logger.warning('    Cannot perform check due to unexpected number of RPM matches')
            install_logger.debug(rpms)

    else:
        install_logger.warning('  Cannot validate {} versions of COS, skipping validation check'.format(num_cos_products))
