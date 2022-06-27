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
import shutil
import stat
import sys
import time
import jinja2
from pprint import pformat
from pathlib import Path

from distutils.version import LooseVersion

import yaml #pylint: disable=import-error

from utils.git import Git
from utils.InstallLogger import get_install_logger, get_log_filename

from utils.vars import *
import utils.InstallerUtils as utils #pylint: disable=wrong-import-position,import-error
from utils.InstallerUtils import getenv #pylint: disable=wrong-import-position,import-error

import utils.pod as pod

# pylint: disable=consider-using-f-string

install_logger = get_install_logger(__name__)


def get_prods(config):
    """A passthrough function to InstallerUtils.get_products."""

    extract_archives = not config.dryrun
    config.location_dict = utils.get_products(config, extract_archives=extract_archives)

    install_logger.info("  Installable products:")
    for item in config.location_dict.installable_products:
        install_logger.info("    {}".format(item))
    if config.location_dict.uninstallable_products:
        install_logger.info("  Ignoring:")
        for item in config.location_dict.uninstallable_products:
            install_logger.info("    {}".format(item))


def install(config):
    """Install COS, Slingshot-host, or SLE"""

    valid_products = 0
    unsuccessful_products = []
    for prod in config.location_dict:
        # only look at entries that are identified as products
        if prod.product:
            # work_dir will not be set for invalid products
            if prod.work_dir and not prod.installed:
                install_logger.info('  installing {}'.format(prod.name))
                loc = prod.work_dir
                product = prod.product

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
                    logname = get_log_filename(config, prod.name)
                    prod.log_name = logname
                    install_logger.info('    Logging to {}'.format(logname))
                    result = config.connection.sudo(cmd, cwd=loc, timeout=900, store_output=logname)
                    install_logger.info('    OK')
                    valid_products += 1
                    if not config.dryrun:
                        prod.installed = True
                except Exception as err:
                    install_logger.error('   Failed')
                    err_summary = {
                        'product': prod.name,
                        'stderr': err.stderr.splitlines()[-5:]
                    }
                    unsuccessful_products.append(err_summary)
                    prod.installed = False

            else:
                install_logger.info('  {} already installed'.format(prod.name))

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
                install_logger.error('    stderr> {}'.format(fmt_line))

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


def verify_product_import(config): #pylint: disable=unused-argument
    """
    Check the status of the pods for a given product.  Assume COS for now.

    Note in S-8000_RevA (step 7 on page 83) they check the image recipes as
    well.  That is not implemented here.  I don't think it would have purpose
    in this context.
    """

    connection = config.connection

    get_jobs_cmd = "kubectl --kubeconfig=/etc/kubernetes/admin.conf get jobs -A"
    get_pods_cmd = "kubectl --kubeconfig=/etc/kubernetes/admin.conf get pods -A"

    # When we track the jobs running we will only classify them "Running" or "Completed"
    jobs_running = {"Completed":[], "Running":[]}
    # The pods could be in a number of various states, but these are the states we know/care about most
    # If the pod is in another state, it will be added to the dict later
    pods_running = {"Completed":[], "Running":[], "Error":[]}

    total_seconds = 0
    timeout_seconds = 1200
    sleep_seconds = 10
    alert_freq = 6
    alert_count = 0

    start_time = datetime.datetime.now()

    while True:

        finished = True

        # Initailly we're gatering a superset of the jobs/pods to monitor
        jobs_list = connection.sudo(get_jobs_cmd).stdout.splitlines()
        jobs_list = [job for job in jobs_list if 'import' in job]

        pods_list = connection.sudo(get_pods_cmd).stdout.splitlines()
        pods_list = [pod for pod in pods_list if 'import' in pod]

        if config.dryrun:
            return

        # Reset these dicts every loop, since we only care about the current state
        jobs_running = {"Completed":[], "Running":[]}
        pods_running = {"Completed":[], "Running":[], "Error":[]}

        # Here we loop through the superset of jobs, and narrow down the list of jobs
        # by checking the job name field specifically. The jobs are either considered
        # "Completed or "Running" based on the "Completions" column.
        # e.g. 0/2 or 1/2 are considered "Running" and 2/2 is considered "Completed"
        for job in jobs_list:
            fields = job.split()
            job_name = fields[1]
            if 'config-import' in job_name or 'recipe-import' in job_name:
                completions = fields[2]
                if not is_ready(completions):
                    jobs_running["Running"] += [job_name]
                    finished = False
                else:
                    jobs_running["Completed"] += [job_name]

        # Pods can be in any number of states, but we will consider any pod that is "Completed"
        # or "Error" to be finsished running. Pods in other states will be waited for, until
        # the timeout expires
        for pod in pods_list:
            fields = pod.split()
            pod_name = fields[1]
            if 'config-import' in pod_name or 'recipe-import' in pod_name:
                pod_state = fields[3]
                if pod_state not in pods_running.keys():
                    pods_running[pod_state] = []
                if pod_state != "Completed" and pod_state != "Error":
                    finished = False
                pods_running[pod_state] += [pod_name]

        total_seconds = int((datetime.datetime.now() - start_time).total_seconds())

        # Exit the loop if any of the following are true:
        # - All the jobs are "Completed" and all of the pods are in "Completed" or "Error" states
        # - There were no jobs/pods found to begin with
        # - the timeout has expired.
        if finished or total_seconds > timeout_seconds:
            break

        # Send info to the user every alert_freq number of loops
        # Only show them jobs and pods that haven't finished yet.
        if alert_count % alert_freq == 0:
            # Count all the import jobs and figure out how many are running
            total_jobs_count = len(jobs_running["Running"]) + len(jobs_running["Completed"])
            jobs_running_count = len(jobs_running["Running"])
            msg = "    Waiting for {} of {} jobs".format(jobs_running_count, total_jobs_count)
            install_logger.info(msg)
            for job_name in jobs_running["Running"]:
                msg = "        {} {:.>32}".format(job_name, "Running")
                install_logger.info(msg)

            # Count all the import pods and figure out how many are running
            total_pods_count = sum([len(v) for v in pods_running.values()])
            pods_running_count = total_pods_count - len(pods_running["Completed"]) - len(pods_running["Error"])
            msg = "    Waiting for {} of {} pods".format(pods_running_count, total_pods_count)
            install_logger.info(msg)
            for pod_state, pod_names in pods_running.items():
                if pod_state == "Completed":
                    continue
                for pod_name in pod_names:
                    msg = "        {} {:.>32}".format(pod_name, pod_state)
                    install_logger.info(msg)

            msg = "    Time elaspsed: {} second(s)".format(total_seconds)
            install_logger.info(msg)

        alert_count += 1
        time.sleep(sleep_seconds)

    msg = "Total time spent waiting for import jobs and pods was {} second(s)".format(total_seconds)
    install_logger.info(msg)

    # finished should only be True if all the jobs/pods are in "Completed" or "Error" states,
    # or if there were no jobs/pods found at all
    if finished:
        failed = False
        total_jobs_count = len(jobs_running["Completed"])
        total_pods_count = len(pods_running["Completed"]) + len(pods_running["Error"])
        msg = "Finished waiting for {} job(s) and {} pod(s).".format(total_jobs_count, total_pods_count)
        install_logger.info(msg)
        for job_state, job_names in jobs_running.items():
            for job_name in job_names:
                msg = "        {}{:.>32}".format(job_name, job_state)
                install_logger.info(msg)
                if job_state != "Completed":
                    # This shouldn't happen unless we add more states to the job_running dict
                    failed = True
        for pod_state, pod_names in pods_running.items():
            for pod_name in pod_names:
                msg = "        {}{:.>32}".format(pod_name, pod_state)
                install_logger.info(msg)
                if pod_state != "Completed":
                    # Some pod has reported an "Error"
                    failed = True
        if failed:
            msg = "There are jobs/pods that failed to complete"
            raise COSProblem(msg)

    elif total_seconds > timeout_seconds:
        msg = "Timed out waiting for import jobs and pods to finish"
        raise TimeOut(msg)
    else:
        msg = "Something went wrong while waiting for the import jobs and pods to finish."
        raise UnexpectedState(msg)

def verify_product_install(config): #pylint: disable=unused-argument
    """ Check if there is a "validate.sh" script in the product
    distrubtion. If there is a script, run it and return the results.
    The validation is considered a success, if:
       - There is a script to run and it succeeds.
       - There is no script to run.
       - The script has already been run and succeeded.
    The validation does not succeeded, if:
       - A validate.sh script runs and fails, hits timeout, etc.
       -
    """
    # Keep track of the validations that explicitly failed.
    validate_failed = []

    install_logger.info("    Starting product validations:")
    goss_exe = shutil.which("goss")

    # Tests we could be running.  We're only going to run the first one we find
    # so do it in preferred order
    tests = ["post-install-validation/validate", "validate.sh"]

    # Loop through all the products in the location_dict and display their validation status.
    # If the product has successfully run an install, check to see if it needs to be validated.
    # Set all the products "validated" field to True or False, depending on the state.
    for product in config.location_dict:
        validated = False
        goss_wanted = True
        if not product.installed:
            # These products haven't succeeded the installation stage.
            install_logger.warning("    {} {:.>32}".format(product.name, "not installed"))
        elif product.installed and not product.validated:
            # Check if these products need to run a validation, then try to run it.
            tests_found = False
            for script in tests:
                if os.path.exists(os.path.join(product.work_dir, script)):
                    tests_found = True
                    if "post-install-validation" in script:
                        # the post-install-validation scripts run the goss tests already
                        goss_wanted = False
                    install_logger.debug(f"Running {script} for {product.name}")
                    try:
                        config.connection.sudo(f"./{script}", cwd=os.path.join(product.work_dir), timeout=600)
                        validated = True
                        install_logger.info("        {} {:.>32}".format(product.name, "succeeded"))
                    except RunTimeoutError as te:
                        install_logger.error("        {} {:.>32}".format(product.name, "timeout"))
                        validate_failed.append(product.name)
                    except:
                        install_logger.error("        {} {:.>32}".format(product.name, "failed"))
                        validate_failed.append(product.name)
                    install_logger.debug(f"Finshed running {script} for {product.name}")
                    break

            if not tests_found:
                # These products have no validate.sh script to run.
                install_logger.info("        {} {:.>32}".format(product.name, "nothing to run"))
                validated = True

        elif product.installed and product.validated:
            # These products already ran and passed the validation stage.
            install_logger.info("        {} {:.>32}".format(product.name, "done"))
            validated = True
        else:
            # These products are in an unknown state, they should be ignored.
            install_logger.warning("        {} {:.>32}".format(product.name, "ignored"))

        product.validated = validated

        # if prodname_short isn't defined, there's nothing to be done
        if product.product and goss_wanted:
            # Run the goss tests if they exist and weren't already run by the product script
            test_dir = os.path.join("/opt/cray/tests/install", product.product)
            if os.path.exists(test_dir):
                if goss_exe:
                    goss_yamls = config.connection.sudo("find {} -name \*goss\*.yaml".format(test_dir)).stdout.splitlines()
                    if len(goss_yamls) > 0:
                        install_logger.info("        Running goss tests ...")
                    for gy in goss_yamls:
                        config.connection.sudo("{} -g {} validate".format(goss_exe, gy))
                else:
                    msg = """
                        The goss executable can't be found, so goss testing will be
                        not be ran for {}""".format(product.name)
                    install_logger.warning(utils.formatted(msg))

    # Raise an exception if any of the validations explicitly failed.
    failed = len(validate_failed)
    if failed:
        msg = f"{failed} validation(s) failed."
        install_logger.error(msg)
        raise COSProblem(msg)

def check_services(config): #pylint: disable=unused-argument
    """Check the cps and nmd services.  Also check dvs and lnet"""
    services = config.connection.sudo('kubectl  --kubeconfig=/etc/kubernetes/admin.conf get pods -A', dryrun=False).stdout.splitlines()
    services = [s for s in services if 'nmd' in s or 'cray-cps' in s]
    fatal = False

    for service in services:
        service_list = service.split()
        name, status = service_list[1], service_list[3]
        if not status.lower() in ["running", "completed"]:
            install_logger.error("Service {} is not ready, status is:\n\t{}".format(name, service))
            fatal = True

    w_ncn_tuples = utils.get_hosts(config, 'ncn-w', exact_match=False)
    w_ncns = [w[1] for w in w_ncn_tuples]

    for node in w_ncns:
        if config.dryrun:
            all_lines = ["dvs","lnet"]
        else:
            all_lines = config.connection.sudo("ssh {} lsmod".format(node)).stdout.splitlines()

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
            install_logger.error("DVS not found in kernel modules on node {}!".format(node))
            fatal = True

        if not found_lnet:
            install_logger.error("lnet not found in kernel modules on node {}!".format(node))
            fatal = True

    if fatal:
        raise UnexpectedState("One or more required services are unavailable.")
    else:
        install_logger.info("DVS, lnet, cps, and nmd services are available on all worker nodes.")


def get_mergeable_repos(config):

    repos = {}

    # only products with an import_branch are mergable
    for product in config.location_dict:
        if product.import_branch:
            repos[product.product] = os.path.basename(product.clone_url.replace('.git', ''))

    install_logger.debug('found mergeable_repos {}'.format(repos))

    return repos


def update_working_branches(config):
    """Merge the product git branch to the working config"""

    # first things first, get a copy of all the config repos
    install_logger.debug("Cloning all of the configuration repositories...")

    # get dict of mergeable repos
    repos = get_mergeable_repos(config)
    git = Git(config)

    for product in config.location_dict:
        if product.import_branch:
            install_logger.debug("processing product {} import_branch {}".format(
                product.product, product.import_branch))

            repo = repos[product.product]
            cos_checkout_dir = git.clone(repo)

            # Get the working branch with product key and --working-branch
            # If the passed working branch doesn't exist,it is created. 

            # check out a local copy of the import_branch (release version)
            git.checkout(repo, product.import_branch)

            working_branch = render_jinja(config, product.name, config.args["working_branch"])
            best_guess = best_guess_working(config, product.name, git, repo)
            if best_guess != working_branch:
                # The working branch does not exist.
                # Checkout best_guess (which will be the closest lower version
                # working_working branch.  working_branch will then be created
                # from it if working_branch does not exist.
                git.checkout(repo, best_guess)
            git.checkout(repo, working_branch, create=True)

            # fourth, merge the import branch to the integration branch
            install_logger.info("  Merging branch {} into {}".format(product.import_branch, working_branch))
            git.merge(repo, product.import_branch)
            git.push(repo)

            # then clean up
            git.cleanup(repo)

            install_logger.info("    OK")

def get_cos_recipe_name(config):
    # if cos_recipe_name was supplied on the command line, just return it
    if "cos_recipe_name" in config.args:
        if config.args["cos_recipe_name"]:
            install_logger.debug("recipe name found in args {}".format(repr(config.args['cos_recipe_name'])))
            return config.args['cos_recipe_name']

    cos_version = config.location_dict.product("cos").version
    pname = "cos-" + cos_version
    install_logger.debug("using product {}".format(pname))

    # if not, lets see if we can find it
    # load previously discovered produts
    product = config.location_dict.get(pname, False)
    if not product:
        # no cos at all in the location_dict, give up
        install_logger.debug("{} not found in the location_dict.".format(pname))
        return None

    if not product.recipe:
        # No recipe was found by get_product_catalog; raise an exception.
        raise COSProblem("A recipe name is needed to build the COS compute image.")


    # return what we've got
    install_logger.debug("recipe found in product catalog {}".format(product.recipe))
    return product.recipe


def update_cfs_commits(config, cfs_template_arg):
    """Update the the commits in a CFS config."""
    cfs_template = copy.deepcopy(cfs_template_arg)

    del cfs_template["name"]
    # don't die if not found
    try:
        del cfs_template["lastUpdated"]
    except Exception:
        pass

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
    repos = get_mergeable_repos(config)
    for product, repo in repos.items():
        install_logger.debug("processing {} {}".format(product, repo))
        prod_version = config.location_dict.product(product).short_version
        install_logger.debug("prod_ver {}".format(prod_version))
        commit, branch = current_repo_branch(config, repo, prod_version)
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


def update_ncn_config(config):
    """Update the config used for NCN Personalization."""

    repos = get_mergeable_repos(config)
    if 'cos' in repos:
        prod_version = config.location_dict.product("cos").short_version
    else:
        prod = repos.keys()[0]
        prod_version = config.location_dict.product(prod).short_version

    base_file = "cfs-config.{}-{}.json".format(
        prod_version,
        config.timestamp)

    # ncn_personalization should always be found in args since it has a
    # default value.
    template_name = config.args.get("ncn_personalization")
    cfs_template = None
    configs = json.loads(config.connection.sudo("cray cfs configurations list --format json").stdout)
    for i, _ in enumerate(configs):
        if configs[i]["name"] == template_name:
            cfs_template = configs[i]
            break

    if not cfs_template:
        err_msg = utils.formatted("""
        Unable to find a cfs configuration named {}.  Was one
        specified via the commandline?""".format(template_name))
        raise NCNPersonalization(err_msg)

    cfs_template = update_cfs_commits(config, cfs_template)

    file_location = os.path.join(config.args["state_dir"], base_file)
    with open(file_location, 'w', encoding='UTF-8') as fhandle:
        json.dump(cfs_template, fhandle)

    outdict = {"template_name": template_name, "file_location": file_location}
    with open(os.path.join(config.args["state_dir"], NCNP_VARS), "w") as fhandle:
        yaml.dump(outdict, fhandle)

    return template_name, file_location


def ncn_personalization(config): #pylint: disable=unused-argument
    """Do the NCN personalization as described in HPE Cray EX System
    Installation and Configuration Guide (1.4.2_S-8000 RevA)"""

    # Get a list of all worker and manaagement ncns. We need to skip the
    # ncn-s00* nodes for now.  So use the m_ncn_tuples + w_ncn_tuples and
    # consider that to be all the ncns.
    all_ncn_tuples = utils.get_ncn_tuples(config)

    ncn_list_xnames = [n[0] for n in all_ncn_tuples]

    ncn_p_file = os.path.join(config.args["state_dir"],NCNP_VARS)
    with open(ncn_p_file, "r") as fhandle:
        ncnp_dict = yaml.load(fhandle, yaml.SafeLoader)

    template_name = ncnp_dict["template_name"]
    pzation_file = ncnp_dict["file_location"]

    # Disable cfs.
    for ncn in ncn_list_xnames:
        config.connection.sudo("cray cfs components update {} --enabled false".format(ncn))

    # Upload the file to CFS.
    config.connection.sudo("cray cfs configurations update {} --file {} --format json".format(template_name, pzation_file))

    # Update the CFS component for all ncns.  Skip the ncn-s* nodes for now;
    # so this is basically the worker and management nodes.
    for ncn in ncn_list_xnames:
        config.connection.sudo("cray cfs components update --desired-config {} --enabled true --format json --error-count 0 --state [] {}".format(template_name, ncn))

    bad_nodes = utils.wait_for_ncn_personalization(config, ncn_list_xnames, timeout=3600)

    if bad_nodes:
        nodes_str = ", ".join(bad_nodes)
        err_msg = utils.formatted("""
            The following nodes failed NCN Personalization:
            {}""".format(nodes_str))
        raise(NCNPersonalization(err_msg))

def best_guess_working(config, product_key, gitobj, repo):
    """Best guess at the working branch.  If the specified branch exists,
    return it.  Otherwise, find the one closest in version but lower."""

    integration_branch = None
    if not config.args.get("working_branch", None):
        raise InstallError("You must supply --working-branch")

    integration_branch = render_jinja(config, product_key, config.args["working_branch"])
    branches = gitobj.ls_remote(repo, just_branches=True)
    branch_versions = {}
    if integration_branch in branches:
        return integration_branch

    product = config.location_dict.get(product_key)
    for branch in branches:
        if product.product in branch and '/' in branch:
            parts = branch.split('/')
            version = parts[-1]
            branch_versions[version] = branch

    prod_version = product.best_version

    branch_versions[prod_version] = integration_branch

    sorted_versions = sorted(list(branch_versions.keys()), key=LooseVersion)

    int_index = sorted_versions.index(prod_version)
    if int_index == 0:
        best_match = 1
    else:
        best_match = int_index - 1
    best_version = sorted_versions[best_match]

    best_integration_branch = branch_versions[best_version]

    install_logger.debug("using integration branch {}".format(integration_branch))

    return best_integration_branch


def current_repo_branch(config, repo, version):
    """
    Find the integration branch corresponding to the passed repo

    Note version is not used at the moment as get_prod_keys() simply
    sorts the key results.  Optimally this section would be re-worked
    so callers provided a specific product_key.
    """

    install_logger.debug("in current_repo_branch with {}".format(repo))

    # this isn't optimal, but derive the product_name from the repo name
    product_name = repo.replace("-config-management", "")
    install_logger.debug("product_name is {}".format(product_name))

    # we don't have a product key here, so look one up based on whats in location_dict
    git = Git(config)
    integration_branch = best_guess_working(config, config.location_dict.product(product_name).key, git, repo)
    install_logger.debug("determined branch is {}".format(integration_branch))

    branches = git.ls_remote(repo)
    found_branch = None

    for branch in branches:
        commit, munged_branch_name = branch.split()
        if integration_branch == munged_branch_name:
            found_branch = branch
            install_logger.debug("found branch match {}".format(found_branch))
            break

    if not found_branch:
        raise UnexpectedState("Can't find working branch '{}' in '{}'".format(
            integration_branch,
            repo))

    commit, ref = found_branch.split()

    if found_branch:
        return commit.strip(), ref.strip()

    return None, None


def wait_for_ims_pod(config, job_id):
    """Wait for an IMS pod after creating an image"""

    out = config.connection.sudo("kubectl --kubeconfig=/etc/kubernetes/admin.conf -n ims describe job {}".format(job_id)).stdout.splitlines()
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
        pod.wait_for_pod(config, pod_name)
    else:
        install_logger.warning("Unable to get pod for job id {}".format(job_id))
        return None, None, None

    # Get the image id and etag
    job_hex = re.sub(r"cray-ims-([0-9abcdef\-]+)-create.*", r"\1", job_id)
    job_info = json.loads(config.connection.sudo("cray ims jobs describe {} --format json".format(job_hex)).stdout)
    resultant_image_id = job_info['resultant_image_id']

    # 'cray ims images describe <resultant_image_id>' would work as well.
    artifacts = json.loads(config.connection.sudo("cray artifacts describe boot-images {}/manifest.json --format json".format(resultant_image_id)).stdout)
    etag = artifacts['artifact']['ETag']

    return pod_name, resultant_image_id, etag

def bos_sessiontemplate_name(config):
    """Get the bos sessiontemplate name."""

    sessiontemplate_name = "cos-sessiontemplate-{}-{}".format(config.location_dict.product("cos").short_version, config.timestamp)

    return  sessiontemplate_name


def create_bootprep_config(config):
    """Generate the bootprep config."""
    # Generate the  CFS config.

    bp_conf_arg = config.args.get("bootprep_config", None)
    if bp_conf_arg:
        install_logger.info("Using the following: `sat bootprep` config: {}".format(bp_conf_arg))
        return

    install_logger.info("Generating the `sat bootprep` config")

    # Generate the configuration section.
    bootprep_dict = {}
    cfs_dict = create_cos_cfs_config(config)
    bootprep_dict["configurations"] = []
    cfs_arg = config.args.get("cfs_config")
    cfs_name = "{}-{}".format(cfs_arg, config.timestamp)

    bp_layers = [
        {
            "name": cfsd["name"],
            "playbook": cfsd["playbook"],
            "git":
                {
                    "url": cfsd["cloneUrl"],
                    "commit": cfsd["commit"]

                }
        } for cfsd in cfs_dict["layers"]
    ]
    cfs_elt = {
        "name": cfs_name,
        "layers": bp_layers
    }
    bootprep_dict["configurations"].append(cfs_elt)

    # Generate the images section.
    cos_recipe_name = get_cos_recipe_name(config)
    cos_image_name = "cos-installer-image-{}".format(config.timestamp)
    images = [
        {
            "name": cos_image_name,
            "description": """COS recipe for compute nodes""",
            "ims": {
                "name": cos_recipe_name,
                "is_recipe": True,
            },
            "configuration": cfs_name,
            "configuration_group_names": [
                "Compute",
            ],
        },
    ]

    bootprep_dict["images"] = images

    # Generate the BOS session templates section.
    source_template_name = config.args["source_bos_sessiontemplate"]
    working_template = json.loads(config.connection.sudo("cray bos sessiontemplate describe {} --format json".format(source_template_name)).stdout)
    sessiontemplate_name = "{}-{}".format(source_template_name, config.timestamp)
    session_templates = [
        {
            "name": sessiontemplate_name,
            "image": cos_image_name,
            "configuration": cfs_name,
            "bos_parameters": {
                "boot_sets": {
                    "compute": {
                        "kernel_parameters": working_template["boot_sets"]["compute"]["kernel_parameters"],
                        "node_roles_groups": ["Compute"],
                        "rootfs_provider_passthrough": working_template["boot_sets"]["compute"]["rootfs_provider_passthrough"],
                    }
                }
            }
        }
    ]

    bootprep_dict["session_templates"] = session_templates

    with open(os.path.join(config.args["state_dir"], SAT_BOOTPREP_CFG), "w", encoding="UTF-8") as fhandle:
        yaml.dump(bootprep_dict, fhandle)


def sat_bootprep(config):
    """Run `sat bootprep`.  This builds images and customized images, and generates a bos sessiontemplate."""

    bp_conf_arg = config.args.get("bootprep_config", None)
    if bp_conf_arg:
        bootprep_if = bp_conf_arg
    else:
        bootprep_if = os.path.join(config.args["state_dir"], SAT_BOOTPREP_CFG)

    ims_public_key = utils.get_ims_public_key(config)
    timeout = 60 * 60 # 1 hour

    # Run `sat bootprep`
    logname = get_log_filename(config, "sat_bootprep")
    install_logger.info("Running `sat bootprep`.  This can take around 30 minutes, depending on the number of layers, images, and bos sessiontemplates.")
    install_logger.info('  Logging to {}'.format(logname))
    config.connection.sudo("sat bootprep run --overwrite-configs --overwrite-images --overwrite-templates --public-key-id {} {}".format(ims_public_key, os.path.relpath(bootprep_if)), timeout=timeout, tee=True, store_output=logname)

    # Read in the configuration used for `sat bootprep` and give a summary.
    with open(bootprep_if, "r", encoding='UTF-8') as fhandle:
        bootprep_dict = yaml.load(fhandle, yaml.SafeLoader)

    cfs_cfg_names = ",".join([cfg["name"] for cfg in bootprep_dict["configurations"]])
    image_names = ",".join([img["name"] for img in bootprep_dict["images"]])
    st_names = ",".join([st["name"] for st in bootprep_dict["session_templates"]])

    install_logger.info("`sat bootprep` summary:")
    install_logger.info("  cfs configs: {}".format(cfs_cfg_names))
    install_logger.info("  images: {}".format(image_names))
    install_logger.info("  BOS sessiontemplates: {}".format(st_names))


def create_dvs_reload_config(config):
    """
    build a ncn reload config, if possible
    """

    # get the cos clone url and transform it to a url cfs is able to use
    ext_name = os.path.basename(config.location_dict.product("cos").clone_url)
    cos_clone_url = "/".join(["https://api-gw-service-nmn.local/vcs/cray", ext_name])

    # get the commit and branch
    cos_commit, cos_branch = current_repo_branch(config, "cos-config-management", None)

    # get a list of the files in the cos config
    git = Git(config)
    repo = "cos-config-management"
    cos = git.clone(repo)
    git.checkout(repo, cos_branch)
    git.pull(repo, quiet=True)
    repofiles = os.listdir(cos)
    git.cleanup(repo)

    # ncn-upgrade.yml must be in the cos config for this method
    if "ncn-upgrade.yml" in repofiles:

        install_logger.debug("found ncn-upgrade.yml in cos config")

        cos_layer = {'cloneUrl': cos_clone_url,
            'commit': cos_commit,
            'name': cos_branch,
            'playbook': 'ncn-upgrade.yml'}

        install_logger.debug("built cos layer of {}".format(cos_layer))

        # get the SHS layer from the ncn_personalization config
        template_name = config.args.get("ncn_personalization")
        cfs_template = None
        cfs_template = json.loads(config.connection.sudo("cray cfs configurations describe {} --format json".format(
            template_name)).stdout)

        shs_layer = None
        for i in cfs_template["layers"]:
            if 'slingshot-host-software-config-management' in i["cloneUrl"]:
                shs_layer = i

        if shs_layer:

            install_logger.debug("found shs layer in {}".format(config.args.get("ncn_personalization")))
            install_logger.debug("found shs layer of {}".format(shs_layer))

            template_name = "ncn_dvs_reload"
            base_file = "dvs_reload.json"

            # build a current config
            cfs_template = update_cfs_commits(config,
                { "layers": [ shs_layer, cos_layer], "name": template_name })

            file_location = os.path.join(config.args["state_dir"], base_file)
            with open(file_location, 'w', encoding='UTF-8') as fhandle:
                json.dump(cfs_template, fhandle)

            config.connection.sudo("cray cfs configurations update {} --file {} --format json".format(
                template_name, file_location))

            install_logger.info("  DVS reload config saved in {}".format(file_location))
            install_logger.info("  Using DVS reload config {}".format(template_name))

            return template_name, file_location

        else:
            raise NCNPersonalization("there is no SHS layer")
    else:
        raise NCNPersonalization("there is no ncn-upgrade.yml in this branch")


def create_cos_cfs_config(config):
    """
    Write a CFS config based on args, and update the commits to the most recent.
    """
    # Update the configuration.
    local_config_path = os.path.join(config.args["state_dir"], CFS_CONFIG_FILENAME)
    cfs_config = config.args.get("cfs_config")

    # Retrieve And Modify An Existing Configuration For COS.
    errout = config.connection.sudo("cray cfs configurations describe {} --format json".format(cfs_config))
    curr_config = json.loads(errout.stdout)
    curr_config = update_cfs_commits(config, curr_config)

    with open(local_config_path, 'w', encoding='UTF-8') as fhandle:
        json.dump(curr_config, fhandle)

    return curr_config


def check_analytics_mount(config, node):
    """Check the analytics mount.  It occassionally takes a few tries."""

    if not hasattr(check_analytics_mount, "cloned_dir"):
        install_logger.debug("Cloning the analytics dir for node {}".format(node))
        utils.get_product_catalog(config)
        try:
           analytics_data = yaml.safe_load(config.all_product_data["analytics"])
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
        git = Git(config)
        repo = "analytics-config-management"
        check_analytics_mount.cloned_dir = git.clone(repo)
        git.checkout(repo, analytics_branch)
        git.pull(repo, quiet=True)

    keep_waiting = True
    timeout = 60
    sleep_time = 10
    waited = 0

    # Unmount Analytics contents on the worker.
    config.connection.sudo("scp roles/analyticsdeploy/files/forcecleanup.sh {}:/tmp".format(node), cwd=check_analytics_mount.cloned_dir)

    while keep_waiting:
        # Sometimes it takes multiple tries for forceleanup, so only warn if
        # it fails.
        config.connection.sudo("ssh {} /bin/bash /tmp/forcecleanup.sh".format(node))
        mounts = config.connection.sudo("ssh {} mount -t dvs".format(node)).stdout.splitlines()
        an_mounts = [m for m in mounts if 'analytics' in m.lower()]
        if an_mounts:
            time.sleep(sleep_time)
            waited += sleep_time
        else:
            keep_waiting = False

        if waited >= timeout:
            install_logger.warning("    Could not unmount the Analytics mounts on {}".format(node))
            keep_waiting = False


def has_lustre_fs(config, node):
    mounts = config.connection.sudo("ssh {} mount -t lustre".format(node)).stdout
    return "lustre" in mounts


def worker_health_check(config):
    """
    Ensure the worker nodes are in a good state prior to starting
    NCN Personalization
    """

    cfs_config_ok = validate_cfs_config(config)

    worker_tuples = utils.get_ncn_tuples(config)
    worker_nodes = [wt[1] for wt in worker_tuples]

    # CPS deployment check
    install_logger.info("  Checking CPS Deployments")
    cps_deployment_data = json.loads(config.connection.sudo("cray cps deployment list --format json").stdout)

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
        component = json.loads(config.connection.sudo(cmd).stdout)
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

    if len(good_cps_nodes) <2:
        install_logger.error("Low number of CPS deployments")
        raise NCNPersonalization("Low number of CPS deployments")

    if bad_status_nodes:
        install_logger.error("Fix CFS component configuration errors before proceeding")
        raise NCNPersonalization("NCN nodes not starting in configured state")

    if not cfs_config_ok:
        install_logger.error("Fix CFS component configuration errors before proceeding")
        raise NCNPersonalization("Errors found in the CFS configuration")

def unload_dvs_and_lnet(config):
    """
    reload dvs using method controlled by --dvs-update-method
    auto mode will try the new mechanism and fall back to the legacy mode
    ncn-upgrade mode will try the new mechanism and fail if not possible
    legacy will use the legacy mode
    """
    dvs_update_method = config.args.get("dvs_update_method")
    if dvs_update_method != "legacy":
        try:
            cfs_reload_config, _ = create_dvs_reload_config(config)

            # delete old session, if exists
            try:
                config.connection.sudo("cray cfs sessions describe cne-install-ncn-reload --format json")
                install_logger.info("  Deleting old CFS session cne-install-ncn-reload")
                config.connection.sudo("cray cfs sessions delete cne-install-ncn-reload")
            except Exception as err:
                pass

            # determine any limits to use
            if config.args.get("worker_nodes", None):
                # get_ncn_tuples will process --worker-nodes, if specified
                worker_tuples = utils.get_ncn_tuples(config, just_workers=True)
                # This stage is only ran on the worker nodes.  Make sure no other node
                # type was specified.
                bad_nodes = [wt[1] for wt in worker_tuples if "ncn-w" not in wt[1]]
                if bad_nodes:
                    raise NCNPersonalization(utils.formatted("""
                        DVS can only be reloaded on worker nodes.  The following nodes are not
                        worker nodes:
                        {}""".format(",".join(bad_nodes))))
                # new style reload needs xnames
                xnames = [wt[0] for wt in worker_tuples]
                limit = ",".join(xnames)
            else:
                limit = "Management_Worker"

            install_logger.info("  Running cfs session cne-install-ncn-reload session using config {}".format(cfs_reload_config))
            k8s_job_line = None
            output = config.connection.sudo("cray cfs sessions create --name cne-install-ncn-reload \
                --configuration-name {} --ansible-limit {}".format(cfs_reload_config, limit),
                timeout=120).stdout.splitlines()

            output = config.connection.sudo("cray cfs sessions describe cne-install-ncn-reload").stdout.splitlines()

            k8s_job_line = [line for line in output if line.lower().startswith('job')][0]

            if k8s_job_line:
                k8s_job = k8s_job_line.split()[2].strip('"')
                install_logger.debug("k8sjob={}  wait for the pod...".format(k8s_job))
                pod.wait_for_pod(config, k8s_job)
            else:
                install_logger.error("Unable to get the K8S job name.")

        except NCNPersonalization as err:
            if dvs_update_method == "auto":
                install_logger.debug("falling back to legacy mode")
                legacy_dvs_reload(config)
            else:
                # they specifically wanted ncn-upgrade mode, so fail
                install_logger.error("Please ensure that your specified COS working branch contains COS 2.3 or later")
                install_logger.error("AND your specified ncn-personalization config contains a slingshot-host-software layer'")
                raise NCNPersonalization("Cannot use the specified dvs_update_method of 'ncn-upgrade'")

    else:
        # legacy mode specified, just call it
        install_logger.debug("dvs_update_method of legacy set")
        legacy_dvs_reload(config)


def legacy_dvs_reload(config):
    """
    legacy (<= COS 2.2) version of the DVS reload proceedure
    this mode performs a DVS reload based on
    """

    worker_tuples = utils.get_ncn_tuples(config, just_workers=True)

    # This stage is only ran on the worker nodes.  Make sure no other node
    # type was specified.
    bad_nodes = [wt[1] for wt in worker_tuples if "ncn-w" not in wt[1]]
    if bad_nodes:
        raise NCNPersonalization(utils.formatted("""
            DVS can only be reloaded on worker nodes.  The following nodes are not
            worker nodes:
            {}""".format(",".join(bad_nodes))))

    config.connection.sudo("scp ncn-w001:/opt/cray/dvs/default/sbin/dvs_reload_ncn /tmp")

    install_logger.debug("get all pods ...")
    all_pods = config.connection.sudo("kubectl --kubeconfig=/etc/kubernetes/admin.conf get pods -Ao wide").stdout.splitlines()

    statedir = config.args.get("state_dir")
    ncnp_cfg = config.args.get("ncn_personalization")

    for w_xname, w_node in worker_tuples:

        install_logger.info("  Unloading DVS and LNET on node: {}".format(w_node))

        # Unmount PE and Analytics on the worker.
        # Check if cps-cm-pm pod is running on the worker.  Delete it if so.
        cps_cm_pm_pods = [cps for cps in all_pods if 'cray-cps-cm-pm' in cps and w_node in cps]
        install_logger.debug("(unload_dvs_and_lnet)cps_cm_pods={}".format(cps_cm_pm_pods))
        if  cps_cm_pm_pods:
            install_logger.info("    Deleting CPS deployment")
            config.connection.sudo("cray cps deployment delete --nodes {}".format(w_node))
            pod_line = cps_cm_pm_pods[0]
            fields = pod_line.split()
            pod_name = fields[1]
            pod.wait_for_pod(config, pod_name, delete=True)

        # Check to see if any UAIs are running on the worker.  Migrate the UAIs and wait for each to finish.
        uais = [ p for p in all_pods if 'uai' in p and w_node in p]
        config.connection.sudo("kubectl --kubeconfig=/etc/kubernetes/admin.conf label node {} --overwrite uas=False".format(w_node))
        for uai in uais:
            fields = uai.split()
            uai_name = fields[1]
            install_logger.info("    Migrating UAI {} off the worker {}".format(uai_name, w_node))
            config.connection.sudo("kubectl --kubeconfig=/etc/kubernetes/admin.conf delete pod -n user {}".format(uai_name))
            install_logger.debug("Waiting for UAI {} to migrate".format(uai_name))
            pod.wait_for_pod(config, uai_name, delete=True)

        # enable cfs.  while the health check verifies a node needs to be configured
        # before you get here, if someone is trying to fix a configuration they may
        # well be re-running this stage and in that case they may have gotten to the
        # point where we disable cfs and that will mean cfs won't schedule anything
        # that uses cfs, ie the unload scripts
        install_logger.debug("enabling cfs on {}".format(w_node))
        config.connection.sudo("cray cfs components update {} --enabled true --error-count 0".format(w_xname))

        # check for conflicting (outstanding) cfs session
        try:
            conflicting_session = config.connection.sudo("cray cfs sessions describe configure-fs-unload-yml --format json")
            raise UnexpectedState("Found cfs session 'configure-fs-unload-yml' already exists")
        except Exception as err:
            pass

        install_logger.info("    Running fs_unload")
        k8s_job_line = None
        output = config.connection.sudo("/tmp/dvs_reload_ncn -c {} -p configure_fs_unload.yml {}".format(ncnp_cfg, w_xname),
            timeout=120).stdout.splitlines()

        # I don't think the k8s jobline not being found should be fatal.
        try:
            k8s_job_line = [line for line in output if line.lower().startswith('services')][0]
        except Exception as err:
            k8s_job_line = None

        if k8s_job_line:
            k8s_job = k8s_job_line.split()[1].strip()
            install_logger.debug("k8sjob={}  wait for the pod...".format(k8s_job))
            pod.wait_for_pod(config, k8s_job)
        else:
            install_logger.debug("WARNING: Unable to get the K8S job name.")

        # Sleep for a minute before unmounting PE.
        install_logger.debug("Let the system settle prior to unmounting Analytics")
        time.sleep(60)

        # check_analytics will run forcecleanup.sh until dvs unmounts cleanly
        install_logger.info("    Unmounting Analytics")
        check_analytics_mount(config, w_node)

        # Unmount PE
        install_logger.info("    Unmounting PE")
        config.connection.sudo("scp tools/unmount_pe.sh {}:/tmp/unmount_pe.sh".format(w_node))
        config.connection.sudo("ssh {} /tmp/unmount_pe.sh".format(w_node))

        # Make sure the reference count for dvs is 0.
        lsmods = config.connection.sudo("ssh {} lsmod".format(w_node)).stdout.splitlines()
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
        output = config.connection.sudo("/tmp/dvs_reload_ncn -D -c {} -p cray_dvs_unload.yml {}".format(ncnp_cfg, w_xname)
                                     ).stdout.splitlines()

        # I don't think the k8s jobline not being found should be fatal.
        try:
            k8s_job_line = [line for line in output if line.lower().startswith('services')][0]
        except Exception as err:
            k8s_job_line = None

        if k8s_job_line:
            k8s_job = k8s_job_line.split()[1].strip()
            install_logger.debug("k8sjob={}.  wait for the pod...".format(k8s_job))
            pod.wait_for_pod(config, k8s_job)
        else:
            install_logger.warning("    (unload_dvs_and_lnet): Unable to get the K8S job name.")

        # make sure dvs and lnet unload succeeded
        lsmods = config.connection.sudo("ssh {} lsmod".format(w_node)).stdout.splitlines()
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
            config.connection.sudo("ssh {} mv /etc/lnet.conf /etc/lnet.conf.previous")
        except Exception as err:
            pass

        # Note the 'for' loop below is only for record-keeping.  The dvs,
        # lustre, and craytrace rpms need to be uninstalled in a specific
        # order because of dependencies.
        rpms = config.connection.sudo("ssh {} rpm -qa".format(w_node)).stdout.splitlines()
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
                config.connection.sudo("ssh {} rpm -e {}".format(w_node, rpm))

        # Enable and run NCN personalization on the worker.
        install_logger.info("    Running NCN personalization")
        config.connection.sudo("cray cfs components update --enabled true --state '[]' \
            --error-count 0 {} --format json".format(w_xname))

        # wait for ncn-personalization to finish.
        bad_nodes = utils.wait_for_ncn_personalization(config, [w_xname])
        if bad_nodes:
            raise NCNPersonalization("NCN Personalization failed for {}".format(w_xname))

        rpms = config.connection.sudo("ssh {} rpm -qa".format(w_node)).stdout.splitlines()
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
        config.connection.sudo("cray cps deployment update --nodes {}".format(w_node))

        # Add the UAIs back to the worker.  Note this is done earlier in the guide
        # (at https://stash.us.cray.com/projects/SHASTA-OS/repos/cos-docs/browse/portal/developer-portal/install/Upgrade_and_Configure_COS.md)
        # But since it's not done for each particular worker, it needs to be done out of order.
        install_logger.info("    Adding UAS label")
        config.connection.sudo("kubectl --kubeconfig=/etc/kubernetes/admin.conf label node {} uas-".format(w_node))

        install_logger.info("    {} OK".format(w_node))

    install_logger.info("  OK")


def boot_cos(config):
    """Boot a COS image"""

    #TODO figure out what sessiontemplate to boot with
    sessiontemplate_name = config.args.get("source_bos_sessiontemplate")

    boot_start_time = datetime.datetime.now()
    # Now reboot the compute nodes
    output = json.loads(config.connection.sudo("cray bos session create --template-uuid {} --operation reboot --format json".format(sessiontemplate_name)).stdout)
    boot_session_job_id = output["job"]
    install_logger.info("  Boot session jobid {} created".format(boot_session_job_id))

    # Wait for the BOS session to "complete" or the BOS pod to be in a "Completed" state
    while True:
        session_desc = json.loads(config.connection.sudo("cray bos session describe {} --format json".format(boot_session_job_id[4:])).stdout)
        in_progress = session_desc["in_progress"]
        complete = session_desc["complete"]
        all_pods =  config.connection.sudo("kubectl --kubeconfig=/etc/kubernetes/admin.conf get pods -n services").stdout.splitlines()
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

def run_hello_world(config):
    """Run a "Hello World" test on a compute node."""
    sat_nodes = json.loads(config.connection.sudo("sat status --format json").stdout)
    node_lst = [ node for node in sat_nodes if "Compute" in node["Role"] and "Ready" in node["State"]]

    if len(node_lst) <= 0:
        raise UnexpectedState("There are no nodes in a 'Ready' state")

    head_compute_xname = node_lst[0]['xname']
    cmd = "ssh {} sinfo --Node --noheader | awk '{{print $1,$4}}'".format(head_compute_xname)
    sinfo_output = config.connection.sudo(cmd).stdout.split("\n")

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
        install_logger.warning("Not all 'Ready'({}) nodes are in 'idle'({}) state".format(
            len(node_lst), len(slurm_idle_node_lst)))

    install_logger.info("Running srun on {} nodes".format(len(slurm_idle_node_lst)))

    srun_node_list =  ",".join(slurm_idle_node_lst)
    cmd = "ssh {} srun -w {} /bin/hostname".format(head_compute_xname,srun_node_list)
    srun_output = [s for s in config.connection.sudo(cmd).stdout.split("\n") if len(s) > 0]

    slurm_idle_node_lst.sort()
    srun_output.sort()

    for out, rdy in zip(srun_output,slurm_idle_node_lst):
        if out != rdy:
            raise TestFailure("unexpected srun output: {} of {}".format(
                srun_output,slurm_idle_node_lst))

    install_logger.info("srun hello_world test succeded")


def cleanup(config): # pylint: disable=unused-argument
    """Clean things up after a run."""

    # remove files containing passwords
    statedir = config.args.get("state_dir")
    config.connection.sudo("rm -f {}/.vcspass {}/get_local_vcspw.sh".format(statedir, statedir))


def hello(config):
    print("hello")
    allout = config.connection.sudo("echo hello")
    install_logger.debug("sudo result: stdout={}, stderr={}".format(allout.stdout, allout.stderr))

def validate_cfs_config(config):
    # Ensure that the cfs configuration has a slingshot-host-software layer if required
    cfs_config_name = config.args.get("cfs_config", None)

    # if we don't have a cfs config just return.  should never happen if we're running a stage
    # that cares about it
    if cfs_config_name is None:
        return

    utils.get_product_catalog(config)
    install_logger.info("  Validating the CFS configuration")

    cfs_ok = True

    for prod in config.location_dict.product("slingshot-host-software"):
        if prod.import_branch:
            cfs_ok = False
            break

    if not cfs_ok:
        cfs_config = json.loads(config.connection.sudo("cray cfs configurations describe {} --format json".format(cfs_config_name)).stdout)
        layers = cfs_config.get("layers", dict())
        for layer in layers:
            url = layer.get("cloneUrl","")
            if "slingshot-host-software-config-management" in url:
                cfs_ok = True
                break

    if cfs_ok:
        install_logger.info("    OK")
    else:
        install_logger.error("   The CFS configuration '{}' must contain a slingshot-host-software layer".format(cfs_config_name))

    return cfs_ok

def validate_weak_symbols(config, failures, flavor="cray_shasta_c", arch="x86_64"):

    kernels = []
    provided = []
    required = dict()
    kmps = []
    work_dirs = []

    install_logger.info("  Performing kernel symbol check on the install media to verify COS and SHS compatibility")

    cos = config.location_dict.product("cos").best
    work_dirs.append(cos.work_dir)
    shscos = config.location_dict.product("slingshot-host-software").type(f"cos")[0]
    work_dirs.append(shscos.work_dir)


    # find the COS kernel
    for fkernel in Path(cos.work_dir).rglob("kernel-{}-[0-9]*.{}.rpm".format(flavor,arch)):
        # do a couple sanity checks
        if not fkernel.name.startswith("kernel-{}-".format(flavor)):
            continue
        if not fkernel.name.endswith(".{}.rpm".format(arch)):
            continue

        p,r = utils.process_rpm(fkernel)
        provided.extend(p)

        kernels.append(fkernel)

    if not kernels:
        install_logger.warning("    Unable to perform weak symbol check: No valid COS kernels found")
        return

    if len(kernels) > 1:
        install_logger.warning("    Unable to perform weak symbol check: Too many COS kernels found")
        return

    kernel = kernels[0]

    install_logger.debug("    kernel found: {}".format(kernel.name))
    # get all kmp files

    for path in work_dirs:
        for kmp in Path(path).rglob("*-kmp-{}-*.{}.rpm".format(flavor,arch)):
            fullkmp = os.path.join(kmp.parent,kmp.name)
            kmps.append(fullkmp)

            p, r = utils.process_rpm(kmp)
            provided.extend(p)
            required[fullkmp] = r

    if not kmps:
        install_logger.warning("    Unable to perform weak symbol check: Unable to find any KMPs")
        return

    missing_symbols = dict()
    for kmp in kmps:
        diff_list = list(set(required[kmp]).difference(provided))
        if diff_list:
            missing_symbols[kmp] = diff_list
            install_logger.debug("      {}".format(kmp))
            for missing in diff_list:
                install_logger.debug("        {}".format(missing))

    if missing_symbols:
        install_logger.error("   Missing kernel symbols found in {} package(s)".format(len(missing_symbols)))
        divider = "-" * (len(config.args['media_dir']) + 3)
        print(divider)
        print("The following packages have unresolved symbols:")
        print(divider)
        current_pdir = ""
        for kmp in missing_symbols:
            pdir = kmp.replace("{}/".format(config.args['media_dir']),"",1).split("/")[1]
            if (current_pdir != pdir):
                print("  {}:".format(pdir))
                current_pdir = pdir
            print("   ", os.path.basename(kmp))
        print(divider)
        install_logger.info("    FAILED")
        failures.append("The versions of SHS and COS being installed have incompatible kernel sets")
        return
    else:
        install_logger.info("    OK")

def validate_cos_ncn_kernel(config, failures):
    """
    verify media prior to installation
    """

    cos = config.location_dict.product("cos").best
    if not cos:
        install_logger.warning('  Cannot perform kernel compatibility check due to no valid COS products')
        return

    install_logger.info('  Performing compatibility check for COS {} on running NCN'.format(cos.name))

    # get the os release version (ie, 15-sp2)
    os_release = utils.get_os().lower()

    # get the running kernel version and format it to match the way it is
    # represented in the COS rpms
    running_kernel = os.uname()[2]

    # need to get rid of the -default and replace - with _ in order to match rpm names
    running_kernel = running_kernel.split('-default')[0].replace('-', '_')

    # find a representive rpm from the cos media
    query_package = 'cray-dvs-kmp-default'
    rpms = []
    for rpm in Path(cos.work_dir).rglob("cray-dvs-kmp-default-*"):
        if "{}-ncn".format(os_release) in str(rpm.parent):
            rpms.append(rpm.name)

    # see if our sample rpm matches the running NCN kernel
    if len(rpms) == 1:
        if running_kernel in rpms[0]:
            install_logger.info('    COS NCN content matches running NCN kernel {}'.format(running_kernel))
            install_logger.debug(rpms)
        else:
            install_logger.debug(rpms)
            install_logger.error('   COS NCN content does NOT match running NCN kernel {}'.format(running_kernel))
            install_logger.error('   It is not recommended to proceed as packages such as DVS which have')
            install_logger.error('   a kernel module will not work correctly on your NCN worker nodes.')
            failures.append('COS NCN content does NOT match running NCN kernel {}'.format(running_kernel))
            return
    elif len(rpms) == 0:
        install_logger.error('   Could not find any the DVS Kernel RPMS for the OS version you are')
        install_logger.error('   running.  This version of COS will not properly support DVS on your')
        install_logger.error('   worker nodes.')
        failures.append('COS NCN content does NOT match running NCN kernel {}'.format(running_kernel))
        return
    else:
        install_logger.warning('  Cannot perform check due to unexpected number of RPM matches')
        install_logger.debug(rpms)
        return

    install_logger.info('    OK')

def validate_products(config):
    """
    verify media prior to installation
    """

    failures = []
    num_cos_products = config.location_dict.product("cos").count
    num_shs_products = len(config.location_dict.product("slingshot-host-software").type("cos"))
    num_shs_versions = 1
    if num_shs_products and config.location_dict.product("slingshot-host-software").count > 1:
        shsvers = []
        for shs in config.location_dict.product("slingshot-host-software"):
            cver = ".".join(shs.version.split("-")[0].split(".")[0:3])
            if cver not in shsvers:
                shsvers.append(cver)

        shsvercount = len(shsvers)
        if shsvercount != 1:
            num_shs_versions = shsvercount

    perform_cos_validations = True
    perform_shs_validations = True
    if num_cos_products != 1:
        install_logger.warning('  Can only run COS validations on a single release at a time ({} found)'.format(num_cos_products))
        perform_cos_validations = False

    if num_shs_versions != 1:
        install_logger.warning('  Can only run Slingshot validations on a single release at a time ({} found)'.format(num_shs_versions))
        perform_shs_validations = False

    if num_shs_products != 1:
        install_logger.warning('  Can only run Slingshot validations if you have a single "cos" subpackage ({} found)'.format(num_shs_products))
        perform_shs_validations = False

    if not perform_cos_validations and not perform_shs_validations:
        install_logger.warning('  Skipping all media validations.')
        return

    if perform_cos_validations:
        # see if the running kernel matches the COS NCN kernel
        validate_cos_ncn_kernel(config, failures)
    else:
        install_logger.warning('  Skipping cos media validations.')

    if perform_cos_validations and perform_shs_validations:
        # see if the kernels and ksyms all match
        validate_weak_symbols(config, failures)
    else:
        install_logger.warning('  Skipping shs media validations.')

    if failures:
        install_logger.error(" Validation failed:")
        for failure in failures:
            install_logger.error("   {}".format(failure))
        raise InstallError("Product media has failed validation")


def render_jinja(config, product_key=None, jinja_string=None):
    """
    render jinja2 string
    """

    install_logger.debug("jinja_string='{}'".format(jinja_string))

    try:
        product = config.location_dict.get(product_key, False)

        # derive a strict x.y.z version from our full version
        x = y = z = 0
        version_list = product.best_version.split(".")
        try:
            x = version_list[0].split("-")[0]
            y = version_list[1].split("-")[0]
            z = version_list[2].split("-")[0]
        except InstallError:
            pass

        version_x_y = "{}.{}".format(x, y)
        version_x_y_z = "{}.{}.{}".format(x, y, z)

        install_logger.debug("AVAIL JINJA2 VARS")
        install_logger.debug("  product_key = {}".format(product_key))
        install_logger.debug("  product_type = {}".format(product.product))
        install_logger.debug("  version_full = {}".format(product.best_bersion))
        install_logger.debug("  version_x_y = {}".format(version_x_y))
        install_logger.debug("  version_x_y_z = {}".format(version_x_y_z))

        t = jinja2.Template(jinja_string, undefined=jinja2.StrictUndefined)

        rendered_string = t.render(
            product_key=product_key,
            product_type=product.product,
            version_full=product.best_version,
            version_x_y=version_x_y,
            version_x_y_z=version_x_y_z
            )

        install_logger.debug("rendered_string='{}'".format(rendered_string))

        return rendered_string

    except Exception as err:

        install_logger.debug("encountered {} when rendering jinja".format(err))
        install_logger.error("Couldn't perform subsitutions in '{}' for product '{}'!".format(jinja_string, product_key))
        raise InstallError("Please check your --working-branch value")
