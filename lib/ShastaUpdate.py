#!/usr/bin/env python3

"""
Copyright 2022 Hewlett Packard Enterprise Development LP
"""

import json
import os
import time
import jinja2
from pathlib import Path

from distutils.version import LooseVersion

from lib.git import Git
from lib.InstallLogger import get_install_logger

from lib.vars import COSProblem, InstallError
import lib.InstallerUtils as utils #pylint: disable=wrong-import-position,import-error

# pylint: disable=consider-using-f-string

install_logger = get_install_logger(__name__)

def get_prods(config):
    """A passthrough function to InstallerUtils.get_products."""

### TODO: create activity session
###       this should probably be a class

    extract_archives = not config.dryrun
    config.location_dict = utils.get_products(config, extract_archives=extract_archives)

    install_logger.info("  Installable products:")
    for item in config.location_dict.installable_products:
        install_logger.info("    %s", item)
    if config.location_dict.uninstallable_products:
        install_logger.info("  Ignoring:")
        for item in config.location_dict.uninstallable_products:
            install_logger.info("    %s", item)

    install_logger.info('  CREATE/READ activity session %s',config.args['activity_session'])


def stub_pre_install_check(config):
    for prod in config.location_dict:
        opargs = { 'dir': prod.work_dir, 'activity-session': config.args['activity_session'], 'product_manifest': prod.product_manifest['version'] }
        install_logger.info('  operation preflight_checks_for_services')
        printopargs(opargs)
    time.sleep(2)


def stub_deliver_product(config):
    for prod in config.location_dict:
        opargs = { 'dir': prod.work_dir, 'activity-session': config.args['activity_session'], 'product_manifest': prod.product_manifest['version'] }
        install_logger.info('  operation loftsman_manifest_upload')
        install_logger.info('  operation s3_upload')
        install_logger.info('  operation nexus_setup')
        install_logger.info('  operation helm_upload')
        install_logger.info('  operation rpm_upload')
        install_logger.info('  operation vcs_upload')
        install_logger.info('  operation ims_upload')
        printopargs(opargs)
    time.sleep(2)


def stub_update_config(config):
    for prod in config.location_dict:
        target_branch = config.args["target_branches"]
        customer_branch = render_jinja(config, prod.name, target_branch[prod.product])
        opargs = { 'dir': prod.work_dir, 'branch': customer_branch, 'activity-session': config.args['activity_session'], 'product_manifest': prod.product_manifest['version'] }
        install_logger.info('  operation update_customer_branch')
        install_logger.info('  operation update_cfs_config (sat bootprep --config)')
        printopargs(opargs)
    time.sleep(2)


def stub_deploy_product(config):
    for prod in config.location_dict:
        opargs = { 'dir': prod.work_dir, 'activity-session': config.args['activity_session'], 'product_manifest': prod.product_manifest['version'] }
        install_logger.info('  operation loftsman_manifest_deploy')
        install_logger.info('  operation set_product_active')
        printopargs(opargs)
    time.sleep(2)


def stub_prepare_images(config):
    opargs = { 'activity-session': config.args['activity_session'], 'bootprep_config': config.args['bootprep_config_managed'] }
    install_logger.info('  operation prepare_images (sat bootprep --images)')
    printopargs(opargs)
    time.sleep(2)


def stub_ncn_rollout(config):
    opargs = { 'method': config.args["update_method_management"], 'activity-session': config.args['activity_session'] }
    install_logger.info('  operation ncn_rollout')
    printopargs(opargs)
    time.sleep(2)


def stub_post_install_service_check(config):
    for prod in config.location_dict:
        opargs = { 'dir': prod.work_dir, 'activity-session': config.args['activity_session'], 'product_manifest': prod.product_manifest['version'] }
        install_logger.info('  operation post_install_check')
        printopargs(opargs)
    time.sleep(2)


def stub_cn_rollout(config):
    opargs = { 'method': config.args["update_method_managed"], 'activity-session': config.args['activity_session'] }
    install_logger.info('  operation cn_rollout')
    printopargs(opargs)
    time.sleep(2)


def stub_post_install_compute_check(config):
    for prod in config.location_dict:
        opargs = { 'dir': prod.work_dir, 'activity-session': config.args['activity_session'], 'product_manifest': prod.product_manifest['version'] }
        install_logger.info('  operation post_install_check')
        printopargs(opargs)
    time.sleep(2)


def printopargs(opargs):
    print(json.dumps(opargs, indent=4))


def get_mergeable_repos(config):

    repos = {}
    # Update the product catalog if necessary.
    utils.get_product_catalog(config)

    # only products with an import_branch are mergable
    for product in config.location_dict:
        if product.import_branch:
            repos[product.product] = os.path.basename(product.clone_url.replace('.git', ''))

    install_logger.debug('found mergeable_repos %s', repos)

    return repos


def update_vcs_config(config):
    """Merge the product git branch to the working config"""

    # first things first, get a copy of all the config repos
    install_logger.debug("Cloning all of the configuration repositories...")
    #
    # get dict of mergeable repos
    repos = get_mergeable_repos(config)
    git = Git(config)

    site_params = config.args["site_params"]
    for product in config.location_dict:
        if product.import_branch:
            install_logger.debug("processing product %s import_branch %s", product.product, product.import_branch)

            repo = repos[product.product]

            # Get the working branch with product key and --customer-branch
            # If the passed working branch doesn't exist,it is created. 

            # check out a local copy of the import_branch (release version)
            git.checkout(repo, product.import_branch)
            working_branch = site_params[product.product]["working_branch"]

            # fourth, merge the import branch to the integration branch
            install_logger.info("  Merging branch %s into %s", product.import_branch, working_branch)
            git.checkout(repo, working_branch, create=True)
            git.merge(repo, product.import_branch)
            git.push(repo)

            # then clean up
            git.cleanup(repo)

            install_logger.info("    OK")

def get_cos_recipe_name(config):
    # if cos_recipe_name was supplied on the command line, just return it
    if "cos_recipe_name" in config.args:
        if config.args["cos_recipe_name"]:
            install_logger.debug("recipe name found in args %s", repr(config.args['cos_recipe_name']))
            return config.args['cos_recipe_name']

    cos_version = config.location_dict.product("cos").version
    pname = "cos-" + cos_version
    install_logger.debug("using product %s", pname)

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


def best_guess_working(config, product_key, gitobj, repo):
    """Best guess at the working branch.  If the specified branch exists,
    return it.  Otherwise, find the one closest in version but lower."""

    integration_branch = None
    if not config.args.get("customer_branch", None):
        raise InstallError("You must supply --customer-branch")

    integration_branch = render_jinja(config, product_key, config.args["customer_branch"])
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


def get_ncn_list_xnames(config):
    """Get a list of all worker and manaagement ncns."""
    # FIXME: We need to skip the ncn-s00* nodes for now.  Is this fixed yet!?

    if not hasattr(get_ncn_list_xnames, "ncn_list_xnames"):
        all_ncn_tuples = utils.get_ncn_tuples(config)
        get_ncn_list_xnames.ncn_list_xnames = [n[0] for n in all_ncn_tuples]

    return get_ncn_list_xnames.ncn_list_xnames

def switch_ncn_enablement(config, template_name, enable=True):
    """Enable or disable the ncns"""
    ncn_list_xnames = get_ncn_list_xnames(config)


    if enable:
        for ncn in ncn_list_xnames:
            config.connection.sudo("cray cfs components update --desired-config {} --enabled true --format json --error-count 0 --state [] {}".format(template_name, ncn))
    else:
    # Disable cfs.
        for ncn in ncn_list_xnames:
            config.connection.sudo("cray cfs components update {} --enabled false".format(ncn))

    return


def cleanup(config): # pylint: disable=unused-argument
    """Clean things up after a run."""

    # remove files containing passwords
    statedir = config.args.get("state_dir")
    config.connection.sudo("rm -f {}/.vcspass {}/get_local_vcspw.sh".format(statedir, statedir))


def validate_weak_symbols(config, failures, flavor="cray_shasta_c", arch="x86_64"):

    kernels = []
    provided = []
    required = {}
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
            install_logger.debug("      %s", kmp)
            for missing in diff_list:
                install_logger.debug("        %s", missing)

    if missing_symbols:
        install_logger.error("   Missing kernel symbols found in %s package(s)", len(missing_symbols))
        divider = "-" * (len(config.args['media_dir']) + 3)
        print(divider)
        print("The following packages have unresolved symbols:")
        print(divider)
        current_pdir = ""
        for kmp in missing_symbols:
            pdir = kmp.replace("{}/".format(config.args['media_dir']),"",1).split("/")[1]
            if current_pdir != pdir:
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

    install_logger.info('  Performing compatibility check for COS %s on running NCN', cos.name)

    # get the os release version (ie, 15-sp2)
    os_release = utils.get_os().lower()

    # get the running kernel version and format it to match the way it is
    # represented in the COS rpms
    running_kernel = os.uname()[2]

    # need to get rid of the -default and replace - with _ in order to match rpm names
    running_kernel = running_kernel.split('-default')[0].replace('-', '_')

    # find a representive rpm from the cos media
    valid_dirs = [f"{os_release}-ncn", f"{os_release}-net-ncn"]
    rpms = []
    for rpm in Path(cos.work_dir).rglob("cray-dvs-kmp-default-*"):
        for vdir in valid_dirs:
            if vdir in str(rpm.parent) and rpm.name not in rpms:
                # in some releases the same rpm is in multiple directories, don't duplicate
                rpms.append(rpm.name)

    # see if our sample rpm matches the running NCN kernel
    if len(rpms) == 1:
        if running_kernel in rpms[0]:
            install_logger.info('    COS NCN content matches running NCN kernel %s', running_kernel)
            install_logger.debug(rpms)
        else:
            install_logger.debug(rpms)
            install_logger.error('   COS NCN content does NOT match running NCN kernel %s', running_kernel)
            install_logger.error('   It is not recommended to proceed as packages such as DVS which have')
            install_logger.error('   a kernel module will not work correctly on your NCN worker nodes.')
            failures.append('COS NCN content does NOT match running NCN kernel %s', running_kernel)
            return
    elif len(rpms) == 0:
        install_logger.error('   Could not find any the DVS Kernel RPMS for the OS version you are')
        install_logger.error('   running.  This version of COS will not properly support DVS on your')
        install_logger.error('   worker nodes.')
        failures.append('COS NCN content does NOT match running NCN kernel %s', running_kernel)
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
        install_logger.debug("  product_key = %s", product_key)
        install_logger.debug("  product_type = %s", product.product)
        install_logger.debug("  version_full = %s", product.best_bersion)
        install_logger.debug("  version_x_y = %s", version_x_y)
        install_logger.debug("  version_x_y_z = %s", version_x_y_z)

        t = jinja2.Template(jinja_string, undefined=jinja2.StrictUndefined)

        rendered_string = t.render(
            product_key=product_key,
            product_type=product.product,
            version_full=product.best_version,
            version_x_y=version_x_y,
            version_x_y_z=version_x_y_z
            )

        install_logger.debug("rendered_string='%s'", rendered_string)

        return rendered_string

    except Exception as err:

        install_logger.debug("encountered %s when rendering jinja", err)
        install_logger.error("Couldn't perform subsitutions in '%s' for product '%s'!", jinja_string, product_key)
        raise InstallError("Please check your --customer-branch value")
