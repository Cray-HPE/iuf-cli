# Copyright 2022 Hewlett Packard Enterprise Development LP

"""
Common utility and helper functions used by the CI.
"""

import datetime
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib

# pylint: disable=consider-using-f-string

def getenv(var):
    """Get an environment variable"""
    # Use os.environ[...] (and NOT os.environ.get(...) so that an exception is
    # raised by default if a key does not exist.
    if var in os.environ:
        return os.environ[var]
    else:
        return None


def split_strip(string_list):
    """
    Handle strings that contain items that are either comma separated or space
    separated.  Strip whitespace off of the items.  Return the items as a list.

    :param string string_list: A string containing one or more items separated
                               by either spaces or commas
    :return: a list
    """
    if "," in string_list:  #pylint: disable=no-else-return
        return [n.strip() for n in string_list.split(',')]
    else:
        return string_list.split()


def download_file(url, whereto, mode='wb'):
    """
    Download a file from $url to $whereto.  Default $perms are wb

    :param url: Where to download the file from.
    :param whereto: Where the file is saved locally.
    :param mode: How to open the file for writing.  Defaults to 'wb'.

    :return: None
    """

    # One megabyte chunk size.
    chunk_size = 1024 * 1024
    req = urllib.request.urlopen(url)
    file_size = int(req.length)
    with open(whereto, mode, encoding="UTF-8") as fhandle:
        total_bytes_read = 0
        while True:
            read_result = req.read(chunk_size)
            bytes_read = len(read_result)
            total_bytes_read += bytes_read
            fhandle.write(read_result)
            if bytes_read < chunk_size or total_bytes_read >= file_size:
                break

    return total_bytes_read

def download_to_mgt_node(connection, url, whereto):
    """Download a file to the management node."""

    filename = os.path.basename(url)
    target = os.path.join(whereto, filename)
    connection.sudo("wget  {} --quiet -O {}".format(url, target))


def check_repos(connection, product, filename):
    """Verify the repos.  This should work for cos and slingshot"""

    if product == "slingshot_host":
        version_re = re.compile(r"slingshot-host-software-((\d+\.){2}\d+-\d+)")
        version = re.search(version_re, filename)
        prod_name_version = "slingshot-host-software-{}".format(version.group(1))
    else:
        #version_re = re.compile("cos-(\d+\.){2}\d+-sle")
        version_re = re.compile(r"(cos-(\d+\.){2}\d+)")
        prod_name_version = re.search(version_re, filename).group(0)

    curl_cmd = "curl -s -k https://packages.local/service/rest/v1/repositories | jq -r '.[] | select(.name | startswith(\"{}\")) | .name'".format(prod_name_version)
    repos = connection.sudo(curl_cmd).stdout.split()

    return (len(repos) > 0, prod_name_version, repos)

def flushprint(txt):
    """Print to stdout and flush it to avoid buffering."""
    print(txt)
    sys.stdout.flush()


def format_url(connection, repo):
    """Format a git url to include the username and password"""
    git_pw = connection.sudo("KUBECONFIG=/etc/kubernetes/admin.conf kubectl get secret -n services vcs-user-credentials --template={{.data.vcs_password}} | base64 --decode").stdout
    git_user = 'crayvcs'

    return "https://{}:{}@api-gw-service-nmn.local/vcs/cray/{}.git".format(git_user, git_pw, repo)


def get_hosts(connection, host_str):
    """Get hosts matching a string; for example 'get_hosts(connection, "w0")'
    will get all worker nodes."""
    xnames = connection.sudo("cray hsm state components list --type node --format json | jq -r .Components[].ID").stdout.splitlines()
    hosts = []

    for xname in xnames:
        hw_desc = json.loads(connection.sudo('cray sls hardware describe {} --format json'.format(xname)).stdout)
        try:
            alias = hw_desc["ExtraProperties"]['Aliases'][0]
        except KeyError:
            pass
        if host_str in alias:
            hosts.append((xname, alias))

    # For some reason, the 'cray hsm state components list ...' command doesn't
    # include ncn-m001.
    if host_str in ['m0', 'ncn']:
        self_xname = connection.sudo("cat /etc/cray/xname").stdout.rstrip()
        hosts.append((self_xname, 'ncn-m001'))

    return hosts

def wait_for_pod(connection, pod_name, timeout=1200, delete=False):
    """Wait for a pod to be either created or deleted."""

    keep_waiting = True
    time_waited = 0
    sleep_time = 10
    while keep_waiting:
        pods = connection.sudo("KUBECONFIG=/etc/kubernetes/admin.conf kubectl get pods -Ao wide", hide=True).stdout.splitlines()
        found = False
        for pod in pods:
            if pod_name in pod:
                found = True
                fields = pod.split()
                running = fields[3]
        if found and delete is False:
            print("found running and delete == False ...")
            if 'running' in running.lower() or 'completed' in running.lower():
                keep_waiting = False
            elif running.lower() =='imagepullbackoff':
                print("WARNING: pod {} in error state: {}".format(pod_name, running))
                keep_waiting = False
            else:
                print("(else) running={} ... no action performed".format(running))
        elif not found and delete is True:
            # The pod has been deleted, so quit waiting.
            keep_waiting = False
        time.sleep(sleep_time)
        time_waited += sleep_time
        if time_waited >= timeout:
            action_str = "delete" if delete else "complete"
            print("WARNING: Timed out waiting {} seconds for pod {} to {}".format(time_waited, pod_name, action_str))
            keep_waiting = False


def git_clone(connection, repo, location):
    """
    Clone a git repository.

    repo is (for example) cos-config-management or csm-config-management
    """

    url = format_url(connection, repo)
    try:
        connection.sudo('bash -c "cd {} && git pull"'.format(os.path.join(location, repo)))
    except Exception as ex:
        print("caught exception={} ==> repo doesn't exist", ex)
        connection.sudo('bash -c "cd {} && git clone {}"'.format(location, url), warn=True)

    return os.path.join(location, repo)


def ls_remote(connection, repo):
    """
    Do a 'git ls-remote'.

    repo is (for example) cos-config-management or csm-config-management
    """
    url = format_url(connection, repo)
    output = connection.sudo("git ls-remote {}".format(url)).stdout
    return output


def download_artifacts(connection, repo, version, release_dist=None,
        whereto=None, onepackage=True):
    """
    Download artifacts from artifactory.

    :param repo: The repo; for example
        https://arti.dev.cray.com/ui/native/shasta-distribution-stable-local/cos
    :param version: The version of the artifacts to filter on.
    :param release_dist: the suse service pack to filter on; e.g, '15-SP2'.

    :return: None
    """

    # Keep imports local so that jobs not needing them won't hit a
    # ModuleNotFoundError if artifactory is not installed.
    from artifactory import ArtifactoryPath #pylint: disable=import-outside-toplevel

    path = ArtifactoryPath(repo.replace("ui/native", "artifactory"))
    locations = []

    if release_dist is not None:
        version_re = re.compile(r".*{}.*{}.*.gz$".format(release_dist, version))
    else:
        version_re = re.compile(r".*{}.*.gz$".format(version))

    dl_urls = [str(p) for p in path if re.match(version_re, str(p))]

    if onepackage:
        dl_urls = [sorted(dl_urls)[-1]]

    if whereto is None:
        location = os.getcwd()
    else:
        location = whereto

    for dl_url in dl_urls:
        download_to_mgt_node(connection, dl_url, location)
        locations.append(os.path.join(location, os.path.basename(dl_url)))
        print("downloaded {} to {}".format(dl_url, location))

    return locations


def wait_for_ncn_personalization(connection, xnames, timeout=600, sleep_time=10):
    """Wait for ncn personalization to complete.
    xnames: a list of xnames to wait for
    timeout: maximum amount of time to wait for NCN personalization to
        complete.
    sleep_time: Time to wait between NCN personalization checks
        (i.e, 'cray cfs components describe ...')
    """

    keep_waiting = True
    start = datetime.datetime.now()
    while keep_waiting:
        found_pending = False
        for xname in xnames:
            desc = json.loads(connection.sudo("cray cfs components describe {} --format json".format(xname)).stdout)
            if desc["configurationStatus"].lower() != "configured":
                print("waiting on {}".format(xname))
                found_pending = True
            if desc["errorCount"] != 0:
                print("WARNING: Found error on node {} while querying the NCN personalization process".format(xname))

        tdiff = datetime.datetime.now() - start
        seconds_waited = tdiff.total_seconds()
        if found_pending:
            time.sleep(sleep_time)
            print("(found_pending) seconds_waited={}".format(seconds_waited))
        else:
            keep_waiting = False

        if seconds_waited >= timeout:
            print("WARNING: timed out waiting for components to go from a "
                   "pending to configured status during ncn personalization")
            keep_waiting = False
        else:
            print("(else, bottom of loop) waited={} seconds, keep_waiting={}, found_pending={}".format(seconds_waited, keep_waiting, found_pending))
        sys.stdout.flush()


class VMConnectionException(Exception):
    """A pass-through class."""


def logit(severity, message):
    """fake logger should make the move to a real logger easier"""
    print(severity.upper(), message)


def run_command(cmd):
    """Run a system command."""

    # log commands to debug channel
    logit('debug', '  >> {}'.format(cmd))

    result = subprocess.run(cmd.split(), stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE, shell=False,
                        check=False, universal_newlines=True)

    # convert to a dict if we can
    try:
        structured_data = json.loads(result.stdout)
    except:
        structured_data = None

    # log errors
    if result.returncode != 0:
        failure = { 'cmd': cmd,
                    'stderr': result.stderr,
                    'stdout': result.stdout,
                    'returncode': result.returncode }
        logit('warn', failure)

    return result.returncode, structured_data, result


class CmdInterface:
    """Wrapper around the subprocess interface to simplify usage."""
    def __init__(self, host, n_retries=0):
        self.installer = True

    def sudo(self, cmd, **kwargs):
        """
        Execute a command.
        """

        # We might need to fiddle with this some.
        result = subprocess.run(cmd.split(), stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, shell=False,
                                check=False, universal_newlines=True)

        # Check the return code.  At this point, fabric would throw an
        # exception; we might want to do something similar.  At least with
        # subprocess we can examine the error.
        if result.returncode != 0:
            print('Warning: "{}" returned non-zero value {}.  stderr={}'.format(
                  cmd.split(), result.returncode, result.stderr))
        return result

    def put(self, source, target):
        """
        A Wrapper around the Connection put.
        """
        shutil.copyfile(source, target)


def get_products(
        media_dir='.',
        extract_archives = True,
        products = {},
        prefixes = { 'cos': 'cos',
                     'cpe': 'cpe',
                     'sles': 'SUSE',
                     'shs': 'slingshot-host-software',
                     'analytics': 'analytics'
                   },
        suffixes = { 'md5': '.tar.gz.MD5.TXT',
                     'tgz': '.tar.gz',
                     'tar': '.tar',
                     'out': '.tar.gz.OUT.TXT'
                   },
        new_product = {
            'archive_type': None,
            'product': None,
            'archive': None,
            'media_dir': None,
            'work_dir': None,
            'md5': None,
            'out': None,
            'archive_check': None
                      }
            ):

    """
    Extract product archives and return product information.

    When called, will extract any product archives found and
    return a dict of product dictionaries:

    {
        "cos-2.3.31-20220131164722": {   # key is the archive name without suffix or directory name
            "archive_type": "tgz",       # type of archive (tar, tgz)
            "product": "cos",            # derived shot product name
            "archive": "cos-2.3.31-20220131164722.tar.gz",  # filename of product archive
            "media_dir": "/admin/johnn/media2",  # path where all products are located.  if not
                                                 # specified, pwd will be used
            "work_dir": "/admin/johnn/media2/cos-2.3.31-20220131164722/cos-2.3.31-20220131164722",
                                                 # absolute path to the contents of the archive.
                                                 # the contents will always be nested inside a
                                                 # directory named after the key in order to 
                                                 # support archive files that contain directory
                                                 # names that are not unique
            "md5": null,                 # if an md5 file is provided, we will refuse to extract 
                                         # the archive if it doesn't match
            "out": null,                 # contains filename of 'out' file, if it exists
            "archive_check": null        # only set when archive is extracted
        }
    }

    Caller should place conditionals on 'product' and 'work_dir'
    as the former indicates the entry has been identified as a
    product, and the latter indicates a valid, extracted product
    has been staged.

    eg:
        for product in products:
        if products[product]['product']:
            work_dir = products[product]['work_dir']
            if work_dir:
                installer = os.path.join(work_dir, 'install.sh')

    """

    # convert to absolute to avoid ambiguity
    media_dir = os.path.abspath(media_dir)

    # let user know what we are working on
    logit('info', 'processing media_dir {}'.format(media_dir))

    # get contents of our media directory
    directory_listing = os.listdir(media_dir)

    logit('debug', 'dir contents: {}'.format(directory_listing))

    # process each item in the directory
    for item in directory_listing:

        logit('debug', 'processing {}'.format(item))

        item_name = None

        # logic to handle item if it is a file
        if os.path.isfile(os.path.join(media_dir, item)):

            # we process suffix first because we want to determine a
            # key name that does not include a suffix so all related
            # files such as md5 files can be stored in the same record

            for suffix in suffixes:

                logit('debug', 'suffix {}'.format(suffix))

                if item.endswith(suffixes[suffix]):

                    logit('debug', 'suffix match {}'.format(suffix))
                    item_name = item.split(suffixes[suffix])[0]
                    logit('debug', 'item_name is {}'.format(item_name))

                    # create a product entry if one doesn't already exist
                    if item_name not in products:
                        logit('debug', 'creating new product entry {}'.format(item_name))
                        products[item_name] = new_product.copy()

                    # handle archive and non-archive entries differently
                    if suffix in ['md5', 'out']:
                        logit('debug', 'item is not an archive')
                        logit('debug', 'adding {}, {}'.format(suffix, item))

                        # read md5 file, parse out and save the md5 sum
                        if suffix == 'md5':
                            logit('debug', 'reading md5 file {}'.format(item))
                            # TODO: exception handling, please
                            with open(os.path.join(media_dir, item)) as md5_file:
                                md5_contents = md5_file.readlines()
                                # TODO: improve this logic
                                md5 = md5_contents[0].split()[0]
                            products[item_name][suffix] = md5
                        else:
                            products[item_name][suffix] = item
                    else:
                        # record archive information
                        logit('debug', 'adding archive_type {}'.format(suffix))
                        logit('debug', 'adding archive {}'.format(item))
                        products[item_name]['archive_type'] = suffix
                        products[item_name]['archive'] = item

            # if there is no suffix, then just use the item name
            if not item_name:

                logit('debug', 'no suffix handling')
                logit('debug', 'setting item_name to {}'.format(item))
                item_name = item
                logit('debug', 'creating new product {}'.format(item_name))
                logit('debug', 'setting archive to {}'.format(item))

                # create entry in dict if it doesn't exist
                if item_name not in products:
                    logit('debug', 'creating new product entry {}'.format(item_name))
                    products[item_name] = new_product.copy()
 
                products[item_name]['archive'] = item

        # item is a directory
        elif os.path.isdir(os.path.join(media_dir, item)):

            item_name = item
            # create entry in dict if it doesn't exist
            if item_name not in products:
                logit('debug', 'creating new product entry {}'.format(item_name))
                products[item_name] = new_product.copy()

            # find the directory created by the tar file and update the work_dir
            work_dir_prefix = os.path.join(media_dir, item_name)
            work_dir_contents = os.listdir(work_dir_prefix)

            # we expect only one directory in the work_dir
            if len(work_dir_contents) == 1:
                products[item_name]['work_dir'] = os.path.join(media_dir, item_name, work_dir_contents[0])
            else:
                logit('warn', 'work_dir contents of {} unexpected'.format(item))

            logit('debug', 'found previously extracted work_dir {}'.format(item))
            logit('debug', 'processing directory {}'.format(item))
            logit('debug', 'new product {}'.format(item_name))
            logit('debug', 'setting work_dir to'.format(item_name))

        # file is of a type that isn't relevant for us
        else:
            item_name = item
            if item_name not in products:
                logit('debug', 'creating new product entry {}'.format(item_name))
                products[item_name] = new_product.copy()
            products[item_name]['archive'] = item
            logit('debug', 'item is not a file or directory')
            logit('debug', 'new product {}'.format(item_name))
            logit('debug', 'setting archive to {}'.format(item))

        for prefix in prefixes:
            if item.startswith(prefixes[prefix]):
                product_type = prefix
                products[item_name]['product'] = product_type
                logit('debug', 'prefix match found {}'.format(prefix))

        products[item_name]['media_dir'] = media_dir

    if extract_archives:

        for product in products:

            logit('debug', 'checking to see if {} needs to be unpacked'.format(product))
            logit('debug', 'products[product]["product"] is "{}"'.format(products[product]['product']))

            # only process items that are identified products
            # don't bother trying to extract something with no archive
            if products[product]['product'] and products[product]['archive']:

                logit('debug', 'inside product test')

                # only process items that have no work_dir (haven't been extracted yet)
                if not products[product]['work_dir']:

                    logit('debug', 'needs workdir')
                    work_dir = os.path.join(products[product]['media_dir'], product)

                    # make dir
                    cmd = 'mkdir -p {}'.format(work_dir)
                    logit('debug', 'performing: {}'.format(cmd))
                    rc, _, msg = run_command(cmd)
                    logit('debug', 'rc {}, msg={}'.format(rc, msg))

                    archive = os.path.join(products[product]['media_dir'], products[product]['archive'])

                    # accomidate compressed archives
                    extra_tar_flags = ''
                    if products[product]['archive_type'] == 'tgz':
                        extra_tar_flags += 'z'

                    # handle md5 sums, if provided
                    dist_sum = products[product]['md5']

                    if dist_sum:

                        # check archive md5
                        cmd = 'md5sum {}'.format(archive)
                        logit('info', 'checking the md5sum of {}'.format(archive))
                        rc, _, msg = run_command(cmd)
                        checked_sum = msg.stdout.split()[0]

                        if dist_sum == checked_sum:

                            logit('info', 'sum validates {}'.format(checked_sum))

                            # extract tarfile
                            logit('info', 'extracting {}'.format(archive))  

                            cmd = 'tar x{}af {} --directory {}'.format(extra_tar_flags, archive, work_dir)
                            logit('debug', 'performing: {}'.format(cmd))
                            rc, _, msg = run_command(cmd)
                            logit('debug', 'rc {}, msg={}'.format(rc, msg))

                            # if tar fails, remove work dir
                            if rc != 0:

                                logit('warn', 'unable to process {}'.format(product))
                                # remove dir to avoid thinking this is a valid workdir
                                cmd = 'rm -Rf {}'.format(work_dir)
                                logit('debug', 'performing: {}'.format(cmd))
                                rc, _, msg = run_command(cmd)
                                logit('debug', 'rc {}, msg={}'.format(rc, msg))

                                products[product]['work_dir'] = None

                            else:

                                # find the directory created by the tar file and update the work_dir
                                work_dir_prefix = os.path.join(products[product]['media_dir'], product)
                                work_dir_contents = os.listdir(work_dir_prefix)

                                if work_dir_contents:
                                    products[product]['work_dir'] = os.path.join(media_dir, product, work_dir_contents[0])

                            # take note that the md5 sum matched
                            products[product]['archive_check'] = 'passed'

                        else:

                            # there is a problem with the archive or sum
                            logit('error', "distribution sum doesn't match archive sum!")
                            logit('error', 'distribution {}'.format(dist_sum))
                            logit('error', 'archive_sum {}'.format(checked_sum))
                            logit('error', 'skipping extraction of {}'.format(archive))

                            # take note that the md5 sum did NOT match
                            products[product]['archive_check'] = 'failed'

                    else:

                        # extract tarfile
                        logit('info', 'extracting {}'.format(archive))  
                        cmd = 'tar x{}af {} --directory {}'.format(extra_tar_flags, archive, work_dir)
                        logit('debug', 'performing: {}'.format(cmd))
                        rc, _, msg = run_command(cmd)
                        logit('debug', 'rc {}, msg={}'.format(rc, msg))

                        # if tar fails, remove work dir so we don't leave invalid
                        # working directories around
                        if rc != 0:

                            logit('warn', 'skipping {}'.format(product))
                            # remove dir to avoid thinking this is a valid workdir
                            cmd = 'rm -Rf {}'.format(work_dir)
                            logit('debug', 'performing: {}'.format(cmd))
                            rc, _, msg = run_command(cmd)
                            logit('debug', 'rc {}, msg={}'.format(rc, msg))

                        else:

                            # find the directory created by the tar file and update the work_dir
                            work_dir_prefix = os.path.join(products[product]['media_dir'], product)
                            work_dir_contents = os.listdir(work_dir_prefix)
                            if work_dir_contents:
                                products[product]['work_dir'] = os.path.join(media_dir, product, work_dir_contents[0])

                else:
                    # find the directory created by the tar file and update the work_dir
                    work_dir_prefix = os.path.join(products[product]['media_dir'], product)
                    work_dir_contents = os.listdir(work_dir_prefix)

                    if work_dir_contents:
                        products[product]['work_dir'] = os.path.join(media_dir, product, work_dir_contents[0])
                    logit('debug', 'found previously extracted work_dir {}'.format(product))
                    
            else:
                logit('warn', 'skipping {}'.format(product))

    return products

