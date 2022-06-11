# Copyright 2022 Hewlett Packard Enterprise Development LP

"""
Common utility and helper functions used by the CI.
"""

import datetime
import json
import os
import re
import shlex
import shutil
import asyncio
import sys
import textwrap
import time
import urllib
import shlex
import shutil
import yaml
import rpm
import prettytable

from utils.InstallLogger import get_install_logger
from utils.vars import *

install_logger = get_install_logger(__name__)
# pylint: disable=consider-using-f-string


class version():
    def __init__(self, short_version, full_version):
        self.short_v = short_version
        self.full_v = full_version

    @property
    def short_version(self):
        return self.short_v

    @property
    def full_version(self):
        return self.full_v


class productVersions():
    def __init__(self):
        self.versions = {}
    def set(self, product, short_version, full_version):
        if product not in self.versions:
            self.versions[product] = version(short_version, full_version)
    def has(self, product):
        return product in self.versions

def print_table(rows, header=None, sort=None, alignments=None):
    table = prettytable.PrettyTable()

    if header:
        table.field_names = header

    if sort:
        table.sortby = sort

    if alignments:
        for column,align in alignments.items():
            table.align[column] = align

    for row in rows:
        table.add_row(row)

    print(table)

def getenv(var):
    """Get an environment variable"""

    if var in os.environ:
        return os.environ[var]
    else:
        return None


def formatted(text):
    """Format a text string for a standard 80-line terminal."""
    wrapper = textwrap.TextWrapper(width=78)
    raw = textwrap.dedent(text).strip()
    msg = wrapper.fill(text=raw)
    return msg


def get_ims_public_key(connection):
    created_public_keys = json.loads(connection.sudo("cray ims public-keys list --format json").stdout)
    inst_pkey_list = [k for k in created_public_keys if k["name"] == "installer_public_key"]
    rsa_pub = os.path.join(os.path.expanduser("~"), ".ssh", "id_rsa.pub")
    if len(inst_pkey_list) <= 0:
        pkey_dict = json.loads(connection.sudo('cray ims public-keys create --name "installer_public_key" --format json --public-key {}'.format(rsa_pub)).stdout)
        public_key = pkey_dict
    else:
        public_key = inst_pkey_list[0]
    ims_public_key_id = public_key['id']

    return ims_public_key_id

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

def get_hosts(connection, host_str, exact_match=True):
    """
    Get hosts matching a string; for example
    'get_hosts(connection, "ncn-w")' will get all worker nodes.
    """

    components_list = json.loads(connection.sudo("cray hsm state components list --type node --format json").stdout)

    xnames = [clist["ID"] for clist in components_list["Components"]]
    hosts = []
    def ncn_sort(tup):
        return tup[1]

    for xname in xnames:
        hw_desc = json.loads(connection.sudo('cray sls hardware describe {} --format json'.format(xname)).stdout)
        try:
            alias = hw_desc["ExtraProperties"]['Aliases'][0]
        except KeyError:
            pass
        if exact_match:
            if host_str == xname or host_str == alias:
                hosts.append((xname, alias))
        elif host_str in xname or host_str in alias:
            hosts.append((xname, alias))

    return sorted(hosts, key=ncn_sort)


def get_ncn_tuples(connection, args, just_workers=False):
    """
    By default, will return list of worker and managment node tuples,
    such as:
        [('x3000c0s1b0n0', 'ncn-m001'), ('x3000c0s3b0n0', 'ncn-m002')]
    The `--worker-nodes`/`-wn` argument affects which nodes are returned.
    """

    # Get a list of worker and management NCN nodes.  If they have asked
    # for just_workers, honor the worker_nodes arg, if specified, otherwise
    # just use the hostname pattern.
    if just_workers:
        if "worker_nodes" in args and args["worker_nodes"]:
            all_ncn_tuples = []
            for node in args["worker_nodes"]:
                all_ncn_tuples += get_hosts(connection, node)
        else:
            w_ncn_tuples = get_hosts(connection, "ncn-w", exact_match=False)
            all_ncn_tuples =  w_ncn_tuples
    else:
        w_ncn_tuples = get_hosts(connection, "ncn-w", exact_match=False)
        m_ncn_tuples = get_hosts(connection, "ncn-m", exact_match=False)
        all_ncn_tuples = w_ncn_tuples + m_ncn_tuples

    return all_ncn_tuples

def process_rpm(rpmpath):
    provides = []
    requires = []

    def symstr(name,value):
        sym = None
        nstr = name.decode('utf-8')
        if nstr.startswith("ksym("):
            sym = "{} = {}".format(nstr,value.decode('utf-8'))

        return sym

    # the rpm module sure is something!
    ts = rpm.TransactionSet("", (rpm._RPMVSF_NOSIGNATURES))
    with rpmpath.open() as fd:
        headers = ts.hdrFromFdno(fd)

        # the rpm api returns the provide name in one list, and the values in another list
        pnames = headers[rpm.RPMTAG_PROVIDENAME]
        pvals = headers[rpm.RPMTAG_PROVIDEVERSION]

        # get the two ordered lists and join them
        for n,v in zip(pnames,pvals):
            sym = symstr(n,v)
            if sym:
                provides.append(sym)

        # same with the requires
        rnames = headers[rpm.RPMTAG_REQUIRENAME]
        rvals = headers[rpm.RPMTAG_REQUIREVERSION]
        for n,v in zip(rnames,rvals):
            sym = symstr(n,v)
            if sym:
                requires.append(sym)

    return provides, requires


def wait_for_ncn_personalization(connection, xnames, timeout=600, sleep_time=30):
    """Wait for ncn personalization to complete.
    xnames: a list of xnames to wait for
    timeout: maximum amount of time to wait for NCN personalization to
        complete.
    sleep_time: Time to wait between NCN personalization checks
        (i.e, 'cray cfs components describe ...')
    """

    keep_waiting = True
    start = datetime.datetime.now()
    bad_nodes = set()
    while keep_waiting:
        found_pending = False
        pending_nodes = []
        configured_nodes = []
        for xname in xnames:
            desc = json.loads(connection.sudo("cray cfs components describe {} --format json".format(xname)).stdout)

            if desc["configurationStatus"].lower() == "configured":
                install_logger.debug("node {} configured".format(xname))
                found_pending = False
                configured_nodes.append(xname)
            else:
                if desc["errorCount"] == 0:
                    install_logger.debug("waiting on {}".format(xname))
                    found_pending = True
                    pending_nodes.append(xname)
                elif desc["errorCount"] != 0:
                    install_logger.warning("      Found error on node {} while querying the NCN personalization process".format(xname))
                    bad_nodes.add(xname)

        tdiff = datetime.datetime.now() - start
        seconds_waited = tdiff.total_seconds()
        if found_pending:
            time.sleep(sleep_time)
            install_logger.debug("(found_pending) seconds_waited={}".format(seconds_waited))
        else:
            keep_waiting = False

        if seconds_waited >= timeout:
            install_logger.warning("Timed out waiting for components to go from a "
                   "pending to configured status during ncn personalization")
            keep_waiting = False
        else:
            install_logger.debug("(else, bottom of loop) waited={} seconds, keep_waiting={}, found_pending={}".format(seconds_waited, keep_waiting, found_pending))

        install_logger.info("      Waited {} seconds, Configured {}, Pending {}, Error {}".format(
            int(seconds_waited),
            len(configured_nodes),
            len(pending_nodes),
            len(bad_nodes)))

    return bad_nodes

class RunOutput():
    def __init__(self, cmd, args, returncode, stdout, stderr):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.cmd = cmd
        self.args = args

class _CmdInterface:
    """Wrapper around the subprocess interface to simplify usage."""
    def __init__(self, n_retries=0, dryrun=False):
        self.installer = True
        self.dryrun = dryrun

    def sudo(self, cmd, dryrun=None, cwd=None, quiet=False, store_output=None, tee=False, timeout=None, **kwargs):
        """
        Execute a command.
        """

        if dryrun is None:
            dryrun = self.dryrun

        if dryrun:
            result = RunOutput(cmd, shlex.split(cmd), 0, "", "Dryrun, command not executed")
        else:
            if store_output:
                output_h = open(store_output, "w", 1)
            else:
                output_h = None

            try:
                result = self.run(cmd, quiet=quiet, output=output_h, cwd=cwd, tee=tee, timeout=timeout, **kwargs)

            except RunException as e:
                install_logger.debug("  >>   cmd      : {}".format(e.cmd))
                if store_output:
                    install_logger.debug("  >>>> log      : {}".format(store_output))
                else:
                    install_logger.debug("  >>>> stdout   : {}".format(e.stdout))
                    install_logger.debug("  >>>> stderr   : {}".format(e.stderr))
                install_logger.debug("  >>>> exit code: {}".format(e.returncode))
                raise
            except RunTimeoutError as e:
                install_logger.debug("  >>   cmd      : {}".format(cmd))
                install_logger.debug("  >>   error    : Execution time exceeded {} seconds".format(timeout))
                raise

        if not quiet:
            if dryrun:
                install_logger.dryrun("  >>   cmd      : {}".format(cmd))
                install_logger.dryrun("  >>>> cwd      : {}".format(cwd))
            else:
                install_logger.debug("  >>   cmd      : {}".format(result.cmd))
                if store_output:
                    install_logger.debug("  >>>> log      : {}".format(store_output))
                else:
                    install_logger.debug("  >>>> stdout   : {}".format(result.stdout))
                    install_logger.debug("  >>>> stderr   : {}".format(result.stderr))
                install_logger.debug("  >>>> exit code: {}".format(result.returncode))

        return result

    def run(self, cmd, output=None, cwd=None, quiet=False, timeout=None, tee=False, **kwargs) -> RunOutput:
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(
            self._stream_subprocess(cmd, output=output, cwd=cwd, quiet=quiet, timeout=timeout, tee_output=tee, **kwargs)
        )

        # asyncio doesn't really do "check=True", so fake it
        if result.returncode:
            raise RunException("{} returned non-zero exit status: {}".format(shlex.split(cmd)[0], result.returncode),
                    cmd,
                    shlex.split(cmd),
                    result.returncode,
                    result.stdout,
                    result.stderr)
        else:
            return result

    async def _read_stream(self, stream, callback):
        while True:
            line = await stream.readline()
            if line:
                callback(line)
            else:
                break

    async def _stream_subprocess(self, cmd, output=None, cwd=None, quiet=False, timeout=None, tee_output=False, **kwargs) -> RunOutput:
        splitc = shlex.split(cmd)

        p = await asyncio.create_subprocess_exec(*splitc,
                                              stdin=None,
                                              stdout=asyncio.subprocess.PIPE,
                                              stderr=asyncio.subprocess.PIPE,
                                              cwd=cwd,
                                              **kwargs)
        out = []
        err = []

        def tee(line, sink, pipe, output):
            line = line.decode('utf-8')
            if output:
                output.write(line)

            line = line.rstrip()
            sink.append(line)
            if tee_output:
                print(line, file=pipe)

        done, pending = await asyncio.wait([
            self._read_stream(p.stdout, lambda l: tee(l, out, sys.stdout, output)),
            self._read_stream(p.stderr, lambda l: tee(l, err, sys.stderr, output)),
        ], timeout=timeout)

        if pending:
            out_s = "\n".join(out)
            err_s = "\n".join(err)
            raise RunTimeoutError("{} execution time exceeded {} seconds".format(splitc[0], timeout),
                    cmd,
                    splitc,
                    "-1",
                    out_s,
                    err_s)

        returncode = await p.wait()

        out_s = "\n".join(out)
        err_s = "\n".join(err)

        ro = RunOutput(cmd=cmd, args=shlex.split(cmd), returncode=returncode, stdout=out_s, stderr=err_s)
        return ro

    def askfirst(self, cmd, **kwargs):
        """Pause for user input after a sudo command."""

        nasks = 0
        keepgoing = True

        # Ask for specific input to avoid an ambiguous answer.  We don't want
        # to continue or exit unless it's exactly what the user wants.
        while nasks < 10 and keepgoing:

            print("Command to be ran: '{}'\nContinue?Y/y/yes or N/n/no".format(cmd))
            answer = input()

            if answer.lower() in  ["n", "no"]:
                print("answer='{}' ==> exiting ...".format(answer))
                sys.exit(0)
                keepgoing = False
            elif answer.lower() in ["y", "yes"]:
                keepgoing = False
            else:
                nasks += 1

            if nasks >= 10:
                install_logger.info("Too many asks.  Exiting")
                sys.exit(0)

        out = None
        try:
            out = self.sudo(cmd, **kwargs)
        except Exception as ex:
            print("Caught exception: {}".format(ex))
            if hasattr(out, "stderr"):
                print("stderr='{}'".format(out.stderr))
            if hasattr(out, "stdout"):
                print("stdout='{}'".format(out.stdout))
            print("Do you want to continue? (Y/y/Yes for yes, anything else to abort")
            answer = input()
            if answer.lower() not in ["y", "yes"]:
                sys.exit(0)

        return out

    def put(self, source, target):
        """
        A Wrapper around the Connection put.
        """
        shutil.copyfile(source, target)
        st = os.stat(source)
        os.chmod(target, st.st_mode)


class CmdMgr:
    """
    This class can be used to manage a single use of CmdInterface
    """

    connection = None

    @staticmethod
    def get_cmd_interface():
        if CmdMgr.connection == None:
            CmdMgr.connection = _CmdInterface()
        return CmdMgr.connection


def get_product_catalog(connection, products=None):
    """
    update product dictionary with gitea urls
    """
    install_logger.debug('determining config-management url for products')
    # get full data for initial image_id
    command = 'kubectl get cm -n services cray-product-catalog -o json'
    product_cat_json = connection.sudo(command, dryrun=False).stdout
    product_cat  = json.loads(product_cat_json)
    all_product_data = product_cat['data']

    if not products:
        return all_product_data

    for product in products:
        if products[product]['import_version']:
            product_version = products[product]['import_version']
        else:
            product_version = products[product]['version']
        install_logger.debug('using product_version {}'.format(product_version))
        try:
            working_type = products[product]['product']
            product_data = yaml.safe_load(all_product_data[working_type])
            matching_versions = []
            # find all keys in the product catalog that match the supplied product
            if product_version in product_data.keys():
                install_logger.debug('{} is an exact match'.format(product_version))
                matching_versions.append(product_version)
            else:
                install_logger.debug('looking for matching version')
                for item in product_data.keys():
                    if str(product_version).startswith(str(item)):
                        matching_versions.append(item)
            # if there is only one match, we consider this the "real" version
            if len(matching_versions) == 1:
                working_version = matching_versions[0]
                products[product]['product_version'] = working_version
                install_logger.debug('found exact version {} for {}'.format(working_version, product))
                product_catalog = product_data[working_version]
                try:
                    products[product]['clone_url'] = product_catalog['configuration']['clone_url']
                    products[product]['import_branch'] = product_catalog['configuration']['import_branch']
                    if "recipes" in product_catalog:
                        products[product]['recipe'] = list(product_catalog['recipes'].keys())[0]
                except Exception as err:
                    # even if we have an exact match in the product catalog, there may
                    # not be git information associated with that product entry
                    working_version = max(product_data.keys())
                    install_logger.debug('no product catalog config data, trying {} for {}'.format(working_version, product))
                    product_catalog = product_data[working_version]
                    products[product]['clone_url'] = product_catalog['configuration']['clone_url']
                    if "recipes" in product_catalog:
                        products[product]['recipe'] = list(product_catalog['recipes'].keys())[0]
            else:
                # if there is no exact match, attempt to get the clone_url anyway since
                # that doesn't change between product versions
                working_version = max(product_data.keys())
                install_logger.debug('no exact version in {}, using {} for {}'.format(matching_versions, working_version, product))
                product_catalog = product_data[working_version]
                products[product]['clone_url'] = product_catalog['configuration']['clone_url']
                if "recipes" in product_catalog:
                    products[product]['recipe'] = list(product_catalog['recipes'].keys())[0]

        except Exception as err:
            install_logger.debug('unable to get all config-management data for {}'.format(product))
            pass

    return products

def get_product_prefixes(desc=False):
    prefixes = {
        'cos': {
            'product': 'cos',
            'description': 'HPE Cray Operating System',
            },
        'SUSE-Backports-SLE': {
            'product': 'sles',
            'description': 'openSUSE packages backports provided by SUSE',
            },
        'SUSE-PTF': {
            'product': 'sles',
            'description': 'PTFs (Program Temporary Fixes) provided by SUSE',
            },
        'SUSE-Products': {
            'product': 'sles',
            'description': 'Base SLE software provided by SUSE',
            },
        'SUSE-Updates': {
            'product': 'sles',
            'description': 'Updates to base SLE software provided by SUSE',
            },
        'slingshot-host-software': {
            'product': 'slingshot-host-software',
            'description': 'Slingshot Host Software and Drivers',
            },
        # 'analytics': 'analytics',
        'uan': {
            'product': 'uan',
            'description': 'HPE Cray User Access Node (UAN) Software',
            },
        # 'cpe': 'cpe',
        # 'cpe-slurm': 'slurm',
        # 'wlm-slurm': 'slurm',
        # 'wlm-pbs': 'pbs',
        # 'cpe-pbs': 'pbs',
        # 'cray-sdu-rda': 'sdu'
        'sma': {
            'product': 'sma',
            'description': 'HPE Cray EX System Monitoring Application',
            },
        'sat': {
            'product': 'sat',
            'description': 'HPE Cray System Admin Toolkit',
            },
    }

    retval = dict()

    for prefix in prefixes:
        if desc:
            retval[prefix] = prefixes[prefix]['description']
        else:
            retval[prefix] = prefixes[prefix]['product']

    return retval

def get_products( connection,
                  media_dir = '.',
                  extract_archives = True,
                  products = None,
                  prefixes = None,
                  suffixes = None,
                  new_product = None ):
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

    if not products:
        products = {}

    if not prefixes:
        prefixes = get_product_prefixes()

    if not suffixes:
        suffixes = { 'md5': '.tar.gz.MD5.TXT',
                     'tgz': '.tar.gz',
                     'tar': '.tar',
                     'out': '.tar.gz.OUT.TXT' }

    if not new_product:
        new_product = { 'archive_type': None,
                        'product': None,
                        'archive': None,
                        'media_dir': None,
                        'work_dir': None,
                        'md5': None,
                        'out': None,
                        'archive_check': None,
                        'version': None,
                        'product_version': None,
                        'import_version': None,
                        'clone_url': None,
                        'import_branch': None,
                        'installed': None,
                        'merged': None
                      }
    # since media_dir defaults to PWD, let's just ignore
    # installer files
    SKIP_FILES=[
        "cne-install",
        "install.log",
        "utils",
        "location_dict.yaml"
        ]

    # convert to absolute to avoid ambiguity
    media_dir = os.path.abspath(media_dir)

    # let user know what we are working on
    install_logger.info('  processing media_dir {}'.format(media_dir))

    # get contents of our media directory
    directory_listing = os.listdir(media_dir)

    install_logger.debug('dir contents: {}'.format(directory_listing))

    # process each item in the directory
    for item in directory_listing:

        install_logger.debug('processing {}'.format(item))
        if item in SKIP_FILES:
            install_logger.debug('skipping installer support file {}'.format(item))
            continue

        item_name = None

        # logic to handle item if it is a file
        if os.path.isfile(os.path.join(media_dir, item)):

            # we process suffix first because we want to determine a
            # key name that does not include a suffix so all related
            # files such as md5 files can be stored in the same record

            for suffix in suffixes:

                install_logger.debug('suffix {}'.format(suffix))

                if item.endswith(suffixes[suffix]):

                    install_logger.debug('suffix match {}'.format(suffix))
                    item_name = item.split(suffixes[suffix])[0]
                    install_logger.debug('item_name is {}'.format(item_name))

                    # create a product entry if one doesn't already exist
                    if item_name not in products:
                        install_logger.debug('creating new product entry {}'.format(item_name))
                        products[item_name] = new_product.copy()

                    # handle archive and non-archive entries differently
                    if suffix in ['md5', 'out']:
                        install_logger.debug('item is not an archive')
                        install_logger.debug('adding {}, {}'.format(suffix, item))

                        # read md5 file, parse out and save the md5 sum
                        if suffix == 'md5':
                            install_logger.debug('reading md5 file {}'.format(item))
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
                        install_logger.debug('adding archive_type {}'.format(suffix))
                        install_logger.debug('adding archive {}'.format(item))
                        products[item_name]['archive_type'] = suffix
                        products[item_name]['archive'] = item

            # if there is no suffix, then just use the item name
            if not item_name:

                install_logger.debug('no suffix handling')
                install_logger.debug('setting item_name to {}'.format(item))
                item_name = item
                install_logger.debug('creating new product {}'.format(item_name))
                install_logger.debug('setting archive to {}'.format(item))

                # create entry in dict if it doesn't exist
                if item_name not in products:
                    install_logger.debug('creating new product entry {}'.format(item_name))
                    products[item_name] = new_product.copy()

                products[item_name]['archive'] = item

        # item is a directory
        elif os.path.isdir(os.path.join(media_dir, item)):

            item_name = item
            # create entry in dict if it doesn't exist
            if item_name not in products:
                install_logger.debug('creating new product entry {}'.format(item_name))
                products[item_name] = new_product.copy()

            # find the directory created by the tar file and update the work_dir
            work_dir_prefix = os.path.join(media_dir, item_name)
            work_dir_contents = os.listdir(work_dir_prefix)

            # we expect only one directory in the work_dir
            if len(work_dir_contents) == 1 and os.path.isdir(os.path.join(media_dir, item_name, work_dir_contents[0])):
                products[item_name]['work_dir'] = os.path.join(media_dir, item_name, work_dir_contents[0])
            else:
                install_logger.warning('    work_dir contents of {} unexpected'.format(item))

            install_logger.info('    found existing work_dir for {}'.format(item))
            install_logger.debug('processing directory {}'.format(item))
            install_logger.debug('new product {}'.format(item_name))
            install_logger.debug('setting work_dir to'.format(item_name))

        # file is of a type that isn't relevant for us
        else:
            item_name = item
            if item_name not in products:
                install_logger.debug('creating new product entry {}'.format(item_name))
                products[item_name] = new_product.copy()
            products[item_name]['archive'] = item
            install_logger.debug('item is not a file or directory')
            install_logger.debug('new product {}'.format(item_name))
            install_logger.debug('setting archive to {}'.format(item))

        for prefix in prefixes:
            if item.startswith(prefix) and item not in ['cne-install', 'install.log']:
                product_type = prefixes[prefix]
                products[item_name]['product'] = product_type
                install_logger.debug('prefix match found {}'.format(prefixes[prefix]))

        products[item_name]['media_dir'] = media_dir

    if extract_archives:

        for product in products:

            install_logger.debug('checking to see if {} needs to be unpacked'.format(product))
            install_logger.debug('products[product]["product"] is "{}"'.format(products[product]['product']))

            # only process items that are identified products
            # don't bother trying to extract something with no archive
            if products[product]['product'] and products[product]['archive']:

                install_logger.debug('inside product test')

                # only process items that have no work_dir (haven't been extracted yet)
                if not products[product]['work_dir']:

                    install_logger.debug('needs workdir')
                    work_dir = os.path.join(products[product]['media_dir'], product)

                    # make dir
                    connection.sudo('mkdir -p {}'.format(work_dir))

                    archive = os.path.join(products[product]['media_dir'], products[product]['archive'])

                    # accomidate compressed archives
                    extra_tar_flags = ''
                    if products[product]['archive_type'] == 'tgz':
                        extra_tar_flags += 'z'

                    # handle md5 sums, if provided
                    dist_sum = products[product]['md5']

                    if dist_sum:

                        install_logger.info('    validating md5 sum for {}'.format(products[product]['archive']))
                        # check archive md5
                        cmd = 'md5sum {}'.format(archive)
                        msg = connection.sudo(cmd)
                        checked_sum = msg.stdout.split()[0]

                        if dist_sum == checked_sum:

                            install_logger.info('      sum validates {}'.format(checked_sum))

                            # extract tarfile
                            install_logger.info('    extracting {}'.format(archive))

                            try:
                                cmd = 'tar x{}af {} --directory {}'.format(extra_tar_flags, archive, work_dir)
                                connection.sudo(cmd)
                            except:
                                # if tar fails, remove work dir
                                install_logger.warning('    unable to process {}'.format(product))
                                # remove dir to avoid thinking this is a valid workdir
                                connection.sudo('rm -Rf {}'.format(work_dir))
                                products[product]['work_dir'] = None
                                continue

                            # find the directory created by the tar file and update the work_dir
                            work_dir_prefix = os.path.join(products[product]['media_dir'], product)
                            work_dir_contents = os.listdir(work_dir_prefix)

                            if work_dir_contents:
                                products[product]['work_dir'] = os.path.join(media_dir, product, work_dir_contents[0])

                            # take note that the md5 sum matched
                            products[product]['archive_check'] = 'passed'

                        else:

                            # there is a problem with the archive or sum
                            install_logger.error("    distribution sum doesn't match archive sum!")
                            install_logger.error('    distribution {}'.format(dist_sum))
                            install_logger.error('    archive_sum {}'.format(checked_sum))
                            install_logger.error('    skipping extraction of {}'.format(archive))

                            # take note that the md5 sum did NOT match
                            products[product]['archive_check'] = 'failed'

                    else:

                        # extract tarfile
                        install_logger.info('    extracting {}'.format(archive))

                        try:
                            cmd = 'tar x{}af {} --directory {}'.format(extra_tar_flags, archive, work_dir)
                            connection.sudo(cmd)
                        except:
                            install_logger.warning('skipping {}'.format(product))
                            # remove dir to avoid thinking this is a valid workdir
                            connection.sudo('rm -Rf {}'.format(work_dir))
                            products[product]['work_dir'] = None
                            continue

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
                    install_logger.debug('found previously extracted work_dir {}'.format(product))

            else:
                install_logger.debug('no archive for {}'.format(product))

    # compute the version for each product
    install_logger.debug('determining version for products')
    for product in products:
        working_name = None
        working_version = None
        pattern = r'(\D+)(\d+.*)'
        results = re.findall(pattern, product)
        install_logger.debug('regex product {}, results {}'.format(product, results))
        if results:
            if len(results[0]) == 2:
                working_name = results[0][0].strip('-')
                working_version = results[0][1]
                for prefix in prefixes:
                    if working_name.lower() == prefix.lower():
                        install_logger.debug('working_name {} matched prefix {}'.format(
                            working_name, prefix))
                        products[product]['version'] = working_version
        # see if an alternate version is used for gitea
        work_dir = products[product]['work_dir']
        if work_dir:
            manifest_dir = os.path.join(products[product]['work_dir'], 'manifests')
            if os.path.isdir(manifest_dir):
                files = os.listdir(manifest_dir)
                for file_name in files:
                    if file_name.endswith('.yaml'):
                        yaml_file = os.path.join(manifest_dir, file_name)
                        with open(yaml_file, 'r') as fhandle:
                            try:
                                yaml_data = yaml.safe_load(fhandle)
                            except Exception as err:
                                yaml_data = None
                        if yaml_data:
                            try:
                                if yaml_data['spec']['charts']:
                                    chart_data = yaml_data['spec']['charts']
                                    for chart in chart_data:
                                        import_job = chart['values']['cray-import-config']['import_job']
                                        import_version = import_job['CF_IMPORT_PRODUCT_VERSION']
                                        products[product]['import_version'] = import_version
                            except Exception as err:
                                pass

    install_logger.info('    OK')
    return products


def get_os():
    """
    return the current os release
    """
    with open('/etc/os-release', 'r') as os_file:
        os_contents = os_file.readlines()
    release = None
    for line in os_contents:
        if line.startswith('VERSION='):
            release = line.split('"')[1]
    return release


def elapsed_time(start_time):
    """
    return elapsed time in H:M:S format
    """
    dt_diff = datetime.datetime.now() - start_time
    seconds_waited = int(dt_diff.total_seconds())
    time_waited = str(datetime.timedelta(seconds=seconds_waited))

    return time_waited
