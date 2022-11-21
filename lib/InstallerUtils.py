# Copyright 2022 Hewlett Packard Enterprise Development LP

"""
Common utility and helper functions used by the CI.
"""

import datetime
import json
import os
import re
import textwrap
import shutil
import yaml
import prettytable
import lib.Products
import rpm

from lib.InstallLogger import get_install_logger

install_logger = get_install_logger(__name__)
# pylint: disable=consider-using-f-string

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


def formatted(text):
    """Format a text string for a standard 80-line terminal."""
    wrapper = textwrap.TextWrapper(width=78)
    raw = textwrap.dedent(text).strip()
    msg = wrapper.fill(text=raw)
    return msg


def get_ims_public_key(config):
    created_public_keys = json.loads(config.connection.sudo("cray ims public-keys list --format json", dryrun=False).stdout)
    inst_pkey_list = [k for k in created_public_keys if k["name"] == "installer_public_key"]
    rsa_pub = os.path.join(os.path.expanduser("~"), ".ssh", "id_rsa.pub")
    if len(inst_pkey_list) <= 0:
        pkey_dict = json.loads(config.connection.sudo('cray ims public-keys create --name "installer_public_key" --format json --public-key {}'.format(rsa_pub)).stdout)
        public_key = pkey_dict
    else:
        public_key = inst_pkey_list[0]
    ims_public_key_id = public_key['id']

    return ims_public_key_id


def get_hosts(config, host_str, exact_match=True):
    """
    Get hosts matching a string; for example
    'get_hosts(connection, "ncn-w")' will get all worker nodes.
    """

    components_list = json.loads(config.connection.sudo("cray hsm state components list --type node --format json").stdout)

    xnames = [clist["ID"] for clist in components_list["Components"]]
    hosts = []
    def ncn_sort(tup):
        return tup[1]

    for xname in xnames:
        hw_desc = json.loads(config.connection.sudo('cray sls hardware describe {} --format json'.format(xname)).stdout)
        try:
            alias = hw_desc["ExtraProperties"]['Aliases'][0]
        except KeyError:
            pass
        if exact_match:
            if host_str in [xname, alias]:
                hosts.append((xname, alias))
        elif host_str in xname or host_str in alias:
            hosts.append((xname, alias))

    return sorted(hosts, key=ncn_sort)


def get_ncn_tuples(config, just_workers=False):
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
        if config.args.get("worker_nodes", False):
            all_ncn_tuples = []
            for node in config.args["worker_nodes"]:
                all_ncn_tuples += get_hosts(config, node)
        else:
            w_ncn_tuples = get_hosts(config, "ncn-w", exact_match=False)
            all_ncn_tuples =  w_ncn_tuples
    else:
        w_ncn_tuples = get_hosts(config, "ncn-w", exact_match=False)
        m_ncn_tuples = get_hosts(config, "ncn-m", exact_match=False)
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


def get_product_catalog(config, all_products=False):
    """
    update product dictionary with gitea urls
    """
    install_logger.debug('determining config-management url for products')
    if not config.all_product_data:
        # get full data for initial image_id
        command = 'kubectl --kubeconfig=/etc/kubernetes/admin.conf get cm -n services cray-product-catalog -o json'
        product_cat_json = config.connection.sudo(command, dryrun=False).stdout
        product_cat  = json.loads(product_cat_json)
        all_product_data = product_cat['data']
        config.all_product_data = all_product_data
    else:
        all_product_data = config.all_product_data

    if all_products:
        return all_product_data

    for product in config.location_dict:
        product_version = product.best_version
        install_logger.debug('using product_version %s', product_version)
        try:
            working_type = product.product
            product_data = yaml.safe_load(all_product_data[working_type])
            matching_versions = []
            # find all keys in the product catalog that match the supplied product
            if product_version in product_data.keys():
                install_logger.debug('%s is an exact match', product_version)
                matching_versions.append(product_version)
            else:
                install_logger.debug('looking for matching version')
                for item in product_data.keys():
                    if str(product_version).startswith(str(item)):
                        matching_versions.append(item)
            # if there is only one match, we consider this the "real" version
            if len(matching_versions) == 1:
                working_version = matching_versions[0]
                product.product_version = working_version
                install_logger.debug('found exact version %s for %s', working_version, product)
                product_catalog = product_data[working_version]
                try:
                    product.clone_url = product_catalog['configuration']['clone_url']
                    product.import_branch = product_catalog['configuration']['import_branch']
                    if "recipes" in product_catalog:
                        product.recipe = list(product_catalog['recipes'].keys())[0]
                except Exception:
                    # even if we have an exact match in the product catalog, there may
                    # not be git information associated with that product entry
                    working_version = max(product_data.keys())
                    install_logger.debug('no product catalog config data, trying %s for %s', working_version, product)
                    product_catalog = product_data[working_version]
                    product.clone_url = product_catalog['configuration']['clone_url']
                    if "recipes" in product_catalog:
                        product.recipe = list(product_catalog['recipes'].keys())[0]
            else:
                # if there is no exact match, attempt to get the clone_url anyway since
                # that doesn't change between product versions
                working_version = max(product_data.keys())
                install_logger.debug('no exact version in %s, using %s for %s', matching_versions, working_version, product)
                product_catalog = product_data[working_version]
                product.clone_url = product_catalog['configuration']['clone_url']
                if "recipes" in product_catalog:
                    product.recipe = list(product_catalog['recipes'].keys())[0]

        except Exception:
            install_logger.debug('unable to get all config-management data for %s', product)


def get_products( config,
                  extract_archives = True,
                  products = None,
                  prefixes = None,
                  suffixes = None ):
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
        if product.product:
            work_dir = product.work_dir
            if work_dir:
                installer = os.path.join(work_dir, 'install.sh')

    """

    products = lib.Products.Products()
    media_dir = config.args["media_dir"]

    if not prefixes:
        prefixes = products.prefixes()

    if not suffixes:
        suffixes = { 'md5': '.tar.gz.MD5.TXT',
                     'tgz': '.tar.gz',
                     'tar': '.tar',
                     'out': '.tar.gz.OUT.TXT' }

    # since media_dir defaults to PWD, let's just ignore
    # installer files
    SKIP_FILES=[
        "cne-install",
        "install.log",
        "utils",
        "hpe",
        "location_dict.yaml"
        ]

    # convert to absolute to avoid ambiguity
    media_dir = os.path.abspath(media_dir)

    # let user know what we are working on
    install_logger.info('  processing media_dir %s', media_dir)

    # get contents of our media directory
    directory_listing = os.listdir(media_dir)

    install_logger.debug('dir contents: %s', directory_listing)

    # process each item in the directory
    for item in directory_listing:

        install_logger.debug('processing %s', item)
        if item in SKIP_FILES:
            install_logger.trace('skipping installer support file %s', item)
            continue

        item_name = None

        # logic to handle item if it is a file
        if os.path.isfile(os.path.join(media_dir, item)):

            # we process suffix first because we want to determine a
            # key name that does not include a suffix so all related
            # files such as md5 files can be stored in the same record

            for suffix in suffixes:

                install_logger.trace('suffix %s', suffix)

                if item.endswith(suffixes[suffix]):

                    install_logger.trace('suffix match %s', suffix)
                    item_name = item.split(suffixes[suffix])[0]
                    install_logger.debug('item_name is %s', item_name)

                    product = products.get(item_name)

                    # handle archive and non-archive entries differently
                    if suffix in ['md5', 'out']:
                        install_logger.debug('item is not an archive')
                        install_logger.debug('adding %s, %s', suffix, item)

                        # read md5 file, parse out and save the md5 sum
                        if suffix == 'md5':
                            install_logger.debug('reading md5 file %s', item)
                            # TODO: exception handling, please
                            with open(os.path.join(media_dir, item), 'r', encoding='UTF-8') as md5_file:
                                md5_contents = md5_file.readlines()
                                # TODO: improve this logic
                                md5 = md5_contents[0].split()[0]
                            product.md5 = md5
                        else:
                            product.out = item
                    else:
                        # record archive information
                        install_logger.debug('adding archive_type %s', suffix)
                        install_logger.debug('adding archive %s', item)
                        product.archive_type = suffix
                        product.archive = item

            # if there is no suffix, then just use the item name
            if not item_name:

                install_logger.debug('setting item_name to %s', item)
                install_logger.debug('creating new product %s', item_name)
                install_logger.debug('setting archive to %s', item)

                product = products.get(item)
                product.archive = item

        # item is a directory
        elif os.path.isdir(os.path.join(media_dir, item)):

            item_name = item
            product = products.get(item_name)

            # find the directory created by the tar file and update the work_dir
            work_dir_prefix = os.path.join(media_dir, item_name)
            work_dir_contents = os.listdir(work_dir_prefix)

            # we expect only one directory in the work_dir
            if len(work_dir_contents) == 1 and os.path.isdir(os.path.join(media_dir, item_name, work_dir_contents[0])):
                product.work_dir = os.path.join(media_dir, item_name, work_dir_contents[0])
            else:
                install_logger.warning('    work_dir contents of %s unexpected', item)

            install_logger.info('    found existing work_dir for %s', item)
            install_logger.debug('processing directory %s', item)
            install_logger.debug('new product %s', item_name)

            # FIXME: Is the debug line below correct?  it doesn't look like we're setting work_dir to anything
            install_logger.debug('setting work_dir to %s', item_name)

        # file is of a type that isn't relevant for us
        else:
            product = products.get(item)
            product.archive = item
            install_logger.debug('item is not a file or directory')
            install_logger.debug('new product %s', item)
            install_logger.debug('setting archive to %s', item)

        for prefix in prefixes:
            if item.startswith(prefix) and item not in ['cne-install', 'install.log']:
                product_type = prefixes[prefix]
                product.product = product_type
                install_logger.debug('prefix match found %s', prefixes[prefix])

        product.media_dir = media_dir

    if extract_archives:

        for product in products:

            install_logger.debug('checking to see if %s needs to be unpacked', product.name)
            install_logger.debug('products[product]["product"] is "%s"', product.product)

            # only process items that are identified products
            # don't bother trying to extract something with no archive
            if product.product and product.archive:

                install_logger.trace('inside product test')

                # only process items that have no work_dir (haven't been extracted yet)
                if not product.work_dir:

                    install_logger.trace('needs workdir')
                    work_dir = os.path.join(product.media_dir, product.name)


                    archive = os.path.join(product.media_dir, product.archive)

                    if product.md5:
                        install_logger.info('    validating md5 sum for %s', product.archive)
                        if product.archive_check == 'passed':
                            install_logger.info('      sum validates %s', product.archive_md5)
                        else:
                            # there is a problem with the archive or sum
                            install_logger.error("    distribution sum doesn't match archive sum!")
                            install_logger.error('    distribution %s', product.md5)
                            install_logger.error('    archive_sum %s', product.archive_md5)
                            install_logger.error('    skipping extraction of %s', archive)
                            continue

                    if config.dryrun:
                        install_logger.dryrun('    skipping extraction of %s', archive)
                        continue

                    # extract tarfile
                    install_logger.info('    extracting %s', archive)

                    try:
                        # make dir
                        if not os.path.isdir(work_dir):
                            os.makedirs(work_dir)

                        shutil.unpack_archive(archive, work_dir)
                    except:
                        # if tar fails, remove work dir
                        install_logger.warning('    unable to process %s', product.name)
                        # remove dir to avoid thinking this is a valid workdir
                        shutil.rmtree(work_dir)
                        product.work_dir = None
                        continue

                    # find the directory created by the tar file and update the work_dir
                    work_dir_contents = os.listdir(work_dir)

                    if work_dir_contents:
                        product.work_dir = os.path.join(work_dir, work_dir_contents[0])

                else:
                    # find the directory created by the tar file and update the work_dir
                    work_dir_prefix = os.path.join(product.media_dir, product.name)
                    work_dir_contents = os.listdir(work_dir_prefix)

                    if work_dir_contents:
                        product.work_dir = os.path.join(work_dir_prefix, work_dir_contents[0])
                    install_logger.debug('found previously extracted work_dir %s', product.name)

            else:
                install_logger.debug('no archive for %s', product.name)

    # compute the version for each product
    install_logger.debug('determining version for products')
    for product in products:
        working_name = None
        working_version = None
        pattern = r'(\D+)(\d+.*)'
        results = re.findall(pattern, product.name)
        install_logger.debug('regex product %s, results %s', product.name, results)
        if results:
            if len(results[0]) == 2:
                working_name = results[0][0].strip('-')
                working_version = results[0][1]
                for prefix in prefixes:
                    if working_name.lower() == prefix.lower():
                        install_logger.debug('working_name %s matched prefix %s',
                            working_name, prefix)
                        product.version = working_version
        # see if an alternate version is used for gitea
        work_dir = product.work_dir
        if work_dir:
            manifest_dir = os.path.join(work_dir, 'manifests')
            if os.path.isdir(manifest_dir):
                files = os.listdir(manifest_dir)
                for file_name in files:
                    if file_name.endswith('.yaml'):
                        yaml_file = os.path.join(manifest_dir, file_name)
                        with open(yaml_file, 'r', encoding='UTF-8') as fhandle:
                            try:
                                yaml_data = yaml.safe_load(fhandle)
                            except Exception:
                                yaml_data = None
                        if yaml_data:
                            try:
                                if yaml_data['spec']['charts']:
                                    chart_data = yaml_data['spec']['charts']
                                    for chart in chart_data:
                                        import_job = chart['values']['cray-import-config']['import_job']
                                        import_version = import_job['CF_IMPORT_PRODUCT_VERSION']
                                        product.import_version = import_version
                            except Exception:
                                pass

    install_logger.info('    OK')
    return products

def get_os():
    """
    return the current os release
    """
    with open('/etc/os-release', 'r', encoding='UTF-8') as os_file:
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
