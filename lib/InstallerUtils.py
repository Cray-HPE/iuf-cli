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
"""
Common utility and helper functions used by the CI.
"""

import datetime
import json
import textwrap
import yaml
import prettytable

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
    wrapper = textwrap.TextWrapper(width=78, break_on_hyphens=False, break_long_words=False)
    raw = textwrap.dedent(text).strip()
    msg = wrapper.fill(text=raw)
    return msg

def format_column(message):
    # pad the mssage to 57 characters then return the first 57 characters to truncate longer values
    padded = f"{message:57}"[0:57]

    return(f"[{padded}]")

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

    for item in all_product_data:
        val = all_product_data[item]
        try:
            all_product_data[item] = yaml.safe_load(val)
        except AttributeError:
            all_product_data[item] = val

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

def elapsed_time(start_time, to_str=True):
    """
    return elapsed time in H:M:S format
    """
    dt_diff = datetime.datetime.now() - start_time
    seconds_waited = int(dt_diff.total_seconds())
    time_waited = str(datetime.timedelta(seconds=seconds_waited))

    if to_str:
        return time_waited
    else:
        return seconds_waited
