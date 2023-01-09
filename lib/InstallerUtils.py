# Copyright 2022 Hewlett Packard Enterprise Development LP

"""
Common utility and helper functions used by the CI.
"""

import datetime
import jinja2
import json
import textwrap
import yaml
import prettytable

from lib.InstallLogger import get_install_logger
from lib.vars import UnexpectedState

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
        all_product_data[item] = yaml.safe_load(val)

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


def merge_dicts(list_of_dicts, overwrite=False):
    """
    Merge a list of dicts and return a dict.
    If the dicts look like:
        [
        {val0: a, val1: b},
        {val0: b, val1: b, val2: c},
        ]
    and overwrite is False, the end result would be
        {val0: a, val1: b, val2: c}

    If overwrite is True, the resulting dict would look like
        {val0: b, val1: b, val2: c}
    """

    if type(list_of_dicts) != list:
        raise UnexpectedState("merge_dicts called with '{}'.  Expected a list.".format(list_of_dicts))

    def merge_two_dicts(dict1, dict2):
        for elt in dict2:
            if elt in dict1:
                if type(dict2[elt]) == dict:
                    merge_two_dicts(dict1[elt], dict2[elt])
                else:
                    dict1[elt] = dict2[elt]

    ret_dict = list_of_dicts[0]
    for adict in list_of_dicts[1:]:
        for elt in adict:
            if overwrite:
                if type(adict[elt]) == dict and elt in ret_dict:
                    merge_two_dicts(ret_dict[elt], adict[elt])
                else:
                    ret_dict[elt] = adict[elt]
            else:
                for key in adict:
                    if key not in ret_dict.keys():
                        ret_dict[key] = adict[key]
    return ret_dict


def render_jinja2(pre_render_vars):

    rendered_config = {}
    for item in pre_render_vars.items():

        # build a set of name/version variables to use
        # with jinja substitution
        name, data = item
        version = None

        # don't render defaults
        if name in ['defaults']:
            continue;

        try:
            # version can't be null, so make sure we
            # end up with a version if it isn't present
            # or isn't valid
            x = y = z = 0
            version_x_y = "{}.{}".format(x, y)
            version_x_y_z = "{}.{}.{}".format(x, y, z)
            version = data['version']
            version_list = version.split(".")
            try:
                x = version_list[0].split("-")[0]
                y = version_list[1].split("-")[0]
                z = version_list[2].split("-")[0]
            except Exception as err:
                pass
            version_x_y = "{}.{}".format(x, y)
            version_x_y_z = "{}.{}.{}".format(x, y, z)
        except Exception as err:
            pass

        # represent product as a yaml formatted string
        stanza = str(yaml.dump({ name: data }))

        # create the jinja template
        t = jinja2.Template(stanza)

        # product_type is just the name of the product for everything except
        # slingshot-host-software.  If this turns out to --not-- be the case,
        # we might need a dict defining product_type.  Another options might
        # be to get rid of product_type.
        product_type = name if name != 'shs' else "slingshot-host-software"

        # render with the name/version variables
        rendered_string = t.render(
            name=name,
            product_type=product_type,
            version=version,
            version_x_y=version_x_y,
            version_x_y_z=version_x_y_z
            )

        # convert back to a dict
        rendered_stanza = yaml.safe_load(rendered_string)

        # add dict back into the rendered config
        rendered_config.update(rendered_stanza)

    return rendered_config

def elapsed_time(start_time):
    """
    return elapsed time in H:M:S format
    """
    dt_diff = datetime.datetime.now() - start_time
    seconds_waited = int(dt_diff.total_seconds())
    time_waited = str(datetime.timedelta(seconds=seconds_waited))

    return time_waited
