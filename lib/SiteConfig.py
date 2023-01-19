

import copy
from distutils.version import LooseVersion
import os
import re
import sys
import yaml

import lib.git as git
import jinja2

from lib.vars import RECIPE_VARS, BP_CONFIG_MANAGED, BP_CONFIG_MANAGEMENT, MEDIA_VERSIONS, UnexpectedState

from lib.InstallerUtils import get_product_catalog, formatted

from lib.InstallLogger import get_install_logger

install_logger = get_install_logger(__name__)

def read_yaml(file_loc):
    """Read a yaml file and return the dictionary."""

    return_dict = {}
    if os.path.exists(file_loc):
        with open(file_loc, "r", encoding='UTF-8') as fhandle:
            return_dict = yaml.load(fhandle, yaml.SafeLoader)
    else:
       raise FileNotFoundError(f"Could not find file {file_loc}")

    return return_dict

def highestVersion(versions_list):
    sorted_vs = sorted(versions_list, key=LooseVersion)
    return sorted_vs[-1]

class SiteConfig():
    def __init__(self, config):

        # recipe_vars <==> product_vars; versions defined for sat.
        self.recipe_vars = {}

        # Customer-supplied parameters
        self.site_vars = {}

        # Interpolated variables, passed to the api.
        self._site_params = {"global": {}, "products": {}}
        self.session_vars = {}
        self.pre_rendered = {}
        self.rendered = {}
        self.bp_managed = []
        self.bp_management = []
        self.product_catalog = {}
        self.mask_recipe_prods = []
        self.state_dir = config.args.get("state_dir")
        self.sv_path = os.path.join(self.state_dir, "session_vars.yaml")
        self.media_versions_path = os.path.join(self.state_dir, MEDIA_VERSIONS)
        self.bpcd = config.args.get("bootprep_config_dir", None)
        self.git = git.Git(config)
        self.stage_enum = config.stages.stage_enum

        errors = []
        # Get the product catalog as the base layer.
        full_product_catalog = get_product_catalog(config, all_products=True)
        for prod in full_product_catalog:
            versions = [elt for elt in list(full_product_catalog[prod].keys()) if elt]

            self.product_catalog[prod] = {"version": highestVersion(versions)}

        recipe_vars_file = config.args.get("recipe_vars", None)

        site_vars_file = config.args.get("site_vars", None)
        tmp_mask = config.args.get("mask_recipe_prods", [])
        if tmp_mask:
            self.mask_recipe_prods = tmp_mask

        if any([var for var in [recipe_vars_file, site_vars_file,
                self.bpcd]]):
            if self.bpcd and os.path.exists(self.bpcd):
                rv_loc = os.path.join(self.bpcd, RECIPE_VARS)
                managed_loc = os.path.join(self.bpcd, "bootprep", BP_CONFIG_MANAGED)
                management_loc = os.path.join(self.bpcd, "bootprep", BP_CONFIG_MANAGEMENT)

                # Don't flag an error if the individual files in bpcd don't exist.
                # They may exist in the individual file arguments.
                if not self.recipe_vars:
                    self.recipe_vars = read_yaml(rv_loc)
                if os.path.exists(managed_loc):
                    self.bp_managed.append(read_yaml(managed_loc))
                if os.path.exists(management_loc):
                    self.bp_management.append(read_yaml(management_loc))
            elif self.bpcd:
                errors.append("The `-bpcd/--bootprep-config-dir {}` was specified but could not be found".format(self.bpcd))

            # If `--recipe-vars` was specified on the commandline, allow it to
            # override what was specified with the `--bootprep-config-dir` option.
            if recipe_vars_file and os.path.exists(recipe_vars_file):
                self.recipe_vars = read_yaml(recipe_vars_file)
            elif recipe_vars_file:
                errors.append("`--recipe-vars {}` was specified but the file could not be found!".format(recipe_vars_file))

            if site_vars_file and os.path.exists(site_vars_file):
                self.site_vars = read_yaml(site_vars_file)
            elif site_vars_file:
                errors.append("`--site-vars/-vp {}` was specified but the file could not be found".format(site_vars_file))

        if errors:
            install_logger.error("Problems were encountered:")
            counter = 1
            for err in errors:
                install_logger.error("\t{}: {}".format(counter, err))
                counter += 1
            sys.exit(1)


    def merge_dicts(self, list_of_dicts):
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
                    if type(dict1[elt]) == dict or type(dict2[elt]) == dict:
                        merge_two_dicts(dict1[elt], dict2[elt])
                    else:
                        dict1[elt] = dict2[elt]
                else:
                    dict1[elt] = dict2[elt]

        ret_dict = copy.deepcopy(list_of_dicts[0])
        for dict_i in list_of_dicts[1:]:
            adict = copy.deepcopy(dict_i)
            for elt in adict:
                if elt in ret_dict:
                    if type(adict[elt]) == dict or type(ret_dict[elt]) == dict:
                        merge_two_dicts(ret_dict[elt], adict[elt])
                    else:
                        ret_dict[elt] = adict[elt]
                else:
                    ret_dict[elt] = adict[elt]

        return ret_dict

    def mask_prods(self):
        if self.recipe_vars:
            for prod in self.mask_recipe_prods:
                # Remove the products specified in `--mask-recipe-prods` from
                # recipe_vars.
                self.recipe_vars.pop(prod, None)


    def organize_merge(self):
        """Merge the dictionaries in the following order:
            1.  Product Catalog
            2.  recipe_vars <==>product_vars
            3.  site_vars – customer/site info
            4.  session_vars – What we get from process-media

            When merging/moving the data around, use copy.deepcopy where
            necessary to preserve the condition of the original dicts
            (recipe_vars, site_vars, etc).  The only dictionaries modified
            from their original state are self.rendered and
            self.pre_rendered.
        """

        # 1. The product catalog
        self.pre_rendered = copy.deepcopy(self.product_catalog)

        # 2.  recipe_vars / product_vars.
        if not (self.recipe_vars or self.bpcd):
            # Clone the repo.

            repo = "hpc-csm-software-recipe"
            versions_dict = {}
            clone_loc = self.git.clone(repo)
            version_re = re.compile("(\d+\.\d+.*$)")
            branches = self.git.ls_remote(repo, just_branches=True)
            for branch in branches:
                parts = branch.split('/')
                if len(parts) < 1:
                    continue
                vers_match = version_re.search(parts[-1])
                if vers_match:
                    version = vers_match.group(1)
                    versions_dict[version] = branch
            if versions_dict:
                highest_version = highestVersion(versions_dict.keys())
                co_branch = versions_dict[highest_version]
            else:
                # We shouldn't hit this with the hpc-csm-software-recipe repo.
                # If that changes, we could get a lot more elaborate and check
                # for integration and master branches.
                co_branch = branches[0]
                install_logger.warning(f"Couldn't find a versioned branch for {repo}.  Assuming branch {co_branch}.")
            self.git.checkout("hpc-csm-software-recipe", co_branch)

            recipe_file = os.path.join(clone_loc, RECIPE_VARS)
            if os.path.exists(recipe_file):
                msg = f"""Neither --recipe-vars nor --bootprep-config-dir
                were specified, so {RECIPE_VARS}  will pulled from the
                branch {co_branch} of the {repo} git repo."""
                install_logger.info(formatted(msg))
                self.recipe_vars = read_yaml(recipe_file)
            else:
                msg = f"""Could not find vcs/{RECIPE_VARS} on branch
                {co_branch} within the {repo} repo. If one is desired, it
                can be specified with the `--bootprep-config-dir` or
                `--recipe-vars` arguments."""
                install_logger.warning(msg)

        if self.recipe_vars:
            self.mask_prods()
            self.pre_rendered = self.merge_dicts([self.pre_rendered, self.recipe_vars])

        # 3. Site vars -- configuration specified by the site.
        if self.site_vars:
            self.pre_rendered = self.merge_dicts([self.pre_rendered, self.site_vars])

    def manage_session_vars(self, session_vars, write=False):
        if session_vars:
            self.session_vars = session_vars
        elif os.path.exists(self.media_versions_path):
            self.session_vars = read_yaml(self.media_versions_path)

        self.pre_rendered = self.merge_dicts([self.pre_rendered, self.session_vars])
        self.interpolate_jinja()

        # This is necessary when running process-media in one run, and then
        # running subsequent stages -- we won't know which versions of products
        # were installed.
        if write and self.session_vars:
            with open(self.media_versions_path, "w") as fhandle:
                yaml.dump(self.session_vars, fhandle)

        with open(self.sv_path, "w") as fhandle:
            install_logger.info("Dumping rendered site variables to {}".format(self.sv_path))
            yaml.dump(self.rendered, fhandle)

    def update_dict_stack(self, stage):
        deliver_stage = self.stage_enum["deliver-product"]
        if stage in self.stage_enum:
            stage_index = self.stage_enum[stage]
        else:
            stage_index = next(reversed(self.stage_enum))
            msg = f"""Unknown stage '{stage}' -- assuming we should update
            the dictionary stack; but the larger problem is probably the
            unknown {stage} stage."""
            install_logger.warning(msg)
        if stage_index >= deliver_stage:
            self.organize_merge()
            self.manage_session_vars(self.session_vars)

    @property
    def site_params(self):
        if self._site_params["products"]:
            return self._site_params
        else:
            self.rendered = read_yaml(self.sv_path)
            self._site_params = {"global": {}, "products": self.rendered}
            return self._site_params

    @property
    def session_vars_path(self):
        return self.sv_path

    def interpolate_jinja(self):
        """Loop through the pre-rendered dictionary and interpolate the jinja variables.
        The input dictionary will contain jinja strings similar to:
            'cos': {'version': '2.4.89', 'working_branch': 'mycos-{{version_x_y_z}}'},
            'cpe': {'version': '22.10.4', 'working_branch': '{{ working_branch }}'},
        The jinja will be interpreted, and may be rendered as follows:
                'cos': {'version': '2.4.89', 'working_branch': 'mycos-2.5.99'},
                'cpe': {'version': '22.10.4', 'working_branch': 'integration-22.06'},
        """
        pre_rendered_defaults = {}
        if "default" in self.pre_rendered:
            pre_rendered_defaults = self.pre_rendered["default"]

        for item in self.pre_rendered.items():

            # build a set of name/version variables to use
            # with jinja substitution
            name, data = item
            version = None

            # don't render defaults
            if name in ['default']:
                continue

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
                except IndexError:
                    pass
                version_x_y = "{}.{}".format(x, y)
                version_x_y_z = "{}.{}.{}".format(x, y, z)
            except KeyError:
                pass
            except Exception as err:
                install_logger.warning("Unhandled exception {}, err={}".format(err.__class__.__name__, err))

            # represent product as a yaml formatted string
            product_defaults = str(yaml.dump(pre_rendered_defaults))
            product_type = name if name != 'shs' else "slingshot-host-software"
 
            # create the jinja template
            t = jinja2.Template(product_defaults)

            # render with name and version info
            rendered_string = t.render(
                name=name,
                product_type=product_type,
                version=version,
                version_x_y=version_x_y,
                version_x_y_z=version_x_y_z,
                )

            rendered_defaults = yaml.safe_load(rendered_string)

            for var in ['working_branch', 'network_type', 'wlm']:
                if var not in rendered_defaults:
                    rendered_defaults[var] = ""

            stanza = str(yaml.dump({ name: data }))
            t = jinja2.Template(stanza)

            # Render with the name/version variables.
            try:
                rendered_string = t.render(
                    name=name,
                    product_type=product_type,
                    version=version,
                    version_x_y=version_x_y,
                    version_x_y_z=version_x_y_z,
                    working_branch=rendered_defaults["working_branch"],
                    network_type=rendered_defaults["network_type"],
                    wlm=rendered_defaults["wlm"]
                    )
            except KeyError as err:
                install_logger.warning("{} not found in Site Params".format(err))

            # convert back to a dict
            rendered_stanza = yaml.safe_load(rendered_string)

            # add dict back into the rendered config
            self.rendered.update(rendered_stanza)
        self._site_params["products"] = self.rendered
