#!/usr/bin/env python3

"""This module contains the staging information and functionality."""

from collections import OrderedDict
import copy
import datetime
import os
import sys
from prettytable import PrettyTable
import yaml
from utils.InstallLogger import get_install_logger
from utils.InstallerUtils import elapsed_time, formatted

import utils.ShastaUpdate as supdate
from utils.vars import RunException, STAGE_HIST_FILENAME

install_logger = get_install_logger(__name__)

# pylint: disable=consider-using-f-string

def format_tabbed_list(alist):
    """Format a list into text that's easier to read.  This is useful when
    a list needs to be displayed to a user."""
    retstr = ""
    for elt in alist:
        retstr += "  {}\n".format(elt)

    return retstr

STAGES_DICT = OrderedDict({
    "process_product_media": {
        "func" : supdate.get_prods,
        "description" : "Inventory and extract products in the media directory for use in subsequent stages."
        },
    "validate_products":  {
        "func" : supdate.validate_products,
        "description" : "Perform product sanity checks."
        },
    "install_products": {
        "func" : supdate.install,
        "description" : "Install products identified in the process_product_media stage."
        },
    "verify_product_import": {
        "func" : supdate.verify_product_import,
        "description" : "Verify all product import PODS and Jobs have completed."
        },
    "verify_product_install": {
        "func" : supdate.verify_product_install,
        "description" : "Verify product installation by running product validations"
        },
    "update_working_branches": {
        "func" : supdate.update_working_branches,
        "description" : formatted("""
                        Update config managment branches identified by --working-branch with the product
                        release branch content for each product being installed that contains a
                        config management repo.  See help for --working-branch for details on
                        setting the working branch.""")
        },
    "update_ncn_config": {
        "func" : supdate.update_ncn_config,
        "description" : formatted("""
                        Update the NCN personalization configuration.  Defaults to 'ncn-personalization',
                        use --ncn-personalization to over-ride.""")
        },
    "worker_health_check": {
        "func":  supdate.worker_health_check,
        "description": "Check the health of the workers prior to beginning NCN Personalization."
        },
    "ncn_personalization": {
        "func" : supdate.ncn_personalization,
        "description" : "Perform NCN personalization."
        },
    "unload_dvs_and_lnet": {
        "func" : supdate.unload_dvs_and_lnet,
        "description" : "Perform a rolling unload, upgrade and reload of DVS and LNET on worker nodes."
        },
    "check_services": {
        "func" : supdate.check_services,
        "description" : "Check CPS, DVS, LNET. and NMD services."
        },
    "create_cos_cfs_config": {
        "func" : supdate.create_cos_cfs_config,
        "description" : "Write the CFS config used for building the COS image."
        },
    "create_bootprep_config": {
        "func" : supdate.create_bootprep_config,
        "description" : "Generate the `sat bootprep` input config."
        },
    "sat_bootprep": {
        "func" : supdate.sat_bootprep,
        "description" : "Run `sat bootprep`."
        },
})

class StageHist:
    def __init__(self, state_dir=None):
        self._stages = list(STAGES_DICT.keys())
        self._status = {}
        self.stage_hist_file = None
        if state_dir is None:
            # This will happen on an 'ls'
            for stage in self._stages:
                self.update(stage, False, False, None)

            return

        self.stage_hist_file = os.path.join(state_dir, STAGE_HIST_FILENAME)
        if os.path.exists(self.stage_hist_file):
            with open(self.stage_hist_file, "r", encoding='UTF-8') as fhandle:
              self._status = yaml.full_load(fhandle)
              return

        for stage in self._stages:
            self._status[stage] = {
                "ran": False,
                "succeeded": False,
                "duration": None,
            }

        with open(self.stage_hist_file, "w", encoding='UTF-8') as fhandle:
            yaml.dump(self._status, fhandle)

    def create_new_status(self):
          for stage in self._stages:
                self._status[stage] = {
                    "ran": False,
                    "succeeded": False,
                    "duration": None,
                }

    def load_status(self):
        with open(self.stage_hist_file, "r", encoding='UTF-8') as fhandle:
              self._status = yaml.full_load(fhandle)

    def dump_status(self):
        if self.properInit:
            with open(self.stage_hist_file, "w", encoding='UTF-8') as fhandle:
                yaml.dump(self._status, fhandle)



    def update(self, stage, ran=False, succeeded=False, duration=None):
        """Update a stage status."""

        self._status[stage] = {
            "ran": ran,
            "succeeded": succeeded,
            "duration": duration
        }
        self.dump_status()

    def set_hist(self, state_dir):
        if not self.stage_hist_file:
            self.stage_hist_file = os.path.join(state_dir, STAGE_HIST_FILENAME)
            if not os.path.exists(self.stage_hist_file):
                self.create_new_status()
            else:
                self.load_status()

    @property
    def properInit(self):
        """
        Indicate whether or not this class has been fully initialized.
        Since a user could simply do an `ls` without having ran anything else,
        we can't fully initialize until we've executed a stage.
        """
        if self.stage_hist_file:
            return True
        else:
            return False

class Stages:
    """Staging information and methods."""
    def __init__(self):
        self._stage_dict = copy.deepcopy(STAGES_DICT)
        self._stages = list(self._stage_dict.keys())
        self._all_stages = self._stages
        self.installer_start = datetime.datetime.now()
        self.stage_hist = StageHist()
        self.skip_stages = []

    def set_hist(self, state_dir):
        """Set up the history file"""
        self.stage_hist.set_hist(state_dir)

    def get(self, long=False, all_stages=True, status=False, list_fmt=False):
        """Get either a printable list and description/status of stages, or return a list
            of stages."""
        if list_fmt:
            if all_stages:
                return self._all_stages
            else:
                return self._stages

        name_row = ["stage"]
        table = PrettyTable()
        if long:
            name_row += ["description"]
        if status:
            name_row += ["status", "duration"]
        table.field_names = name_row
        for stage in self._stage_dict.keys():
            val_row = [stage]

            #Add the description if long was specified.
            if long:
                description = self._stage_dict[stage].get("description", False)
                val_row += [description]

            # Print the status and duration if status was specified.
            if status:
                ran = self.stage_hist._status[stage]["ran"]
                succeeded = self.stage_hist._status[stage]["succeeded"]
                duration = self.stage_hist._status[stage]["duration"]
                if duration == None:
                    duration = "N/A"

                run_result = "Not Ran"
                if ran == True:
                    if succeeded == True:
                        run_result = "Succeeded"
                    elif succeeded == False:
                        run_result = "Failed"
                elif ran:
                    run_result = ran

                val_row += [run_result, duration]

            table.add_row(val_row)
        table.align = "l"
        return table.get_string()

    def get_help(self):
        """Get the stages in a format that coincides with the help output."""
        table = PrettyTable(header=False, border=False)
        table.field_names = ["Stage", "Description"]
        for stage in STAGES_DICT:
            table.add_row([stage, STAGES_DICT[stage]["description"]])
        table.align = "l"

        return table.get_string()

    def checkInit(self, state_dir):
        if not self.stage_hist.properInit:
            self.stage_hist.set_hist(state_dir)


    def exec_stage(self, args_dict, stage):
        """Run a stage."""

        self.checkInit(args_dict['state_dir'])

        try:
            stage_start = datetime.datetime.now()
            self._stage_dict[stage]["func"](args_dict)
            duration = elapsed_time(stage_start)
            install_logger.info("  stage completed in %s", duration)
            self.stage_hist.update(stage, True, True, duration)

        except RunException as err:
            # if this was an unhandled, failed command, print details
            duration = elapsed_time(stage_start)
            install_logger.info("  aborting stage after %s", duration)
            install_logger.debug("Exception while executing %s", stage, exc_info=True)
            print("")
            install_logger.critical("The following command failed while executing %s:", stage)
            print("")
            install_logger.error("   CMD: %s", err.cmd)
            for item in ["stdout", "stderr", "returncode"]:
                if hasattr(err, item):
                    rawmsg = str(eval("err."+item))
                    msglines = rawmsg.splitlines()
                    for line in msglines:
                        if line:
                            install_logger.error("%s: %s", item, line)
            print("")
            install_logger.info("Aborting install after %s", elapsed_time(self.installer_start))
            print("")
            self.stage_hist.update(stage, ran=True, succeeded=False,duration=duration)

            sys.exit(1)

        except Exception as err:
            duration = elapsed_time(stage_start)
            install_logger.info("  aborting stage after %s", duration)
            install_logger.debug("Exception while executing %s", stage, exc_info=True)
            print("")
            install_logger.critical("A '%s' error", err)
            install_logger.critical("occured while executing %s", stage)

            print("")
            install_logger.info("Aborting install after %s", elapsed_time(self.installer_start))
            print("")

            self.stage_hist.update(stage, ran=True, succeeded=False, duration=duration)
            sys.exit(1)

    def set_skipped(self, skipped_stages=[]):
            for stage in skipped_stages:
                self.stage_hist.update(stage, ran="Skipped", succeeded="Skipped", duration="N/A")

    def validate(self, state_dir, begin_stage, end_stage, run_stages, skip_stages):
        """Resolve the argument logic and ensure the stages are valid."""
        # Do some error-handling with the stage-related args
        stages_list = self.get(list_fmt=True)

        self.checkInit(state_dir)
        # wait to exit until we process all the stages
        error=False

        # if they've specified begin or end along with run_stages, we no longer have any idea
        # what they're talking about.  Don't allow it.
        if begin_stage is not None or end_stage is not None:
            if run_stages:
                install_logger.error("argument -r/--run-stages: not allowed with -b/--begin-stage or -e/--end-stage")
                error=True

        # run and skip at the same time is equally nonsensical
        if run_stages and skip_stages:
            install_logger.error("argument -r/--run-stages: not allowed with -s/--skip-stages")
            error=True

        if begin_stage is None:
            begin_stage = stages_list[0]

        if end_stage is None:
            end_stage = stages_list[-1]

        if begin_stage not in stages_list:
            install_logger.error("invalid stage (%s) found in --begin-stage", begin_stage)
            error=True

        if end_stage not in stages_list:
            install_logger.error("invalid stage (%s) found in --end-stage.", end_stage)
            error=True

        if run_stages:
            for stage in run_stages:
                if stage not in stages_list:
                    install_logger.error("invalid stage (%s) found in --run-stages", stage)
                    error=True

        # Let args_dict['run_stages'] override begin_stage and end_stage; that is,
        # if all three were passed in, run_stages wins.  Loop against stages_list
        # to preserve the execution order.
        if run_stages:
            got_first_stage = False
            for stage in stages_list:
                if stage in run_stages and not got_first_stage:
                    begin_stage = stage
                    end_stage = stage
                    got_first_stage = True
                elif stage in run_stages:
                    end_stage = stage

        # Determine if any stages can be skipped.
        if skip_stages is None:
            skip_stages = []

        if not isinstance(skip_stages, list):
            # This may happen if it skip_stages was passed it via the input file.
            print("'-s/--skip-stages' argument should be a list")
            sys.exit(1)

        for sstage in skip_stages:
            if sstage not in stages_list:
                install_logger.error("invalid stage (%s) found in --skip-stages", sstage)
                error=True

        self.set_skipped(skip_stages)

        # don't need to log the help information, just print and exit
        if error:
            stages_formatted = self.get(long=True)
            print("\nAll stages must be one of the following:\n{}".format(stages_formatted))
            sys.exit(1)

        begin_idx = stages_list.index(begin_stage)
        end_idx = stages_list.index(end_stage)

        self._stages = [stages_list[i] for i in range(begin_idx, end_idx + 1) if stages_list[i] not in skip_stages]
        install_logger.debug("stages=%s",self._stages)
        install_logger.debug("(begin,end)_idx = (%s,%s)", begin_idx, end_idx)
