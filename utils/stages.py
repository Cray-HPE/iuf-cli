#!/usr/bin/env python3
"""
Copyright 2022 Hewlett Packard Enterprise Development LP
"""

"""This module contains the staging information and functionality."""


import copy
import datetime
import os
import sys
from prettytable import PrettyTable
import yaml
from utils.InstallLogger import get_install_logger
from utils.InstallerUtils import elapsed_time

# Note: utils.ShastaUpdate needs to be imported `as supdate` because of the
# Naming convention used in vars.py to get at the functions.
import utils.ShastaUpdate as supdate
from utils.vars import RunException, STAGE_HIST_FILENAME, STAGE_DICT, NOABORT_STAGES

install_logger = get_install_logger(__name__)

# pylint: disable=consider-using-f-string

class StageHist:
    def __init__(self, state_dir, all_stages=[]):
        self._stages = all_stages
        self._status = {}
        self._stage_hist_file = None
        self._stage_hist_file = os.path.join(state_dir, STAGE_HIST_FILENAME)

        # Load the stage history from a previous run and return.
        if os.path.exists(self._stage_hist_file):
            with open(self._stage_hist_file, "r", encoding='UTF-8') as fhandle:
              self._status = yaml.full_load(fhandle)
              return

        # We have not returned.  This must be a new run.
        for stage in self._stages:
            self._status[stage] = {
                "ran": False,
                "succeeded": False,
                "duration": None,
            }

        self.dump_status()


    def create_new_status(self):
          for stage in self._stages:
                self._status[stage] = {
                    "ran": False,
                    "succeeded": False,
                    "duration": None,
                }


    def load_status(self):
        with open(self._stage_hist_file, "r", encoding='UTF-8') as fhandle:
              self._status = yaml.full_load(fhandle)


    def dump_status(self):
        with open(self._stage_hist_file, "w", encoding='UTF-8') as fhandle:
            yaml.dump(self._status, fhandle)


    def update(self, stage, ran=False, succeeded=False, duration=None):
        """Update a stage status."""

        self._status[stage] = {
            "ran": ran,
            "succeeded": succeeded,
            "duration": duration
        }
        self.dump_status()


class Stages():
    """Staging information and methods."""
    def __init__(self, stage_dict={}, state_dir='state'):
        self._stage_dict = copy.deepcopy(stage_dict)
        self._stages = list(self._stage_dict.keys())
        self._all_stages = self._stages
        self.installer_start = datetime.datetime.now()
        self.stage_hist = StageHist(state_dir, self._all_stages)
        self.skip_stages = []
        self._noabort_stages = NOABORT_STAGES

    @property
    def stage_hist_file(self):
        return self.stage_hist._stage_hist_file

    @property
    def beginStage(self):
        return self._stages[0]

    @property
    def endStage(self):
        return self._stages[-1]

    def abortable(self, stage):
        """Return true if a stage can be aborted."""
        return stage not in self._noabort_stages

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

                run_result = "N/A"
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


    def exec_stage(self, config, stage):
        """Run a stage."""

        #self.checkInit(args_dict['state_dir'])

        try:
            stage_func = eval("supdate.{}".format(self._stage_dict[stage]["func"]))
            stage_start = datetime.datetime.now()
            stage_func(config)
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

        #self.checkInit(state_dir)
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

def get_stage_help():
    """Get the stages in a format that coincides with the help output."""
    table = PrettyTable(header=False, border=False)
    table.field_names = ["Stage", "Description"]
    for stage in STAGE_DICT:
        table.add_row([stage, STAGE_DICT[stage]["description"]])
    table.align = "l"

    return table.get_string()
