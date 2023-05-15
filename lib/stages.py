#!/usr/bin/env python3
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

"""This module contains the staging information and functionality."""


import copy
import datetime
import os
import sys
from prettytable import PrettyTable
import time
import yaml
from lib.InstallLogger import get_install_logger
from lib.InstallerUtils import elapsed_time, format_column

from lib.vars import RunException, STAGE_HIST_FILENAME, STAGE_DICT, NOABORT_STAGES

install_logger = get_install_logger(__name__)

# pylint: disable=consider-using-f-string

class StageHist:
    def __init__(self, state_dir, all_stages=[]):
        self._stages = all_stages
        self._status = {}
        self._stage_hist_file = os.path.join(state_dir, STAGE_HIST_FILENAME)
        self.summary = {}

        # Load the stage history from a previous run and return.
        if os.path.exists(self._stage_hist_file):
            with open(self._stage_hist_file, "r", encoding='UTF-8') as fhandle:
                all_data = yaml.full_load(fhandle)
                self._status = all_data["status"]
                self.summary = all_data["summary"]
            return

        # We have not returned.  This must be a new run.
        for stage in self._stages:
            self._status[stage] = {
                "ran": False,
                "succeeded": False,
                "duration": None,
            }

        self.dump_status()


    def load_status(self):
        with open(self._stage_hist_file, "r", encoding='UTF-8') as fhandle:
              all_data = yaml.full_load(fhandle)
              self._status = all_data["status"]
              self.summary = all_data["summary"]


    def dump_status(self):
        dump_dict = {
            "status": self._status,
            "summary": self.summary
        }
        with open(self._stage_hist_file, "w", encoding='UTF-8') as fhandle:
            yaml.dump(dump_dict, fhandle, sort_keys=False)


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
        self.current_stage = None
        self.summary = {}
        self._run_stages = []
        self._start_at = None
        self.stage_enum = {}
        for index, key in enumerate(STAGE_DICT):
            self.stage_enum[key] = index

        self.set_summary("command line", " ".join(sys.argv))

    @property
    def stage_hist_file(self):
        return self.stage_hist._stage_hist_file

    @property
    def beginStage(self):
        return self._stages[0]

    @property
    def endStage(self):
        return self._stages[-1]

    @property
    def start_at(self):
        return self._start_at


    @start_at.setter
    def start_at(self, value):
        self._start_at = value

    @property
    def stages(self):
        if not self._run_stages:
            start_at = self.start_at
            all_stages = self.get(list_fmt=True, all_stages=True)
            exec_stages = self.get(list_fmt=True, all_stages=False)
            start = True if start_at == None else False

            for stage in all_stages:
                if stage != start_at and start == False:
                    continue
                elif stage == start_at:
                    start = True
                if stage in exec_stages:
                    self._run_stages.append(stage)

        return self._run_stages

    def set_stages(self, stages):
        """Set the stages to the input parameter.  Used on an `iuf resume`."""
        self._stages = stages


    def set_summary(self, summary_key, summary_value):
        """Set a key (summary_key) in the summary dict to a particular value
        (summary_value)"""
        self.stage_hist.summary[summary_key] = summary_value


    def get_summary(self, load=False):
        """Get the high-level summary.  Return it as a block of text."""
        if load:
            self.stage_hist.load_status()
        return_str = []

        for elt in self.stage_hist.summary:
            # we're not adding this anymore, but it can still exist in older stage.yaml files
            if elt == "ran_stages":
                continue
            return_str.append("{}: {}".format(elt.replace("_", " "), self.stage_hist.summary[elt]))

        # Insert a header if there is any history.
        if return_str:
            return_str.insert(0, "Install Summary")

        return "\n".join(return_str)

    def abortable(self, stage):
        """Return true if a stage can be aborted."""
        return stage not in self._noabort_stages

    def get_stage_status(self, activity):
        return_list = []
        for stage in self._stage_dict.keys():
            return_list.append({
                "stage": stage,
                "description": self._stage_dict[stage].get("description", False),
                "succeeded": self.stage_hist._status[stage]["succeeded"],
                "duration": self.stage_hist._status[stage]["duration"],
                "ran": self.run_status(stage)
            })
        return return_list

    def run_status(self, stage):
        ran = self.stage_hist._status[stage]["ran"]
        succeeded = self.stage_hist._status[stage]["succeeded"]
        run_result = "N/A"
        if ran == True:
            if succeeded == True:
                run_result = "Succeeded"
            elif succeeded == False:
                run_result = "Failed"
        elif ran:
            run_result = ran
        elif succeeded:
            run_result = succeeded

        return run_result

    def get(self, long=False, all_stages=True,
            status=False, list_fmt=False, summary=False):
        """Get either a printable list and description/status of stages, or return a list
            of stages."""
        if list_fmt:
            if all_stages:
                return self._all_stages
            else:
                return self._stages

        name_row = ["Stage"]
        table = PrettyTable()
        if long:
            name_row += ["Description"]
        if status:
            name_row += ["Status", "Duration"]
        table.field_names = name_row
        for stage in self._stage_dict.keys():
            val_row = [stage]

            #Add the description if long was specified.
            if long:
                description = self._stage_dict[stage].get("description", False)
                val_row += [description]

            # Print the status and duration if status was specified.
            if status:
                duration = self.stage_hist._status[stage]["duration"]
                if duration == None:
                    duration = "N/A"

                run_result = self.run_status(stage)

                val_row += [run_result, duration]

            table.add_row(val_row)
        table.align = "l"
        return table.get_string()

    def exec_stage(self, config, workflow, sessionid, stage):
        """Run a stage."""

        prefix=format_column(f"STAGE: {stage}")
        config.logger.info(f"{prefix} BEG Argo workflow: {workflow}")

        arg_comment = config.args.get("comment", None)
        if type(arg_comment) is list:
            arg_comment = " ".join(arg_comment)
        if arg_comment:
            state_args = {"comment": arg_comment, "workflow_id": workflow,
                          "status": config.args["func"].__name__.replace("process_", "")}
            config.activity.state(state_args)
            # Sleep 1, so that the next entry is not over-written if less than a second has elapsed.
            time.sleep(1)

        comment = f"Run {stage}"
        state_args = {
            "state": "in_progress",
            "status": "Running",
            "session": sessionid,
            "workflow_id": workflow,
            "comment": comment,
            "command": " ".join(sys.argv),
            "media_dir": config.args.get("media_dir")
        }
        utime = config.activity.state(state_args)

        # Execute the stage.
        failed = True
        status = "Unknown"
        duration = "Unknown"
        try:
            self.current_stage = stage
            stage_start = datetime.datetime.now()
            status = config.activity.monitor_workflow(workflow)
            if status == "Succeeded":
                failed = False
            duration = elapsed_time(stage_start)
            config.logger.info(f"{prefix} END {status} in {duration}")

        except RunException as err:
            # if this was an unhandled, failed command, print details
            duration = elapsed_time(stage_start)
            config.logger.error(f"{prefix} END {status} in {duration}")

            # update the current state with the failure
            config.activity.state({"timestamp":utime, "status":"Failed"})
            # put the whole process into debug
            config.activity.state({"state": "debug", "comment": f"Exception occurred while executing {err.cmd}"})

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

        except Exception as err:
            duration = elapsed_time(stage_start)
            config.logger.error(f"{prefix} END {status} in {duration}")
            # update the current state with the failure
            config.activity.state({"timestamp":utime, "status":"Failed"})
            # put the whole process into debug
            config.activity.state({"state":"debug", "comment":str(err)})

            install_logger.debug("Exception while executing %s", stage, exc_info=True)
            print("")
            install_logger.critical("A '%s' error", err)
            install_logger.critical("occurred while executing %s", stage)

        if failed:
            print("")
            install_logger.info("Aborting install after %s", elapsed_time(self.installer_start))
            print("")
            self.stage_hist.update(stage, ran=False, succeeded="Paused",duration=duration)
            # update the current state with the failure
            config.activity.state({"timestamp": utime, "status":"Failed"})
            # put the whole process into debug
            config.activity.state({"state":"debug"})

            print(self.get_summary())
            sys.exit(1)
        else:
            config.activity.state({"timestamp":utime, "status":"Succeeded"})
            self.stage_hist.update(stage, True, True, duration=duration)

    def set_skipped(self, skipped_stages=[]):
            for stage in skipped_stages:
                self.stage_hist.update(stage, ran="Skipped", succeeded="Skipped", duration="N/A")

    def set_paused(self, stage):
        self.stage_hist.update(stage, ran=False, succeeded="Paused")

    def reset(self):
        for stage in self.get(list_fmt=True, all_stages=True):
            self.stage_hist.update(stage)

    def validate(self, state_dir, begin_stage, end_stage, run_stages, skip_stages):
        """Resolve the argument logic and ensure the stages are valid."""
        # Do some error-handling with the stage-related args
        stages_list = self.get(list_fmt=True)

        # wait to exit until we process all the stages
        error=False

        # if they've specified begin or end along with run_stages, we no longer have any idea
        # what they're talking about.  Don't allow it.
        if begin_stage is not None or end_stage is not None:
            if run_stages:
                install_logger.error("argument -r/--run-stages: not allowed with -b/--begin-stage or -e/--end-stage")
                error=True

        # # run and skip at the same time is equally nonsensical
        # if run_stages and skip_stages:
        #     install_logger.error("argument -r/--run-stages: not allowed with -s/--skip-stages")
        #     error=True

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

        if run_stages:
            self._stages = []
            for stage in stages_list:
                if stage in run_stages and stage not in skip_stages:
                    self._stages.append(stage)
        else:
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
