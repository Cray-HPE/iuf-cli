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

"""The CSM Install and Upgrade Framework (IUF) CLI"""

import argparse
import atexit
import logging
import os
import re
import shutil
import signal
import sys
import textwrap
import time
import traceback

from lib.vars import *
from lib.InstallLogger import *

from lib.InstallerUtils import elapsed_time, formatted

from lib.ConfigFile import ConfigFile
from lib.ConfigFile import InvertableArgument
import lib.Config
import lib.Activity
from lib.Activity import ACTIVITY_VALID_STATES
# Import lib.stages without the 'as stages' to prevent namespace collisions.
import lib.stages

import yaml

# pylint: disable=consider-using-f-string

install_logger = get_install_logger()

def validate_stages(config):
    """Validate that all of the various stage related args passed in are valid"""

    errors = []

    # load all of the stage variables
    begin_stage = config.args.get('begin_stage')
    end_stage = config.args.get('end_stage')
    run_stages = config.args.get('run_stages')
    skip_stages = config.args.get('skip_stages')
    state_dir = config.args.get('state_dir')

    config.stages.validate(state_dir, begin_stage, end_stage, run_stages, skip_stages)
    local_stages = config.stages.get(all_stages=False, list_fmt=True)
    if config.args.get("func", "") not in [process_install]:
        # No need to do all the error checking if we're just calling
        # 'ls', 'activity', etc.
        return
    else:
        # Check and set the media directory.
        config.check_media_dir(local_stages)

    media_dir = config.args.get('media_dir')

    concurrency = config.args.get("concurrency", None)
    if concurrency is not None:
        try:
            concurrency = int(concurrency)
            if concurrency != None and concurrency < 1:
                errors.append("`--concurrency` requires at least one thread.")
        except ValueError:
            errors.append("`--concurrency` requires a positive integer argument.")

    bpcd = config.args.get("bootprep_config_dir", None)
    rvars = config.args.get("recipe_vars", None)
    bpc_managed = config.args.get("bootprep_config_managed", None)
    bpc_management = config.args.get("bootprep_config_management", None)

    in_bootprep_stage = False
    # Check if any of the stages requiring site-vars, etc are being called.
    # Further error-checking related to this is done in SiteConfig.
    if any(stage in local_stages for stage in ["update-vcs-config",
        "update-cfs-config"]):
        in_bootprep_stage = True
        if not (bpcd or rvars or config.args.get("site_vars", None)):
            errors.append("bootprep or vcs stages were called, but none of "
                          "--site-vars/-sv, --bootprep-config-dir/-bpcd, nor --recipe-vars/-rv "
                          "were specified. At least one of them needs to be specified")

    def warn_or_error(msg, non_bp_suppress=False):
        if in_bootprep_stage:
            errors.append(msg)
        elif not non_bp_suppress:
            install_logger.warning(msg)

    # Allow `--bootprep-config-dir` to override some commandline flags if
    # they were not specified.
    if bpcd:
        found_managed = False
        found_management = False
        managed_path = os.path.join(bpcd, "bootprep", BP_CONFIG_MANAGED)
        management_path = os.path.join(bpcd, "bootprep", BP_CONFIG_MANAGEMENT)
        if not rvars:
            # `--recipe-vars` was not specified on the commandline.  Allow
            # The corresponding `bootprep-config-dir` file to override it.
            rv_path = os.path.join(bpcd, RECIPE_VARS)
            if os.path.exists(rv_path):
                config.args["recipe_vars"] = rv_path
            else:
               warn_or_error(f"{RECIPE_VARS} could not be found in {bpcd}")
        if bpc_managed:
            if not os.path.exists(bpc_managed):
                warn_or_error(f"--bootprep-config-managed {bpc_managed} was specified, but could not be found")
            else:
                found_managed = True
        else:
            # `--bootprep-config-managed` was not specified via the
            # commandline but `bootprep-config-dir` was.  Allow the
            # `--bootprep-config-dir` option to override it.
            if os.path.exists(managed_path):
                config.args["bootprep_config_managed"] = managed_path
                found_managed = True

        if bpc_management:
            if not os.path.exists(bpc_management):
                warn_or_error(f"--bootprep-config-management {bpc_management} was specified, but could not be found")
            else:
                found_management = True
        else:
            # Ditto for `bootprep-config-management`.
            if os.path.exists(management_path):
                config.args["bootprep_config_management"] = management_path
                found_management = True
            #else:
                # Issue a warning that the path doesn't exist
                # warn_or_error(f"{BP_CONFIG_MANAGEMENT} was not found in {os.path.join(bpcd, "bootprep")}")
        if not (found_managed or found_management):
            msg = f"""
            --bootprep-config-dir was specified, but neither {BP_CONFIG_MANAGED}
            nor {BP_CONFIG_MANAGEMENT} could be found.  Add at least one of them
            to the following directory: {os.path.join(bpcd, "bootprep")}, or specify them
            with the `--bootprep-config-managed` and/or
            `--bootprep-config-management` argument(s)."""
            warn_or_error(formatted(msg))
        elif found_managed and found_management:
            # The optimal case.
            pass
        elif not found_managed:
            warn_msg = f"""
            The managed images will not be built because the
            input file could not be found.  If this was not intended, add {BP_CONFIG_MANAGED}
            to the {os.path.join(bpcd, "bootprep")} directory, or specify it with the `--bootprep-config-managed`
            argument.
            """
            install_logger.warning(formatted(warn_msg))
        elif not found_management:
            warn_msg = f"""
            The management images will not be built because the
            input file could not be found.  If this was not intended, add {BP_CONFIG_MANAGEMENT}
            to the {os.path.join(bpcd, "bootprep")} directory, or specify it with the `--bootprep-config-management`
            argument.
            """
            install_logger.warning(formatted(warn_msg))
    else:
        if (bpc_managed or bpc_management) and in_bootprep_stage:
            if not bpc_managed:
                install_logger.warning("--bootprep-config-management was specified without "
                                       "--bootprep-config-managed.  The managed images will not be built.")
            elif not bpc_management:
                install_logger.warning("--bootprep-config-managed was specified without --bootprep-config-management.  "
                                       "The management images will not be built.")
        elif not (bpc_managed or bpc_management) and in_bootprep_stage:
            err_msg = formatted("""A bootprep stage was specified, but neither --bootprep-config-management
            nor --bootprep-config-managed were specified.""")
            errors.append(err_msg)

    if "mask_recipe_prods" in config.args and config.args["mask_recipe_prods"]:
        mrp_type = type(config.args["mask_recipe_prods"]).__name__
        if mrp_type != "list":
            # This shouldn't happen via the commandline, but can happen when
            # specifying the argument via the input file ('-i/--input-file).
            errors.append(f"-mrp/--mask-recipe-prods requires a list, but the argument was passed as {mrp_type}")

    got_bootprep_error = False

    def check_bootprep_arg(arg_name):
        """Ensure that if a `sat bootprep` config was specified (and the
        stage is being run) it exists.
        """

        nonlocal got_bootprep_error, media_dir

        if got_bootprep_error:
            # If there is already an error, it just results in duplicate
            # messages printed.
            return

        # activity is required, so it should never be "undefined".
        activity = config.args.get("activity", "undefined")

        arg_value = config.args.get(arg_name, None)
        param_name = "--{}".format(arg_name.replace("_", "-"))
        if not arg_value:
            return
        elif not os.path.exists(arg_value):
            got_bootprep_error = True
            errors.append(f"{param_name} {arg_value} was specified, but the file could not be found")
            return
        arg_val_basename = os.path.basename(arg_value)
        if not media_dir:
            err_msg = f"""A media directory was not 
            found and was not specified while 
            running a bootprep stage.  It must be a subdirectory under 
            {config.media_base_dir}.  Was process-media ran?"""
            warn_or_error(err_msg, non_bp_suppress=True)
            got_bootprep_error = True
            return

        elif media_dir and not os.path.exists(media_dir):
            # The media directory is necessary during the bootprep stage
            # because bootprep files are copied to the media directory.
            err_msg = f"""`{arg_name} {arg_value}` was specified, but 
            `-m/--media-dir` does not exist.  The 
            media directory is necessary during the bootprep stages. """
            warn_or_error(err_msg, non_bp_suppress=True)
            got_bootprep_error = True
            return

        new_config_loc = os.path.join(media_dir, f".bootprep-{activity}", arg_val_basename)
        if not os.path.exists(new_config_loc):
            os.makedirs(new_config_loc)

        if os.path.exists(new_config_loc):
            if os.path.isdir(new_config_loc):
                shutil.rmtree(new_config_loc)
            else:
                os.remove(new_config_loc)
        if os.path.isdir(arg_value):
            # copytree, ignore statement is to stop possible recurssion error
            shutil.copytree(src=arg_value, dst=new_config_loc, ignore=shutil.ignore_patterns(f".bootprep-{activity}"))
        else:
            shutil.copy(arg_value, new_config_loc)

        # Change the commandline arguments to the new location, relative to media_dir to make things
        # seemless to the user.
        relative_name = f"relative_{arg_name}"
        config.args[relative_name] = os.path.relpath(new_config_loc, start=media_dir)

    # Ensure the validity of the `--bootprep-config-dir`,
    # `--bootprep-config-managed`, and `--bootprep-config-managment` arguments.
    # Copy the bootprep files to the media directory for sat.

    # Process the `--bootprep-config-dir first.  It's not a fully-implemented option,
    # so it would be better if the fully-supported options were ran later, incase
    # there is any overwritting of files.
    check_bootprep_arg("bootprep_config_dir")
    check_bootprep_arg("bootprep_config_managed")
    check_bootprep_arg("bootprep_config_management")

    if errors:
        install_logger.error("Problems were encountered:")
        counter = 1
        for err in errors:
            install_logger.error("\t{}: {}".format(counter, err))
            counter += 1
        sys.exit(1)

    # If there were no errors, write the stages that are supposed to be
    # executed out for a subsequent resume or restart.
    if config.args.get("func", "") == process_install:
        with open(os.path.join(state_dir, "curr_stages.yaml"), "w") as fhandle:
            yaml.dump(local_stages, fhandle)

    # Check the disk usage for the RBD mount.
    free_percent = config.rbd_mount_free_space()
    if free_percent < 10.0:
        warning_msg = f"""Free space on {config.media_base_dir} is
        only {free_percent}.  If disk space is too low, {sys.argv[0]} will
        not be able to write logs or other neccessary files"""
        install_logger.warning(textwrap.dedent(warning_msg))

def process_activity(config):
    state = config.args.get("state", None)
    if state:
        try:
            config.activity.state({
                "timestamp": config.args.get("time", None),
                "workflow_id": config.args.get("argo_workflow_id", None),
                "status": config.args.get("status", "n/a"),
                "comment": config.args.get("comment", None),
                "state": state,
                "create": config.args.get("create", False),
            })
        except Exception as e:
            install_logger.error(f"{e}")
            install_logger.debug(traceback.format_exc())
            sys.exit(1)
    elif config.args.get("create", False) :
        install_logger.error("The `--create` flag was passed without the <state> positional parameter")
        install_logger.error("Please include one of the following states: {}".format(", ".join(ACTIVITY_VALID_STATES)))
        sys.exit(1)

    if "activity" in config.args and config.args["activity"]:
        # If json is present in the args call to_json method of Activity class
        if config.args["output"] == "json":
            print(config.activity.to_json())
        elif config.args["output"] == "yaml":
            # If yaml is present in the args call to_yaml method of Activity class
            print(config.activity.to_yaml())
        else:
            print(config.activity)
    else:
        process_list_activity(config)


def process_install(config):
    """Run the install"""

    install_logger.info(f"[ACTIVITY: {config.activity.name:47}] BEG Install started at {config.stages.installer_start}")
    config.activity.create_activity()
    config.activity.run_stages(resume=False)

    config.activity.state({"state": "waiting_admin"})
    duration = elapsed_time(config.stages.installer_start)
    install_logger.info(f"[ACTIVITY: {config.activity.name:47}] END Completed in {duration}")

    summary = config.stages.get_summary()
    dashes = "----------------"
    print("{}\n{}\n{}".format(dashes, summary, dashes))


def process_list(config): #pylint: disable=unused-argument
    """Process the arguments to the list subparser"""

    summary = None

    if config.partial_init:
        # No activity was specified, and config is only partially initialized.
        stages = lib.stages.Stages(state_dir="/tmp", stage_dict=STAGE_DICT)
        all_stages = stages.get(long=True, list_fmt=False, status=False)
    else:
        if config.args.get("format", None) == "csv":
            all_stages = ','.join(config.stages.get(long=True, status=True, all_stages=True, list_fmt=False))
        else:
            all_stages = config.stages.get(long=True, status=True, all_stages=True, list_fmt=False)
            summary = config.stages.get_summary(load=True)

    print(all_stages)
    if summary:
        print(summary)


def process_debug_level(config):

    install_logger_file_init(config)
    if config.args.get("level", None):
        if config.args["level"] in LOG_LEVELS.keys():
            level = LOG_LEVELS[config.args["level"]]
            install_logger_stream_init(level, config.args.get("verbose", False))
        else:
            # when this runs we don't have a logger yet, just print and exit
            print("ERROR: Unrecognized log level: {}".format(config.args["level"]))
            sys.exit(1)
    else:
        install_logger_stream_init(verbose=config.args.get("verbose", False))

    log_dir_base = config.args["log_dir"]
    log_dir = os.path.join(log_dir_base, config.timestamp)
    if "func" in config.args and config.args["func"] == process_install:
        install_logger.info("All logs will be stored in {}".format(log_dir))
    return

def get_comment(text_or_list):
    """Take an list or a string and return it as a string."""
    comment = None
    if text_or_list and type(text_or_list) is list:
        comment = " ".join(text_or_list)
    elif text_or_list:
        comment = text_or_list

    return comment

def process_resume(config):
    """Resume an activity"""
    config.activity.resume()


def process_restart(config):
    """Restart an activity"""
    config.activity.restart()


def process_abort(config):
    """Kill the running processes and abort the run."""
    activity_name = config.activity.abort_activity()
    install_logger.info(f"Aborted activity {activity_name}")


def process_list_activity(config):
    activities = lib.Activity.list_activity()
    if len(activities) > 0:
        print(activities)
    else:
        print("No Activities found")


def process_workflow(config):

    text = config.activity.workflow_info()
    print(text)

def update_logger_config(config):
    """
    Sets the static CmdInterface interface to "dryrun" mode
    """
    if config.dryrun:
        addLoggingLevel('DRYRUN', logging.INFO + 1)
        install_logger.dryrun("Dryrun enabled")

    addLoggingLevel('TRACE', logging.DEBUG - 1)

def check_environment_variables():
    """
    Go over a list of variables that we don't want set and remove them
    """

    CLEAN_ENV = [
        "CRAY_FORMAT",
    ]

    for var in CLEAN_ENV:
        if var in os.environ:
            del os.environ[var]

def log_state_files(config):
    if not hasattr(config, "args"):
        return
    log_dir_base = config.args["log_dir"]
    logdir = os.path.join(log_dir_base, config.timestamp)
    if hasattr(config, "stages_file"):
        if os.path.exists(config.stages_file):
            shutil.copy2(config.stages_file, logdir)

def print_extra_summary(config):
    if not hasattr(config, "args"):
        return
    log_dir_base = config.args["log_dir"]
    install_log = os.path.join(log_dir_base, config.timestamp, "install.log")

    log_hints = []
    error_messages = []
    artifacts = []
    parse_lines = ["INFO","ERR","CRIT"]

    if os.path.exists(install_log):
        skip_re = re.compile(r" END \[\d+\] \[Failed\]")
        with open(install_log, "r") as fhandle:
            lines = fhandle.readlines()
            for line in lines:
                if any(llevel in line for llevel in parse_lines):
                    try:
                        _, level, msg = line.strip().split(None, 2)
                        message = msg.strip()
                        if level == "ERR" or level == "CRIT":
                            if " END call-hook-script" in message or re.match(skip_re, message):
                                continue
                            error_messages.append(message)
                        if level == "INFO":
                            if "Logs from the failed" in message:
                                log_hints.append(message)
                            if message.startswith("Successfully created CFS configuration "):
                                artifacts.append(message)
                            if message.startswith("Successfully created BOS session template "):
                                artifacts.append(message)
                            if message.startswith("Creation of image "):
                                artifacts.append(message)
                    except:
                        pass

        if len(artifacts) > 0:
            print("")
            print("Artifacts created:")
            for artifact in list(dict.fromkeys(artifacts)):
                print("  ", artifact)

        if config.activity.bootprep_commands:
            print("sat can be run manually with the following commands:")
            print(config.activity.bootprep_commands)

        if len(error_messages) > 0:
            print("")
            print("Error Summary:")
            for message in list(dict.fromkeys(error_messages)):
                print("  ", message)

            if len(log_hints) > 0:
                print("")
                for hint in list(dict.fromkeys(log_hints)):
                    print("  ", hint)


def initialization(config, args):
    """Config/Activity-specific initialization."""

    # Convert the args to a dict, and use that rather than the argparse
    # object.  This is so that we have a dictionary of all options, so that
    # we can more easily determine which values can be passed via an input
    # deck.

    try:
        config.args = vars(args)
    except UndefinedActivity:
        # This happens in a subcommand that doesn't require an activity.
        return

    process_debug_level(config)
    update_logger_config(config)
    shorten_log_status()

    install_logger.debug("IUF Command: {}".format(" ".join(sys.argv)))
    install_logger.debug(f"args: {config.args}")

    init_errors = []

    if not lib.Activity.valid_activity_name(args.activity):
        init_errors.append(f"Activity {args.activity} invalid. Names must only contain lowercase letters, numbers, periods, and dashes")

    if len(sys.argv) < 2:
        init_errors.append("{} requires at least 1 argument".format(sys.argv[0]))

    config.stages = lib.stages.Stages(stage_dict=STAGE_DICT, state_dir=config.args["state_dir"])
    validate_stages(config)

    atexit.register(log_state_files, config)
    atexit.register(print_extra_summary, config)

    return init_errors


def get_answer():
    """Read input and handle related exceptions."""
    answer = ""
    try:
        answer = input()
    except (RuntimeError, EOFError):
        # ctrl-c is being pressed repeatedly. Return anything except the
        # allowed answers.
        answer = "Invalid"
    return answer

# Interrupt variables
already_answered = False
answer_text = None
mypid = os.getpid()

def main():
    """Main entry point."""
    check_environment_variables()
    config = lib.Config.Config()

    parser = argparse.ArgumentParser(description=
        """
       The CSM Install and Upgrade Framework (IUF) CLI.
        """
    )

    parser.add_argument("-i", "--input-file", action="store", help="""YAML input
        file used to provide arguments to `iuf`. Command line arguments will
        override entries in the input file.
        Can also be set via the IUF_INPUT_FILE environment variable.""")

    parser.add_argument("-w", "--write-input-file", action="store_true",
        help="""Create an input file for iuf populated with the command line
        options specified and exit.  This input file can be specified with
        the `-i` option on subsequent runs.  Using an input file simplifies
        iuf commands with many options.  Note that the general iuf command
        does not change; so for a long iuf command, add this flag to the
        command to write the input file.""")

    ### TODO, make default value product_vars.yaml 'recipe', fallback to pwd
    parser.add_argument("-a", "--activity", action="store",
        help="""Activity name.  Must be a unique identifier.
        Activity names must contain only lowercase letters (a-z), numbers (0-9), periods (.), and dashes (-).
        Can also be set via the IUF_ACTIVITY environment variable.""")

    concurrency_help = """During stage processing Argo runs workflow steps in parallel.
        By default up to 10 steps will be executed simultaneously.  Use `--concurrency N`
        to decrease the limit to N.   Increasing this limit is not recommended."""
    parser.add_argument("-c", "--concurrency", action="store", default=None,
        help=concurrency_help)

    parser.add_argument("-b", "--base-dir", action="store",
        help="""Base directory for state and log file directories. Defaults to ${RBD_BASE_DIR}/iuf/[activity], where ${RBD_BASE_DIR} is /etc/cray/upgrade/csm.""")

    parser.add_argument("-s", "--state-dir", action="store",
        help="A directory used to store the current state of stages, used by `iuf` but primarily not of interest to users. Defaults to [base-dir]/state.")

    #### TODO handle both directories or list of files
    parser.add_argument("-m", "--media-dir", action="store",
        help="""Location of installation media to be used. Defaults to ${RBD_BASE_DIR}/[activity],
        where ${RBD_BASE_DIR} is /etc/cray/upgrade/csm. `iuf` cannot access installation media
        outside of ${RBD_BASE_DIR}, however input files provided by other `iuf` arguments can exist
        outside of ${RBD_BASE_DIR}.""")

    # Host to extract the media onto. Defaults to ncn-m001.
    parser.add_argument("-mh", "--media-host", action="store",
        help=argparse.SUPPRESS, default="ncn-m001")

    parser.add_argument("--log-dir", action="store",
        help="Location used to store log files. Defaults to [base-dir]/log.")

    # hide DRYRUN for now until it's production ready
    parser.add_argument("--dryrun", action=InvertableArgument, default=False, help=argparse.SUPPRESS)

    levelhelp = list(LOG_LEVELS.keys())
    levelhelp.remove('DRYRUN')
    parser.add_argument("-l", "--level", action="store", default='INFO',
        help="""Set the log message level that determines what is displayed on `iuf` standard output.
        Messages of this level or higher are displayed.""", choices=levelhelp)

    parser.add_argument("-v", "--verbose", action="store_true",
        help=argparse.SUPPRESS)

    # TODO: The stage-dir usage could use more verbiage. We might want to add
    # this to the install docs.  Describing it in detail in the help section
    # might be inappropriate.  Essentially, we need it because not all the stages
    # are fully independent; for example, we have an unpack stage, and an
    # install stage.  The install stage needs to install what was unpacked.

    subparsers = parser.add_subparsers(title="subcommands", metavar='{run,activity,list-stages|ls,resume,restart,abort,list-activities|la,workflow}')
    stage_list = lib.stages.get_stage_help()

    run_sp = subparsers.add_parser("run", description='Run IUF stages to execute install, upgrade and/or deploy operations for a given activity.',
        epilog="Valid stages:\n{}".format(stage_list),
        formatter_class=argparse.RawTextHelpFormatter)

    run_sp.add_argument("-b", "--begin-stage", action="store",
        help="The first stage to execute. Defaults to process-media")

    run_sp.add_argument("-e", "--end-stage", action="store",
        help="The last stage to execute.  Defaults to post-install-check")

    run_sp.add_argument("-r", "--run-stages", nargs="+", action="store",
        help="Run the specified stages only. This argument is not compatible with `-b`, `-e`, or `-s`.")

    run_sp.add_argument("-s", "--skip-stages", nargs="+", action="store",
        help="Skip the execution of the specified stages.")

    run_sp.add_argument("-f", "--force", action="store_true", default=False,
        help="Force re-execution of stage operations.")

    run_sp.add_argument("-bc", "--bootprep-config-managed", action="store",
        help="""`sat bootprep` config file for managed (compute and
        application) nodes.  Note the path is relative to $PWD, unless an
        absolute path is specified.  Omit this argument to skip building the
        managed images (and ensure the `--bootprep-config-dir` option is not
        specified).""")

    run_sp.add_argument("-bm", "--bootprep-config-management", action="store",
        help="""`sat bootprep` config file for management NCNs.  Note the
        path is relative to $PWD, unless an absolute path is specified. Omit
        this argument to skip building the management images (and ensure the
        `--bootprep-config-dir` option is not specified).""")

    run_sp.add_argument("-bpcd", "--bootprep-config-dir", action="store",
        help="""Directory containing HPE `product_vars.yaml` and
        `sat bootprep` configuration files. The expected content is:
            $(BOOTPREP_CONFIG_DIR)/product_vars.yaml
            $(BOOTPREP_CONFIG_DIR)/bootprep/compute-and-uan-bootprep.yaml
            $(BOOTPREP_CONFIG_DIR)/bootprep/management-bootprep.yaml
        Note the path is relative to $PWD, unless an absolute path is specified.""")

    run_sp.add_argument("-rv", "--recipe-vars", action="store",
        help="""Path to a recipe variables YAML file. HPE provides the
        `product_vars.yaml` recipe variables file with each release. Note
        the path is relative to $PWD, unless an absolute path is specified.""")

    run_sp.add_argument("-sv", "--site-vars", action="store", default=None,
        help="""Path to a site variables YAML file. This file allows the user to override values defined in
        the recipe variables YAML file. Defaults to ${RBD_BASE_DIR}/${IUF_ACTIVITY}/site_vars.yaml if it exists,
        otherwise defaults to ${RBD_BASE_DIR}/site_vars.yaml.
        Note the path is relative to $PWD, unless an absolute path is specified.""")

    run_sp.add_argument("-mrs", "--managed-rollout-strategy", action="store",
        choices=["reboot", "stage"],
        default="stage",
        help="""Method to update the managed nodes. Accepted values are 'reboot' (reboot nodes _now_) or
        'stage' (set up nodes to reboot into new image after next WLM job). Defaults to 'stage'.""")

    run_sp.add_argument("-cmrp", "--concurrent-management-rollout-percentage",
        action="store", default=20, type=int,
        help="""Limit the number of management nodes that roll out
        concurrently based on the percentage specified. Must be an integer
        between 1-100. Defaults to 20 (percent).""")

    run_sp.add_argument("--limit-managed-rollout", action="store",
            nargs='+', default=["Compute"],
       help="""Override list used to target specific nodes only when rolling out
       managed nodes.  Arguments should be xnames or HSM node groups.
       Defaults to the Compute role.""")

    run_sp.add_argument("--limit-management-rollout", action="store", nargs='+',
        default=[],
        help="""List used to target specific hostnames or HSM management role_subrole only when rolling 
        out management nodes. Hostname arguments can only belong to a single node type. For example, 
        both master and worker hostnames can not be provided at the same time. Defaults to an empty list
        which means no nodes will be rolled out.""")

    run_sp.add_argument("-mrp", "--mask-recipe-prods", action="store", nargs='+',
        help="""If `--recipe-vars` is specified, mask the versions found within the recipe variables YAML
        file for the specified products, such that the largest version of the package already installed on
        the system (found in the product catalog) is used instead of the version supplied in the HPC CSM
        Software Recipe. Note that the versions found via `--site-vars` (or the versions being installed)
        will override it as well.""")

    run_sp.set_defaults(func=process_install, skip_stages=list(), run_stages=list())

    list_sp = subparsers.add_parser("list-stages", description='List IUF stage information and status for a given activity specified via `-a`.', aliases=["ls"])
    list_sp.set_defaults(func=process_list)

    resume_sp = subparsers.add_parser("resume", description='Resume a previously aborted or failed IUF session for a given activity.')
    resume_sp.set_defaults(func=process_resume)
    resume_sp.add_argument("comment", action="store", nargs="*", help="Add a comment to the activity log")

    restart_sp = subparsers.add_parser("restart", description='Restart a previously aborted or failed IUF session for a given activity.')
    restart_sp.set_defaults(func=process_restart)
    restart_sp.add_argument("-f", "--force", action="store_true",
        help="""Force all operations to be re-executed irrespective if they
        have been successful in the past.""")
    restart_sp.add_argument("comment", action="store", nargs="*", help="Add a comment to the activity log")

    abort_sp = subparsers.add_parser("abort", description='Abort an IUF session for a given activity immediately or after the current stage completes.')
    abort_sp.set_defaults(func=process_abort)

    # help for the comment flag should be something like,
    # """Adds a comment to the "Comment" row if the `iuf activity ...` table.
    # The option is hidden because it doesn't seem to be working in the backend.
    abort_sp.add_argument("comment", action="store", nargs="*", help="Add a comment to the activity log")

    abort_sp.add_argument("-f", "--force", action="store_true",
        help="""Force the abort immediately.""")

    activity_sp = subparsers.add_parser("activity", description='Create, display, or annotate activity information.')
    activity_sp.add_argument("--time", help="A time value used when creating or modifying an activity entry. Must match an existing time value to modify that entry. Defaults to now.", action="store")
    activity_sp.add_argument("--create", help="Create a new activity entry.", action="store_true", default=False)
    activity_sp.add_argument("--comment", help="A comment to be associated with an activity entry.", action="store")
    activity_sp.add_argument("--status", help="A status value to be associated with an activity entry.", action="store", default="n/a", choices=lib.Activity.ACTIVITY_VALID_STATUS)
    activity_sp.add_argument("--argo-workflow-id", help="An Argo workflow identifier to be associated with an activity entry.", default=None)
    activity_sp.add_argument("state", nargs="?", help="activity state value", action="store", default=None, choices=lib.Activity.ACTIVITY_VALID_STATES)
    activity_sp.add_argument("--output", "-o", choices=["json","yaml", "text"], default="text",help="Specify the output format. Options are 'json','yaml or 'text'. Default is 'text'.")
    activity_sp.set_defaults(func=process_activity)

    list_activity_sp = subparsers.add_parser("list-activities",
        description="List all IUF activities stored in argo.", aliases=["la"])
    list_activity_sp.set_defaults(func=process_list_activity)

    workflow_sp = subparsers.add_parser("workflow", description="List workflows or information for a particular workflow")
    workflow_sp.add_argument("workflows", action="store", help="workflow to look up", nargs="*")
    workflow_sp.add_argument("--debug", "-d", action="store_true", help="Give more granular details about the workflow")
    workflow_sp.set_defaults(func=process_workflow)

    # create a config object to store all of the configured options in the parser
    configfile = ConfigFile()
    configfile.load_parser_defaults(parser)

    # allow the input file to be defined by environment variable
    env_input_file = os.getenv("IUF_INPUT_FILE", None)
    if env_input_file:
        parser.set_defaults(input_file=env_input_file)

    # parse the arguments once to get input file
    tmp_vars = vars(parser.parse_args())

    # load any defaults from the input file
    configfile.set_defaults(parser,tmp_vars)

    if tmp_vars["write_input_file"]:
        configfile.write(tmp_vars)

    # allow the activity to be defined in the environment
    env_session = os.getenv("IUF_ACTIVITY", None)
    if env_session:
        parser.set_defaults(activity=env_session)

    # parse the command line for real
    args = parser.parse_args()
    init_errors = initialization(config, args)
    if init_errors:
        for err in init_errors:
            install_logger.error(err)
        parser.print_help(sys.stderr)
        sys.exit(1)

    if not args.activity and hasattr(args, "func") and args.func in [process_install, process_workflow, process_abort]:
        print("ERROR: --activity is required.")
        parser.print_help(sys.stderr)
        sys.exit(1)

    def process_ctrl_c(sig, frameg):
        """Override a ctrl-c interrupt."""
        global already_answered, answer_text, mypid

        thispid = os.getpid()
        if thispid != mypid:
            # The interrupt is sent to all processes, so return
            # if this is not the parent process.
            return

        if already_answered and answer_text:
            print(answer_text)
            return

        activity = config.args.get("activity", None)
        script_name = sys.argv[0]

        print("Would you like to abort this run?")
        print("    Enter Y, y, or yes to abort after the current stage completes.")
        print("    Enter F, f, or force to abort the current stage immediately.")
        print("    Enter D, d, or disconnect to exit the IUF CLI.  The install will continue in the background, however no logs will be collected.")
        print("")
        print("    Enter <return> to resume monitoring.")
        print("    NOTE: The IUF CLI will remain connected until Argo completes the abort process.  Use the disconnect option to exit the IUF CLI immediately.")
        print("    NOTE: All logging will be suspended when disconnected.")

        literal_answer = get_answer()
        answer = literal_answer.lower()

        if answer in ["y", "yes"]:
            answer_text = "Aborting after the current stage completes."
            if not already_answered:
                print(answer_text)
                already_answered = True
                process_abort(config)

        elif answer in ["d", "disconnect"]:
            answer_text = "Attempting to exit the CLI cleanly."
            print(answer_text)
            if not already_answered:
                already_answered = True
                config.activity.abort_activity(background_only=True)
                print(f"Use `{script_name} -a {activity} resume` to re-connect")
                print("Bye!")

        elif answer in ["f", "force"]:
            answer_text = "Forcing an immediate abort."
            already_answered = True
            config.args["force"] = True
            config.activity.abort_activity()
            print("Bye!")
        else:
            print("Continuing...")

        #  Save cycles if if ctrl-c is being held down.
        time.sleep(.25)

    signal.signal(signal.SIGINT, process_ctrl_c)

    try:
        args.func(config)
    except AttributeError as e:
        if args and not "func" in args:
            install_logger.error("A subcommand was not specified.")
        else:
            install_logger.error("An unexpected error occurred: {}".format(e))
        sys.exit(1)
    except Exception as e:
        install_logger.error("An unexpected error occurred: {}".format(e))
        raise

if __name__ == "__main__":
    main()
