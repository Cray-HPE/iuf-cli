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
# Configuration File functions
import os
import sys
import yaml
import argparse

class InvertableArgument(argparse.Action):
    def __init__(self, option_strings, dest, default=None, required=False, help=None):

        if default is None:
            raise ValueError('You must provide a default with Yes/No action')
        if len(option_strings)!=1:
            raise ValueError('Only single argument is allowed with YesNo action')
        opt = option_strings[0]
        if not opt.startswith('--'):
            raise ValueError('Yes/No arguments must be prefixed with --')

        opt = opt[2:]
        opts = ['--' + opt, '--no-' + opt]
        super(InvertableArgument, self).__init__(opts, dest, nargs=0, const=None,
                                          default=default, required=required, help=help)
    def __call__(self, parser, namespace, values, option_strings=None):
        if option_strings.startswith('--no-'):
            setattr(namespace, self.dest, False)
        else:
            setattr(namespace, self.dest, True)

class ConfigFile:
    remove_args = [
            "func",
            "input_file",
            "write_input_file",
            ]

    all_parser_options = dict()
    choice_map = dict()
    choice_alias = dict()

    def set_defaults(self, parser, args):
        config = args['input_file']
        write_file = args['write_input_file']
        if not config:
            return

        # ok, we have a config file to load
        if os.path.exists(config):
            try:
                with open(config, 'r', encoding='UTF-8') as f:
                    file_defaults = yaml.safe_load(f)

                    # remove any null values from the defaults
                    file_defaults = {k:v for k,v in file_defaults.items() if v is not None}
            except:
                print("ERROR: Unable to parse input file: {}".format(config))
                sys.exit(1)
        else:
            # if write file is specified, it's ok for the file to not exist
            if not write_file:
                print("ERROR: Input file {} does not exist.".format(config))
                sys.exit(1)

            return

        # don't allow certain things in the input deck for sanity.  For now this is
        # the same list of things we don't allow to be written out.
        for section in file_defaults:
            for arg in self.remove_args:
                if arg in file_defaults[section]:
                    del file_defaults[section][arg]

        # update the internal storage to include all file updates
        for section in self.all_parser_options:
            if section in file_defaults:
                self.all_parser_options[section].update(**file_defaults[section])

        # now that we've done all that, insert the config file defaults into the parser
        defaults = file_defaults.get("global",dict())
        parser.set_defaults(**defaults)
        for action in parser._actions:
            if action.dest != "==SUPPRESS==":
                continue

            for choice in action.choices:
                current = action.choices[choice]
                # see if this is an alias section
                if choice in self.choice_alias:
                    defaults = file_defaults.get(self.choice_alias[choice],dict())
                else:
                    if "func" in current._defaults:
                        name = current._defaults["func"].__name__
                        config_section = self.choice_map[name]
                        defaults = file_defaults.get(config_section, dict())
                    else:
                        defaults = dict()

                current._defaults.update(**defaults)

                # need to set the defaults in two places
                for argument in current._actions:
                    if argument.dest in defaults:
                        argument.default = defaults[argument.dest]

    # write out the input file and quit
    def write(self, args):
        config = args['input_file']
        write_file = args['write_input_file']

        # this will probably be checked before write is called, but just be sure
        if not write_file:
            return

        if not config:
            # config file wasn't specified but write was, give up
            print("ERROR: --write-input-file requires --input-file")
            sys.exit(1)

        update_sections = ["global"]
        if "func" in args:
            update_sections.append(self.choice_map[args["func"].__name__])

        # get rid of some things we don't want in the input file
        #clean_args = {key: value for key, value in args.items() if value is not None}
        write_args = self.all_parser_options.copy()

        for section in update_sections:
            for arg in self.remove_args:
                if arg in write_args[section]:
                    del write_args[section][arg]

            for arg in write_args[section]:
                if arg in args:
                    write_args[section][arg] = args[arg]

        if args['dryrun']:
            print("DRYRUN: Not creating/updating {}".format(config))
            print(yaml.dump(write_args, indent=4, sort_keys=False))
        else:
            with open(config, "w") as f:
                yaml_data = yaml.dump(write_args, f, indent=4, sort_keys=False)
                print("Successfully wrote {}".format(config))

        sys.exit(0)

    def load_parser_defaults(self, parser):
        self.process_actions(parser._actions)

    def process_actions(self, actions, section="global"):
        # _actions from parser is a list of class objects
        self.all_parser_options[section] = dict()
        for action in actions:
            if action.dest == "help":
                continue
            if action.dest == "==SUPPRESS==":
                for choice in sorted(action.choices.keys()):
                    if "func" in action.choices[choice]._defaults:
                        name = action.choices[choice]._defaults["func"].__name__
                        if name in self.choice_map:
                            # we've already processed something for this function, don't duplicate
                            self.choice_alias[choice] = self.choice_map[name]
                            continue

                        self.choice_map[name] = choice
                        self.process_actions(action.choices[choice]._actions, choice)
                continue

            self.all_parser_options[section][action.dest] = action.default
