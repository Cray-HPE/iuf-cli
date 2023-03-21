#!/bin/sh

# This script is used in the %check section of the spec file
script=$1

if [ -z "$script" ]; then
    echo "$0: <iuf location>"
    exit 1
fi

exitval=0
errors=""

# no args
echo -n "TEST: 'iuf' script with no args: "
output=`$script 2>&1`
if [ "$output" = "A subcommand was not specified." ]; then
    echo "PASS"
else
    echo "FAIL"
    errors="$errors $output"
    exitval=1
fi

echo -n "TEST: 'iuf --help': "
output=`$script --help 2>&1`
res=$?
if [ "$res" = "0" ]; then
    echo "PASS"
else
    echo "FAIL"
    errors="$errors $output"
    exitval=1
fi

if [ "$exitval" = "0" ]; then
    echo "All tests passed."
else
    echo "Tests failed:"
    echo "$errors"
fi
exit $exitval