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

import asyncio
import shlex
import sys
import lib.InstallLogger
from lib.vars import RunException, RunTimeoutError

install_logger = lib.InstallLogger.get_install_logger(__name__)

class RunOutput():
    def __init__(self, cmd, args, returncode, stdout, stderr):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.cmd = cmd
        self.args = args

class _CmdInterface:
    """Wrapper around the subprocess interface to simplify usage."""
    def __init__(self, n_retries=0, dryrun=False):
        self.installer = True
        self.dryrun = dryrun

    def sudo(self, cmd, dryrun=None, cwd=None, quiet=False, store_output=None, tee=False, timeout=None, **kwargs):
        """
        Execute a command.
        """
        if dryrun is None:
            dryrun = self.dryrun

        if dryrun:
            result = RunOutput(cmd, shlex.split(cmd), 0, "", "Dryrun, command not executed")
        else:
            if store_output:
                output_h = open(store_output, "w", 1, encoding='UTF-8')
            else:
                output_h = None

            try:
                result = self.run(cmd, quiet=quiet, output=output_h, cwd=cwd, tee=tee, timeout=timeout, **kwargs)

            except RunException as e:
                install_logger.debug("  >>   cmd      : %s", e.cmd)
                if store_output:
                    install_logger.debug("  >>>> log      : %s", store_output)
                else:
                    install_logger.debug("  >>>> stdout   : %s", e.stdout)
                    install_logger.debug("  >>>> stderr   : %s", e.stderr)
                install_logger.debug("  >>>> exit code: %s", e.returncode)
                raise
            except RunTimeoutError:
                install_logger.debug("  >>   cmd      : %s", cmd)
                install_logger.debug("  >>   error    : Execution time exceeded %s seconds", timeout)
                raise

        if not quiet:
            if dryrun:
                install_logger.dryrun("  >>   cmd      : %s", cmd)
                install_logger.dryrun("  >>>> cwd      : %s", cwd)
            else:
                install_logger.debug("  >>   cmd      : %s", result.cmd)
                if store_output:
                    install_logger.debug("  >>>> log      : %s", store_output)
                else:
                    install_logger.debug("  >>>> stdout   : %s", result.stdout)
                    install_logger.debug("  >>>> stderr   : %s", result.stderr)
                install_logger.debug("  >>>> exit code: %s", result.returncode)

        return result

    def run(self, cmd, output=None, cwd=None, quiet=False, timeout=None, tee=False, **kwargs) -> RunOutput:
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(
            self._stream_subprocess(cmd, output=output, cwd=cwd, quiet=quiet, timeout=timeout, tee_output=tee, **kwargs)
        )

        # asyncio doesn't really do "check=True", so fake it
        if result.returncode:
            raise RunException("{} returned non-zero exit status: {}".format(shlex.split(cmd)[0], result.returncode),
                    cmd,
                    shlex.split(cmd),
                    result.returncode,
                    result.stdout,
                    result.stderr)
        else:
            return result

    async def _read_stream(self, stream, callback):
        while True:
            line = await stream.readline()
            if line:
                callback(line)
            else:
                break

    async def _stream_subprocess(self, cmd, output=None, cwd=None, quiet=False, timeout=None, tee_output=False, **kwargs) -> RunOutput:
        splitc = shlex.split(cmd)

        p = await asyncio.create_subprocess_exec(*splitc,
                                              limit=256*1024,
                                              stdin=None,
                                              stdout=asyncio.subprocess.PIPE,
                                              stderr=asyncio.subprocess.PIPE,
                                              cwd=cwd,
                                              **kwargs)
        out = []
        err = []

        def tee(line, sink, pipe, output):
            line = line.decode('utf-8')
            if output:
                output.write(line)

            line = line.rstrip()
            sink.append(line)
            if tee_output:
                print(line, file=pipe)

        _, pending = await asyncio.wait([
            self._read_stream(p.stdout, lambda l: tee(l, out, sys.stdout, output)),
            self._read_stream(p.stderr, lambda l: tee(l, err, sys.stderr, output)),
        ], timeout=timeout)

        if pending:
            out_s = "\n".join(out)
            err_s = "\n".join(err)
            raise RunTimeoutError("{} execution time exceeded {} seconds".format(splitc[0], timeout),
                    cmd,
                    splitc,
                    "-1",
                    out_s,
                    err_s)

        returncode = await p.wait()

        out_s = "\n".join(out)
        err_s = "\n".join(err)

        ro = RunOutput(cmd=cmd, args=shlex.split(cmd), returncode=returncode, stdout=out_s, stderr=err_s)
        return ro


class CmdMgr:
    """
    This class can be used to manage a single use of CmdInterface
    """

    connection = None

    @staticmethod
    def get_cmd_interface():
        if CmdMgr.connection is None:
            CmdMgr.connection = _CmdInterface()
        return CmdMgr.connection


