import asyncio
import shlex
import sys
import utils.InstallLogger
import shutil
import os
from utils.vars import RunException, RunTimeoutError

install_logger = utils.InstallLogger.get_install_logger(__name__)

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
                output_h = open(store_output, "w", 1)
            else:
                output_h = None

            try:
                result = self.run(cmd, quiet=quiet, output=output_h, cwd=cwd, tee=tee, timeout=timeout, **kwargs)

            except RunException as e:
                install_logger.debug("  >>   cmd      : {}".format(e.cmd))
                if store_output:
                    install_logger.debug("  >>>> log      : {}".format(store_output))
                else:
                    install_logger.debug("  >>>> stdout   : {}".format(e.stdout))
                    install_logger.debug("  >>>> stderr   : {}".format(e.stderr))
                install_logger.debug("  >>>> exit code: {}".format(e.returncode))
                raise
            except RunTimeoutError as e:
                install_logger.debug("  >>   cmd      : {}".format(cmd))
                install_logger.debug("  >>   error    : Execution time exceeded {} seconds".format(timeout))
                raise

        if not quiet:
            if dryrun:
                install_logger.dryrun("  >>   cmd      : {}".format(cmd))
                install_logger.dryrun("  >>>> cwd      : {}".format(cwd))
            else:
                install_logger.debug("  >>   cmd      : {}".format(result.cmd))
                if store_output:
                    install_logger.debug("  >>>> log      : {}".format(store_output))
                else:
                    install_logger.debug("  >>>> stdout   : {}".format(result.stdout))
                    install_logger.debug("  >>>> stderr   : {}".format(result.stderr))
                install_logger.debug("  >>>> exit code: {}".format(result.returncode))

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

        done, pending = await asyncio.wait([
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

    def askfirst(self, cmd, **kwargs):
        """Pause for user input after a sudo command."""

        nasks = 0
        keepgoing = True

        # Ask for specific input to avoid an ambiguous answer.  We don't want
        # to continue or exit unless it's exactly what the user wants.
        while nasks < 10 and keepgoing:

            print("Command to be ran: '{}'\nContinue?Y/y/yes or N/n/no".format(cmd))
            answer = input()

            if answer.lower() in  ["n", "no"]:
                print("answer='{}' ==> exiting ...".format(answer))
                sys.exit(0)
                keepgoing = False
            elif answer.lower() in ["y", "yes"]:
                keepgoing = False
            else:
                nasks += 1

            if nasks >= 10:
                install_logger.info("Too many asks.  Exiting")
                sys.exit(0)

        out = None
        try:
            out = self.sudo(cmd, **kwargs)
        except Exception as ex:
            print("Caught exception: {}".format(ex))
            if hasattr(out, "stderr"):
                print("stderr='{}'".format(out.stderr))
            if hasattr(out, "stdout"):
                print("stdout='{}'".format(out.stdout))
            print("Do you want to continue? (Y/y/Yes for yes, anything else to abort")
            answer = input()
            if answer.lower() not in ["y", "yes"]:
                sys.exit(0)

        return out

    def put(self, source, target):
        """
        A Wrapper around the Connection put.
        """
        shutil.copyfile(source, target)
        st = os.stat(source)
        os.chmod(target, st.st_mode)


class CmdMgr:
    """
    This class can be used to manage a single use of CmdInterface
    """

    connection = None

    @staticmethod
    def get_cmd_interface():
        if CmdMgr.connection == None:
            CmdMgr.connection = _CmdInterface()
        return CmdMgr.connection


