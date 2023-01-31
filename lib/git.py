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
import base64
import os
import shutil

from lib.vars import GitError, RunException
from lib.InstallLogger import get_install_logger
install_logger = get_install_logger(__name__)

class Git:
    """
    This class defines commonly used git functions so everyone can do things the same way

    repo in all cases is the short repo name i.e. cos-config-management or csm-config-management
    quiet in all cases is a True/False that gets passed to connection.sudo that suppresses debug output

    Most of these functions will work normally with dryrun enabled, only functions that change things
    are disabled or modified.  Pushes are skipped, merges are performed but not committed.
    """

    env = os.environ.copy()
    state_dir = None
    connection = None
    # we /could/ use vcs.hostname.whatever here but we'd need to figure out the fqdn
    # and that seems like a lot of unnecessary effort given the .local address
    vcs = "api-gw-service-nmn.local"
    vcs_user = None
    logger = None
    clone_info = dict()
    dryrun = False

    def __init__(self, config):
        """
        Initialize various variables when the class is created
        """
        # all clones live in the statedir
        self.state_dir = config.args["state_dir"]
        # don't blow up if ssl is wrong
        self.env["GIT_SSL_NO_VERIFY"] = "true"
        # move HOME to statedir for git commands
        # probably not needed anymore but doesn't break anything
        self.env["HOME"] = self.state_dir

        # use the connection object passed in
        self.connection = config.connection

        # see if we need dryrun set
        self.dryrun = config.dryrun

        # create an askpass script so the git password is never exposed
        askpass = os.path.join(self.state_dir, "askpass.sh")

        if not os.path.exists(askpass):
            f = open(askpass, "w")
            f.write("#!/bin/sh\necho $(kubectl --kubeconfig=/etc/kubernetes/admin.conf get secret -n services vcs-user-credentials --template={{.data.vcs_password}} | base64 --decode)")
            f.close()

        # make sure askpass is executable by root only
        os.chmod(askpass, 0o700)

        self.env["GIT_ASKPASS"] = askpass

        # try to get the vcs_user directly
        try:
            # this is safe to run during a dryrun
            vcs_user_encoded = self.connection.sudo("kubectl --kubeconfig=/etc/kubernetes/admin.conf get secret -n services vcs-user-credentials --template={{.data.vcs_username}}", quiet=True, dryrun=False)
            self.vcs_user = base64.b64decode(vcs_user_encoded).decode()
        except:
            # we tried, use the default
            self.vcs_user = "crayvcs"

    def branch(self, repo, **kwargs):
        """
        Execute "git branch" on a given repo

        By default returns the current checked out branch
        If extra_opts are specified, returns the subprocess result object
        """
        fullcommand = "branch"
        quiet = kwargs.get("quiet", True)
        extra_opts = kwargs.get("extra_opts", None)

        if extra_opts:
            fullcommand = fullcommand + " " + extra_opts

        repoinfo = self.get_repoinfo(repo)
        result = self.run(fullcommand, subdir=repo, quiet=quiet, dryrun=False)
        branch = None

        # if they're passing in extra options, we can't assume we're going to have the usual
        # default branch output, so just give them the result object
        if not extra_opts:
            for line in result.stdout.splitlines():
                if line.startswith("*"):
                    branch = line.split(" ")[1].strip()
                    break

            if not branch:
                raise GitError("Unable to determine the current branch for {}".format(repo))

            return branch
        else:
            return result

    def branch_exists(self, repo, branch):
        """
        See if a branch exists in the repo already.

        Returns True/False
        """
        repoinfo = self.get_repoinfo(repo)
        remote = "remotes/" + self.get_remote(repo)
        found = False

        branches = self.run("branch -a", subdir=repo, quiet=True, dryrun=False).stdout.splitlines()
        for line in branches:
            if remote in line:
                sline = line.split(remote + "/")[1].strip()
            elif line.startswith("*"):
                sline = line.split(" ")[1].strip()
            else:
                sline = line.strip()

            if sline == branch:
                found = True
                break

        return found

    def contains(self, repo, commit):
        branches = self.run("branch -r --contains {}".format(commit), subdir=repo, quiet=True).stdout.splitlines()
        return [branch.replace("origin/", "").strip() for branch in branches]


    def checkout(self, repo, branch, quiet=False, create=False):
        """
        Checkout objects in a repo, mostly branches.

        By default it will blow up if the branch doesn't exist, if create=True is passed
        it will set up the new branch and remotes
        """
        repoinfo = self.get_repoinfo(repo)
        result = None

        # see if the branch exists and create it if necessary
        if create and not self.branch_exists(repo, branch):
            if self.dryrun:
                base_branch = self.branch(repo)
            result = self.run("checkout -b {}".format(branch), subdir=repo, quiet=quiet, dryrun=False)
            remote = self.get_remote(repo)
            if not self.dryrun:
                self.push(repo, "--set-upstream {} {}".format(remote, branch))
            else:
                # for a dryrun just set the branch tracking to point at the source branch so pulls still "work"
                self.branch(repo, extra_opts="--set-upstream-to={}/{} {}".format(remote, base_branch, branch), quiet=False)
        else:
            result = self.run("checkout {}".format(branch), subdir=repo, quiet=quiet, dryrun=False)

        return result

    def cleanup(self, repo):
        """
        Clean up the clone of a repo.  Doesn't do anything if it can't find data about the clone

        Returns nothing
        """
        data = self.clone_info.get(repo, None)
        if data is not None:
            clonedir = data.get("clonedir", None)
            if clonedir and os.path.exists(clonedir):
                install_logger.debug("git cleaning up {}".format(clonedir))
                shutil.rmtree(clonedir)

            del self.clone_info[repo]

    def clone(self, repo):
        """
        Clones a repo to statedir.  If the repo is already checked out, set it back to the
        default branch and pull.

        Returns the clone directory
        """
        clone_url = self.get_vcs_url(repo)
        repoinfo = self.clone_info.get(repo)
        if repoinfo:
            clonedir = repoinfo["clonedir"]
        else:
            self.clone_info[repo] = dict()
            clonedir = os.path.join(self.state_dir, repo)
            self.clone_info[repo]["clonedir"] = clonedir

        # if the path already exists, just get it up todate
        if os.path.exists(clonedir):
            default_branch = self.default_branch(repo)
            self.checkout(repo, default_branch, quiet=True)
            self.pull(repo, quiet=True)
        else:
            self.run("clone {} {}".format(clone_url, clonedir), dryrun=False)

        return clonedir

    def default_branch(self, repo):
        """
        Get the default branch for the repo

        Returns the default branch
        """
        repoinfo = self.get_repoinfo(repo)
        default_branch = repoinfo.get("default_branch")

        if not default_branch:
            remote = self.get_remote(repo)
            origin = self.run("remote show {}".format(remote), subdir=repo, quiet=True, dryrun=False)
            for branch in origin.stdout.splitlines():
                if "HEAD branch" in branch:
                    default_branch=branch.split(":")[1].strip()
                    self.clone_info[repo]["default_branch"] = default_branch
                    break

        return default_branch

    def get_remote(self, repo):
        """
        Get the remote defined in the clone.   Usually origin but you never know.

        Returns the remote
        """
        repoinfo = self.get_repoinfo(repo)
        remote = repoinfo.get("remote")
        if not remote:
            remote = self.run("remote", subdir=repo, quiet=True, dryrun=False).stdout.strip()
            self.clone_info[repo]["remote"] = remote

        return remote

    def get_repoinfo(self, repo):
        """
        Get the dict containing information on the current repo.   Currently explodes
        if the value doesn't exist (i.e. you didn't clone the repo first).

        Returns a dict containing anything we stored about the clone
        """
        repoinfo = self.clone_info.get(repo)
        if not repoinfo:
            raise GitError("Unable to locate a clone of repo {}".format(repo))

        return repoinfo


    def ls_remote(self, repo, just_branches=False):
        """
        Do a 'git ls-remote'.

        Returns the splitlines() output of the command so it can be iterated on by the caller
        """
        vcs_url = self.get_vcs_url(repo)
        # this can be run during a dry run, it doesn't change anything
        output = self.run("ls-remote {}".format(vcs_url), quiet=True, dryrun=False)
        if just_branches and output:
            remotes = [line.split()[1] for line in output.stdout.splitlines()]
        else:
            remotes = output.stdout.splitlines()

        remotes = [rem.replace('remotes/origin', '').replace('refs/heads/', '') for rem in remotes]

        return remotes



    def merge(self, repo, merge_branch, message=None):
        """
        Merge a target branch into the current checked out branch.

        An optional message can be passed in, but that is only used in the event a merge
        commit is created which is unusual.

        Returns the subprocess result
        """
        repoinfo = self.get_repoinfo(repo)
        current_branch = self.branch(repo)
        fullcommand = "merge"
        if self.dryrun:
            # for a dryrun, perform the merge but don't let git auto-commit the changes
            fullcommand = fullcommand + " --no-commit --no-ff"

        if message:
            fullcommand = fullcommand + " -m \"" + message + "\""

        if self.branch_exists(repo, merge_branch):
            # get the merge branch tracked and up to date before the merge
            self.checkout(repo, merge_branch, quiet=True)
            self.pull(repo, quiet=True)
            self.checkout(repo, current_branch, quiet=True)
            result = self.run("{} {}".format(fullcommand,merge_branch), subdir=repo, dryrun=False)
            if self.dryrun:
                install_logger.dryrun("Merge successful: {}".format(result.stdout))
        else:
            raise GitError("Merge source branch {} does not exist.".format(merge_branch))

        return result

    def pull(self, repo, quiet=False):
        """
        Do a 'git pull'

        Returns the subprocess result
        """
        repoinfo = self.get_repoinfo(repo)
        result = self.run("pull", subdir=repo, quiet=quiet, dryrun=False)

    def push(self, repo, extra_opts=None):
        """
        Do a 'git push'.

        Optionally extra arguments can be passed in (i.e. --set-origin)

        Returns the subprocess result
        """
        repoinfo = self.get_repoinfo(repo)
        fullcommand = "push"
        if extra_opts:
            fullcommand = fullcommand + " " + extra_opts

        result = self.run(fullcommand, subdir=repo)

        return result

    def run(self, cmd, subdir=None, quiet=False, dryrun=None):
        """
        Wrapper around connection.sudo that does git specific things, like passing in the environment
        containing the GIT_ASKPASS variable, etc.

        Returns the subprocess result
        """
        # honor dryrun options
        if dryrun is None:
            dryrun = self.dryrun

        workdir = self.state_dir
        if subdir is not None:
            workdir = os.path.join(workdir,subdir)
            if not self.dryrun and not os.path.exists(workdir):
                # We don't expect the directory to exist during a dryrun, the commands won't execute anyway
                raise GitError("Specified git working directory {} does not exist.".format(workdir))

        fullcommand = "git -C {} {}".format(workdir,cmd)
        try:
            result = self.connection.sudo(fullcommand, env=self.env, quiet=quiet, dryrun=dryrun)
        except RunException as err:
            install_logger.error("git command failed: {}.".format(fullcommand))
            install_logger.error("         Error was: {}".format(err.stderr))
            raise

        return result

    def get_vcs_url(self, repo):
        """
        Format the URL needed to access git with the vcs_user

        Returns the URL
        """
        vcs_url = "https://{}@{}/vcs/cray/{}.git".format(self.vcs_user, self.vcs, repo)

        return vcs_url


