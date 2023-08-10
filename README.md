# iuf-cli

Install and Upgrade Framework (IUF) CLI

### Contributing

1. Determine the latest CSM release your change applies to. This determines the
   branch you start from. For CSM 1.4, use `release/1.4`. For CSM 1.5, use
   `release/1.5`. For CSM 1.6, use `main`.

1. Check out the branch determined in the previous state and ensure it's up to
   date. For example, for the main branch:

   ```
   git checkout main
   git pull
   ```

1. Create a development branch from this branch:

   ```
   git checkout -b JIRA-XYZ-short-description
   ```

1. Make changes, commit, and push:

   ```
   git commit -av
   git push -u origin JIRA-XYZ-short-description
   ```

1. Open a pull request from your branch to the appropriate base branch
   determined in the first step.

1. Once approved, merge the pull request.

1. Backport the change to any release branches that need it. For example,
   you may need to backport a change from main to `release/1.5` and
   `release/1.4`.

   To do this, add a comment of the form `/backport BRANCH` to the pull request.
   This will kick off a GitHub Actions workflow that will create a new PR.

### Tagging a new release

Follow this procedure to tag a new release, so that it can be included in a CSM
version.

1. Pull the latest version of the branch to which you merged and ensure your
   commit is the `HEAD` of this branch. For example, for `release/1.5`:

   ```
   git checkout release/1.5
   git pull
   ```

1. Tag the latest commit on this branch with a tag of the form `vX.Y.Z`
   where `X.Y` is the CSM version and `Z` is incremented.

   ```
   git tag vX.Y.Z
   ```

1. Push the tag.

   ```
   git push origin vX.Y.Z
   ```

1. Monitor the [tags in the iuf-cli Jenkins multibranch pipeline](https://jenkins.algol60.net/job/Cray-HPE/job/iuf-cli/view/tags/)
   and verify the build of the newly pushed tag is successful.

1. Verify that the new `iuf-cli` RPM has been published to the stable csm-rpms repository
   in Artifactory under the [iuf-cli directory](https://artifactory.algol60.net/ui/repos/tree/General/csm-rpms/hpe/stable/sle-15sp4/iuf-cli).

1. Create a PR to change the IUF-CLI version in CSM. Specifically, change the version in
   [csm/rpm/cray/csm/sle-15sp4/index.yaml](https://github.com/Cray-HPE/csm/blob/main/rpm/cray/csm/sle-15sp4/index.yaml).
