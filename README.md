# iuf-cli

Install and Upgrade Framework (IUF) CLI

### Contributing

1. Create a development branch.
2. Make changes, put up a PR, get reviews
3. Merge changes
4. Tag the merged commit

   i. Find the commit sha of your merged changes (e.g. 1234abc)

   ii. Locally, checkout the correct release branch and pull the latest commits

   iii. Tag your commit with an incremented tag by running `git tag -a vX.X.X 1234abc -m "Jira-ticket-number description of change"`.

   iv. Push the new tag by running `git push origin vX.X.X`

5. Verify that the rpm has built with the new tag in [IUF-CLI rpm in arifactory](https://artifactory.algol60.net/ui/repos/tree/General/csm-rpms/hpe/stable/sle-15sp4/iuf-cli).
6. Create a PR to change the IUF-CLI version in CSM. Specifically, change the version in [csm/rpm/cray/csm/sle-15sp4/index.yaml](https://github.com/Cray-HPE/csm/blob/main/rpm/cray/csm/sle-15sp4/index.yaml).
