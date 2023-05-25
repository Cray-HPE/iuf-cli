# iuf-cli

Install and Upgrade Framework (IUF) CLI

### Contributing

1. Create a development branch.
2. Tag your development branch by incrementing the tag version.
        
    i. Get a list of current tags by running `git tag`.

    ii. Tag your branch by running `git tag  -a vX.X.X -m "Jira-ticket-number description of change"`.

3. Put up a PR, get reviews, merge PR.
4. Verify that the rpm has built with the new tag in [IUF-CLI rpm in arifactory](https://artifactory.algol60.net/ui/repos/tree/General/csm-rpms/hpe/stable/sle-15sp4/iuf-cli).
5. Create a PR to change the IUF-CLI version in CSM. Specifically, change the version in [csm/rpm/cray/csm/sle-15sp4/index.yaml](https://github.com/Cray-HPE/csm/blob/main/rpm/cray/csm/sle-15sp4/index.yaml).
