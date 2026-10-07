# GitHub repository setup

## Repository details

| Field | Value |
| --- | --- |
| Repository name | `foresight` |
| Repository URL | [astrial-muhammadali/foresight](https://github.com/astrial-muhammadali/foresight) |
| Description | ForeSight partner integrations for ETH, CESM and CNR, with Kafka messaging, MinIO image storage and tested C3I workflows. |
| Default branch | `main` |
| Suggested topics | `foresight`, `kafka`, `minio`, `python`, `sensor-data`, `computer-vision`, `c3i` |
| Visibility | Choose the visibility intended by the project owner; the tracked files contain no live deployment configuration. |
| README | Included at the repository root |
| Git ignore rules | Included at the repository root and under `ETH/` |
| License | [MIT](../LICENSE), selected when the GitHub repository was created |

The GitHub repository has been created. Its initial MIT license commit is preserved in the project's history alongside the prepared ETH, CESM, and CNR structure.

## Publishing changes

Run Git commands from the local `foresight` repository root. Review changes and staged content before committing:

```powershell
git status --short
git diff --cached --stat
git diff --cached
```

The tracked ETH configuration template is `ETH/.env.example`. The populated `ETH/.env`, virtual environment, and runtime reports must stay ignored. Credential-bearing email drafts and private deployment details are maintained outside this repository.

The initial project commit already exists. For subsequent changes, stage the intended files and use a descriptive commit message:

```powershell
git add .
git commit -m "Describe the change"
```

The prepared checkout uses `origin` for the repository below. Add it only if it is not already configured, then push the branch:

```powershell
git remote add origin https://github.com/astrial-muhammadali/foresight.git
git push -u origin main
```

If `origin` already exists, inspect it with `git remote -v` and update it deliberately if required. Never put a GitHub token or password inside the remote URL or any tracked file. Authenticate through your local Git credential manager or GitHub CLI.

The repository includes an offline test workflow for ETH on Python 3.11 and 3.14. It does not require GitHub secrets for Kafka or MinIO and does not run live service tests.

## ETH onboarding

Share the repository URL and attach `ETH/docs/USER_GUIDE.md` to the onboarding email. Provide the real environment configuration privately. The recipient copies `ETH/.env.example` to `ETH/.env`, enters the supplied values, and runs the offline suite, Kafka diagnostic, and live workflow in that order.

The public guide uses placeholders. It can be attached to the email without including the private deployment settings.
