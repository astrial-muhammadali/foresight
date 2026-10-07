# GitHub repository setup

## Repository details

| Field | Value |
| --- | --- |
| Repository name | `foresight` |
| Description | ForeSight partner integrations for ETH, CESM and CNR, with Kafka messaging, MinIO image storage and tested C3I workflows. |
| Default branch | `main` |
| Suggested topics | `foresight`, `kafka`, `minio`, `python`, `sensor-data`, `computer-vision`, `c3i` |
| Visibility | Choose the visibility intended by the project owner; the tracked files contain no live deployment configuration. |
| README | Included at the repository root |
| Git ignore rules | Included at the repository root and under `ETH/` |
| License | No license has been added; the project owner can select one separately. |

Create an **empty** GitHub repository named `foresight`. Leave GitHub's automatic README, `.gitignore`, and license initialization disabled for this initial import, because the local repository already supplies its files.

## Initial publication

Run Git commands from the local `foresight` repository root. Review staged content before making the first commit:

```powershell
git status --short
git diff --cached --stat
git diff --cached
```

The tracked ETH configuration template is `ETH/.env.example`. The populated `ETH/.env`, virtual environment, and runtime reports must stay ignored. Credential-bearing email drafts and private deployment details are maintained outside this repository.

If the initial commit has not yet been created:

```powershell
git add .
git commit -m "Initialize ForeSight partner integrations"
```

After the GitHub repository exists, replace the placeholder owner below with the actual owner. Add `origin` only if it is not already configured:

```powershell
git remote add origin https://github.com/OWNER/foresight.git
git push -u origin main
```

If `origin` already exists, inspect it with `git remote -v` and update it deliberately if required. Never put a GitHub token or password inside the remote URL or any tracked file. Authenticate through your local Git credential manager or GitHub CLI.

The repository includes an offline test workflow for ETH on Python 3.11 and 3.14. It does not require GitHub secrets for Kafka or MinIO and does not run live service tests.

## ETH onboarding

Share the repository URL and attach `ETH/docs/USER_GUIDE.md` to the onboarding email. Provide the real environment configuration privately. The recipient copies `ETH/.env.example` to `ETH/.env`, enters the supplied values, and runs the offline suite, Kafka diagnostic, and live workflow in that order.

The public guide uses placeholders. It can be attached to the email without including the private deployment settings.
