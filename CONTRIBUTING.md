# Contributing to ForeSight

Keep partner-specific code in `ETH/`, `CESM/`, or `CNR/`. Describe shared repository conventions at the root. CESM and CNR are placeholders until their own implementations and contracts are agreed.

## Configuration

Commit configuration schemas and sanitized `.env.example` files. Keep actual Kafka/MinIO endpoints, credentials, certificates, and deployment-specific output in ignored local files. Use fictitious hosts such as `kafka.example.invalid` in documentation and mocked tests.

Before committing, review `git status --short` and `git diff --cached`. Confirm that `.env`, virtual environments, downloaded images, workflow reports, and private handoff documents are absent from the staged files. Do not use force-add to include ignored local configuration.

## ETH changes

Run commands from `ETH/` using its virtual environment:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Update the [user guide](ETH/docs/USER_GUIDE.md) when changing commands, configuration, or workflow behavior. Changes to the message contract should update producers, the consumer, validation, and relevant tests together.

Run live tests against an explicitly configured test environment when validating service integration. They create remote messages, image objects, and test-group commits. Record the outcome in the change description without copying credentials or private server addresses.

## Pull requests

Describe the problem, resulting behavior, affected partner, and validation performed. Identify any contract or configuration changes that collaborators must adopt.
