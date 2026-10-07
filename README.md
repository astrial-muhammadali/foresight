# ForeSight

Partner integrations for the ForeSight project. This repository organizes the ETH, CESM, and CNR contributions in separate folders, with shared repository documentation at the root.

## Integrations

| Partner | Directory | Status |
| --- | --- | --- |
| ETH | [ETH/](ETH/README.md) | Python integration with Kafka messages, MinIO JPEG storage, a C3I consumer, and offline/live tests |
| CESM | [CESM/](CESM/README.md) | Reserved for the CESM integration; implementation pending |
| CNR | [CNR/](CNR/README.md) | Reserved for the CNR integration; implementation pending |

```text
foresight/
├── ETH/                  # ETH application, sample image, tests, and user guide
├── CESM/                 # CESM integration placeholder
├── CNR/                  # CNR integration placeholder
├── docs/
│   └── GITHUB_SETUP.md   # Repository details and initial publishing steps
├── .github/workflows/   # Offline ETH tests for pushes and pull requests
├── .gitignore
├── CONTRIBUTING.md
└── README.md
```

## Start with ETH

Use Python 3.11 or newer. From the repository root in PowerShell:

```powershell
Set-Location ETH
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
if (-not (Test-Path -LiteralPath '.env')) {
    Copy-Item -LiteralPath '.env.example' -Destination '.env'
}
```

Populate `ETH/.env` with the connection details supplied privately for your environment. The tracked template intentionally leaves server addresses and credentials blank. Keep `.env` local; it is excluded from Git.

Then validate the application without accessing external services:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Follow the [ETH User and Integration Guide](ETH/docs/USER_GUIDE.md) for configuration, credential changes, workflow diagrams, file responsibilities, live tests, and troubleshooting. The [ETH README](ETH/README.md) also contains full JSON examples and optional local server setup.

## Configuration and collaboration

Actual server addresses, credentials, private connection files, and generated reports do not belong in the repository. Each partner should keep a sanitized configuration example alongside its own implementation. There is no shared runtime configuration for CESM or CNR yet.

GitHub Actions runs the ETH offline tests with no service credentials. Live workflow tests are explicit commands that publish Kafka messages and upload images; they are not run by CI.

See [Contributing](CONTRIBUTING.md) for change and verification conventions and [GitHub setup](docs/GITHUB_SETUP.md) for the repository description and first-push instructions.
