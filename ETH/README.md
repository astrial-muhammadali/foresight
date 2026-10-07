# ForeSight ETH → C3I

A Python integration demonstrator for ETH environmental readings and camera
observations. Kafka carries UTF-8 JSON. MinIO stores JPEGs. C3I prints readings
and detections and retrieves each referenced image.

For setup, credential changes, workflow diagrams, file responsibilities, and
step-by-step testing, start with the [User and Integration Guide](docs/USER_GUIDE.md).

This integration lives in `ETH/` within the [ForeSight repository](../README.md).
Server addresses and credentials are supplied privately and belong in `ETH/.env`.
The tracked `.env.example` intentionally contains no live connection details.

The sample values and detections are synthetic. This project does not connect
to ETH hardware or run an object detection model.

## Architecture

```text
ETH environmental sensors
    └─ JSON → Kafka: foresight.eth.environment → C3I

ETH camera / edge AI
    ├─ JPEG → MinIO: foresight-eth-images
    └─ after successful upload:
         detections + image reference
             → Kafka: foresight.eth.observations → C3I
                                                   └─ fetch JPEG from MinIO
                                                        → received_images/
```

The producers use `device_id` as the Kafka key, request `acks="all"`, and wait
for acknowledgement before reporting topic, partition and offset. Kafka keys
provide per-partition ordering; there is no global ordering across topics.

The C3I group is selected by `KAFKA_CONSUMER_GROUP` (`foresight-c3i` in code
and in the configuration template). It commits each record explicitly after
validation and processing, including image download. A failed record stops
the consumer with its topic, partition and offset; it is not silently skipped.
Restart after correcting the cause. With the default `earliest` reset policy,
delivery is **at least once**: processing
may repeat if the process exits before a commit or a commit reply is lost.

Kafka and MinIO do not share a transaction. If publication fails after upload,
the JPEG remains in MinIO and its reference is logged. A send timeout has an
ambiguous outcome, so the code does not delete the image automatically. This
demonstrator has no outbox, automatic orphan cleanup or exactly-once guarantee.

## Files

```text
ETH/
├── config.py                 # Environment loading and logging
├── messages.py               # Shared validation and UTC object keys
├── kafka_service.py          # Acknowledged JSON producer
├── minio_service.py          # Bucket operations and JPEG transfers
├── send_environment.py       # Send one sample sensor message
├── send_observation.py       # Upload one JPEG, then publish detections
├── c3i_consumer.py           # Process both topics and download images
├── requirements.txt          # Pinned direct runtime dependencies
├── .env.example              # Every application configuration variable
├── .gitignore
├── README.md
├── docs/
│   └── USER_GUIDE.md           # Setup, credentials, workflow, files, and testing
├── sample.jpg                # Valid synthetic 512×512 JPEG
├── received_images/
│   └── .gitkeep
└── tests/
    ├── __init__.py
    ├── test_project.py       # Offline contract and flow tests
    ├── test_kafka_auth.py    # Offline authentication validation and client wiring
    ├── test_connection_diagnostic.py # Offline diagnostic tests
    ├── test_kafka_connection.py     # Explicitly run live connection check
    ├── test_workflow.py             # Explicitly run Kafka/MinIO/C3I workflow
    └── test_workflow_runner.py      # Offline workflow regression tests
```

Installation also creates a local `.venv/`; Python may create `__pycache__/`.
Both are ignored, as are `.env` and downloaded images.

## Install Python and dependencies

Use Python **3.11 or newer** from [python.org](https://www.python.org/downloads/).
On Windows, install the Python launcher and verify `py --version`. The project
has been checked locally with Python 3.14.4. Kafka and MinIO servers are separate
prerequisites; installing their Python clients does not start either server.

PowerShell, starting at the `foresight` repository root (skip `Set-Location`
if already inside `ETH/`):

```powershell
Set-Location .\ETH
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
if (-not (Test-Path -LiteralPath '.env')) {
    Copy-Item -LiteralPath '.env.example' -Destination '.env'
}
```

Copy the example only for initial setup; preserve an existing `.env`. Explicit
interpreter paths avoid PowerShell activation-policy issues.

Linux/macOS, from `ETH/`:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
if [ ! -f .env ]; then cp .env.example .env; fi
```

The requested `kafka-python-ng` distribution imports as `kafka`. Do not install
the separate `kafka` or `kafka-python` distributions into this virtual environment.

## Configuration

Edit `.env` next to `config.py`. Existing shell variables take precedence. Loading
configuration or importing a module does not connect to a broker or storage.
The table lists code defaults. The tracked `.env.example` selects SASL/PLAIN
but leaves Kafka/MinIO addresses and credentials blank. Copy it to `.env` and
fill in the connection details shared privately before running. The template
cannot connect as supplied. `.env` interpolation is disabled so `${...}` in a
password stays literal.

| Variable | Default | Meaning |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Comma-separated broker addresses |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT` or `SASL_SSL` |
| `KAFKA_SASL_MECHANISM` | `PLAIN` | `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512`; match the broker |
| `KAFKA_SASL_USERNAME` | empty | Required for SASL; never committed in the example |
| `KAFKA_SASL_PASSWORD` | empty | Required for SASL; preserved exactly as supplied |
| `KAFKA_SSL_CAFILE` | empty | Optional PEM CA file for `SSL`/`SASL_SSL`; relative to project |
| `KAFKA_ENVIRONMENT_TOPIC` | `foresight.eth.environment` | Environmental topic |
| `KAFKA_OBSERVATIONS_TOPIC` | `foresight.eth.observations` | Observation topic |
| `KAFKA_CONSUMER_GROUP` | `foresight-c3i` | Consumer group |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | Initial/reset position: `earliest` or `latest` |
| `KAFKA_SEND_TIMEOUT_SECONDS` | `30` | Per-call metadata/acknowledgement/flush wait |
| `MINIO_ENDPOINT` | `localhost:9000` | S3 API host and port, without URL scheme |
| `MINIO_ACCESS_KEY` | `minioadmin` | Public local-development access key |
| `MINIO_SECRET_KEY` | `minioadmin` | Public local-development secret key |
| `MINIO_SECURE` | `false` | `true` for HTTPS, `false` for HTTP |
| `MINIO_BUCKET` | `foresight-eth-images` | Image bucket; created on first upload |
| `MINIO_TIMEOUT_SECONDS` | `15` | Storage socket read timeout |
| `RECEIVED_IMAGES_DIR` | `received_images` | Absolute path or path relative to project |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |

Timeouts must be greater than zero and at most 60 seconds. Connection retries
and successive operations can make total execution longer than one timeout.
Keep `earliest` for reliable retries on a new group: with `latest`, restarting
before the group's first successful commit can skip an uncommitted record
because no saved offset exists yet.
Keep `INFO` to display the required readings, boxes and image paths. `DEBUG`
adds exception traces. Credentials are excluded from the settings representation.

## Connect to your existing Kafka server with username/password

For a broker exposing **SASL_PLAINTEXT + PLAIN**, both the ETH producers and
C3I consumer use the same connection settings. The host below is fictitious;
use the address and credentials shared privately in the local, ignored `.env`:

```dotenv
KAFKA_BOOTSTRAP_SERVERS=kafka.example.invalid:29093
KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=your-existing-kafka-username
KAFKA_SASL_PASSWORD='your-existing-kafka-password'
KAFKA_SSL_CAFILE=
```

Replace those illustrative values with the existing broker credentials. The
actual `.env.example` leaves them blank so an unconfigured client fails before
attempting to connect. Single-quote a password containing spaces, `#` or `$`
using dotenv syntax. Do not put real credentials in `.env.example`. Supplying
credentials with `PLAINTEXT`/`SSL` raises an error instead of silently ignoring
authentication.

For a deployment mounting `./saslconfig/kafka_server_jaas.conf`, the server's
`KafkaServer` section defines accepted clients using `user_<username>`:

```text
KafkaServer {
    org.apache.kafka.common.security.plain.PlainLoginModule required
    user_eth_client="REPLACE_WITH_THE_EXISTING_OR_CHOSEN_PASSWORD";
};
```

In that illustrative entry, the Python username is `eth_client` (without
`user_`). Use the existing entry if it is already configured; do not replace
other users or broker login options in your JAAS file. Java's own `username`
and `password` options are broker login credentials, not a substitute for the
`user_<username>` entry that accepts client logins. See
[Confluent's SASL/PLAIN documentation](https://docs.confluent.io/platform/7.7/security/authentication/sasl/plain/overview.html).

Start the consumer and senders with the same commands below; no new Python
dependencies are needed. This client configuration does not create users on
the Kafka server; its JAAS configuration is maintained on the server. Live
authentication and complete Kafka/MinIO/C3I round trips have been verified
in the development environment. Rerun the connection diagnostic and workflow
test after changing credentials or endpoints; see the
[credential change workflow](docs/USER_GUIDE.md#credential-change-workflow).

### Compose configuration notes

Move `restart: always` out of ZooKeeper's `environment` and alongside `image`.
Quote `ZOOKEEPER_SASL_ENABLED: "false"` in Kafka's environment: Compose expects
boolean-looking environment values to be strings. These are
[Compose service settings](https://docs.docker.com/reference/compose-file/services/).

Declare the binding addresses explicitly under `kafka1.environment` to make
the two listeners unambiguous (some Confluent images derive these automatically).
The advertised external hostname below is a placeholder, not a usable server address:

```yaml
KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9093,SASL_PLAINTEXT://0.0.0.0:29093"
KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka1:9093,SASL_PLAINTEXT://kafka.example.invalid:29093"
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT"
KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
KAFKA_SASL_ENABLED_MECHANISMS: "PLAIN"
ZOOKEEPER_SASL_ENABLED: "false"
```

Retain the existing JAAS mount and `KAFKA_OPTS`. The internal `kafka1:9093`
listener is plaintext and is not the authenticated external endpoint. The
inter-broker SASL mechanism setting does not enable SASL on that plaintext
listener. Ensure clients can reach the advertised external address and port.

Pin both Confluent images to an explicit, compatible release for your existing
ZooKeeper deployment instead of `latest`. Confluent Platform 8.0 removed
ZooKeeper support; do not upgrade this deployment to that line without a
migration. See the [Confluent Docker configuration reference](https://docs.confluent.io/platform/current/installation/docker/config-reference.html).

`SASL_PLAINTEXT` authenticates but does **not encrypt passwords or messages**.
For an encrypted connection, the broker must offer `SASL_SSL`; then select that
protocol in `.env` and optionally set `KAFKA_SSL_CAFILE` for a private CA.
Certificate and hostname verification remain enabled. Changing the Python
protocol alone does not add TLS to the current listener.

## Start Kafka without containers

This section is an alternative local setup. If using the existing authenticated
server above, skip starting a second broker. To use the following plaintext
local broker, set `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` and
`KAFKA_SECURITY_PROTOCOL=PLAINTEXT`, and clear both `KAFKA_SASL_USERNAME` and
`KAFKA_SASL_PASSWORD` (also clear `KAFKA_SSL_CAFILE`).

Use an existing local broker, or download and unpack the binary distribution
from [Apache Kafka downloads](https://kafka.apache.org/downloads/). Install a
JDK (Java 17 is suitable for the following Kafka 3.9 example) and make `java`
available in the terminal. These commands use **Kafka 3.9.x KRaft**, following
the [official 3.9 quickstart](https://kafka.apache.org/39/getting-started/quickstart/).
Do not install ZooKeeper. Broker compatibility with this pinned Python client
must be checked before adopting a different Kafka release.

In PowerShell, change to your extracted Kafka directory, then run:

```powershell
# First setup only: do not reformat an existing broker's storage.
$foresightClusterId = & .\bin\windows\kafka-storage.bat random-uuid
.\bin\windows\kafka-storage.bat format --standalone -t $foresightClusterId -c .\config\kraft\reconfig-server.properties

# Keep this terminal running. Use only this command on subsequent starts.
.\bin\windows\kafka-server-start.bat .\config\kraft\reconfig-server.properties
```

For Linux/macOS, run in the extracted Kafka directory:

```bash
# First setup only.
foresight_cluster_id="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t "$foresight_cluster_id" -c config/kraft/reconfig-server.properties
bin/kafka-server-start.sh config/kraft/reconfig-server.properties
```

Kafka 4.x uses a different configuration path (`config/server.properties`);
consult its matching quickstart. The server commands above are deliberately
version-specific, not a claim that every broker version has been tested.

In a second PowerShell terminal in the Kafka directory, create both topics:

```powershell
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --if-not-exists --topic foresight.eth.environment --partitions 1 --replication-factor 1
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --if-not-exists --topic foresight.eth.observations --partitions 1 --replication-factor 1
```

On Linux/macOS replace `.\bin\windows\kafka-topics.bat` with
`bin/kafka-topics.sh`. If you override topic names in `.env`, create those names
instead. One broker and replication factor 1 are sufficient for this demo;
`acks="all"` does not add redundancy to a single broker.

## Start MinIO without containers

Use an existing MinIO server or an approved local installation. As of
2026-10-04, the [MinIO community server repository](https://github.com/minio/minio)
is archived and documents source-only distribution. Do not assume old binary
download links provide a maintained server. Confirm the server edition/build
you intend to use. The Python client remains the requested `minio` SDK.

For a local community source build, install the Go toolchain required by that
repository (its documented minimum is Go 1.24), then run in PowerShell:

```powershell
go install github.com/minio/minio@latest
$foresightGoPath = go env GOPATH

# Public demo credentials, for local development only.
$env:MINIO_ROOT_USER = 'minioadmin'
$env:MINIO_ROOT_PASSWORD = 'minioadmin'
& "$foresightGoPath\bin\minio.exe" server "$env:LOCALAPPDATA\ForeSight\minio-data" --address '127.0.0.1:9000' --console-address '127.0.0.1:9001'
```

If MinIO is already installed, use its executable instead of the Go install
step. On Linux/macOS:

```bash
go install github.com/minio/minio@latest
export MINIO_ROOT_USER=minioadmin
export MINIO_ROOT_PASSWORD=minioadmin
"$(go env GOPATH)/bin/minio" server "$HOME/.local/share/foresight/minio-data" --address 127.0.0.1:9000 --console-address 127.0.0.1:9001
```

Keep MinIO running. The SDK uses port **9000**, while the browser console uses
[localhost:9001](http://localhost:9001). `MINIO_ROOT_USER` and
`MINIO_ROOT_PASSWORD` configure the **server process**, separately from the
application's `.env`. Match the application's access and secret keys to those
server credentials. The first upload creates `foresight-eth-images`; the
consumer does not create buckets. No production credentials are included.

## Run the demonstration

Start the C3I consumer in a new PowerShell terminal at the repository root
(skip `Set-Location` if already inside `ETH/`):

```powershell
Set-Location .\ETH
.\.venv\Scripts\python.exe c3i_consumer.py
```

Send environmental data from another terminal in the project folder:

```powershell
.\.venv\Scripts\python.exe send_environment.py
```

Then send a JPEG observation:

```powershell
.\.venv\Scripts\python.exe send_observation.py
```

Optional arguments:

```powershell
.\.venv\Scripts\python.exe send_environment.py --incident-id INC-2026-000032 --device-id eth-sensor-02
.\.venv\Scripts\python.exe send_observation.py --incident-id INC-2026-000032 --device-id eth-camera-02 --image .\sample.jpg
```

On Linux/macOS replace `.\.venv\Scripts\python.exe` with `.venv/bin/python`.
Each script supports `--help`. The default sample image resolves relative to
the project even when launched elsewhere. An explicit relative `--image`
path resolves against the terminal's current directory.

Expected output includes `Delivered topic=... partition=0 offset=...`, the
incident/timestamp/device, all five sensor readings, a unique frame ID, the
detection count, named bounding boxes, and `Saved image to ...`.

Downloads use a SHA-256 hash of the bucket and object key as the `.jpg` filename
inside `received_images/`. This avoids path traversal, Windows filename issues
and collisions across devices/dates. Replayed messages replace the same file
after a successful download. Temporary downloads never replace a good image
until the new JPEG passes its framing check.

Use Ctrl-C to stop the consumer cleanly. Senders exit with code 0 on success,
1 on a handled failure, and 130 when interrupted. A clean consumer stop returns
0; processing or service failures return 1.

## JSON examples and validation

Environmental topic: **`foresight.eth.environment`**

```json
{
  "incident_id": "INC-2026-000031",
  "timestamp": "2026-10-04T17:20:01Z",
  "source": "ETH",
  "device_id": "eth-sensor-01",
  "data": {
    "co2": {"value": 450.25, "unit": "ppm"},
    "temperature": {"value": 23.6, "unit": "C"},
    "humidity": {"value": 48.2, "unit": "%RH"},
    "pressure": {"value": 1013.25, "unit": "hPa"},
    "ch4": {"value": 2.15, "unit": "ppm"}
  }
}
```

Observation topic: **`foresight.eth.observations`**

```json
{
  "incident_id": "INC-2026-000031",
  "timestamp": "2026-10-04T17:20:10Z",
  "source": "ETH",
  "device_id": "eth-camera-01",
  "frame_id": "frame-000123",
  "image": {
    "bucket": "foresight-eth-images",
    "object_key": "eth-camera-01/2026/10/04/frame-000123.jpg",
    "content_type": "image/jpeg"
  },
  "detections": {
    "num_boxes": 2,
    "boxes": [
      {"x": 120, "y": 85, "w": 90, "h": 210, "cls_id": 1},
      {"x": 250, "y": 180, "w": 110, "h": 130, "cls_id": 4}
    ]
  }
}
```

Every message requires non-empty IDs, `source="ETH"`, and an ISO 8601 UTC
timestamp. Senders use `datetime.now(timezone.utc)` and emit `Z`; validation
also accepts `+00:00`. The object key date comes from that same timestamp.

| Reading | JSON unit | Inclusive range |
| --- | --- | --- |
| CO2 | `ppm` | 0–5000 |
| Temperature | `C` | −40–85 |
| Humidity | `%RH` | 0–100 |
| Pressure | `hPa` | No physical range enforced; awaiting specification |
| CH4 | `ppm` | 1–10000 |

Values must be finite JSON numbers that fit the magnitude of float32.
Booleans, numeric strings, NaN and infinity are rejected. JSON has no float32
wire type; this code does not quantize or promise binary float32 precision.

Coordinates use top-left origin, x rightwards, y downwards. Each of `x`, `y`,
`w`, `h` must be an integer in 0–512; `cls_id` and `num_boxes` must be integers
in 0–255. The count must exactly match `len(boxes)`. Class mapping:
1=person, 2=door, 3=fire, 4=smoke. Other valid IDs display as `unknown`.

## Assumptions to confirm with ETH/C3I

- Confirm the pressure range; the code deliberately enforces none beyond
  numeric/finite float32 magnitude checks.
- Temperature uses `C` on the wire, matching the supplied JSON, and means °C.
- Coordinate bounds are enforced individually. No unrequested constraint on
  `x+w` or `y+h`, or prohibition on zero-size boxes, is imposed.
- Camera device/frame IDs use 1–128 ASCII letters, digits, `_` or `-`, starting
  with a letter/digit. Confirm this convention against real ETH identifiers.
- Sample imagery and boxes are illustrative; supplying a different image does
  not recompute detections. The JPEG check verifies start/end markers, not full
  image decoding. The consumer accepts only the configured image bucket.
- Kafka authentication must match the broker's listener and existing account.
  SASL/PLAIN matches the provided configuration; TLS requires a broker-side TLS
  listener as well as client configuration. MinIO HTTPS uses `MINIO_SECURE`.
- Confirm the target Kafka release and MinIO server build. The offline tests
  exercise application behavior, not live broker/S3 protocol compatibility.

## Verification

### Run the complete workflow against Kafka and MinIO

Fill Kafka and MinIO connection settings in the ignored `ETH/.env`, then run
from the repository root (skip `Set-Location` if already inside `ETH/`):

```powershell
Set-Location .\ETH
.\.venv\Scripts\python.exe .\tests\test_workflow.py --create-topics --count 3
```

This command sends **three environmental messages and three observations**, and
uploads three copies of the bundled synthetic `sample.jpg` with unique object
keys. It uses the real configured services, shared application message builders,
Kafka/MinIO service classes, and the C3I message handler. No separate consumer
terminal is needed for this test.

For every pair, the runner sends the environmental sample, uploads the JPEG,
and publishes the observation only after a successful upload. Its separate
consumer group starts before publication and verifies the exact acknowledged
topic/partition/offset records. Unrelated messages are ignored. Each received
JSON payload and Kafka key must match the sent data; every downloaded image
must have the same SHA-256 as the source JPEG. The runner commits only after
verification and checks those commits on the broker.

`--create-topics` explicitly permits creation of missing configured topics with
one partition and replication factor 1. Existing topics are preserved. Omit the
flag if your operator manages topic creation. Topic visibility and create, write,
read and test-group permissions are required for the corresponding operations.
The image bucket is created if absent and requires upload/download access.

Each run uses a unique `TEST-...` incident prefix and a
`foresight-workflow-TEST-...` consumer group. It leaves your configured C3I
group's offsets untouched. Existing C3I consumers may also receive these sample
messages because the test writes to the configured application topics.

Results are saved beneath your configured download directory:

```text
received_images/workflow-tests/TEST-<UTC timestamp>-<unique suffix>/
├── report.json        # Status, stage, sent JSON, offsets, object keys and hashes
└── images/            # JPEGs downloaded by the C3I handler
```

The report is updated after each acknowledged send, upload and verified receipt.
Kafka messages, MinIO images and the report are retained for inspection. A failed
or timed-out run may have published some data; inspect its report before retrying.
An interrupted worker can leave the last report status as `running`; that is not
a successful result. Credentials and raw service error responses are not written
to the report.

Defaults are one pair and a 120-second overall deadline. Other examples:

```powershell
# One pair using the bundled sample and existing topics.
.\.venv\Scripts\python.exe .\tests\test_workflow.py

# Use your own JPEG and allow more time (up to 300 seconds).
.\.venv\Scripts\python.exe .\tests\test_workflow.py --image C:\images\sample.jpg --count 2 --timeout 180
```

Exit codes: `0` all checks passed, `1` workflow failed, `2` invalid
configuration/arguments, `124` overall deadline exceeded, `130` interrupted.
The workflow runs only when explicitly invoked, never during offline test
discovery. The sample boxes remain illustrative when you supply another image.

### Live Kafka connection test

After entering the real connection details in the ignored `ETH/.env`, run
from PowerShell at the repository root (skip `Set-Location` if already inside `ETH/`):

```powershell
Set-Location .\ETH
.\.venv\Scripts\python.exe .\tests\test_kafka_connection.py
```

This lighter, read-only script loads the project's `.env` and checks an authenticated metadata
request, visibility of both configured topics, partition leaders, and a consumer
connection that queries partition offsets. It prints `PASS`/`FAIL` results and
does not display credentials or raw Kafka error responses. Shell environment
variables retain precedence over `.env`.
Put real credentials in `.env`, not `.env.example`; the latter is a reusable
template and intentionally has blank server address and credential fields.

This is a read-only connection test: it creates no topics, sends no messages,
fetches no message bodies, joins no consumer group, and commits no offsets.
It verifies metadata/offset access, **not** message READ/WRITE or consumer-group
permissions. A missing/invisible topic is reported as a failure; the test does
not attempt to create it.
If authentication passes but topic visibility fails, ask the broker operator
to verify that `foresight.eth.environment` and `foresight.eth.observations`
exist and that your account can describe them (or use your overridden names).

An overall 30-second deadline prevents indefinite retries. You can extend the
deadline or select another environment file:

```powershell
.\.venv\Scripts\python.exe .\tests\test_kafka_connection.py --timeout 60
.\.venv\Scripts\python.exe .\tests\test_kafka_connection.py --env-file C:\path\to\.env
```

Exit codes: `0` success, `1` connection/topic check failed, `2` invalid
configuration/arguments, `124` deadline exceeded, `130` interrupted. The script
also works from another working directory when invoked using its absolute path.
It runs only when explicitly invoked; importing it or running the offline test
suite below does not connect to Kafka.

### Offline tests

Run from the project directory:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe -m compileall -q config.py messages.py kafka_service.py minio_service.py send_environment.py send_observation.py c3i_consumer.py tests
.\.venv\Scripts\python.exe -c "import config, messages, kafka_service, minio_service, send_environment, send_observation, c3i_consumer; print('Imports OK')"
.\.venv\Scripts\python.exe -m pip check
```

Tests cover configuration, contract boundaries, invalid JSON, upload-before-send,
failure handling, unknown classes, explicit per-record commits, safe downloads,
authentication settings shared by both clients, credential handling, and an
offline round trip that compares JPEG bytes. They use standard-library
`unittest` and substitute external SDK clients; no additional test dependencies
or running services are required.

For a manual acceptance check, start both services, run the consumer and both
senders, then verify the downloaded JPEG matches `sample.jpg`. The automated
workflow above performs this hash verification for you. Manual checksum commands:

```powershell
Get-FileHash .\sample.jpg -Algorithm SHA256
Get-ChildItem .\received_images\*.jpg | Get-FileHash -Algorithm SHA256
```

Only compare files from this demonstration. A default observation should have
the same checksum as `sample.jpg`.

## Troubleshooting

| Symptom | Check / action |
| --- | --- |
| `No module named kafka` or import conflicts | Use the project `.venv` and install `requirements.txt`. Avoid other packages that also provide the `kafka` namespace. |
| Cannot connect to Kafka | Check port 29093 for the configured authenticated server (9092 for the alternative local demo), bootstrap addresses and advertised listener. A bootstrap connection alone does not guarantee the advertised address is reachable. |
| Missing SASL credentials | Fill `KAFKA_SASL_USERNAME` and `KAFKA_SASL_PASSWORD` in the ignored `.env`, or set them as process variables. |
| Kafka authentication fails | Match `SASL_PLAINTEXT` + `PLAIN` to this server, and match credentials to a `user_<username>` entry in its JAAS file. The username does not include the `user_` prefix. Confirm which settings are overridden by shell variables. |
| Kafka certificate verification fails | Use the hostname covered by the broker certificate and the correct trusted CA. Do not disable verification. The supplied plaintext listener cannot accept TLS connections. |
| Kafka metadata/send timeout | Create both configured topics and verify broker availability and permissions. Check broker/client version compatibility. |
| Consumer sees no new output | The group resumes its committed offsets. Send another sample, or use a new `KAFKA_CONSUMER_GROUP` with `earliest` to replay. Changing `earliest` alone does not reset a group's commits. |
| MinIO connection refused | Start MinIO; use S3 port 9000, not console port 9001. Check HTTP versus HTTPS. |
| Access denied or signature mismatch | Check the application's MinIO keys against the server and permissions to inspect/create the bucket, upload, and download. |
| Missing or invalid JPEG | Check `--image`; renaming a PNG to `.jpg` does not make it a JPEG. |
| Image exists but observation failed | Upload succeeded and Kafka acknowledgement failed. Inspect the logged object reference and broker messages before retrying; duplicate messages/orphaned images are possible. |
| Consumer stops on an invalid record | Review its logged topic/partition/offset and fix the producer or missing image. Restarting retries uncommitted records. A permanently malformed record requires a deliberate operator decision about that group's offset; this demo does not discard it automatically. |
| Windows Kafka batch command fails | Check JDK/PATH, use a short extraction path and follow the matching Kafka release documentation. WSL/Linux is an alternative if the native distribution does not run in your environment. |
| Need diagnostic details | Set `LOG_LEVEL=DEBUG` and rerun. |

SDK references: [kafka-python-ng](https://pypi.org/project/kafka-python-ng/) and
[MinIO Python SDK](https://github.com/minio/minio-py).
