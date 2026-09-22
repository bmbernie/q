# q / qd

`q` is a lightweight terminal client for submitting Codex-backed queries.

`qd` is the persistent local daemon that owns query execution, durable state,
named sessions, logging, recovery, and cancellation.

## Architecture

```text
q
 |
 | Unix-domain IPC
 v
qd
 |
 | HTTP / NDJSON
 v
chatgpt-worker-broker
 |
 | JSON-RPC over stdio
 v
codex app-server
 |
 v
ChatGPT-backed Codex access

`q` is intentionally thin. `qd` remains alive independently of the invoking
terminal and owns query lifecycle and persistence.

## Requirements

- Python 3.13+
- `chatgpt-worker-broker` running locally
- Codex App Server configured by the broker
- ChatGPT/Codex authentication already established

### Broker compatibility

q/qd v0.2.0 requires the worker-broker capabilities present in:

    q-v0.2.0-backend

That broker tag resolves to:

    919f89d56e16d816335a4227662dd4140e2b32ce

Required broker capabilities include:

- streamed Codex query transport
- persistent Codex thread resume
- Codex turn interruption
- App Server interactive-request relay
- interactive approval policy routing to the user

An older broker, including the original q/qd v0.1.0 backend state, is not
sufficient for the full v0.2.0 feature set.

The default broker endpoint is:

```text
http://127.0.0.1:8792
```

## Install

From the repository:

    ./scripts/install-user.sh

The installer:

- installs the package into .venv
- installs q and qd under ~/.local/bin
- creates ~/.config/q/config.toml if it does not already exist
- installs and enables qd.service
- preserves an existing config file

Verify:

    command -v q
    command -v qd
    systemctl --user is-enabled qd.service
    systemctl --user is-active qd.service

## Configuration

Default config:

    ~/.config/q/config.toml

Current defaults:

    model = "gpt-5.6-luna"
    reasoning_effort = "xhigh"
    tools = "enabled"
    sandbox = "read-only"
    log_rotate_bytes = 16777216
    log_rotate_archives = 5
    query_log_retention_days = 30
    query_log_cleanup_interval_seconds = 3600

`log_rotate_bytes` controls size-based rotation for the global and
named-session NDJSON logs. The default is 16 MiB.

`log_rotate_archives` controls how many rotated generations are retained.
The default is 5.

Set `log_rotate_bytes = 0` to disable size-based rotation.

Per-query logs are not rotated.

Per-query logs are instead governed by retention:

    query_log_retention_days = 30
    query_log_cleanup_interval_seconds = 3600

`query_log_retention_days` controls how long completed, failed,
cancelled, and indeterminate query logs are retained. The default is
30 days.

Set `query_log_retention_days = 0` to disable per-query log cleanup.

`query_log_cleanup_interval_seconds` controls how often qd checks for
expired per-query logs. The default is 3600 seconds.

Cleanup runs once when qd starts and then periodically while the daemon
is running.

Only the per-query NDJSON file is removed. The durable SQLite query
record is preserved, including its state, timestamps, backend IDs, and
other metadata.

Queued and running query logs are never eligible for retention cleanup.

The daemon reads configuration at startup.

After changing the file:

    systemctl --user restart qd.service

## Basic usage

Attached streaming query:

    q "Explain this function"

Named persistent session:

    q -s reverse-engineering "Analyze this function"
    q -s reverse-engineering "Continue from the previous result"

Reasoning override:

    q -r high "Review this"
    q -r xhigh "Review this carefully"

Model override:

    q -m gpt-5.6-sol "Review this"

Tools:

    q --tools "Inspect the repository"
    q --no-tools "Answer without using tools"

Sandbox:

    q --sandbox read-only "Inspect this project"
    q --sandbox workspace-write "Make the requested changes"

## Detached queries

Detach and print the query ID:

    q -q "Run this task"

Detach silently:

    q -qq "Run this task"

All queries are logged. -q and -qq control terminal behavior only.

## Input sources

Prompt argument:

    q "Explain this"

stdin:

    cat notes.txt | q

Prompt plus stdin:

    cat source.c | q "Analyze this code"

File input:

    q -f source.c "Analyze this"

Multiple files:

    q -f old.c -f new.c "Compare these"

Files are currently treated as UTF-8 text input.

## Streaming

Streaming is the default.

Buffer the answer until completion:

    q --no-stream "Explain this"

## Query management

Show query status:

    qd status QUERY_ID

Show full persisted query metadata:

    qd show QUERY_ID

List queries:

    qd queries

List queries for one named session:

    qd queries -s SESSION

List queued or running queries:

    qd queries --running

List named sessions:

    qd sessions

Replay or follow assistant output:

    qd follow QUERY_ID

Cancel a query:

    qd cancel QUERY_ID

Check the q state database:

    qd db-check

Create a consistent database backup:

    qd db-backup

Or choose an explicit destination:

    qd db-backup ./q-backup.sqlite3

Without a path, backups are written under:

    ~/.local/share/q/backups/

using names like:

    q-YYYYMMDD-HHMMSS.sqlite3

`qd db-backup` uses SQLite's backup API while qd remains running, so the
snapshot is transactionally consistent even when the live database is in
WAL mode.

Before publishing the snapshot, q verifies it with SQLite
`PRAGMA quick_check`.

Backup files:

- are standalone SQLite files
- do not require `-wal` or `-shm` sidecars
- are created with mode `0600`
- are never allowed to overwrite an existing destination

The command returns structured JSON containing the backup path, size, and
quick-check result.

`qd db-check` runs SQLite `PRAGMA quick_check` through the same `QStore`
connection used by qd and reports the active durability/concurrency
settings, including:

- journal mode
- synchronous mode
- foreign-key enforcement
- busy timeout
- WAL autocheckpoint threshold

A healthy database reports:

    "ok": true
    "quick_check": [
      "ok"
    ]

The command exits with status 0 when the check is healthy and a nonzero
status when SQLite reports a problem.

q uses SQLite WAL mode with `synchronous = FULL`, foreign-key enforcement,
and a 5000 ms busy timeout. These settings are applied explicitly when
the state store opens.

## Logs

Logging is always enabled.

Global durable event log:

    ~/.local/state/q/q.ndjson

Per-query event logs:

    ~/.local/state/q/queries/<QUERY_ID>.ndjson

Named-session event logs:

    ~/.local/state/q/sessions/<SESSION>.ndjson

The global and named-session logs use size-based rotation. With the
default configuration, each active log rotates at 16 MiB and retains up
to five archives:

    q.ndjson
    q.ndjson.1
    q.ndjson.2
    ...
    q.ndjson.5

`.1` is the most recently rotated file. Older generations move toward
`.5`.

`qd logs` reads retained archives in chronological order before the
current log. `qd logs -f` continues following across a rollover.

Per-query logs are kept as individual durable query records and are not
rotated. By default, terminal-query logs are retained for 30 days and
then removed by qd's retention cleanup.

Deleting an expired per-query log does not delete the query's SQLite
metadata. Commands such as `qd status` and `qd queries` continue to
report the durable query record.

If `qd follow QUERY_ID` is used after that query's per-query log has
expired, it reports:

    qd: per-query log is not retained

Service diagnostics:

    journalctl --user -u qd.service

Show the global durable event stream:

    qd logs

Follow new global events:

    qd logs -f

Show one named session:

    qd logs -s SESSION

Follow one named session:

    qd logs -f -s SESSION

qd logs exposes raw durable NDJSON events.

qd follow QUERY_ID is the cleaner assistant-answer view.

## Durable state

SQLite database:

    ~/.local/share/q/q.sqlite3

Runtime socket:

    $XDG_RUNTIME_DIR/q/qd.sock

Runtime lock:

    $XDG_RUNTIME_DIR/q/qd.lock

Default state directory:

    ~/.local/state/q/

## Query states

Queries use these durable states:

    queued
    running
    completed
    failed
    indeterminate
    cancelled

Typical lifecycle:

    queued
      |
      v
    running
      |
      +-- completed
      +-- failed
      +-- cancelled
      +-- indeterminate

Terminal states are not silently replayed.

## Named sessions

A q session name maps to an opaque Codex thread ID.

A newly accepted session initially has:

    state = pending
    backend_thread_id = NULL

After Codex creates the thread:

    state = active
    backend_thread_id = 01a0...

The binding is one-time. An active session cannot silently switch to another
backend thread.

Queries for the same named session are serialized.

Queries for unrelated sessions can execute concurrently.

## Restart behavior

qd owns recovery.

Completed queries remain replayable after daemon restart because qd follow can
reconstruct their output from durable per-query NDJSON logs.

Queued queries are safe to schedule again after restart because they had not yet
crossed the local execution boundary into an upstream turn.

A query found in running state after daemon restart becomes:

    indeterminate

qd never blindly replays that upstream operation.

If that query belonged to a named session, the session is marked:

    broken

## Cancellation

qd cancel QUERY_ID records the cancellation request and interrupts the active
Codex turn through the broker when thread and turn IDs are available.

Successful interruption transitions the query to:

    cancelled

## Security

The systemd service uses:

    UMask=0077

The q IPC socket is created with mode 0600.

The runtime directory is created with mode 0700.

## Tools

Tools are enabled by default unless configuration or an explicit q option
disables them.

Both non-interactive and interactive Codex tool flows are supported.

For an attached query, q can mediate App Server requests including:

- command execution approval
- file-change approval
- tool user input
- MCP elicitation
- additional permission approval

When Codex requests approval, q displays only the decisions offered by that
specific request.

Example:

    [q interaction] command approval
      command: ...
      reason: ...
      [y] accept once
      [e] accept with proposed exec-policy amendment
      [c] cancel

Interaction responses are sent back through qd and the worker broker to the
pending Codex App Server request.

For detached queries submitted with -q or -qq, qd never auto-approves an
interactive request. It selects a safe legal negative response: decline when
available, otherwise cancel.

Interactive requests and responses are recorded in the durable q event logs.

## Service control

Status:

    systemctl --user status qd.service

Restart:

    systemctl --user restart qd.service

Stop:

    systemctl --user stop qd.service

Start:

    systemctl --user start qd.service

Service logs:

    journalctl --user -u qd.service

## Development

Run the full test suite:

    .venv/bin/python -m pytest -q

Check whitespace errors:

    git diff --check

Current package version:

    0.2.0
