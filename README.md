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

## Logs

Logging is always enabled.

Global durable event log:

    ~/.local/state/q/q.ndjson

Per-query event logs:

    ~/.local/state/q/queries/<QUERY_ID>.ndjson

Named-session event logs:

    ~/.local/state/q/sessions/<SESSION>.ndjson

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

Non-interactive Codex tools are supported.

Current limitation: App Server requests that require interactive client-side
approval or user input are not yet mediated through q. The broker currently
rejects those interactive App Server-to-client requests.

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

    0.1.0

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

## Logs

Logging is always enabled.

Global durable event log:

    ~/.local/state/q/q.ndjson

Per-query event logs:

    ~/.local/state/q/queries/<QUERY_ID>.ndjson

Named-session event logs:

    ~/.local/state/q/sessions/<SESSION>.ndjson

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

Non-interactive Codex tools are supported.

Current limitation: App Server requests that require interactive client-side
approval or user input are not yet mediated through q. The broker currently
rejects those interactive App Server-to-client requests.

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

    0.1.0
