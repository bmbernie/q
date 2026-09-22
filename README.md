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
- safely restarts an already-running qd only after acquiring its restart guard
- waits for qd IPC readiness before returning

When upgrading an active installation, `install-user.sh` first asks the
running daemon to acquire a restart guard.

The guard provides an atomic execution boundary:

- new query submissions are rejected while the guard is active
- queued queries cannot transition to running while the guard is active
- if any query is already running, guard acquisition fails and the installer
  exits without restarting qd
- queued queries remain durable and are scheduled again by normal qd restart
  recovery

A successful guard acquisition returns an opaque owner token. Only that token
can renew or release the guard, preventing an unrelated client from releasing
another installer's guard.

Restart guards use a 300-second lease. `install-user.sh` renews the lease
immediately before restarting qd. If renewal fails or ownership has been lost,
the installer aborts instead of performing an unsafe restart.

If the installer exits or is killed while holding a guard, the lease eventually
expires automatically. qd then accepts submissions again and allows queued
queries to cross into running state, so a dead installer cannot leave the
daemon permanently guarded.

A refused upgrade leaves the existing daemon and running query untouched.
After the active query finishes, run the installer again.

The guard applies to `install-user.sh`. A direct:

    systemctl --user restart qd.service

does not perform this installer preflight and therefore retains the normal
restart semantics described below.

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
    backup_retention_count = 20
    backup_retention_days = 90
    backup_cleanup_interval_seconds = 3600

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

Managed database backups also have bounded retention:

    backup_retention_count = 20
    backup_retention_days = 90
    backup_cleanup_interval_seconds = 3600

`backup_retention_count` targets the maximum number of retained
q-managed backups after age cleanup. The default is 20. The actual
retained count can exceed this value when necessary to preserve the
newest backup in each managed backup class.

`backup_retention_days` removes q-managed backups older than the
configured age. The default is 90 days.

The count and age limits are independent. Set either value to `0` to
disable that particular limit. Set both to `0` to disable automatic
backup cleanup entirely.

When backup retention is enabled, `backup_cleanup_interval_seconds`
controls how often qd runs cleanup. The default is 3600 seconds. Cleanup
also runs once when qd starts.

Only backups carrying q's managed-backup provenance marker are eligible
for automatic deletion. Explicit custom-path backups created with:

    qd db-backup PATH

are not marked as managed and are never removed automatically.

For every managed backup class, q always protects the newest available
backup. The managed classes are:

- normal q-generated backups
- pre-restore rollback backups
- pre-migration rollback backups

Unmarked backup files, including backups created before managed-backup
retention existed, are ignored by automatic cleanup.

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

Run a complete operational diagnostic:

    qd doctor

`qd doctor` is read-only and emits structured JSON. It checks both the
local q installation and the running daemon and broker.

The report includes:

- q/qd client and daemon version compatibility
- configuration validity and active defaults
- config file type and permissions
- SQLite database type and permissions
- state directory permissions
- backup directory permissions
- daemon Unix socket type and mode
- daemon IPC reachability
- SQLite `PRAGMA quick_check`
- database schema version
- WAL, synchronous, foreign-key, busy-timeout, and autocheckpoint settings
- chatgpt-worker-broker reachability and `/health` status

A healthy deployment reports:

    "ok": true

and exits with status 0.

The `version` section compares the package version loaded by the `qd`
client process with the package version loaded by the running daemon:

    "version": {
      "client": "0.8.0",
      "daemon": "0.8.0",
      "match": true,
      "ok": true
    }

If q is upgraded on disk but qd has not yet been restarted, the versions
can differ. In that case `match` and the version check's `ok` field are
false, the rest of the diagnostics are still reported, and `qd doctor`
exits with status 1.

Restarting qd loads the newly installed package version:

    systemctl --user restart qd.service

If any required diagnostic fails, `qd doctor` still attempts to emit the
complete report, sets:

    "ok": false

and exits with a nonzero status.

Diagnostic network/IPC operations are bounded so `qd doctor` cannot wait
indefinitely on an unresponsive dependency.

The daemon IPC diagnostic uses a 5 second deadline. Its JSON result
includes:

    "timed_out": false
    "timeout_seconds": 5.0

If the daemon accepts the connection but does not complete the diagnostic
within that window, the IPC check is marked unhealthy with
`"timed_out": true`, and `qd doctor` exits nonzero.

The chatgpt-worker-broker `/health` request also uses a 5 second deadline.
A broker health timeout is reported as a failed broker diagnostic rather
than blocking the command indefinitely.

These deadlines apply only to operational diagnostics. Normal attached
queries, streaming output, log following, and other long-lived q
operations keep their existing streaming behavior.

If qd cannot be reached, local configuration, filesystem, and socket
diagnostics remain available while daemon-side database and broker
diagnostics are reported as unavailable.

Expected secure filesystem modes are:

    ~/.config/q/config.toml          0600
    ~/.local/share/q/q.sqlite3       0600
    ~/.local/state/q/                0700
    ~/.local/share/q/backups/        0700
    $XDG_RUNTIME_DIR/q/qd.sock       0600

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

A backup created with the default destination is registered as a
q-managed backup and participates in automatic retention.

A backup created with an explicit destination:

    qd db-backup PATH

is treated as operator-managed. It receives no managed-backup provenance
marker and is never automatically deleted by q, even if its filename
resembles a normal q backup.

The command returns structured JSON containing the backup path, size, and
quick-check result.

Restore q state from a validated backup:

    systemctl --user stop qd.service
    qd db-restore ~/.local/share/q/backups/q-YYYYMMDD-HHMMSS.sqlite3
    systemctl --user start qd.service
    qd db-check

`qd db-restore` is intentionally an offline operation. It refuses to run
while qd holds the daemon lock; the command does not stop or restart the
service automatically.

Before replacing the live database, restore:

- verifies the source with SQLite `PRAGMA quick_check`
- validates that the backup schema version is within q's supported range
- rejects schemas newer than the installed q release
- rejects schemas older than q's minimum supported migration version
- creates a consistent backup of the current live database
- checkpoints committed WAL contents
- removes stale live `-wal` and `-shm` sidecars
- atomically replaces the live database
- reopens the restored database through `QStore`
- automatically runs any required ordered schema migrations
- runs another SQLite quick check after migration

A backup does not have to use the current schema version. If its
schema version is still supported by q's migration chain, restore accepts
the backup and upgrades it automatically after replacement.

For example, with the current schema version 2, a valid schema version 1
backup is restored and then migrated through the normal version 1 to
version 2 migration.

The restore result reports both:

- `source_schema_version`: schema version contained in the backup
- `schema_version`: final schema version after restore and migration

When restoring an older schema, q may therefore create two distinct
safety snapshots:

- `pre-restore-*` protects the database that was live before restore
- `pre-migrate-*` protects the restored historical database before its
  schema migration

Backups whose schema version is newer than the installed q release are
rejected before the live database is replaced.

The automatic rollback snapshot is stored under:

    ~/.local/share/q/backups/

with a name like:

    pre-restore-YYYYMMDD-HHMMSS-ffffff.sqlite3

Pre-restore snapshots are registered as q-managed rollback backups and
therefore participate in the configured backup-retention policy. The
newest pre-restore snapshot is always protected from automatic cleanup.

If validation fails before replacement, the live database is left
unchanged.

If restore fails after the backup has already replaced the live database,
q automatically attempts to recover the previous live state from the
`pre-restore-*` snapshot.

Before automatic rollback, q preserves the failed restored database under:

    ~/.local/share/q/backups/

using a name like:

    failed-restore-YYYYMMDD-HHMMSS-ffffff.sqlite3

Failed-restore images are diagnostic artifacts. They are not registered
as managed backups and are not removed by automatic backup retention.

On a successful automatic rollback:

- the previous live database is restored from the `pre-restore-*` snapshot
- the rollback database is reopened through `QStore`
- SQLite `PRAGMA quick_check` must succeed
- the original restore operation still exits with an error
- the error reports that automatic rollback succeeded
- the `failed-restore-*` diagnostic image is preserved

If automatic rollback also fails, q reports both the original restore
failure and the rollback failure. It also reports the diagnostic
`failed-restore-*` path when preservation succeeded.

The `pre-restore-*` snapshot remains available as an explicit recovery
point after both successful restores and failed restore attempts.

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

### Database schema migrations

q uses ordered forward migrations for durable SQLite schema changes.

The current schema version is:

    2

Historical schema version 1 databases are upgraded automatically to
version 2 when q opens them.

Before an automatic migration, q creates a validated backup under:

    ~/.local/share/q/backups/

using a name like:

    pre-migrate-v1-to-v2-YYYYMMDD-HHMMSS-ffffff.sqlite3

Pre-migration snapshots are registered as q-managed rollback backups and
participate in the configured backup-retention policy. The newest
pre-migration snapshot is always protected from automatic cleanup.

The version 1 to version 2 migration preserves existing sessions and
queries while allowing pending sessions to exist before a backend Codex
thread ID has been assigned.

Each migration step updates the schema version transactionally and
validates foreign-key consistency before committing.

Databases whose schema version is newer than the version supported by
the installed q release are refused without being modified.

Opening an already-current database does not create a migration backup.

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

`install-user.sh` protects this boundary with qd's restart guard. Once the
guard is acquired, new submissions are refused and queued work cannot become
running until the guard is released, its lease expires, or the daemon restarts.
If running work already exists, the installer refuses the restart instead.

Restart-guard ownership is represented by an opaque token. The default lease is
300 seconds, and the installer renews that lease immediately before restart.
Automatic lease expiry ensures abandoned guards do not permanently block qd.

A query found in running state after an unguarded daemon restart becomes:

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
