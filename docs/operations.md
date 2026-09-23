# Operations

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
- chatgpt-query-broker reachability and `/health` status

A healthy deployment reports:

    "ok": true

and exits with status 0.

The `version` section compares the package version loaded by the `qd`
client process with the package version loaded by the running daemon:

    "version": {
      "client": "0.10.0",
      "daemon": "0.10.0",
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

The chatgpt-query-broker `/health` request also uses a 5 second deadline.
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

`qd db-backup` backs up the SQLite state database only. Managed attachment files under `~/.local/share/q/attachments/` are not included and must be backed up separately when needed.

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
