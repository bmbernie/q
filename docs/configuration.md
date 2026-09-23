# Configuration

Default config:

    ~/.config/q/config.toml

Current defaults:

    model = "gpt-6-luna"
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
