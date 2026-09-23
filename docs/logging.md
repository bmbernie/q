# Logging

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
