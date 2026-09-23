# State and Recovery

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
