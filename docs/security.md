# Security

The systemd service uses:

    UMask=0077

The q IPC socket is created with mode 0600.

The runtime directory is created with mode 0700.

### Sandbox

Tool availability and sandbox permissions are separate controls.

The sandbox is the enforcement boundary for local filesystem access:

    read-only

Allows local inspection but prevents workspace modification. Use this when
the query does not need to change local files.

    workspace-write

Allows the agent to modify files in the permitted workspace. This is
appropriate for coding, editing, and other tasks that intentionally change
the working tree.

    danger-full-access

Allows substantially broader local access. Use this only when the task
explicitly requires it and the requested actions are trusted.

Disabling tools is not a substitute for a restrictive sandbox. Codex may
retain native capabilities such as file-change or patch operations even when
broker-controlled tools are disabled. In particular, a writable sandbox can
still permit local modification. Use `--sandbox read-only` when preventing
local writes is a security requirement.

## Tools

Tools are enabled by default unless configuration or an explicit q option
disables them.

`--tools` enables the backend tool policy for the query.

`--no-tools` requests that broker-controlled tool features and integrations
be disabled. It does not by itself guarantee that the model has no native
local-operation capability; sandbox policy remains authoritative for local
access.

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

Interaction responses are sent back through qd and the query broker to the
pending Codex App Server request.

For detached queries submitted with -q or -qq, qd never auto-approves an
interactive request. It selects a safe legal negative response: decline when
available, otherwise cancel.

Interactive requests and responses are recorded in the durable q event logs.
