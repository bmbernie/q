# Security Policy

## Supported versions

Security reports should be tested against the current `main` branch when
possible. Older releases and tags may not receive security backports.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability.

Use GitHub's private vulnerability reporting feature from the repository's
**Security** tab and select **Report a vulnerability**.

Please include, where applicable:

- the affected version, tag, or commit
- operating system and Python version
- the affected component (`q`, `qd`, IPC, attachments, database, backup, or
  restore)
- reproduction steps or a minimal proof of concept
- the security impact
- any known mitigations or prerequisites

Do not include credentials, tokens, or unrelated private data in a report.

## Security boundaries

`q` and `qd` are designed as per-user local components. Security reports are
particularly useful when behavior crosses that boundary unexpectedly, such as:

- access to another user's daemon, data, or managed attachments
- unintended file disclosure, overwrite, or permission changes
- escaping the intended IPC authorization boundary
- attachment or backup handling that affects files outside the intended scope
- command or code execution that is not an expected consequence of explicit
  local-user input

Behavior that requires an already-authorized local user to deliberately control
their own command-line arguments, environment, or configuration is not by
itself considered a privilege-boundary violation.

## Disclosure

There is currently no fixed response or remediation SLA. Public disclosure of
an unresolved vulnerability should be coordinated through the private report.
