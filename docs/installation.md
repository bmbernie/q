# Installation

## Requirements

- Python 3.13+
- `chatgpt-query-broker` running locally
- Codex App Server configured by the broker
- ChatGPT/Codex authentication already established

### Broker compatibility

q/qd requires a compatible `chatgpt-query-broker` deployment.

Required broker capabilities include:

- streamed query transport
- persistent conversation resume
- turn interruption for supported providers
- interactive-request relay for supported providers
- interactive approval routing to the user

The broker must support the query API and provider-routing behavior required by this q/qd release.

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
