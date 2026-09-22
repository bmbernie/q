#!/usr/bin/env bash
set -euo pipefail

ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.." &&
  pwd
)"

VENV="$ROOT/.venv"
LOCAL_BIN="${HOME}/.local/bin"
CONFIG_DIR="${XDG_CONFIG_HOME:-${HOME}/.config}/q"
SYSTEMD_DIR="${HOME}/.config/systemd/user"

RESTART_GUARD_ACQUIRED=0

release_restart_guard() {
  if [ "$RESTART_GUARD_ACQUIRED" -ne 1 ]; then
    return
  fi

  set +e

  "$VENV/bin/python" - <<'PYRELEASE'
import asyncio

from q.ipc import IPCClient
from q.paths import resolve_paths


async def release() -> None:
    try:
        result = await asyncio.wait_for(
            IPCClient(
                resolve_paths().socket_path
            ).request(
                "restart_guard_release"
            ),
            timeout=5.0,
        )

    except Exception:
        return

    if (
        isinstance(result, dict)
        and result.get("released") is True
    ):
        print(
            "released qd restart guard"
        )


asyncio.run(release())
PYRELEASE
}

trap release_restart_guard EXIT

printf '%s\n' '--- q install ---'
printf 'root: %s\n' "$ROOT"

if systemctl --user is-active --quiet qd.service; then
  if [ ! -x "$VENV/bin/python" ]; then
    printf '%s\n' \
      'cannot safely restart active qd: Python environment is unavailable' \
      >&2
    exit 1
  fi

  "$VENV/bin/python" - <<'PYGUARD'
import asyncio

from q.ipc import IPCClient
from q.paths import resolve_paths


async def acquire() -> None:
    try:
        result = await asyncio.wait_for(
            IPCClient(
                resolve_paths().socket_path
            ).request(
                "restart_guard_acquire"
            ),
            timeout=5.0,
        )

    except Exception as exc:
        raise SystemExit(
            "cannot safely restart active qd: "
            "restart guard unavailable: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    if not isinstance(result, dict):
        raise SystemExit(
            "cannot safely restart active qd: "
            "invalid restart guard response"
        )

    running = result.get(
        "running_query_ids"
    )

    if result.get("acquired") is not True:
        if isinstance(running, list):
            detail = (
                ", ".join(
                    str(item)
                    for item in running
                )
                or "unknown"
            )
        else:
            detail = "unknown"

        raise SystemExit(
            "refusing to restart qd while "
            "queries are running: "
            f"{detail}"
        )

    if result.get("guarded") is not True:
        raise SystemExit(
            "cannot safely restart active qd: "
            "restart guard was not activated"
        )

    print("qd restart guard acquired")


asyncio.run(acquire())
PYGUARD

  RESTART_GUARD_ACQUIRED=1
fi

if [ ! -x "$VENV/bin/python" ]; then
  python3 -m venv "$VENV"
fi

"$VENV/bin/python" -m pip install -e "$ROOT"

mkdir -p \
  "$LOCAL_BIN" \
  "$CONFIG_DIR" \
  "$SYSTEMD_DIR"

ln -sfn \
  "$VENV/bin/q" \
  "$LOCAL_BIN/q"

ln -sfn \
  "$VENV/bin/qd" \
  "$LOCAL_BIN/qd"

if [ ! -e "$CONFIG_DIR/config.toml" ]; then
  install \
    -m 600 \
    "$ROOT/config.example.toml" \
    "$CONFIG_DIR/config.toml"

  printf 'created config: %s\n' \
    "$CONFIG_DIR/config.toml"
else
  printf 'preserved config: %s\n' \
    "$CONFIG_DIR/config.toml"
fi

python3 - \
  "$ROOT/systemd/qd.service.in" \
  "$SYSTEMD_DIR/qd.service" \
  "$ROOT" <<'PY'
from pathlib import Path
import sys

template_path = Path(sys.argv[1])
output_path = Path(sys.argv[2])
root = sys.argv[3]

text = template_path.read_text(
    encoding="utf-8"
)

if "@Q_ROOT@" not in text:
    raise SystemExit(
        "qd.service.in is missing @Q_ROOT@"
    )

text = text.replace(
    "@Q_ROOT@",
    root,
)

output_path.write_text(
    text,
    encoding="utf-8",
)

output_path.chmod(0o600)
PY

systemctl --user daemon-reload
systemctl --user enable qd.service
systemctl --user restart qd.service

# A successful restart destroys the guarded
# daemon process. The newly started daemon has
# no restart guard active.
RESTART_GUARD_ACQUIRED=0

"$VENV/bin/python" - <<'PYREADY'
import asyncio

from q.ipc import IPCClient
from q.paths import resolve_paths


async def wait_for_qd() -> None:
    socket_path = resolve_paths().socket_path
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 10.0
    last_error = None

    while loop.time() < deadline:
        try:
            result = await asyncio.wait_for(
                IPCClient(
                    socket_path
                ).request("ping"),
                timeout=1.0,
            )

            if (
                isinstance(result, dict)
                and result.get("ok") is True
            ):
                print("qd IPC ready")
                return

            last_error = RuntimeError(
                f"unexpected ping result: {result!r}"
            )

        except Exception as exc:
            last_error = exc

        await asyncio.sleep(0.1)

    raise SystemExit(
        "qd did not become IPC-ready "
        f"within 10 seconds: {last_error}"
    )


asyncio.run(wait_for_qd())
PYREADY

printf '\n%s\n' '--- installed ---'
printf 'q:  %s\n' "$LOCAL_BIN/q"
printf 'qd: %s\n' "$LOCAL_BIN/qd"
printf 'config: %s\n' \
  "$CONFIG_DIR/config.toml"
printf 'service: %s\n' \
  "$SYSTEMD_DIR/qd.service"

systemctl --user is-active qd.service
