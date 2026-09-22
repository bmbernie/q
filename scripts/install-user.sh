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

printf '%s\n' '--- q install ---'
printf 'root: %s\n' "$ROOT"

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
systemctl --user enable --now qd.service

printf '\n%s\n' '--- installed ---'
printf 'q:  %s\n' "$LOCAL_BIN/q"
printf 'qd: %s\n' "$LOCAL_BIN/qd"
printf 'config: %s\n' \
  "$CONFIG_DIR/config.toml"
printf 'service: %s\n' \
  "$SYSTEMD_DIR/qd.service"

systemctl --user is-active qd.service
