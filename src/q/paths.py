from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True, slots=True)
class QPaths:
    socket_path: Path
    lock_path: Path
    database_path: Path
    state_dir: Path
    config_path: Path


def resolve_paths(
    *,
    environ: Mapping[str, str] | None = None,
    uid: int | None = None,
) -> QPaths:
    env = os.environ if environ is None else environ

    actual_uid = (
        os.getuid()
        if uid is None
        else uid
    )

    home_raw = env.get(
        "HOME",
        str(Path.home()),
    )
    home = Path(home_raw).expanduser()

    runtime_raw = env.get(
        "XDG_RUNTIME_DIR"
    )

    runtime = (
        Path(runtime_raw)
        if runtime_raw
        else Path(
            f"/run/user/{actual_uid}"
        )
    )

    data_raw = env.get(
        "XDG_DATA_HOME"
    )
    data_home = (
        Path(data_raw)
        if data_raw
        else home / ".local" / "share"
    )

    config_raw = env.get(
        "XDG_CONFIG_HOME"
    )
    config_home = (
        Path(config_raw)
        if config_raw
        else home / ".config"
    )

    state_raw = env.get(
        "XDG_STATE_HOME"
    )
    state_home = (
        Path(state_raw)
        if state_raw
        else home / ".local" / "state"
    )

    runtime_q = runtime / "q"

    return QPaths(
        socket_path=(
            runtime_q / "qd.sock"
        ),
        lock_path=(
            runtime_q / "qd.lock"
        ),
        database_path=(
            data_home
            / "q"
            / "q.sqlite3"
        ),
        state_dir=(
            state_home / "q"
        ),
        config_path=(
            config_home
            / "q"
            / "config.toml"
        ),
    )
