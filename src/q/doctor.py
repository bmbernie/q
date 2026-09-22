from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import Any

from .config import (
    ConfigError,
    load_config,
)
from .paths import QPaths


def _mode(value: int) -> str:
    return format(
        stat.S_IMODE(value),
        "04o",
    )


def _file_check(
    path: Path,
    *,
    required: bool,
    expected_mode: int,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "path": str(path),
        "required": required,
    }

    try:
        info = path.stat()

    except FileNotFoundError:
        result.update(
            {
                "ok": not required,
                "exists": False,
            }
        )
        return result

    except OSError as exc:
        result.update(
            {
                "ok": False,
                "exists": False,
                "error": str(exc),
            }
        )
        return result

    mode = stat.S_IMODE(
        info.st_mode
    )
    is_file = stat.S_ISREG(
        info.st_mode
    )

    result.update(
        {
            "exists": True,
            "type": (
                "file"
                if is_file
                else "other"
            ),
            "mode": _mode(
                info.st_mode
            ),
            "expected_mode": format(
                expected_mode,
                "04o",
            ),
            "readable": os.access(
                path,
                os.R_OK,
            ),
            "writable": os.access(
                path,
                os.W_OK,
            ),
        }
    )

    result["ok"] = (
        is_file
        and mode == expected_mode
        and result["readable"]
        and result["writable"]
    )

    return result


def _directory_check(
    path: Path,
    *,
    required: bool,
    expected_mode: int,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "path": str(path),
        "required": required,
    }

    try:
        info = path.stat()

    except FileNotFoundError:
        result.update(
            {
                "ok": not required,
                "exists": False,
            }
        )
        return result

    except OSError as exc:
        result.update(
            {
                "ok": False,
                "exists": False,
                "error": str(exc),
            }
        )
        return result

    mode = stat.S_IMODE(
        info.st_mode
    )
    is_dir = stat.S_ISDIR(
        info.st_mode
    )

    result.update(
        {
            "exists": True,
            "type": (
                "directory"
                if is_dir
                else "other"
            ),
            "mode": _mode(
                info.st_mode
            ),
            "expected_mode": format(
                expected_mode,
                "04o",
            ),
            "readable": os.access(
                path,
                os.R_OK,
            ),
            "writable": os.access(
                path,
                os.W_OK,
            ),
            "searchable": os.access(
                path,
                os.X_OK,
            ),
        }
    )

    result["ok"] = (
        is_dir
        and mode == expected_mode
        and result["readable"]
        and result["writable"]
        and result["searchable"]
    )

    return result


def socket_check(
    path: Path,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "path": str(path),
    }

    try:
        info = path.lstat()

    except FileNotFoundError:
        result.update(
            {
                "ok": False,
                "exists": False,
            }
        )
        return result

    except OSError as exc:
        result.update(
            {
                "ok": False,
                "exists": False,
                "error": str(exc),
            }
        )
        return result

    mode = stat.S_IMODE(
        info.st_mode
    )
    is_socket = stat.S_ISSOCK(
        info.st_mode
    )

    result.update(
        {
            "exists": True,
            "type": (
                "socket"
                if is_socket
                else "other"
            ),
            "mode": _mode(
                info.st_mode
            ),
            "expected_mode": "0600",
        }
    )

    result["ok"] = (
        is_socket
        and mode == 0o600
    )

    return result


def configuration_check(
    paths: QPaths,
) -> dict[str, Any]:
    try:
        config = load_config(
            paths.config_path
        )

    except ConfigError as exc:
        return {
            "ok": False,
            "path": str(
                paths.config_path
            ),
            "exists": (
                paths.config_path
                .exists()
            ),
            "error": str(exc),
        }

    return {
        "ok": True,
        "path": str(
            paths.config_path
        ),
        "exists": (
            paths.config_path
            .exists()
        ),
        "source": (
            "file"
            if paths.config_path.exists()
            else "defaults"
        ),
        "model": config.model,
        "reasoning_effort": (
            config.reasoning_effort
        ),
        "tools": config.tools.value,
        "sandbox": config.sandbox,
    }


def local_doctor(
    paths: QPaths,
) -> dict[str, Any]:
    backup_dir = (
        paths.database_path.parent
        / "backups"
    )

    configuration = (
        configuration_check(
            paths
        )
    )

    filesystem = {
        "config": _file_check(
            paths.config_path,
            required=False,
            expected_mode=0o600,
        ),
        "database": _file_check(
            paths.database_path,
            required=True,
            expected_mode=0o600,
        ),
        "state_dir": (
            _directory_check(
                paths.state_dir,
                required=True,
                expected_mode=0o700,
            )
        ),
        "backup_dir": (
            _directory_check(
                backup_dir,
                required=False,
                expected_mode=0o700,
            )
        ),
    }

    socket = socket_check(
        paths.socket_path
    )

    filesystem_ok = all(
        item["ok"]
        for item in filesystem.values()
    )

    return {
        "ok": (
            configuration["ok"]
            and filesystem_ok
            and socket["ok"]
        ),
        "configuration": configuration,
        "filesystem": filesystem,
        "socket": socket,
    }
