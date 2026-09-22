from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path

from .models import ToolsPolicy


DEFAULT_MODEL = "gpt-5.6-luna"
DEFAULT_REASONING_EFFORT = "xhigh"
DEFAULT_TOOLS_POLICY = ToolsPolicy.ENABLED
DEFAULT_SANDBOX = "read-only"
DEFAULT_LOG_ROTATE_BYTES = (
    16 * 1024 * 1024
)
DEFAULT_LOG_ROTATE_ARCHIVES = 5

_ALLOWED_KEYS = {
    "model",
    "reasoning_effort",
    "tools",
    "sandbox",
    "log_rotate_bytes",
    "log_rotate_archives",
}

_ALLOWED_SANDBOXES = {
    "read-only",
    "workspace-write",
    "danger-full-access",
}


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class QConfig:
    model: str = DEFAULT_MODEL
    reasoning_effort: str = (
        DEFAULT_REASONING_EFFORT
    )
    tools: ToolsPolicy = (
        DEFAULT_TOOLS_POLICY
    )
    sandbox: str = DEFAULT_SANDBOX
    log_rotate_bytes: int = (
        DEFAULT_LOG_ROTATE_BYTES
    )
    log_rotate_archives: int = (
        DEFAULT_LOG_ROTATE_ARCHIVES
    )


def _nonempty_string(
    value,
    *,
    name: str,
) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
    ):
        raise ConfigError(
            f"{name} must be a "
            "non-empty string"
        )

    return value.strip()


def _nonnegative_int(
    value,
    *,
    name: str,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 0
    ):
        raise ConfigError(
            f"{name} must be a "
            "non-negative integer"
        )

    return value


def load_config(
    path: Path,
) -> QConfig:
    path = path.expanduser()

    try:
        raw = path.read_bytes()

    except FileNotFoundError:
        return QConfig()

    except OSError as exc:
        raise ConfigError(
            f"cannot read config "
            f"{path}: {exc}"
        ) from exc

    try:
        data = tomllib.loads(
            raw.decode("utf-8")
        )

    except (
        UnicodeDecodeError,
        tomllib.TOMLDecodeError,
    ) as exc:
        raise ConfigError(
            f"invalid config {path}: "
            f"{exc}"
        ) from exc

    if not isinstance(data, dict):
        raise ConfigError(
            "config root must be a table"
        )

    unknown = (
        set(data)
        - _ALLOWED_KEYS
    )

    if unknown:
        names = ", ".join(
            sorted(unknown)
        )

        raise ConfigError(
            "unknown config key(s): "
            f"{names}"
        )

    model = _nonempty_string(
        data.get(
            "model",
            DEFAULT_MODEL,
        ),
        name="model",
    )

    reasoning_effort = (
        _nonempty_string(
            data.get(
                "reasoning_effort",
                DEFAULT_REASONING_EFFORT,
            ),
            name="reasoning_effort",
        )
    )

    tools_raw = _nonempty_string(
        data.get(
            "tools",
            DEFAULT_TOOLS_POLICY.value,
        ),
        name="tools",
    )

    try:
        tools = ToolsPolicy(
            tools_raw
        )

    except ValueError as exc:
        raise ConfigError(
            "tools must be "
            "'enabled' or 'disabled'"
        ) from exc

    sandbox = _nonempty_string(
        data.get(
            "sandbox",
            DEFAULT_SANDBOX,
        ),
        name="sandbox",
    )

    if sandbox not in _ALLOWED_SANDBOXES:
        raise ConfigError(
            "sandbox must be one of: "
            "read-only, workspace-write, "
            "danger-full-access"
        )

    log_rotate_bytes = (
        _nonnegative_int(
            data.get(
                "log_rotate_bytes",
                DEFAULT_LOG_ROTATE_BYTES,
            ),
            name="log_rotate_bytes",
        )
    )

    log_rotate_archives = (
        _nonnegative_int(
            data.get(
                "log_rotate_archives",
                DEFAULT_LOG_ROTATE_ARCHIVES,
            ),
            name="log_rotate_archives",
        )
    )

    if (
        log_rotate_bytes > 0
        and log_rotate_archives == 0
    ):
        raise ConfigError(
            "log_rotate_archives must be "
            "at least 1 when rotation "
            "is enabled"
        )

    return QConfig(
        model=model,
        reasoning_effort=(
            reasoning_effort
        ),
        tools=tools,
        sandbox=sandbox,
        log_rotate_bytes=(
            log_rotate_bytes
        ),
        log_rotate_archives=(
            log_rotate_archives
        ),
    )
