import pytest

from q.config import (
    ConfigError,
    QConfig,
    load_config,
)
from q.models import ToolsPolicy


def test_missing_config_uses_defaults(
    tmp_path,
):
    config = load_config(
        tmp_path / "missing.toml"
    )

    assert config == QConfig()

    assert (
        config.model
        == "gpt-5.6-luna"
    )
    assert (
        config.reasoning_effort
        == "xhigh"
    )
    assert (
        config.tools
        == ToolsPolicy.ENABLED
    )
    assert (
        config.sandbox
        == "read-only"
    )


def test_custom_config(
    tmp_path,
):
    path = tmp_path / "config.toml"

    path.write_text(
        '''
model = "gpt-5.6-sol"
reasoning_effort = "high"
tools = "disabled"
sandbox = "workspace-write"
'''.lstrip(),
        encoding="utf-8",
    )

    config = load_config(path)

    assert (
        config.model
        == "gpt-5.6-sol"
    )
    assert (
        config.reasoning_effort
        == "high"
    )
    assert (
        config.tools
        == ToolsPolicy.DISABLED
    )
    assert (
        config.sandbox
        == "workspace-write"
    )


def test_unknown_key_is_rejected(
    tmp_path,
):
    path = tmp_path / "config.toml"

    path.write_text(
        'modle = "typo"\n',
        encoding="utf-8",
    )

    with pytest.raises(
        ConfigError
    ):
        load_config(path)


def test_invalid_tools_is_rejected(
    tmp_path,
):
    path = tmp_path / "config.toml"

    path.write_text(
        'tools = "sometimes"\n',
        encoding="utf-8",
    )

    with pytest.raises(
        ConfigError
    ):
        load_config(path)


def test_invalid_sandbox_is_rejected(
    tmp_path,
):
    path = tmp_path / "config.toml"

    path.write_text(
        'sandbox = "anything"\n',
        encoding="utf-8",
    )

    with pytest.raises(
        ConfigError
    ):
        load_config(path)



def test_log_rotation_config(
    tmp_path,
):
    path = tmp_path / "config.toml"

    path.write_text(
        """
log_rotate_bytes = 4096
log_rotate_archives = 3
""".lstrip(),
        encoding="utf-8",
    )

    config = load_config(path)

    assert (
        config.log_rotate_bytes
        == 4096
    )
    assert (
        config.log_rotate_archives
        == 3
    )


def test_rotation_requires_archive_when_enabled(
    tmp_path,
):
    path = tmp_path / "config.toml"

    path.write_text(
        """
log_rotate_bytes = 4096
log_rotate_archives = 0
""".lstrip(),
        encoding="utf-8",
    )

    with pytest.raises(
        ConfigError
    ):
        load_config(path)


def test_query_log_retention_config(
    tmp_path,
):
    path = tmp_path / "config.toml"

    path.write_text(
        """
query_log_retention_days = 14
query_log_cleanup_interval_seconds = 900
""".lstrip(),
        encoding="utf-8",
    )

    config = load_config(path)

    assert (
        config.query_log_retention_days
        == 14
    )
    assert (
        config
        .query_log_cleanup_interval_seconds
        == 900
    )


def test_retention_requires_positive_interval(
    tmp_path,
):
    path = tmp_path / "config.toml"

    path.write_text(
        """
query_log_retention_days = 30
query_log_cleanup_interval_seconds = 0
""".lstrip(),
        encoding="utf-8",
    )

    with pytest.raises(
        ConfigError
    ):
        load_config(path)
