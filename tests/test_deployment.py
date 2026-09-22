from pathlib import Path

from q.config import (
    QConfig,
    load_config,
)


ROOT = Path(__file__).resolve().parents[1]


def test_example_config_matches_defaults():
    config = load_config(
        ROOT / "config.example.toml"
    )

    assert config == QConfig()


def test_systemd_template_has_required_runtime():
    text = (
        ROOT
        / "systemd"
        / "qd.service.in"
    ).read_text(
        encoding="utf-8"
    )

    assert (
        "WorkingDirectory=@Q_ROOT@"
        in text
    )
    assert (
        "ExecStart=@Q_ROOT@/.venv/bin/qd serve"
        in text
    )
    assert (
        "Requires=chatgpt-worker-broker.service"
        in text
    )
    assert "UMask=0077" in text


def test_install_script_preserves_existing_config():
    text = (
        ROOT
        / "scripts"
        / "install-user.sh"
    ).read_text(
        encoding="utf-8"
    )

    assert (
        'if [ ! -e "$CONFIG_DIR/config.toml" ]'
        in text
    )
    assert (
        "systemctl --user enable qd.service"
        in text
    )
    assert (
        "systemctl --user restart qd.service"
        in text
    )
    assert (
        '.request("ping")'
        in text
    )
    assert (
        "qd IPC ready"
        in text
    )
    assert (
        '"restart_guard_acquire"'
        in text
    )
    assert (
        '"restart_guard_release"'
        in text
    )
    assert (
        "qd restart guard acquired"
        in text
    )
    assert (
        "RESTART_GUARD_ACQUIRED=1"
        in text
    )
    assert (
        "trap release_restart_guard EXIT"
        in text
    )
    assert (
        "refusing to restart qd while "
        in text
    )
    assert (
        "queries are running: "
        in text
    )
