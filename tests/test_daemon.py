import asyncio
import socket

import pytest

from q.daemon import (
    DaemonAlreadyRunning,
    DaemonLock,
    _systemd_notify,
    serve,
)
from q.ipc import IPCClient
from q.paths import resolve_paths


class UnusedBroker:
    def stream_query(self, **kwargs):
        raise AssertionError(
            "broker should not be used"
        )

    async def interrupt(
        self,
        **kwargs,
    ):
        raise AssertionError(
            "broker should not be used"
        )


def test_resolve_paths_uses_xdg(
    tmp_path,
):
    paths = resolve_paths(
        environ={
            "HOME": (
                str(tmp_path / "home")
            ),
            "XDG_RUNTIME_DIR": (
                str(tmp_path / "run")
            ),
            "XDG_DATA_HOME": (
                str(tmp_path / "data")
            ),
            "XDG_STATE_HOME": (
                str(tmp_path / "state")
            ),
        },
        uid=1234,
    )

    assert paths.socket_path == (
        tmp_path
        / "run"
        / "q"
        / "qd.sock"
    )

    assert paths.lock_path == (
        tmp_path
        / "run"
        / "q"
        / "qd.lock"
    )

    assert paths.database_path == (
        tmp_path
        / "data"
        / "q"
        / "q.sqlite3"
    )

    assert paths.state_dir == (
        tmp_path
        / "state"
        / "q"
    )

    assert paths.config_path == (
        tmp_path
        / "home"
        / ".config"
        / "q"
        / "config.toml"
    )


def test_resolve_paths_defaults(
    tmp_path,
):
    paths = resolve_paths(
        environ={
            "HOME": str(tmp_path),
        },
        uid=4321,
    )

    assert paths.socket_path == (
        (
            __import__("pathlib")
            .Path(
                "/run/user/4321"
            )
        )
        / "q"
        / "qd.sock"
    )

    assert paths.database_path == (
        tmp_path
        / ".local"
        / "share"
        / "q"
        / "q.sqlite3"
    )

    assert paths.state_dir == (
        tmp_path
        / ".local"
        / "state"
        / "q"
    )

    assert paths.config_path == (
        tmp_path
        / ".config"
        / "q"
        / "config.toml"
    )


def test_daemon_lock_is_exclusive(
    tmp_path,
):
    path = (
        tmp_path / "qd.lock"
    )

    first = DaemonLock(path)
    second = DaemonLock(path)

    first.acquire()

    try:
        with pytest.raises(
            DaemonAlreadyRunning
        ):
            second.acquire()

    finally:
        first.release()

    second.acquire()
    second.release()


def test_serve_exposes_ping(
    tmp_path,
):
    async def run():
        paths = resolve_paths(
            environ={
                "HOME": str(tmp_path),
                "XDG_RUNTIME_DIR": (
                    str(
                        tmp_path
                        / "runtime"
                    )
                ),
                "XDG_DATA_HOME": (
                    str(
                        tmp_path
                        / "data"
                    )
                ),
                "XDG_STATE_HOME": (
                    str(
                        tmp_path
                        / "state"
                    )
                ),
            }
        )

        stop_event = asyncio.Event()
        ready_event = asyncio.Event()

        task = asyncio.create_task(
            serve(
                paths=paths,
                broker=UnusedBroker(),
                stop_event=(
                    stop_event
                ),
                ready_event=(
                    ready_event
                ),
            )
        )

        await asyncio.wait_for(
            ready_event.wait(),
            timeout=1,
        )

        client = IPCClient(
            paths.socket_path
        )

        result = await client.request(
            "ping"
        )

        assert result == {
            "ok": True,
        }

        stop_event.set()

        await asyncio.wait_for(
            task,
            timeout=1,
        )

        assert (
            not paths.socket_path.exists()
        )

    asyncio.run(run())



def test_serve_runs_query_log_cleanup(
    tmp_path,
    monkeypatch,
):
    async def run():
        paths = resolve_paths(
            environ={
                "HOME": str(tmp_path),
                "XDG_RUNTIME_DIR": (
                    str(
                        tmp_path
                        / "runtime"
                    )
                ),
                "XDG_DATA_HOME": (
                    str(
                        tmp_path
                        / "data"
                    )
                ),
                "XDG_STATE_HOME": (
                    str(
                        tmp_path
                        / "state"
                    )
                ),
            }
        )

        paths.config_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        paths.config_path.write_text(
            (
                "query_log_retention_days"
                " = 30\n"
                "query_log_cleanup_interval_seconds"
                " = 3600\n"
            ),
            encoding="utf-8",
        )

        calls = []

        def fake_cleanup(
            *,
            store,
            state_dir,
            retention_days,
            now=None,
        ):
            calls.append(
                (
                    state_dir,
                    retention_days,
                )
            )

        monkeypatch.setattr(
            "q.daemon.cleanup_query_logs",
            fake_cleanup,
        )

        stop_event = asyncio.Event()
        ready_event = asyncio.Event()

        task = asyncio.create_task(
            serve(
                paths=paths,
                broker=UnusedBroker(),
                stop_event=stop_event,
                ready_event=ready_event,
            )
        )

        await asyncio.wait_for(
            ready_event.wait(),
            timeout=1,
        )

        assert calls == [
            (
                paths.state_dir,
                30,
            )
        ]

        stop_event.set()

        await asyncio.wait_for(
            task,
            timeout=1,
        )

    asyncio.run(run())


def test_db_restore_refuses_while_daemon_lock_is_held(
    tmp_path,
    monkeypatch,
):
    from q.daemon import _db_restore_main
    from q.models import ToolsPolicy
    from q.store import QStore

    paths = resolve_paths(
        environ={
            "HOME": str(tmp_path),
            "XDG_RUNTIME_DIR": str(
                tmp_path / "runtime"
            ),
            "XDG_DATA_HOME": str(
                tmp_path / "data"
            ),
            "XDG_STATE_HOME": str(
                tmp_path / "state"
            ),
        },
    )

    backup = tmp_path / "backup.sqlite3"

    source = QStore(backup)

    source.create_session(
        session_name="restore-proof",
        backend_thread_id="thread-restore",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
    )

    source.close()

    monkeypatch.setattr(
        "q.daemon.resolve_paths",
        lambda: paths,
    )

    held = DaemonLock(paths.lock_path)
    held.acquire()

    try:
        with pytest.raises(
            SystemExit,
            match="requires qd to be stopped",
        ):
            _db_restore_main(
                [str(backup)]
            )
    finally:
        held.release()


def test_db_restore_command_restores_when_offline(
    tmp_path,
    monkeypatch,
    capsys,
):
    import json

    from q.daemon import _db_restore_main
    from q.models import ToolsPolicy
    from q.store import QStore

    paths = resolve_paths(
        environ={
            "HOME": str(tmp_path),
            "XDG_RUNTIME_DIR": str(
                tmp_path / "runtime"
            ),
            "XDG_DATA_HOME": str(
                tmp_path / "data"
            ),
            "XDG_STATE_HOME": str(
                tmp_path / "state"
            ),
        },
    )

    live = QStore(paths.database_path)
    live.close()

    backup = tmp_path / "backup.sqlite3"

    source = QStore(backup)

    source.create_session(
        session_name="restored",
        backend_thread_id="thread-restored",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
    )

    source.close()

    monkeypatch.setattr(
        "q.daemon.resolve_paths",
        lambda: paths,
    )

    _db_restore_main(
        [str(backup)]
    )

    output = json.loads(
        capsys.readouterr().out
    )

    assert output["ok"] is True
    assert output["rollback_backup"] is not None

    restored = QStore(
        paths.database_path
    )

    assert restored.get_session(
        "restored"
    ) is not None

    restored.close()


def test_serve_runs_backup_cleanup(
    tmp_path,
    monkeypatch,
):
    async def run():
        paths = resolve_paths(
            environ={
                "HOME": str(tmp_path),
                "XDG_RUNTIME_DIR": str(
                    tmp_path
                    / "runtime"
                ),
                "XDG_DATA_HOME": str(
                    tmp_path
                    / "data"
                ),
                "XDG_STATE_HOME": str(
                    tmp_path
                    / "state"
                ),
            }
        )

        paths.config_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        paths.config_path.write_text(
            (
                "backup_retention_count = 7\n"
                "backup_retention_days = 30\n"
                "backup_cleanup_interval_seconds"
                " = 3600\n"
            ),
            encoding="utf-8",
        )

        calls = []

        def fake_cleanup(
            *,
            backup_dir,
            retention_count,
            retention_days,
            now=None,
        ):
            calls.append(
                (
                    backup_dir,
                    retention_count,
                    retention_days,
                )
            )

        monkeypatch.setattr(
            "q.daemon.cleanup_managed_backups",
            fake_cleanup,
        )

        stop_event = asyncio.Event()
        ready_event = asyncio.Event()

        task = asyncio.create_task(
            serve(
                paths=paths,
                broker=UnusedBroker(),
                stop_event=stop_event,
                ready_event=ready_event,
            )
        )

        await asyncio.wait_for(
            ready_event.wait(),
            timeout=1,
        )

        assert calls == [
            (
                paths.database_path.parent
                / "backups",
                7,
                30,
            )
        ]

        stop_event.set()

        await asyncio.wait_for(
            task,
            timeout=1,
        )

    asyncio.run(run())



def test_systemd_notify_sends_readiness(
    tmp_path,
    monkeypatch,
):
    notify_path = (
        tmp_path / "notify.sock"
    )

    receiver = socket.socket(
        socket.AF_UNIX,
        socket.SOCK_DGRAM,
    )

    try:
        receiver.bind(str(notify_path))
        receiver.settimeout(1.0)

        monkeypatch.setenv(
            "NOTIFY_SOCKET",
            str(notify_path),
        )

        _systemd_notify("READY=1")

        assert receiver.recv(64) == b"READY=1"

    finally:
        receiver.close()
