import asyncio

import pytest

from q.daemon import (
    DaemonAlreadyRunning,
    DaemonLock,
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
