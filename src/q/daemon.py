from __future__ import annotations

import asyncio
import errno
import fcntl
import os
import signal
import sys
from pathlib import Path

from .broker import HTTPBroker
from .engine import QueryEngine
from .ipc import IPCServer
from .paths import (
    QPaths,
    resolve_paths,
)
from .service import QDService
from .store import QStore


DEFAULT_BROKER_URL = (
    "http://127.0.0.1:8792"
)


class DaemonAlreadyRunning(
    RuntimeError
):
    pass


class DaemonLock:
    def __init__(
        self,
        path: Path,
    ):
        self.path = path.expanduser()
        self._fd: int | None = None

    def acquire(self) -> None:
        if self._fd is not None:
            raise RuntimeError(
                "daemon lock is already held"
            )

        self.path.parent.mkdir(
            parents=True,
            exist_ok=True,
            mode=0o700,
        )
        self.path.parent.chmod(
            0o700
        )

        fd = os.open(
            self.path,
            (
                os.O_CREAT
                | os.O_RDWR
            ),
            0o600,
        )

        os.fchmod(
            fd,
            0o600,
        )

        try:
            fcntl.flock(
                fd,
                (
                    fcntl.LOCK_EX
                    | fcntl.LOCK_NB
                ),
            )

        except OSError as exc:
            os.close(fd)

            if exc.errno in {
                errno.EACCES,
                errno.EAGAIN,
            }:
                raise (
                    DaemonAlreadyRunning(
                        "qd is already running"
                    )
                ) from exc

            raise

        os.ftruncate(fd, 0)
        os.write(
            fd,
            (
                f"{os.getpid()}\n"
            ).encode(),
        )

        self._fd = fd

    def release(self) -> None:
        fd = self._fd
        self._fd = None

        if fd is None:
            return

        try:
            fcntl.flock(
                fd,
                fcntl.LOCK_UN,
            )
        finally:
            os.close(fd)


async def serve(
    *,
    paths: QPaths,
    broker,
    stop_event: asyncio.Event,
    ready_event: (
        asyncio.Event | None
    ) = None,
) -> None:
    lock = DaemonLock(
        paths.lock_path
    )
    lock.acquire()

    store: QStore | None = None
    engine: QueryEngine | None = None
    server: IPCServer | None = None

    try:
        store = QStore(
            paths.database_path
        )

        engine = QueryEngine(
            store=store,
            broker=broker,
            state_dir=(
                paths.state_dir
            ),
        )

        service = QDService(
            store=store,
            engine=engine,
        )

        server = IPCServer(
            paths.socket_path,
            service.handle,
        )

        await server.start()

        if ready_event is not None:
            ready_event.set()

        await stop_event.wait()

    finally:
        if server is not None:
            await server.aclose()

        if engine is not None:
            await engine.aclose()

        if store is not None:
            store.close()

        lock.release()


def _install_signal_handlers(
    stop_event: asyncio.Event,
) -> None:
    loop = asyncio.get_running_loop()

    for signum in (
        signal.SIGINT,
        signal.SIGTERM,
    ):
        loop.add_signal_handler(
            signum,
            stop_event.set,
        )


async def _main() -> None:
    paths = resolve_paths()

    broker_url = os.environ.get(
        "Q_BROKER_URL",
        DEFAULT_BROKER_URL,
    )

    broker = HTTPBroker(
        broker_url
    )

    stop_event = asyncio.Event()
    _install_signal_handlers(
        stop_event
    )

    try:
        await serve(
            paths=paths,
            broker=broker,
            stop_event=stop_event,
        )

    finally:
        await broker.aclose()


def main(
    argv: list[str] | None = None,
) -> None:
    args = (
        list(sys.argv[1:])
        if argv is None
        else list(argv)
    )

    # Preserve the existing behavior:
    #
    #   qd
    #
    # starts the daemon. "qd serve" is an
    # explicit equivalent for systemd and
    # manual administration.
    if args and args[0] != "serve":
        from .management import (
            main as management_main,
        )

        management_main(args)
        return

    if args == ["serve"]:
        args = []

    if args:
        raise SystemExit(
            "usage: qd [serve|COMMAND]"
        )

    try:
        asyncio.run(
            _main()
        )

    except DaemonAlreadyRunning as exc:
        raise SystemExit(
            str(exc)
        ) from exc
