import asyncio
import io

from q import __version__
from q.ipc import IPCFrame
from q.management import (
    build_parser,
    run_management,
)


class FakeClient:
    def __init__(
        self,
        *,
        result=None,
        frames=None,
    ):
        self.result = result
        self._frames = frames or []
        self.calls = []

    async def request(
        self,
        method,
        params=None,
    ):
        self.calls.append(
            (
                method,
                params,
            )
        )
        return self.result

    def frames(
        self,
        method,
        params=None,
    ):
        self.calls.append(
            (
                method,
                params,
            )
        )

        async def generate():
            for frame in self._frames:
                yield frame

        return generate()


def parse(*args):
    return build_parser().parse_args(
        list(args)
    )


def test_status():
    async def run():
        client = FakeClient(
            result={
                "query_id": "q-1",
                "state": "completed",
            }
        )
        stdout = io.StringIO()

        status = await run_management(
            args=parse(
                "status",
                "q-1",
            ),
            client=client,
            stdout=stdout,
        )

        assert status == 0
        assert '"query_id": "q-1"' in (
            stdout.getvalue()
        )
        assert client.calls == [
            (
                "status",
                {
                    "query_id": "q-1",
                },
            )
        ]

    asyncio.run(run())


def test_queries_filters():
    async def run():
        client = FakeClient(
            result={
                "queries": [],
            }
        )
        stdout = io.StringIO()

        await run_management(
            args=parse(
                "queries",
                "-s",
                "alpha",
                "--running",
            ),
            client=client,
            stdout=stdout,
        )

        assert client.calls == [
            (
                "queries",
                {
                    "running": True,
                    "session_name": (
                        "alpha"
                    ),
                },
            )
        ]

    asyncio.run(run())


def test_sessions():
    async def run():
        client = FakeClient(
            result={
                "sessions": [],
            }
        )
        stdout = io.StringIO()

        await run_management(
            args=parse("sessions"),
            client=client,
            stdout=stdout,
        )

        assert client.calls == [
            (
                "sessions",
                None,
            )
        ]

    asyncio.run(run())


def test_cancel():
    async def run():
        client = FakeClient(
            result={
                "query_id": "q-9",
                "state": "running",
                "cancel_requested_at": (
                    "now"
                ),
            }
        )
        stdout = io.StringIO()

        await run_management(
            args=parse(
                "cancel",
                "q-9",
            ),
            client=client,
            stdout=stdout,
        )

        assert client.calls == [
            (
                "cancel",
                {
                    "query_id": "q-9",
                },
            )
        ]

        assert (
            '"cancel_requested_at": "now"'
            in stdout.getvalue()
        )

    asyncio.run(run())


def test_follow_prints_only_answer_deltas():
    async def run():
        client = FakeClient(
            frames=[
                IPCFrame.event_frame(
                    "r",
                    "state",
                    {
                        "state": "running",
                    },
                ),
                IPCFrame.event_frame(
                    "r",
                    "delta",
                    {
                        "delta": "A",
                    },
                ),
                IPCFrame.event_frame(
                    "r",
                    "broker",
                    {
                        "event": {
                            "type": "event",
                        },
                    },
                ),
                IPCFrame.event_frame(
                    "r",
                    "delta",
                    {
                        "delta": "B",
                    },
                ),
                IPCFrame.result_frame(
                    "r",
                    {
                        "state": "completed",
                    },
                ),
            ]
        )

        stdout = io.StringIO()

        status = await run_management(
            args=parse(
                "follow",
                "q-1",
            ),
            client=client,
            stdout=stdout,
        )

        assert status == 0
        assert stdout.getvalue() == (
            "AB\n"
        )

    asyncio.run(run())



def test_logs_prints_raw_ndjson_lines():
    async def run():
        client = FakeClient(
            frames=[
                IPCFrame.event_frame(
                    "r",
                    "log",
                    {
                        "path": "/tmp/q.ndjson",
                        "line": (
                            '{"type":"delta",'
                            '"delta":"hello"}'
                        ),
                    },
                ),
                IPCFrame.result_frame(
                    "r",
                    {
                        "path": "/tmp/q.ndjson",
                        "follow": False,
                    },
                ),
            ]
        )

        stdout = io.StringIO()

        status = await run_management(
            args=parse(
                "logs",
                "-s",
                "alpha",
            ),
            client=client,
            stdout=stdout,
        )

        assert status == 0
        assert stdout.getvalue() == (
            '{"type":"delta",'
            '"delta":"hello"}\n'
        )

        assert client.calls == [
            (
                "logs",
                {
                    "follow": False,
                    "session_name": (
                        "alpha"
                    ),
                },
            )
        ]

    asyncio.run(run())



def test_follow_reports_unretained_log():
    async def run():
        client = FakeClient(
            frames=[
                IPCFrame.event_frame(
                    "r",
                    "notice",
                    {
                        "type": (
                            "log_retention"
                        ),
                        "query_id": "q-old",
                        "log_retained": False,
                        "message": (
                            "per-query log "
                            "is not retained"
                        ),
                    },
                ),
                IPCFrame.result_frame(
                    "r",
                    {
                        "query_id": "q-old",
                        "state": "completed",
                        "log_retained": False,
                    },
                ),
            ]
        )

        stdout = io.StringIO()

        status = await run_management(
            args=parse(
                "follow",
                "q-old",
            ),
            client=client,
            stdout=stdout,
        )

        assert status == 0
        assert stdout.getvalue() == (
            "qd: per-query log "
            "is not retained\n"
        )

    asyncio.run(run())



def test_db_check():
    async def run():
        client = FakeClient(
            result={
                "ok": True,
                "quick_check": ["ok"],
                "journal_mode": "wal",
                "synchronous": 2,
                "foreign_keys": True,
                "busy_timeout": 5000,
                "wal_autocheckpoint": 1000,
            }
        )
        stdout = io.StringIO()

        status = await run_management(
            args=parse("db-check"),
            client=client,
            stdout=stdout,
        )

        assert status == 0
        assert client.calls == [
            (
                "db_check",
                None,
            )
        ]

        rendered = stdout.getvalue()

        assert '"ok": true' in rendered
        assert (
            '"journal_mode": "wal"'
            in rendered
        )

    asyncio.run(run())


def test_db_check_failure_returns_nonzero():
    async def run():
        client = FakeClient(
            result={
                "ok": False,
                "quick_check": [
                    "corrupt"
                ],
            }
        )
        stdout = io.StringIO()

        status = await run_management(
            args=parse("db-check"),
            client=client,
            stdout=stdout,
        )

        assert status == 1
        assert '"ok": false' in (
            stdout.getvalue()
        )

    asyncio.run(run())


def test_db_backup_default():
    async def run():
        client = FakeClient(
            result={
                "ok": True,
                "path": (
                    "/tmp/backup.sqlite3"
                ),
                "size_bytes": 123,
                "quick_check": ["ok"],
            }
        )
        stdout = io.StringIO()

        status = await run_management(
            args=parse("db-backup"),
            client=client,
            stdout=stdout,
        )

        assert status == 0
        assert client.calls == [
            (
                "db_backup",
                {},
            )
        ]
        assert '"size_bytes": 123' in (
            stdout.getvalue()
        )

    asyncio.run(run())


def test_db_backup_resolves_custom_path(
    tmp_path,
    monkeypatch,
):
    async def run():
        monkeypatch.chdir(tmp_path)

        client = FakeClient(
            result={
                "ok": True,
                "path": str(
                    tmp_path
                    / "snapshot.sqlite3"
                ),
                "size_bytes": 123,
                "quick_check": ["ok"],
            }
        )
        stdout = io.StringIO()

        status = await run_management(
            args=parse(
                "db-backup",
                "snapshot.sqlite3",
            ),
            client=client,
            stdout=stdout,
        )

        assert status == 0
        assert client.calls == [
            (
                "db_backup",
                {
                    "path": str(
                        (
                            tmp_path
                            / "snapshot.sqlite3"
                        ).resolve()
                    ),
                },
            )
        ]

    asyncio.run(run())


def _doctor_paths(
    tmp_path,
):
    import os
    import socket

    from q.paths import QPaths

    runtime = tmp_path / "runtime"
    runtime.mkdir()
    runtime.chmod(0o700)

    socket_path = (
        runtime / "qd.sock"
    )

    sock = socket.socket(
        socket.AF_UNIX,
        socket.SOCK_STREAM,
    )

    try:
        sock.bind(
            str(socket_path)
        )

    finally:
        sock.close()

    socket_path.chmod(0o600)

    data_dir = (
        tmp_path / "data"
    )
    data_dir.mkdir()
    data_dir.chmod(0o700)

    database_path = (
        data_dir / "q.sqlite3"
    )
    database_path.write_bytes(
        b"test"
    )
    database_path.chmod(0o600)

    backup_dir = (
        data_dir / "backups"
    )
    backup_dir.mkdir()
    backup_dir.chmod(0o700)

    state_dir = (
        tmp_path / "state"
    )
    state_dir.mkdir()
    state_dir.chmod(0o700)

    config_dir = (
        tmp_path / "config"
    )
    config_dir.mkdir()
    config_dir.chmod(0o700)

    config_path = (
        config_dir / "config.toml"
    )

    config_path.write_text(
        (
            'model = "gpt-5.6-luna"\n'
            'reasoning_effort = "xhigh"\n'
            'tools = "enabled"\n'
            'sandbox = "read-only"\n'
        ),
        encoding="utf-8",
    )
    config_path.chmod(0o600)

    return QPaths(
        socket_path=socket_path,
        lock_path=(
            runtime / "qd.lock"
        ),
        database_path=(
            database_path
        ),
        state_dir=state_dir,
        config_path=config_path,
    )


def test_doctor_healthy():
    async def run():
        import json
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as raw:
            paths = _doctor_paths(
                Path(raw)
            )

            client = FakeClient(
                result={
                    "ok": True,
                    "version": {
                        "daemon": __version__,
                    },
                    "database": {
                        "ok": True,
                        "schema_version": 2,
                    },
                    "broker": {
                        "ok": True,
                        "reachable": True,
                        "health": {
                            "ok": True,
                            "workers": 5,
                        },
                    },
                }
            )

            stdout = io.StringIO()

            status = await run_management(
                args=parse("doctor"),
                client=client,
                stdout=stdout,
                paths=paths,
            )

            result = json.loads(
                stdout.getvalue()
            )

            assert status == 0
            assert result["ok"] is True

            assert (
                result[
                    "configuration"
                ]["ok"]
                is True
            )

            assert (
                result[
                    "daemon"
                ]["socket"]["ok"]
                is True
            )

            assert (
                result[
                    "daemon"
                ]["ipc"]["ok"]
                is True
            )

            assert (
                result[
                    "database"
                ]["ok"]
                is True
            )

            assert (
                result[
                    "broker"
                ]["ok"]
                is True
            )

            assert client.calls == [
                (
                    "doctor",
                    None,
                )
            ]

    asyncio.run(run())


def test_doctor_unhealthy_remote_returns_nonzero():
    async def run():
        import json
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as raw:
            paths = _doctor_paths(
                Path(raw)
            )

            client = FakeClient(
                result={
                    "ok": False,
                    "version": {
                        "daemon": __version__,
                    },
                    "database": {
                        "ok": True,
                    },
                    "broker": {
                        "ok": False,
                        "reachable": False,
                    },
                }
            )

            stdout = io.StringIO()

            status = await run_management(
                args=parse("doctor"),
                client=client,
                stdout=stdout,
                paths=paths,
            )

            result = json.loads(
                stdout.getvalue()
            )

            assert status == 1
            assert result["ok"] is False

            assert (
                result["broker"]["ok"]
                is False
            )

    asyncio.run(run())


def test_doctor_reports_daemon_ipc_failure():
    async def run():
        import json
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as raw:
            paths = _doctor_paths(
                Path(raw)
            )

            class DownClient:
                async def request(
                    self,
                    method,
                    params=None,
                ):
                    raise FileNotFoundError(
                        "daemon unavailable"
                    )

            stdout = io.StringIO()

            status = await run_management(
                args=parse("doctor"),
                client=DownClient(),
                stdout=stdout,
                paths=paths,
            )

            result = json.loads(
                stdout.getvalue()
            )

            assert status == 1
            assert result["ok"] is False

            assert (
                result[
                    "daemon"
                ]["socket"]["ok"]
                is True
            )

            assert (
                result[
                    "daemon"
                ]["ipc"]["ok"]
                is False
            )

            assert (
                result[
                    "database"
                ]["available"]
                is False
            )

            assert (
                result[
                    "broker"
                ]["available"]
                is False
            )

    asyncio.run(run())


def test_doctor_reports_invalid_local_config():
    async def run():
        import json
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as raw:
            paths = _doctor_paths(
                Path(raw)
            )

            paths.config_path.write_text(
                "unknown_setting = true\n",
                encoding="utf-8",
            )
            paths.config_path.chmod(
                0o600
            )

            client = FakeClient(
                result={
                    "ok": True,
                    "version": {
                        "daemon": __version__,
                    },
                    "database": {
                        "ok": True,
                    },
                    "broker": {
                        "ok": True,
                    },
                }
            )

            stdout = io.StringIO()

            status = await run_management(
                args=parse("doctor"),
                client=client,
                stdout=stdout,
                paths=paths,
            )

            result = json.loads(
                stdout.getvalue()
            )

            assert status == 1
            assert result["ok"] is False

            assert (
                result[
                    "configuration"
                ]["ok"]
                is False
            )

    asyncio.run(run())


def test_doctor_reports_daemon_ipc_timeout(
    monkeypatch,
):
    async def run():
        import json
        import tempfile
        from pathlib import Path

        import q.management as management

        monkeypatch.setattr(
            management,
            "DOCTOR_IPC_TIMEOUT_SECONDS",
            0.01,
        )

        class HangingClient:
            async def request(
                self,
                method,
                params=None,
            ):
                assert method == "doctor"

                await asyncio.Event().wait()

        with tempfile.TemporaryDirectory() as raw:
            paths = _doctor_paths(
                Path(raw)
            )

            stdout = io.StringIO()

            status = await run_management(
                args=parse("doctor"),
                client=HangingClient(),
                stdout=stdout,
                paths=paths,
            )

            result = json.loads(
                stdout.getvalue()
            )

            assert status == 1
            assert result["ok"] is False

            ipc = result[
                "daemon"
            ]["ipc"]

            assert ipc["ok"] is False
            assert (
                ipc["timed_out"]
                is True
            )
            assert (
                ipc["timeout_seconds"]
                == 0.01
            )
            assert (
                "timed out after 0.01 seconds"
                in ipc["error"]
            )

            assert (
                result["database"][
                    "available"
                ]
                is False
            )

            assert (
                result["broker"][
                    "available"
                ]
                is False
            )

    asyncio.run(run())


def test_doctor_reports_version_mismatch():
    async def run():
        import json
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as raw:
            paths = _doctor_paths(
                Path(raw)
            )

            client = FakeClient(
                result={
                    "ok": True,
                    "version": {
                        "daemon": "stale-test-version",
                    },
                    "database": {
                        "ok": True,
                    },
                    "broker": {
                        "ok": True,
                    },
                }
            )

            stdout = io.StringIO()

            status = await run_management(
                args=parse("doctor"),
                client=client,
                stdout=stdout,
                paths=paths,
            )

            result = json.loads(
                stdout.getvalue()
            )

            assert status == 1
            assert result["ok"] is False

            assert result["version"] == {
                "ok": False,
                "client": __version__,
                "daemon": "stale-test-version",
                "match": False,
            }

            assert (
                result["database"]["ok"]
                is True
            )
            assert (
                result["broker"]["ok"]
                is True
            )

    asyncio.run(run())
