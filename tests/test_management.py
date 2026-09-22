import asyncio
import io

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
