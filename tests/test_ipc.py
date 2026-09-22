import asyncio
import json

import pytest
from pathlib import Path

from q.ipc import (
    IPCClient,
    IPCFrame,
    IPCRemoteError,
    IPCRequest,
    IPCServer,
    default_socket_path,
)


def test_default_socket_path_uses_xdg_runtime():
    path = default_socket_path(
        environ={
            "XDG_RUNTIME_DIR": (
                "/tmp/runtime-test"
            ),
        }
    )

    assert path == (
        Path("/tmp/runtime-test")
        / "q"
        / "qd.sock"
    )


def test_result_round_trip(
    tmp_path,
):
    async def run():
        async def handler(
            request: IPCRequest,
        ):
            assert (
                request.method
                == "ping"
            )

            yield IPCFrame.result_frame(
                request.request_id,
                {
                    "ok": True,
                    "value": (
                        request.params[
                            "value"
                        ]
                    ),
                },
            )

        socket = (
            tmp_path / "qd.sock"
        )

        server = IPCServer(
            socket,
            handler,
        )
        await server.start()

        try:
            client = IPCClient(
                socket
            )

            result = (
                await client.request(
                    "ping",
                    {
                        "value": 17,
                    },
                )
            )

            assert result == {
                "ok": True,
                "value": 17,
            }

            assert (
                socket.stat().st_mode
                & 0o777
            ) == 0o600

        finally:
            await server.aclose()

        assert not socket.exists()

    asyncio.run(run())


def test_streaming_events_then_result(
    tmp_path,
):
    async def run():
        async def handler(
            request: IPCRequest,
        ):
            yield IPCFrame.event_frame(
                request.request_id,
                "delta",
                {
                    "text": "A",
                },
            )

            yield IPCFrame.event_frame(
                request.request_id,
                "delta",
                {
                    "text": "B",
                },
            )

            yield IPCFrame.result_frame(
                request.request_id,
                {
                    "state": (
                        "completed"
                    ),
                },
            )

        socket = (
            tmp_path / "qd.sock"
        )
        server = IPCServer(
            socket,
            handler,
        )
        await server.start()

        try:
            client = IPCClient(
                socket
            )

            frames = []

            async for frame in (
                client.frames(
                    "submit",
                    {},
                    request_id="req-1",
                )
            ):
                frames.append(frame)

            assert [
                frame.type
                for frame in frames
            ] == [
                "event",
                "event",
                "result",
            ]

            assert [
                frame.data["text"]
                for frame in frames[:2]
            ] == [
                "A",
                "B",
            ]

            assert (
                frames[-1].result[
                    "state"
                ]
                == "completed"
            )

        finally:
            await server.aclose()

    asyncio.run(run())


def test_remote_error_is_typed(
    tmp_path,
):
    async def run():
        async def handler(
            request: IPCRequest,
        ):
            yield IPCFrame.error_frame(
                request.request_id,
                code="not_found",
                message="query missing",
                details={
                    "query_id": "q-1",
                },
            )

        socket = (
            tmp_path / "qd.sock"
        )
        server = IPCServer(
            socket,
            handler,
        )
        await server.start()

        try:
            client = IPCClient(
                socket
            )

            with pytest.raises(
                IPCRemoteError
            ) as caught:
                await client.request(
                    "status",
                    {
                        "query_id": (
                            "q-1"
                        ),
                    },
                )

            assert (
                caught.value.code
                == "not_found"
            )
            assert (
                caught.value.details
                == {
                    "query_id": "q-1",
                }
            )

        finally:
            await server.aclose()

    asyncio.run(run())


def test_invalid_request_gets_error_frame(
    tmp_path,
):
    async def run():
        async def handler(
            request: IPCRequest,
        ):
            raise AssertionError(
                "handler must not run"
            )

            yield

        socket = (
            tmp_path / "qd.sock"
        )
        server = IPCServer(
            socket,
            handler,
        )
        await server.start()

        try:
            reader, writer = (
                await asyncio.open_unix_connection(
                    str(socket)
                )
            )

            writer.write(
                b'{"request_id":"bad"}\n'
            )
            await writer.drain()

            line = await reader.readline()
            value = json.loads(line)

            assert (
                value["request_id"]
                == "bad"
            )
            assert (
                value["type"]
                == "error"
            )
            assert (
                value["error"]["code"]
                == "invalid_request"
            )

            writer.close()
            await writer.wait_closed()

        finally:
            await server.aclose()

    asyncio.run(run())


def test_concurrent_clients_do_not_cross_streams(
    tmp_path,
):
    async def run():
        async def handler(
            request: IPCRequest,
        ):
            value = request.params[
                "value"
            ]

            if value == "slow":
                await asyncio.sleep(
                    0.05
                )

            yield IPCFrame.event_frame(
                request.request_id,
                "delta",
                {
                    "text": value,
                },
            )

            yield IPCFrame.result_frame(
                request.request_id,
                {
                    "value": value,
                },
            )

        socket = (
            tmp_path / "qd.sock"
        )
        server = IPCServer(
            socket,
            handler,
        )
        await server.start()

        async def collect(value):
            client = IPCClient(
                socket
            )

            seen = []

            async for frame in (
                client.frames(
                    "query",
                    {
                        "value": value,
                    },
                )
            ):
                seen.append(frame)

            return seen

        try:
            slow, fast = (
                await asyncio.gather(
                    collect("slow"),
                    collect("fast"),
                )
            )

            assert (
                slow[0].data["text"]
                == "slow"
            )
            assert (
                slow[-1].result[
                    "value"
                ]
                == "slow"
            )

            assert (
                fast[0].data["text"]
                == "fast"
            )
            assert (
                fast[-1].result[
                    "value"
                ]
                == "fast"
            )

        finally:
            await server.aclose()

    asyncio.run(run())
