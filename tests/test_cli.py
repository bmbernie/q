import argparse
import asyncio
import io
from pathlib import Path

import pytest

from q.cli import (
    CLIError,
    build_input,
    build_parser,
    run_query,
)
from q.ipc import IPCFrame


class FakeClient:
    def __init__(
        self,
        frames=None,
        result=None,
    ):
        self.sent = []
        self._frames = (
            frames or []
        )
        self._result = result

    def frames(
        self,
        method,
        params,
    ):
        self.sent.append(
            (
                method,
                params,
            )
        )

        async def generate():
            for frame in self._frames:
                yield frame

        return generate()

    async def request(
        self,
        method,
        params,
    ):
        self.sent.append(
            (
                method,
                params,
            )
        )
        return self._result


def parse(*args):
    return build_parser().parse_args(
        list(args)
    )


def test_quiet_levels():
    assert parse("hello").quiet == 0
    assert parse("-q", "hello").quiet == 1
    assert parse("-qq", "hello").quiet == 2


def test_build_input_from_prompt_only():
    result = build_input(
        prompt_parts=[
            "hello",
            "world",
        ],
        file_paths=[],
        stdin=TTYStringIO(),
    )

    assert result == "hello world"


def test_build_input_from_stdin_only():
    stdin = io.StringIO(
        "from pipe\n"
    )

    result = build_input(
        prompt_parts=[],
        file_paths=[],
        stdin=stdin,
    )

    assert result == "from pipe\n"


def test_build_input_combines_prompt_stdin_and_file(
    tmp_path,
):
    path = tmp_path / "note.txt"
    path.write_text(
        "file body\n",
        encoding="utf-8",
    )

    stdin = io.StringIO(
        "pipe body\n"
    )

    result = build_input(
        prompt_parts=[
            "analyze",
            "this",
        ],
        file_paths=[
            str(path),
        ],
        stdin=stdin,
    )

    assert result.startswith(
        "analyze this\n\n"
        "----- stdin -----\n"
        "pipe body\n"
    )

    assert (
        f"----- file: {path.resolve()} -----"
        in result
    )

    assert result.endswith(
        "file body\n"
    )


def test_attached_streaming_query(
    tmp_path,
):
    async def run():
        args = parse(
            "-s",
            "alpha",
            "-r",
            "xhigh",
            "--no-tools",
            "hello",
        )

        frames = [
            IPCFrame.event_frame(
                "request",
                "delta",
                {
                    "delta": "HELLO",
                },
            ),
            IPCFrame.result_frame(
                "request",
                {
                    "query_id": "q-1",
                    "state": "completed",
                },
            ),
        ]

        client = FakeClient(
            frames=frames
        )

        stdout = io.StringIO()

        old_cwd = Path.cwd()

        try:
            import os
            os.chdir(tmp_path)

            status = await run_query(
                args=args,
                client=client,
                stdin=TTYStringIO(),
                stdout=stdout,
            )
        finally:
            os.chdir(old_cwd)

        assert status == 0
        assert stdout.getvalue() == (
            "HELLO\n"
        )

        method, params = (
            client.sent[0]
        )

        assert method == "submit"
        assert (
            params["session_name"]
            == "alpha"
        )
        assert (
            params["reasoning_effort"]
            == "xhigh"
        )
        assert (
            params["tools"]
            == "disabled"
        )
        assert params["detached"] is False

    asyncio.run(run())


def test_no_stream_buffers_answer(
    tmp_path,
):
    async def run():
        args = parse(
            "--no-stream",
            "hello",
        )

        client = FakeClient(
            frames=[
                IPCFrame.event_frame(
                    "r",
                    "delta",
                    {
                        "delta": "A",
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

        status = await run_query(
            args=args,
            client=client,
            stdin=TTYStringIO(),
            stdout=stdout,
        )

        assert status == 0
        assert stdout.getvalue() == "AB\n"

    asyncio.run(run())


def test_q_detaches_and_prints_query_id():
    async def run():
        args = parse(
            "-q",
            "background",
        )

        client = FakeClient(
            result={
                "query_id": "q-123",
                "state": "queued",
            }
        )

        stdout = io.StringIO()

        status = await run_query(
            args=args,
            client=client,
            stdin=TTYStringIO(),
            stdout=stdout,
        )

        assert status == 0
        assert stdout.getvalue() == (
            "q-123\n"
        )

        assert (
            client.sent[0][1][
                "detached"
            ]
            is True
        )

    asyncio.run(run())


def test_qq_detaches_silently():
    async def run():
        args = parse(
            "-qq",
            "background",
        )

        client = FakeClient(
            result={
                "query_id": "q-123",
                "state": "queued",
            }
        )

        stdout = io.StringIO()

        status = await run_query(
            args=args,
            client=client,
            stdin=TTYStringIO(),
            stdout=stdout,
        )

        assert status == 0
        assert stdout.getvalue() == ""

    asyncio.run(run())


def test_empty_input_rejected():
    async def run():
        with pytest.raises(
            CLIError
        ):
            await run_query(
                args=parse(),
                client=FakeClient(),
                stdin=TTYStringIO(),
                stdout=io.StringIO(),
            )

    asyncio.run(run())


class TTYStringIO(io.StringIO):
    def isatty(self):
        return True
