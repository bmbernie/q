import argparse
import asyncio
import io
from pathlib import Path

import pytest

from q.cli import (
    CLIError,
    _interactive_result,
    _safe_interaction_result,
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
            "--backend",
            "web",
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
            params["backend"]
            == "web"
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


def test_command_approval_interaction(
    tmp_path,
):
    async def run():
        args = parse("hello")

        client = FakeClient(
            frames=[
                IPCFrame.event_frame(
                    "r",
                    "interaction",
                    {
                        "type": "interaction",
                        "query_id": "q-1",
                        "interaction_id": "i-1",
                        "method": (
                            "item/commandExecution/"
                            "requestApproval"
                        ),
                        "params": {
                            "command": "echo hello",
                            "cwd": "/tmp",
                            "reason": "test",
                        },
                    },
                ),
                IPCFrame.event_frame(
                    "r",
                    "delta",
                    {
                        "delta": "DONE",
                    },
                ),
                IPCFrame.result_frame(
                    "r",
                    {
                        "state": "completed",
                    },
                ),
            ],
            result={
                "responded": True,
            },
        )

        stdin = TTYStringIO(
            "y\n"
        )
        stdout = io.StringIO()
        stderr = io.StringIO()

        status = await run_query(
            args=args,
            client=client,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
        )

        assert status == 0
        assert stdout.getvalue() == (
            "DONE\n"
        )

        assert (
            "command approval"
            in stderr.getvalue()
        )

        assert client.sent[1] == (
            "respond_interaction",
            {
                "query_id": "q-1",
                "interaction_id": "i-1",
                "result": {
                    "decision": "accept",
                },
            },
        )

    asyncio.run(run())


def test_tool_user_input_interaction():
    async def run():
        args = parse("hello")

        client = FakeClient(
            frames=[
                IPCFrame.event_frame(
                    "r",
                    "interaction",
                    {
                        "type": "interaction",
                        "query_id": "q-2",
                        "interaction_id": "i-2",
                        "method": (
                            "item/tool/"
                            "requestUserInput"
                        ),
                        "params": {
                            "questions": [
                                {
                                    "header": (
                                        "Environment"
                                    ),
                                    "id": "env",
                                    "question": (
                                        "Choose one"
                                    ),
                                    "options": [
                                        {
                                            "label": "prod",
                                            "description": (
                                                "Production"
                                            ),
                                        },
                                        {
                                            "label": "dev",
                                            "description": (
                                                "Development"
                                            ),
                                        },
                                    ],
                                }
                            ],
                        },
                    },
                ),
                IPCFrame.result_frame(
                    "r",
                    {
                        "state": "completed",
                    },
                ),
            ],
            result={
                "responded": True,
            },
        )

        stdin = TTYStringIO(
            "2\n"
        )

        status = await run_query(
            args=args,
            client=client,
            stdin=stdin,
            stdout=io.StringIO(),
            stderr=io.StringIO(),
        )

        assert status == 0

        assert client.sent[1] == (
            "respond_interaction",
            {
                "query_id": "q-2",
                "interaction_id": "i-2",
                "result": {
                    "answers": {
                        "env": {
                            "answers": [
                                "dev"
                            ],
                        },
                    },
                },
            },
        )

    asyncio.run(run())


def test_permissions_interaction_can_deny():
    async def run():
        args = parse("hello")

        client = FakeClient(
            frames=[
                IPCFrame.event_frame(
                    "r",
                    "interaction",
                    {
                        "type": "interaction",
                        "query_id": "q-3",
                        "interaction_id": "i-3",
                        "method": (
                            "item/permissions/"
                            "requestApproval"
                        ),
                        "params": {
                            "permissions": {
                                "network": {
                                    "enabled": True,
                                },
                            },
                        },
                    },
                ),
                IPCFrame.result_frame(
                    "r",
                    {
                        "state": "completed",
                    },
                ),
            ],
            result={
                "responded": True,
            },
        )

        status = await run_query(
            args=args,
            client=client,
            stdin=TTYStringIO(
                "n\n"
            ),
            stdout=io.StringIO(),
            stderr=io.StringIO(),
        )

        assert status == 0

        assert client.sent[1] == (
            "respond_interaction",
            {
                "query_id": "q-3",
                "interaction_id": "i-3",
                "result": {
                    "permissions": {},
                    "scope": "turn",
                },
            },
        )

    asyncio.run(run())



def test_command_approval_respects_available_decisions():
    result = _interactive_result(
        method=(
            "item/commandExecution/"
            "requestApproval"
        ),
        params={
            "command": "touch /tmp/test",
            "availableDecisions": [
                "accept",
                {
                    "acceptWithExecpolicyAmendment": {
                        "execpolicy_amendment": [
                            "touch",
                            "/tmp/test",
                        ],
                    },
                },
                "cancel",
            ],
        },
        input_stream=TTYStringIO(
            "c\n"
        ),
        output_stream=io.StringIO(),
    )

    assert result == {
        "decision": "cancel",
    }


def test_safe_approval_falls_back_to_cancel_when_decline_unavailable():
    result = _safe_interaction_result(
        (
            "item/commandExecution/"
            "requestApproval"
        ),
        {
            "availableDecisions": [
                "accept",
                "cancel",
            ],
        },
    )

    assert result == {
        "decision": "cancel",
    }
