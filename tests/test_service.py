import asyncio

from q import __version__
import sqlite3
from pathlib import Path

import pytest

from q.config import QConfig
from q.engine import QueryEngine
from q.ipc import (
    IPCClient,
    IPCRemoteError,
    IPCRequest,
    IPCServer,
)
from q.models import (
    QueryState,
    ToolsPolicy,
)
from q.service import QDService
from q.store import QStore


class FakeBroker:
    def __init__(self):
        self.calls = []
        self.started = {}
        self.release = {}
        self.interrupts = []
        self.interrupted = set()

    def block(self, prompt):
        self.started[prompt] = (
            asyncio.Event()
        )
        self.release[prompt] = (
            asyncio.Event()
        )

    def stream_query(
        self,
        *,
        input_text,
        cwd,
        model,
        reasoning_effort,
        backend,
        thread_id,
        tools_policy,
        ephemeral,
        sandbox,
    ):
        return self._stream(
            input_text=input_text,
            cwd=cwd,
            model=model,
            reasoning_effort=(
                reasoning_effort
            ),
            backend=backend,
            thread_id=thread_id,
            tools_policy=tools_policy,
            ephemeral=ephemeral,
            sandbox=sandbox,
        )

    async def _stream(
        self,
        **kwargs,
    ):
        prompt = kwargs[
            "input_text"
        ]

        self.calls.append(
            dict(kwargs)
        )

        thread_id = (
            kwargs["thread_id"]
            or f"thread-{prompt}"
        )
        turn_id = f"turn-{prompt}"

        started = self.started.get(
            prompt
        )

        if started is not None:
            started.set()

        yield {
            "type": "thread",
            "thread_id": thread_id,
            "backend": (
                kwargs["backend"]
                or "codex"
            ),
        }

        yield {
            "type": "turn",
            "thread_id": thread_id,
            "turn_id": turn_id,
        }

        gate = self.release.get(
            prompt
        )

        if gate is not None:
            await gate.wait()

        if turn_id in self.interrupted:
            yield {
                "type": "completed",
                "thread_id": thread_id,
                "turn_id": turn_id,
                "status": (
                    "interrupted"
                ),
                "error": None,
            }
            return

        yield {
            "type": "delta",
            "thread_id": thread_id,
            "turn_id": turn_id,
            "delta": f"answer:{prompt}",
        }

        yield {
            "type": "completed",
            "thread_id": thread_id,
            "turn_id": turn_id,
            "status": "completed",
            "error": None,
        }

    async def interrupt(
        self,
        *,
        thread_id,
        turn_id,
    ):
        self.interrupts.append(
            (
                thread_id,
                turn_id,
            )
        )

        self.interrupted.add(
            turn_id
        )

        prompt = turn_id.removeprefix(
            "turn-"
        )

        gate = self.release.get(
            prompt
        )

        if gate is not None:
            gate.set()


def make_stack(
    tmp_path,
    *,
    config=None,
):
    if config is None:
        config = QConfig()

    store = QStore(
        tmp_path / "q.sqlite3"
    )
    broker = FakeBroker()

    counter = iter(
        f"q-{number}"
        for number in range(
            1,
            100,
        )
    )

    engine = QueryEngine(
        store=store,
        broker=broker,
        state_dir=(
            tmp_path / "state"
        ),
        query_id_factory=(
            lambda: next(counter)
        ),
        default_model=(
            config.model
        ),
        default_reasoning_effort=(
            config.reasoning_effort
        ),
        default_tools_policy=(
            config.tools
        ),
    )

    service = QDService(
        store=store,
        engine=engine,
        config=config,
    )

    socket = (
        tmp_path / "qd.sock"
    )

    server = IPCServer(
        socket,
        service.handle,
    )

    return (
        store,
        broker,
        engine,
        server,
        socket,
    )


def test_attached_submit_streams_and_completes(
    tmp_path,
):
    async def run():
        (
            store,
            broker,
            engine,
            server,
            socket,
        ) = make_stack(tmp_path)

        await server.start()

        try:
            client = IPCClient(socket)
            frames = []

            async for frame in (
                client.frames(
                    "submit",
                    {
                        "input": "hello",
                        "cwd": "/tmp",
                    },
                )
            ):
                frames.append(frame)

            assert (
                frames[-1].type
                == "result"
            )
            assert (
                frames[-1].result[
                    "state"
                ]
                == "completed"
            )

            deltas = [
                frame.data["delta"]
                for frame in frames
                if frame.type == "event"
                and frame.event == "delta"
            ]

            assert deltas == [
                "answer:hello"
            ]

            assert (
                broker.calls[0][
                    "model"
                ]
                == "gpt-5.6-luna"
            )
            assert (
                broker.calls[0][
                    "reasoning_effort"
                ]
                == "xhigh"
            )

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())


def test_detached_submit_returns_before_completion(
    tmp_path,
):
    async def run():
        (
            store,
            broker,
            engine,
            server,
            socket,
        ) = make_stack(tmp_path)

        broker.block("long")

        await server.start()

        try:
            client = IPCClient(socket)

            result = await client.request(
                "submit",
                {
                    "input": "long",
                    "cwd": "/tmp",
                    "detached": True,
                    "stdout_attached": (
                        False
                    ),
                },
            )

            assert (
                result["query_id"]
                == "q-1"
            )
            assert (
                result["detached"]
                is True
            )

            await asyncio.wait_for(
                broker.started[
                    "long"
                ].wait(),
                timeout=1,
            )

            current = store.get_query(
                "q-1"
            )

            assert current is not None
            assert (
                current.state
                == QueryState.RUNNING
            )

            broker.release[
                "long"
            ].set()

            completed = (
                await engine.wait(
                    "q-1"
                )
            )

            assert (
                completed.state
                == QueryState.COMPLETED
            )

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())


def test_management_methods(
    tmp_path,
):
    async def run():
        (
            store,
            broker,
            engine,
            server,
            socket,
        ) = make_stack(tmp_path)

        await server.start()

        try:
            client = IPCClient(socket)

            result = await client.request(
                "submit",
                {
                    "input": "remember",
                    "cwd": "/tmp",
                    "session_name": (
                        "alpha"
                    ),
                },
            )

            query_id = result[
                "query_id"
            ]

            status = (
                await client.request(
                    "status",
                    {
                        "query_id": (
                            query_id
                        ),
                    },
                )
            )

            assert (
                status["state"]
                == "completed"
            )

            shown = await client.request(
                "show",
                {
                    "query_id": query_id,
                },
            )

            assert (
                shown[
                    "prompt_metadata"
                ]["input"]
                == "remember"
            )

            queries = (
                await client.request(
                    "queries",
                    {
                        "session_name": (
                            "alpha"
                        ),
                    },
                )
            )

            assert [
                item["query_id"]
                for item
                in queries["queries"]
            ] == [query_id]

            sessions = (
                await client.request(
                    "sessions"
                )
            )

            assert (
                sessions["sessions"][0][
                    "session_name"
                ]
                == "alpha"
            )

            assert (
                sessions["sessions"][0][
                    "backend_thread_id"
                ]
                == "thread-remember"
            )

            assert (
                sessions["sessions"][0][
                    "backend_bindings"
                ]
                == {
                    "codex": "thread-remember",
                }
            )

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())


def test_follow_replays_runtime_events(
    tmp_path,
):
    async def run():
        (
            store,
            broker,
            engine,
            server,
            socket,
        ) = make_stack(tmp_path)

        await server.start()

        try:
            client = IPCClient(socket)

            result = await client.request(
                "submit",
                {
                    "input": "follow-me",
                    "cwd": "/tmp",
                },
            )

            frames = []

            async for frame in (
                client.frames(
                    "follow",
                    {
                        "query_id": (
                            result[
                                "query_id"
                            ]
                        ),
                    },
                )
            ):
                frames.append(frame)

            assert any(
                frame.type == "event"
                and frame.event == "delta"
                and frame.data["delta"]
                == "answer:follow-me"
                for frame in frames
            )

            assert (
                frames[-1].type
                == "result"
            )

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())


def test_cancel_running_query(
    tmp_path,
):
    async def run():
        (
            store,
            broker,
            engine,
            server,
            socket,
        ) = make_stack(tmp_path)

        broker.block("cancel")

        await server.start()

        try:
            client = IPCClient(socket)

            submitted = (
                await client.request(
                    "submit",
                    {
                        "input": "cancel",
                        "cwd": "/tmp",
                        "detached": True,
                    },
                )
            )

            await asyncio.wait_for(
                broker.started[
                    "cancel"
                ].wait(),
                timeout=1,
            )

            while True:
                query = store.get_query(
                    submitted[
                        "query_id"
                    ]
                )

                assert query is not None

                if (
                    query.upstream_turn_id
                    is not None
                ):
                    break

                await asyncio.sleep(0)

            cancelled = (
                await client.request(
                    "cancel",
                    {
                        "query_id": (
                            submitted[
                                "query_id"
                            ]
                        ),
                    },
                )
            )

            assert (
                cancelled[
                    "cancel_requested_at"
                ]
                is not None
            )

            final = await engine.wait(
                submitted["query_id"]
            )

            assert (
                final.state
                == QueryState.CANCELLED
            )

            assert broker.interrupts == [
                (
                    "thread-cancel",
                    "turn-cancel",
                )
            ]

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())


def test_unknown_query_returns_not_found(
    tmp_path,
):
    async def run():
        (
            store,
            broker,
            engine,
            server,
            socket,
        ) = make_stack(tmp_path)

        await server.start()

        try:
            client = IPCClient(socket)

            with pytest.raises(
                IPCRemoteError
            ) as caught:
                await client.request(
                    "status",
                    {
                        "query_id": (
                            "q-missing"
                        ),
                    },
                )

            assert (
                caught.value.code
                == "not_found"
            )

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())



def test_existing_session_inherits_tools_policy(
    tmp_path,
):
    async def run():
        (
            store,
            broker,
            engine,
            server,
            socket,
        ) = make_stack(tmp_path)

        await server.start()

        try:
            client = IPCClient(socket)

            first = await client.request(
                "submit",
                {
                    "input": "first-disabled",
                    "cwd": "/tmp",
                    "session_name": "locked",
                    "tools": "disabled",
                },
            )

            assert (
                first["tools_policy"]
                == "disabled"
            )

            second = await client.request(
                "submit",
                {
                    "input": "second-inherit",
                    "cwd": "/tmp",
                    "session_name": "locked",
                },
            )

            assert (
                second["tools_policy"]
                == "disabled"
            )

            assert (
                broker.calls[0][
                    "tools_policy"
                ]
                == ToolsPolicy.DISABLED
            )

            # Existing Codex threads do not receive
            # a tools override on resume.
            assert (
                broker.calls[1][
                    "tools_policy"
                ]
                is None
            )

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())



def test_follow_completed_query_uses_persisted_log(
    tmp_path,
):
    async def run():
        (
            store,
            broker,
            engine,
            server,
            socket,
        ) = make_stack(tmp_path)

        await server.start()

        try:
            client = IPCClient(socket)

            result = await client.request(
                "submit",
                {
                    "input": "persisted-follow",
                    "cwd": "/tmp",
                },
            )

            query_id = result[
                "query_id"
            ]

            assert (
                engine.has_runtime(
                    query_id
                )
                is False
            )

            frames = []

            async for frame in (
                client.frames(
                    "follow",
                    {
                        "query_id": (
                            query_id
                        ),
                    },
                )
            ):
                frames.append(frame)

            deltas = [
                frame.data["delta"]
                for frame in frames
                if (
                    frame.type == "event"
                    and frame.event
                    == "delta"
                )
            ]

            assert deltas == [
                "answer:persisted-follow"
            ]

            assert (
                frames[-1].type
                == "result"
            )
            assert (
                frames[-1].result[
                    "state"
                ]
                == "completed"
            )

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())



def test_logs_expose_global_and_session_ndjson(
    tmp_path,
):
    async def run():
        (
            store,
            broker,
            engine,
            server,
            socket,
        ) = make_stack(tmp_path)

        await server.start()

        try:
            client = IPCClient(socket)

            result = await client.request(
                "submit",
                {
                    "input": "log-me",
                    "cwd": "/tmp",
                    "session_name": (
                        "log-session"
                    ),
                },
            )

            query_id = result[
                "query_id"
            ]

            global_lines = []

            async for frame in (
                client.frames(
                    "logs",
                    {
                        "follow": False,
                    },
                )
            ):
                if (
                    frame.type == "event"
                    and frame.event == "log"
                ):
                    global_lines.append(
                        frame.data["line"]
                    )

            session_lines = []

            async for frame in (
                client.frames(
                    "logs",
                    {
                        "follow": False,
                        "session_name": (
                            "log-session"
                        ),
                    },
                )
            ):
                if (
                    frame.type == "event"
                    and frame.event == "log"
                ):
                    session_lines.append(
                        frame.data["line"]
                    )

            assert any(
                query_id in line
                for line in global_lines
            )

            assert any(
                '"delta":"answer:log-me"'
                in line
                for line in global_lines
            )

            assert any(
                query_id in line
                for line in session_lines
            )

            assert any(
                '"delta":"answer:log-me"'
                in line
                for line in session_lines
            )

            assert (
                tmp_path
                / "state"
                / "q.ndjson"
            ).exists()

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())



def test_config_defaults_apply_to_new_query(
    tmp_path,
):
    async def run():
        config = QConfig(
            model="gpt-5.6-sol",
            reasoning_effort="high",
            tools=ToolsPolicy.DISABLED,
            sandbox="workspace-write",
        )

        (
            store,
            broker,
            engine,
            server,
            socket,
        ) = make_stack(
            tmp_path,
            config=config,
        )

        await server.start()

        try:
            client = IPCClient(socket)

            result = await client.request(
                "submit",
                {
                    "input": "configured",
                    "cwd": "/tmp",
                    "backend": "web",
                },
            )

            assert (
                result["model"]
                == "gpt-5.6-sol"
            )
            assert (
                result["reasoning_effort"]
                == "high"
            )
            assert (
                result["tools_policy"]
                == "disabled"
            )

            call = broker.calls[0]

            assert (
                call["backend"]
                == "web"
            )
            assert (
                call["model"]
                == "gpt-5.6-sol"
            )
            assert (
                call["reasoning_effort"]
                == "high"
            )
            assert (
                call["tools_policy"]
                == ToolsPolicy.DISABLED
            )
            assert (
                call["sandbox"]
                == "workspace-write"
            )

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())



def test_existing_session_inherits_saved_model_and_reasoning(
    tmp_path,
):
    async def run():
        first_config = QConfig(
            model="gpt-5.6-luna",
            reasoning_effort="xhigh",
            tools=ToolsPolicy.ENABLED,
            sandbox="read-only",
        )

        (
            store,
            broker,
            engine,
            server,
            socket,
        ) = make_stack(
            tmp_path,
            config=first_config,
        )

        await server.start()

        try:
            client = IPCClient(socket)

            first = await client.request(
                "submit",
                {
                    "input": "first-session-turn",
                    "cwd": "/tmp",
                    "session_name": "stable",
                },
            )

            assert first["model"] == "gpt-5.6-luna"
            assert first["reasoning_effort"] == "xhigh"

            # Simulate daemon configuration changing after
            # the named session was originally created.
            engine.default_model = "gpt-5.6-sol"
            engine.default_reasoning_effort = "high"

            second = await client.request(
                "submit",
                {
                    "input": "second-session-turn",
                    "cwd": "/tmp",
                    "session_name": "stable",
                },
            )

            assert second["model"] == "gpt-5.6-luna"
            assert second["reasoning_effort"] == "xhigh"

            assert broker.calls[1]["model"] == "gpt-5.6-luna"
            assert (
                broker.calls[1]["reasoning_effort"]
                == "xhigh"
            )

            # Explicit per-turn overrides are allowed, but
            # they do not rewrite the saved session defaults.
            third = await client.request(
                "submit",
                {
                    "input": "override-turn",
                    "cwd": "/tmp",
                    "session_name": "stable",
                    "model": "gpt-5.6-sol",
                    "reasoning_effort": "high",
                },
            )

            assert third["model"] == "gpt-5.6-sol"
            assert third["reasoning_effort"] == "high"

            session = store.get_session("stable")

            assert session is not None
            assert session.model == "gpt-5.6-luna"
            assert session.reasoning_effort == "xhigh"

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())


def test_respond_interaction_ipc_method():
    class StubEngine:
        def __init__(self):
            self.calls = []

        async def respond_interaction(
            self,
            query_id,
            interaction_id,
            result,
        ):
            self.calls.append(
                (
                    query_id,
                    interaction_id,
                    result,
                )
            )

    async def run():
        engine = StubEngine()

        service = QDService(
            store=object(),
            engine=engine,
        )

        request = IPCRequest(
            request_id="request-1",
            method="respond_interaction",
            params={
                "query_id": "q-1",
                "interaction_id": "i-1",
                "result": {
                    "decision": "accept",
                },
            },
        )

        frames = [
            frame
            async for frame
            in service.handle(request)
        ]

        assert len(frames) == 1
        assert frames[0].type == "result"

        assert frames[0].result == {
            "responded": True,
            "query_id": "q-1",
            "interaction_id": "i-1",
        }

        assert engine.calls == [
            (
                "q-1",
                "i-1",
                {
                    "decision": "accept",
                },
            )
        ]

    asyncio.run(run())



def test_follow_reports_unretained_log(
    tmp_path,
):
    async def run():
        (
            store,
            broker,
            engine,
            server,
            socket,
        ) = make_stack(tmp_path)

        await server.start()

        try:
            client = IPCClient(socket)

            result = await client.request(
                "submit",
                {
                    "input": "expire-me",
                    "cwd": "/tmp",
                },
            )

            query_id = result[
                "query_id"
            ]

            query = store.get_query(
                query_id
            )

            assert query is not None

            Path(
                query.log_path
            ).unlink()

            frames = []

            async for frame in (
                client.frames(
                    "follow",
                    {
                        "query_id": (
                            query_id
                        ),
                    },
                )
            ):
                frames.append(frame)

            notices = [
                frame
                for frame in frames
                if (
                    frame.type == "event"
                    and
                    frame.event == "notice"
                )
            ]

            assert len(notices) == 1
            assert (
                notices[0]
                .data["log_retained"]
                is False
            )
            assert (
                notices[0]
                .data["message"]
                == (
                    "per-query log "
                    "is not retained"
                )
            )

            assert (
                frames[-1].type
                == "result"
            )
            assert (
                frames[-1]
                .result["log_retained"]
                is False
            )

            # Metadata remains available.
            assert (
                store.get_query(
                    query_id
                )
                is not None
            )

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())



def test_db_check_reports_store_health(
    tmp_path,
):
    async def run():
        (
            store,
            broker,
            engine,
            server,
            socket,
        ) = make_stack(tmp_path)

        await server.start()

        try:
            client = IPCClient(socket)

            result = await client.request(
                "db_check"
            )

            assert result["ok"] is True
            assert (
                result["quick_check"]
                == ["ok"]
            )
            assert (
                result["journal_mode"]
                == "wal"
            )
            assert (
                result["synchronous"]
                == 2
            )
            assert (
                result["foreign_keys"]
                is True
            )
            assert (
                result["busy_timeout"]
                == 5000
            )

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())


def test_db_backup_creates_consistent_snapshot(
    tmp_path,
):
    async def run():
        (
            store,
            broker,
            engine,
            server,
            socket,
        ) = make_stack(tmp_path)

        await server.start()

        try:
            client = IPCClient(socket)

            destination = (
                tmp_path
                / "snapshot.sqlite3"
            )

            result = await client.request(
                "db_backup",
                {
                    "path": str(
                        destination
                    ),
                },
            )

            assert result["ok"] is True
            assert (
                result["quick_check"]
                == ["ok"]
            )
            assert destination.exists()

            snapshot = sqlite3.connect(
                destination
            )

            assert (
                snapshot.execute(
                    "PRAGMA quick_check"
                ).fetchone()[0]
                == "ok"
            )

            snapshot.close()

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())


def test_doctor_reports_database_and_broker_health(
    tmp_path,
):
    async def run():
        (
            store,
            broker,
            engine,
            server,
            socket,
        ) = make_stack(tmp_path)

        async def health():
            return {
                "ok": True,
                "workers": 5,
            }

        broker.health = health

        await server.start()

        try:
            client = IPCClient(socket)

            result = await client.request(
                "doctor"
            )

            assert result["ok"] is True

            assert result["version"] == {
                "daemon": __version__,
            }

            assert (
                result["database"]["ok"]
                is True
            )

            assert (
                result["database"][
                    "schema_version"
                ]
                == 3
            )

            assert (
                result["database"][
                    "expected_schema_version"
                ]
                == 3
            )

            assert (
                result["database"][
                    "settings_ok"
                ]
                is True
            )

            assert (
                result["broker"]["ok"]
                is True
            )

            assert (
                result["broker"][
                    "reachable"
                ]
                is True
            )

            assert (
                result["broker"][
                    "health"
                ]["workers"]
                == 5
            )

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())


def test_doctor_reports_unhealthy_broker(
    tmp_path,
):
    async def run():
        (
            store,
            broker,
            engine,
            server,
            socket,
        ) = make_stack(tmp_path)

        async def health():
            return {
                "ok": False,
                "workers": 0,
            }

        broker.health = health

        await server.start()

        try:
            client = IPCClient(socket)

            result = await client.request(
                "doctor"
            )

            assert result["ok"] is False
            assert (
                result["database"]["ok"]
                is True
            )
            assert (
                result["broker"]["ok"]
                is False
            )
            assert (
                result["broker"][
                    "reachable"
                ]
                is True
            )

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())


def test_restart_guard_refuses_running_query(
    tmp_path,
):
    async def run():
        (
            store,
            broker,
            engine,
            server,
            socket,
        ) = make_stack(tmp_path)

        broker.block("guard-running")

        await server.start()

        try:
            client = IPCClient(socket)

            submitted = await client.request(
                "submit",
                {
                    "input": "guard-running",
                    "cwd": "/tmp",
                    "detached": True,
                },
            )

            await asyncio.wait_for(
                broker.started[
                    "guard-running"
                ].wait(),
                timeout=1,
            )

            result = await client.request(
                "restart_guard_acquire"
            )

            assert (
                result["acquired"]
                is False
            )
            assert (
                result["guarded"]
                is False
            )
            assert result["token"] is None
            assert (
                result["reason"]
                == "running_queries"
            )
            assert (
                result[
                    "running_query_ids"
                ]
                == [
                    submitted["query_id"]
                ]
            )

            broker.release[
                "guard-running"
            ].set()

            final = await engine.wait(
                submitted["query_id"]
            )

            assert (
                final.state
                == QueryState.COMPLETED
            )

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())


def test_restart_guard_rejects_new_submit(
    tmp_path,
):
    async def run():
        (
            store,
            _broker,
            engine,
            server,
            socket,
        ) = make_stack(tmp_path)

        await server.start()

        try:
            client = IPCClient(socket)

            result = await client.request(
                "restart_guard_acquire"
            )

            assert (
                result["acquired"]
                is True
            )
            assert (
                result["guarded"]
                is True
            )
            assert isinstance(
                result["token"],
                str,
            )
            assert result["token"]
            assert result["reason"] is None
            assert (
                result["lease_seconds"]
                == 300.0
            )

            token = result["token"]

            with pytest.raises(
                IPCRemoteError
            ) as caught:
                await client.request(
                    "submit",
                    {
                        "input": "blocked",
                        "cwd": "/tmp",
                        "detached": True,
                    },
                )

            assert (
                caught.value.code
                == "conflict"
            )

            released = await client.request(
                "restart_guard_release",
                {
                    "token": token,
                },
            )

            assert released == {
                "released": True,
                "guarded": False,
            }

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())


def test_restart_guard_freezes_queued_boundary(
    tmp_path,
):
    async def run():
        (
            store,
            broker,
            engine,
            server,
            _socket,
        ) = make_stack(tmp_path)

        broker.block("guard-queued")

        await server.start()

        try:
            query = await engine.submit(
                input_text="guard-queued",
                cwd="/tmp",
                detached=True,
                stdout_attached=False,
            )

            (
                acquired,
                token,
                running,
            ) = (
                engine
                .try_acquire_restart_guard()
            )

            assert acquired is True
            assert isinstance(token, str)
            assert token
            assert running == []

            # Give the scheduled task an
            # opportunity to reach _execute().
            await asyncio.sleep(0)

            current = store.get_query(
                query.query_id
            )

            assert current is not None
            assert (
                current.state
                == QueryState.QUEUED
            )
            assert (
                broker.started[
                    "guard-queued"
                ].is_set()
                is False
            )

            assert (
                engine.release_restart_guard(
                    token
                )
                is True
            )

            await asyncio.wait_for(
                broker.started[
                    "guard-queued"
                ].wait(),
                timeout=1,
            )

            current = store.get_query(
                query.query_id
            )

            assert current is not None
            assert (
                current.state
                == QueryState.RUNNING
            )

            broker.release[
                "guard-queued"
            ].set()

            final = await engine.wait(
                query.query_id
            )

            assert (
                final.state
                == QueryState.COMPLETED
            )

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())


def test_restart_guard_requires_owner_token(
    tmp_path,
):
    async def run():
        (
            store,
            _broker,
            engine,
            server,
            socket,
        ) = make_stack(tmp_path)

        await server.start()

        try:
            client = IPCClient(socket)

            acquired = await client.request(
                "restart_guard_acquire"
            )

            token = acquired["token"]

            wrong_release = (
                await client.request(
                    "restart_guard_release",
                    {
                        "token": "wrong-token",
                    },
                )
            )

            assert (
                wrong_release["released"]
                is False
            )
            assert (
                wrong_release["guarded"]
                is True
            )

            wrong_renew = (
                await client.request(
                    "restart_guard_renew",
                    {
                        "token": "wrong-token",
                    },
                )
            )

            assert (
                wrong_renew["renewed"]
                is False
            )
            assert (
                wrong_renew["guarded"]
                is True
            )

            renewed = await client.request(
                "restart_guard_renew",
                {
                    "token": token,
                },
            )

            assert renewed["renewed"] is True
            assert renewed["guarded"] is True

            released = await client.request(
                "restart_guard_release",
                {
                    "token": token,
                },
            )

            assert released == {
                "released": True,
                "guarded": False,
            }

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())


def test_restart_guard_lease_expiry_resumes_execution(
    tmp_path,
):
    async def run():
        (
            store,
            broker,
            engine,
            server,
            _socket,
        ) = make_stack(tmp_path)

        engine.restart_guard_lease_seconds = (
            0.05
        )

        broker.block("lease-expiry")

        await server.start()

        try:
            query = await engine.submit(
                input_text="lease-expiry",
                cwd="/tmp",
                detached=True,
                stdout_attached=False,
            )

            (
                acquired,
                token,
                running,
            ) = (
                engine
                .try_acquire_restart_guard()
            )

            assert acquired is True
            assert token is not None
            assert running == []

            await asyncio.sleep(0)

            current = store.get_query(
                query.query_id
            )

            assert current is not None
            assert (
                current.state
                == QueryState.QUEUED
            )

            await asyncio.wait_for(
                broker.started[
                    "lease-expiry"
                ].wait(),
                timeout=1,
            )

            assert (
                engine.restart_guarded
                is False
            )

            current = store.get_query(
                query.query_id
            )

            assert current is not None
            assert (
                current.state
                == QueryState.RUNNING
            )

            broker.release[
                "lease-expiry"
            ].set()

            final = await engine.wait(
                query.query_id
            )

            assert (
                final.state
                == QueryState.COMPLETED
            )

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())


def test_sessions_report_multiple_backend_bindings(
    tmp_path,
):
    async def run():
        (
            store,
            broker,
            engine,
            server,
            socket,
        ) = make_stack(tmp_path)

        store.create_session(
            session_name="multi",
            model="gpt-5.6-luna",
            reasoning_effort="xhigh",
            tools_policy=ToolsPolicy.ENABLED,
        )

        store.bind_session_backend(
            "multi",
            "codex",
            "thread-codex",
        )
        store.bind_session_backend(
            "multi",
            "web",
            "thread-web",
        )

        await server.start()

        try:
            client = IPCClient(socket)

            result = await client.request(
                "sessions"
            )

            session = result["sessions"][0]

            assert (
                session["session_name"]
                == "multi"
            )

            assert (
                session["backend_thread_id"]
                is None
            )

            assert (
                session["backend_bindings"]
                == {
                    "codex": "thread-codex",
                    "web": "thread-web",
                }
            )

        finally:
            await server.aclose()
            await engine.aclose()
            store.close()

    asyncio.run(run())
