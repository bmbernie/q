import asyncio

from q.engine import QueryEngine
from q.models import (
    QueryState,
    SessionState,
    ToolsPolicy,
)
from q.store import QStore


class FakeBroker:
    def __init__(self):
        self.calls = []
        self.started = {}
        self.release = {}
        self.interrupts = []
        self.interrupted = set()
        self.fail_after_turn = set()

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
        thread_id,
        tools_policy,
        ephemeral,
        sandbox,
    ):
        return self._stream_query(
            input_text=input_text,
            cwd=cwd,
            model=model,
            reasoning_effort=(
                reasoning_effort
            ),
            thread_id=thread_id,
            tools_policy=tools_policy,
            ephemeral=ephemeral,
            sandbox=sandbox,
        )

    async def _stream_query(
        self,
        **kwargs,
    ):
        prompt = kwargs[
            "input_text"
        ]

        call = dict(kwargs)
        self.calls.append(call)

        thread_id = (
            kwargs["thread_id"]
            or f"thread-{prompt}"
        )
        turn_id = f"turn-{prompt}"

        event = self.started.get(
            prompt
        )

        if event is not None:
            event.set()

        yield {
            "type": "thread",
            "thread_id": thread_id,
        }

        yield {
            "type": "turn",
            "thread_id": thread_id,
            "turn_id": turn_id,
        }

        if prompt in self.fail_after_turn:
            raise RuntimeError(
                "simulated stream failure"
            )

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
                "status": "interrupted",
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


def make_engine(tmp_path):
    store = QStore(
        tmp_path / "q.sqlite3"
    )

    broker = FakeBroker()

    counter = iter(
        (
            f"q-{number}"
            for number in range(
                1,
                100,
            )
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
    )

    return (
        store,
        broker,
        engine,
    )


def test_same_session_serializes(
    tmp_path,
):
    async def run():
        store, broker, engine = (
            make_engine(tmp_path)
        )

        broker.block("first")

        first = await engine.submit(
            input_text="first",
            cwd="/tmp",
            session_name="alpha",
        )

        await broker.started[
            "first"
        ].wait()

        second = await engine.submit(
            input_text="second",
            cwd="/tmp",
            session_name="alpha",
        )

        await asyncio.sleep(0.02)

        assert [
            call["input_text"]
            for call in broker.calls
        ] == ["first"]

        queued = store.get_query(
            second.query_id
        )

        assert queued is not None
        assert (
            queued.state
            == QueryState.QUEUED
        )

        broker.release[
            "first"
        ].set()

        first_done = await engine.wait(
            first.query_id
        )
        second_done = await engine.wait(
            second.query_id
        )

        assert (
            first_done.state
            == QueryState.COMPLETED
        )
        assert (
            second_done.state
            == QueryState.COMPLETED
        )

        assert [
            call["input_text"]
            for call in broker.calls
        ] == [
            "first",
            "second",
        ]

        await engine.aclose()
        store.close()

    asyncio.run(run())


def test_different_sessions_run_concurrently(
    tmp_path,
):
    async def run():
        store, broker, engine = (
            make_engine(tmp_path)
        )

        broker.block("alpha")
        broker.block("beta")

        alpha = await engine.submit(
            input_text="alpha",
            cwd="/tmp",
            session_name="a",
        )

        beta = await engine.submit(
            input_text="beta",
            cwd="/tmp",
            session_name="b",
        )

        await asyncio.wait_for(
            broker.started[
                "alpha"
            ].wait(),
            timeout=1,
        )

        await asyncio.wait_for(
            broker.started[
                "beta"
            ].wait(),
            timeout=1,
        )

        assert len(
            broker.calls
        ) == 2

        broker.release[
            "alpha"
        ].set()
        broker.release[
            "beta"
        ].set()

        assert (
            await engine.wait(
                alpha.query_id
            )
        ).state == QueryState.COMPLETED

        assert (
            await engine.wait(
                beta.query_id
            )
        ).state == QueryState.COMPLETED

        await engine.aclose()
        store.close()

    asyncio.run(run())


def test_session_thread_is_bound_and_reused(
    tmp_path,
):
    async def run():
        store, broker, engine = (
            make_engine(tmp_path)
        )

        first = await engine.submit(
            input_text="one",
            cwd="/tmp",
            session_name="persist",
        )

        await engine.wait(
            first.query_id
        )

        session = store.get_session(
            "persist"
        )

        assert session is not None
        assert (
            session.state
            == SessionState.ACTIVE
        )
        assert (
            session.backend_thread_id
            == "thread-one"
        )

        second = await engine.submit(
            input_text="two",
            cwd="/tmp",
            session_name="persist",
        )

        await engine.wait(
            second.query_id
        )

        assert (
            broker.calls[0][
                "thread_id"
            ]
            is None
        )
        assert (
            broker.calls[0][
                "tools_policy"
            ]
            == ToolsPolicy.ENABLED
        )

        assert (
            broker.calls[1][
                "thread_id"
            ]
            == "thread-one"
        )
        assert (
            broker.calls[1][
                "tools_policy"
            ]
            is None
        )

        assert (
            broker.calls[0][
                "ephemeral"
            ]
            is False
        )
        assert (
            broker.calls[1][
                "ephemeral"
            ]
            is False
        )

        await engine.aclose()
        store.close()

    asyncio.run(run())


def test_detached_query_is_owned_by_engine(
    tmp_path,
):
    async def run():
        store, broker, engine = (
            make_engine(tmp_path)
        )

        query = await engine.submit(
            input_text="detached",
            cwd="/tmp",
            detached=True,
            stdout_attached=False,
        )

        done = await engine.wait(
            query.query_id
        )

        assert (
            done.detached
            is True
        )
        assert (
            done.stdout_attached
            is False
        )
        assert (
            done.state
            == QueryState.COMPLETED
        )

        assert (
            (
                tmp_path
                / "state"
                / "queries"
                / (
                    f"{query.query_id}"
                    ".ndjson"
                )
            ).exists()
        )

        await engine.aclose()
        store.close()

    asyncio.run(run())


def test_running_query_can_be_interrupted(
    tmp_path,
):
    async def run():
        store, broker, engine = (
            make_engine(tmp_path)
        )

        broker.block("cancel-me")

        query = await engine.submit(
            input_text="cancel-me",
            cwd="/tmp",
        )

        await broker.started[
            "cancel-me"
        ].wait()

        while True:
            current = store.get_query(
                query.query_id
            )

            assert current is not None

            if (
                current.upstream_turn_id
                is not None
            ):
                break

            await asyncio.sleep(
                0
            )

        await engine.cancel(
            query.query_id
        )

        done = await engine.wait(
            query.query_id
        )

        assert (
            done.state
            == QueryState.CANCELLED
        )

        assert broker.interrupts == [
            (
                "thread-cancel-me",
                "turn-cancel-me",
            )
        ]

        await engine.aclose()
        store.close()

    asyncio.run(run())


def test_uncertain_stream_breaks_session(
    tmp_path,
):
    async def run():
        store, broker, engine = (
            make_engine(tmp_path)
        )

        broker.fail_after_turn.add(
            "uncertain"
        )

        query = await engine.submit(
            input_text="uncertain",
            cwd="/tmp",
            session_name="fragile",
        )

        done = await engine.wait(
            query.query_id
        )

        assert (
            done.state
            == QueryState.INDETERMINATE
        )

        session = store.get_session(
            "fragile"
        )

        assert session is not None
        assert (
            session.state
            == SessionState.BROKEN
        )

        await engine.aclose()
        store.close()

    asyncio.run(run())



def test_recover_resumes_queued_query(
    tmp_path,
):
    async def run():
        store, broker, engine = (
            make_engine(tmp_path)
        )

        store.create_query(
            query_id="q-recovered",
            session_name=None,
            model="gpt-5.6-luna",
            reasoning_effort="xhigh",
            tools_policy=(
                ToolsPolicy.ENABLED
            ),
            prompt_metadata={
                "input": "recovered",
                "cwd": "/tmp",
                "sandbox": "read-only",
            },
            log_path=str(
                tmp_path
                / "state"
                / "queries"
                / "q-recovered.ndjson"
            ),
            session_log_path=None,
            detached=True,
            stdout_attached=False,
        )

        await engine.recover()

        done = await engine.wait(
            "q-recovered"
        )

        assert (
            done.state
            == QueryState.COMPLETED
        )

        assert [
            call["input_text"]
            for call in broker.calls
        ] == ["recovered"]

        await engine.aclose()
        store.close()

    asyncio.run(run())


def test_recover_does_not_replay_running_query(
    tmp_path,
):
    async def run():
        store, broker, engine = (
            make_engine(tmp_path)
        )

        store.create_session(
            session_name="uncertain",
            backend_thread_id=(
                "thread-existing"
            ),
            model="gpt-5.6-luna",
            reasoning_effort="xhigh",
            tools_policy=(
                ToolsPolicy.ENABLED
            ),
        )

        store.create_query(
            query_id="q-running",
            session_name="uncertain",
            model="gpt-5.6-luna",
            reasoning_effort="xhigh",
            tools_policy=(
                ToolsPolicy.ENABLED
            ),
            prompt_metadata={
                "input": "do not replay",
                "cwd": "/tmp",
                "sandbox": "read-only",
            },
            log_path=str(
                tmp_path
                / "state"
                / "queries"
                / "q-running.ndjson"
            ),
            session_log_path=str(
                tmp_path
                / "state"
                / "sessions"
                / "uncertain.ndjson"
            ),
            detached=True,
            stdout_attached=False,
        )

        store.transition_query(
            "q-running",
            QueryState.RUNNING,
        )

        store.set_query_backend(
            "q-running",
            backend_thread_id=(
                "thread-existing"
            ),
            upstream_turn_id=(
                "turn-existing"
            ),
        )

        await engine.recover()

        query = store.get_query(
            "q-running"
        )
        session = store.get_session(
            "uncertain"
        )

        assert query is not None
        assert session is not None

        assert (
            query.state
            == QueryState.INDETERMINATE
        )
        assert (
            query.error_type
            == "daemon_restart"
        )
        assert (
            session.state
            == SessionState.BROKEN
        )

        # Critical invariant: no new broker
        # submission occurred.
        assert broker.calls == []

        await engine.aclose()
        store.close()

    asyncio.run(run())


class InteractiveBroker:
    def __init__(self):
        self.responses = []
        self.response_event = (
            asyncio.Event()
        )
        self.response_result = None

    def stream_query(
        self,
        *,
        input_text,
        cwd,
        model,
        reasoning_effort,
        thread_id,
        tools_policy,
        ephemeral,
        sandbox,
    ):
        return self._stream()

    async def _stream(self):
        yield {
            "type": "thread",
            "thread_id": "thread-i",
        }

        yield {
            "type": "turn",
            "thread_id": "thread-i",
            "turn_id": "turn-i",
        }

        yield {
            "type": "server_request",
            "interaction_id": "interaction-i",
            "thread_id": "thread-i",
            "turn_id": "turn-i",
            "method": (
                "item/fileChange/"
                "requestApproval"
            ),
            "params": {
                "threadId": "thread-i",
                "turnId": "turn-i",
                "itemId": "item-i",
                "startedAtMs": 1,
            },
        }

        await self.response_event.wait()

        yield {
            "type": "completed",
            "thread_id": "thread-i",
            "turn_id": "turn-i",
            "status": "completed",
            "error": None,
        }

    async def respond_interaction(
        self,
        *,
        interaction_id,
        result,
    ):
        self.responses.append(
            (
                interaction_id,
                result,
            )
        )
        self.response_result = result
        self.response_event.set()

    async def interrupt(
        self,
        *,
        thread_id,
        turn_id,
    ):
        pass


def test_attached_interaction_can_be_answered(
    tmp_path,
):
    async def run():
        store = QStore(
            tmp_path / "q.sqlite3"
        )
        broker = InteractiveBroker()

        engine = QueryEngine(
            store=store,
            broker=broker,
            state_dir=(
                tmp_path / "state"
            ),
            query_id_factory=(
                lambda: "q-interactive"
            ),
        )

        query = await engine.submit(
            input_text="interactive",
            cwd="/tmp",
        )

        interaction = None

        async for event in engine.watch(
            query.query_id
        ):
            if (
                event.get("type")
                == "interaction"
            ):
                interaction = event
                break

        assert interaction is not None
        assert (
            interaction[
                "interaction_id"
            ]
            == "interaction-i"
        )

        await engine.respond_interaction(
            query.query_id,
            "interaction-i",
            {
                "decision": "accept",
            },
        )

        done = await engine.wait(
            query.query_id
        )

        assert (
            done.state
            == QueryState.COMPLETED
        )

        assert broker.responses == [
            (
                "interaction-i",
                {
                    "decision": "accept",
                },
            )
        ]

        await engine.aclose()
        store.close()

    asyncio.run(run())


def test_detached_interaction_declines_automatically(
    tmp_path,
):
    async def run():
        store = QStore(
            tmp_path / "q.sqlite3"
        )
        broker = InteractiveBroker()

        engine = QueryEngine(
            store=store,
            broker=broker,
            state_dir=(
                tmp_path / "state"
            ),
            query_id_factory=(
                lambda: "q-detached-i"
            ),
        )

        query = await engine.submit(
            input_text="interactive",
            cwd="/tmp",
            detached=True,
            stdout_attached=False,
        )

        done = await engine.wait(
            query.query_id
        )

        assert (
            done.state
            == QueryState.COMPLETED
        )

        assert broker.responses == [
            (
                "interaction-i",
                {
                    "decision": "decline",
                },
            )
        ]

        await engine.aclose()
        store.close()

    asyncio.run(run())


def test_detached_approval_uses_cancel_when_decline_unavailable():
    from q.engine import (
        _detached_interaction_result,
    )

    result = _detached_interaction_result(
        (
            "item/commandExecution/"
            "requestApproval"
        ),
        {
            "availableDecisions": [
                "accept",
                {
                    "acceptWithExecpolicyAmendment": {
                        "execpolicy_amendment": [
                            "touch",
                            "/tmp/example",
                        ],
                    },
                },
                "cancel",
            ],
        },
    )

    assert result == {
        "decision": "cancel",
    }


def test_emit_rotates_global_and_session_logs_only(
    tmp_path,
):
    async def run():
        store = QStore(
            tmp_path / "q.sqlite3"
        )
        broker = FakeBroker()

        state_dir = (
            tmp_path / "state"
        )
        query_dir = (
            state_dir / "queries"
        )
        session_dir = (
            state_dir / "sessions"
        )

        query_dir.mkdir(
            parents=True
        )
        session_dir.mkdir(
            parents=True
        )

        global_log = (
            state_dir / "q.ndjson"
        )
        session_log = (
            session_dir
            / "rotate.ndjson"
        )
        query_log = (
            query_dir
            / "q-rotate.ndjson"
        )

        global_log.write_text(
            "G" * 30,
            encoding="utf-8",
        )
        session_log.write_text(
            "S" * 30,
            encoding="utf-8",
        )
        query_log.write_text(
            "Q" * 30,
            encoding="utf-8",
        )

        store.create_session(
            session_name="rotate",
            model="gpt-5.6-luna",
            reasoning_effort="xhigh",
            tools_policy=(
                ToolsPolicy.ENABLED
            ),
        )

        store.create_query(
            query_id="q-rotate",
            session_name="rotate",
            model="gpt-5.6-luna",
            reasoning_effort="xhigh",
            tools_policy=(
                ToolsPolicy.ENABLED
            ),
            prompt_metadata={
                "input": "rotation test",
                "cwd": "/tmp",
                "sandbox": "read-only",
            },
            log_path=str(
                query_log
            ),
            session_log_path=str(
                session_log
            ),
            detached=False,
            stdout_attached=True,
        )

        engine = QueryEngine(
            store=store,
            broker=broker,
            state_dir=state_dir,
            log_rotate_bytes=32,
            log_rotate_archives=2,
        )

        await engine._emit(
            "q-rotate",
            {
                "type": "probe",
                "value": "rotation",
            },
        )

        assert (
            state_dir
            / "q.ndjson.1"
        ).read_text(
            encoding="utf-8"
        ) == "G" * 30

        assert (
            session_dir
            / "rotate.ndjson.1"
        ).read_text(
            encoding="utf-8"
        ) == "S" * 30

        assert not (
            query_dir
            / "q-rotate.ndjson.1"
        ).exists()

        global_current = (
            global_log.read_text(
                encoding="utf-8"
            )
        )
        session_current = (
            session_log.read_text(
                encoding="utf-8"
            )
        )
        query_current = (
            query_log.read_text(
                encoding="utf-8"
            )
        )

        assert '"type":"probe"' in (
            global_current
        )
        assert '"type":"probe"' in (
            session_current
        )

        assert query_current.startswith(
            "Q" * 30
        )
        assert '"type":"probe"' in (
            query_current
        )

        await engine.aclose()
        store.close()

    asyncio.run(run())
