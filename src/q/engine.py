from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import (
    AsyncIterator,
    Callable,
)
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .broker import (
    Broker,
    BrokerHTTPError,
    BrokerProtocolError,
)
from .logs import append_log_line
from .models import (
    QueryRecord,
    QueryState,
    SessionState,
    ToolsPolicy,
)
from .store import QStore


_TERMINAL_STATES = {
    QueryState.COMPLETED,
    QueryState.FAILED,
    QueryState.INDETERMINATE,
    QueryState.CANCELLED,
}


def _timestamp() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def _detached_interaction_result(
    method: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    if method in {
        "item/commandExecution/requestApproval",
        "item/fileChange/requestApproval",
    }:
        available = params.get(
            "availableDecisions"
        )

        if not isinstance(
            available,
            list,
        ) or not available:
            available = [
                "accept",
                "acceptForSession",
                "decline",
                "cancel",
            ]

        for decision in (
            "decline",
            "cancel",
        ):
            if decision in available:
                return {
                    "decision": decision,
                }

        raise BrokerProtocolError(
            "approval request has no safe "
            "non-accept decision"
        )

    if method == "mcpServer/elicitation/request":
        return {
            "action": "decline",
        }

    if method == "item/permissions/requestApproval":
        return {
            "permissions": {},
            "scope": "turn",
        }

    if method == "item/tool/requestUserInput":
        answers: dict[
            str,
            dict[str, list[str]],
        ] = {}

        questions = params.get(
            "questions",
            [],
        )

        if isinstance(
            questions,
            list,
        ):
            for question in questions:
                if not isinstance(
                    question,
                    dict,
                ):
                    continue

                question_id = question.get(
                    "id"
                )

                if isinstance(
                    question_id,
                    str,
                ) and question_id:
                    answers[
                        question_id
                    ] = {
                        "answers": [],
                    }

        return {
            "answers": answers,
        }

    raise BrokerProtocolError(
        "unsupported interactive "
        f"request method {method!r}"
    )


@dataclass(slots=True)
class _Runtime:
    condition: asyncio.Condition = field(
        default_factory=asyncio.Condition
    )
    events: list[dict[str, Any]] = field(
        default_factory=list
    )
    done: bool = False
    interrupt_sent: bool = False
    pending_interactions: dict[
        str,
        str,
    ] = field(
        default_factory=dict
    )


class QueryEngine:
    def __init__(
        self,
        *,
        store: QStore,
        broker: Broker,
        state_dir: Path,
        query_id_factory: Callable[[], str]
        | None = None,
        default_model: str = "gpt-5.6-luna",
        default_reasoning_effort: str = "xhigh",
        default_tools_policy: ToolsPolicy = (
            ToolsPolicy.ENABLED
        ),
        log_rotate_bytes: int = (
            16 * 1024 * 1024
        ),
        log_rotate_archives: int = 5,
    ):
        self.store = store
        self.broker = broker
        self.state_dir = (
            state_dir.expanduser()
        )

        self.default_model = default_model
        self.default_reasoning_effort = (
            default_reasoning_effort
        )
        self.default_tools_policy = (
            default_tools_policy
        )
        self.log_rotate_bytes = (
            log_rotate_bytes
        )
        self.log_rotate_archives = (
            log_rotate_archives
        )

        self._query_id_factory = (
            query_id_factory
            or (
                lambda: (
                    "q-"
                    + uuid.uuid4().hex
                )
            )
        )

        self._tasks: dict[
            str,
            asyncio.Task[None],
        ] = {}

        self._runtimes: dict[
            str,
            _Runtime,
        ] = {}

        self._session_locks: dict[
            str,
            asyncio.Lock,
        ] = {}

        self._closing = False

        self.restart_guard_lease_seconds = 300.0
        self._restart_guard_token: str | None = None
        self._restart_guard_expiry_task: (
            asyncio.Task[None] | None
        ) = None

        self._execution_allowed = asyncio.Event()
        self._execution_allowed.set()

    async def submit(
        self,
        *,
        input_text: str,
        cwd: str,
        session_name: str | None = None,
        model: str | None = None,
        backend: str | None = None,
        reasoning_effort: str | None = None,
        tools_policy: ToolsPolicy | None = None,
        sandbox: str = "read-only",
        detached: bool = False,
        stdout_attached: bool = True,
    ) -> QueryRecord:
        if self._closing:
            raise RuntimeError(
                "query engine is closing"
            )

        if self.restart_guarded:
            raise RuntimeError(
                "query submissions are disabled "
                "while restart guard is active"
            )

        if not input_text:
            raise ValueError(
                "input_text must not be empty"
            )

        session_log_path = None

        effective_model = (
            model
            or self.default_model
        )
        effective_reasoning_effort = (
            reasoning_effort
            or self.default_reasoning_effort
        )
        effective_tools_policy = (
            tools_policy
            or self.default_tools_policy
        )

        if session_name is not None:
            session = self.store.get_session(
                session_name
            )

            if session is None:
                session = (
                    self.store.create_session(
                        session_name=(
                            session_name
                        ),
                        model=effective_model,
                        reasoning_effort=(
                            effective_reasoning_effort
                        ),
                        tools_policy=(
                            effective_tools_policy
                        ),
                    )
                )

            elif session.state in {
                SessionState.ARCHIVED,
                SessionState.BROKEN,
            }:
                raise RuntimeError(
                    "session is not available: "
                    f"{session.state.value}"
                )

            else:
                if model is None:
                    effective_model = (
                        session.model
                    )

                if reasoning_effort is None:
                    effective_reasoning_effort = (
                        session.reasoning_effort
                    )

                if (
                    tools_policy is not None
                    and session.tools_policy
                    != tools_policy
                ):
                    raise ValueError(
                        "tools policy is fixed for "
                        "an existing session"
                    )

                effective_tools_policy = (
                    session.tools_policy
                )

            session_log_path = str(
                self.state_dir
                / "sessions"
                / f"{session_name}.ndjson"
            )

        query_id = (
            self._query_id_factory()
        )

        log_path = str(
            self.state_dir
            / "queries"
            / f"{query_id}.ndjson"
        )

        query = self.store.create_query(
            query_id=query_id,
            session_name=session_name,
            model=effective_model,
            reasoning_effort=(
                effective_reasoning_effort
            ),
            tools_policy=(
                effective_tools_policy
            ),
            prompt_metadata={
                "input": input_text,
                "cwd": cwd,
                "sandbox": sandbox,
                "backend": backend,
            },
            log_path=log_path,
            session_log_path=(
                session_log_path
            ),
            detached=detached,
            stdout_attached=(
                stdout_attached
            ),
        )

        self._schedule(
            query_id
        )

        return query

    def has_runtime(
        self,
        query_id: str,
    ) -> bool:
        return query_id in self._runtimes

    @property
    def restart_guarded(self) -> bool:
        return (
            self._restart_guard_token
            is not None
        )

    def try_acquire_restart_guard(
        self,
    ) -> tuple[
        bool,
        str | None,
        list[QueryRecord],
    ]:
        if self._closing:
            raise RuntimeError(
                "query engine is closing"
            )

        if self.restart_guarded:
            return False, None, []

        running = self.store.list_queries(
            state=QueryState.RUNNING
        )

        if running:
            return False, None, running

        token = uuid.uuid4().hex

        self._restart_guard_token = token
        self._execution_allowed.clear()
        self._arm_restart_guard_expiry(
            token
        )

        return True, token, []

    def renew_restart_guard(
        self,
        token: str,
    ) -> bool:
        if (
            not self.restart_guarded
            or token
            != self._restart_guard_token
        ):
            return False

        self._arm_restart_guard_expiry(
            token
        )

        return True

    def release_restart_guard(
        self,
        token: str,
    ) -> bool:
        if (
            not self.restart_guarded
            or token
            != self._restart_guard_token
        ):
            return False

        self._clear_restart_guard(
            cancel_expiry=True
        )

        return True

    def _arm_restart_guard_expiry(
        self,
        token: str,
    ) -> None:
        previous = (
            self._restart_guard_expiry_task
        )

        if previous is not None:
            previous.cancel()

        self._restart_guard_expiry_task = (
            asyncio.create_task(
                self._expire_restart_guard(
                    token
                ),
                name="q:restart-guard-expiry",
            )
        )

    async def _expire_restart_guard(
        self,
        token: str,
    ) -> None:
        try:
            await asyncio.sleep(
                self.restart_guard_lease_seconds
            )

        except asyncio.CancelledError:
            return

        if (
            self._restart_guard_token
            == token
        ):
            self._clear_restart_guard(
                cancel_expiry=False
            )

    def _clear_restart_guard(
        self,
        *,
        cancel_expiry: bool,
    ) -> None:
        task = (
            self._restart_guard_expiry_task
        )

        self._restart_guard_expiry_task = None
        self._restart_guard_token = None
        self._execution_allowed.set()

        if (
            cancel_expiry
            and task is not None
        ):
            task.cancel()

    def _schedule(
        self,
        query_id: str,
    ) -> None:
        if query_id in self._tasks:
            return

        runtime = _Runtime()

        self._runtimes[
            query_id
        ] = runtime

        task = asyncio.create_task(
            self._run(
                query_id=query_id,
            ),
            name=f"q:{query_id}",
        )

        self._tasks[
            query_id
        ] = task

        task.add_done_callback(
            lambda _task, qid=query_id: (
                self._tasks.pop(
                    qid,
                    None,
                )
            )
        )

    async def recover(self) -> None:
        # A RUNNING record means qd had already
        # crossed the local execution boundary.
        # We cannot prove whether the upstream
        # turn completed, so never replay it.
        for query in self.store.list_queries(
            state=QueryState.RUNNING,
        ):
            self.store.transition_query(
                query.query_id,
                QueryState.INDETERMINATE,
                error_type="daemon_restart",
                error_message=(
                    "qd restarted while query "
                    "was running"
                ),
            )

            if query.session_name is not None:
                self.store.mark_session_broken(
                    query.session_name
                )

            await self._emit(
                query.query_id,
                {
                    "type": "state",
                    "state": (
                        QueryState
                        .INDETERMINATE
                        .value
                    ),
                    "error_type": (
                        "daemon_restart"
                    ),
                    "error_message": (
                        "qd restarted while query "
                        "was running"
                    ),
                },
            )

        # QUEUED means execution never crossed
        # into RUNNING, so no upstream turn was
        # started by this query. It is safe to
        # put back into the scheduler.
        for query in self.store.list_queries(
            state=QueryState.QUEUED,
        ):
            self._schedule(
                query.query_id
            )

    async def wait(
        self,
        query_id: str,
    ) -> QueryRecord:
        runtime = self._runtimes.get(
            query_id
        )

        if runtime is not None:
            async with runtime.condition:
                await runtime.condition.wait_for(
                    lambda: runtime.done
                )

        query = self.store.get_query(
            query_id
        )

        if query is None:
            raise KeyError(
                query_id
            )

        return query

    async def watch(
        self,
        query_id: str,
    ) -> AsyncIterator[dict[str, Any]]:
        runtime = self._runtimes.get(
            query_id
        )

        if runtime is None:
            raise KeyError(
                query_id
            )

        index = 0

        while True:
            async with runtime.condition:
                await runtime.condition.wait_for(
                    lambda: (
                        index
                        < len(runtime.events)
                        or runtime.done
                    )
                )

                batch = list(
                    runtime.events[
                        index:
                    ]
                )
                index = len(
                    runtime.events
                )
                done = runtime.done

            for event in batch:
                yield event

            if done and (
                index
                >= len(runtime.events)
            ):
                return

    async def respond_interaction(
        self,
        query_id: str,
        interaction_id: str,
        result: dict[str, Any],
    ) -> None:
        query = self.store.get_query(
            query_id
        )

        if query is None:
            raise KeyError(
                query_id
            )

        if query.state != QueryState.RUNNING:
            raise RuntimeError(
                "query is not running"
            )

        runtime = self._runtimes.get(
            query_id
        )

        if runtime is None:
            raise RuntimeError(
                "query has no active runtime"
            )

        method = (
            runtime.pending_interactions.pop(
                interaction_id,
                None,
            )
        )

        if method is None:
            raise KeyError(
                interaction_id
            )

        try:
            await self.broker.respond_interaction(
                interaction_id=interaction_id,
                result=result,
            )

        except Exception:
            runtime.pending_interactions[
                interaction_id
            ] = method
            raise

        await self._emit(
            query_id,
            {
                "type": (
                    "interaction_response"
                ),
                "interaction_id": (
                    interaction_id
                ),
                "method": method,
                "result": result,
            },
        )

    async def cancel(
        self,
        query_id: str,
    ) -> QueryRecord:
        query = self.store.get_query(
            query_id
        )

        if query is None:
            raise KeyError(
                query_id
            )

        if query.state in _TERMINAL_STATES:
            return query

        query = (
            self.store.mark_cancel_requested(
                query_id
            )
        )

        runtime = self._runtimes.get(
            query_id
        )

        if query.state == QueryState.QUEUED:
            query = (
                self.store.transition_query(
                    query_id,
                    QueryState.CANCELLED,
                )
            )

            await self._emit(
                query_id,
                {
                    "type": "state",
                    "state": (
                        QueryState.CANCELLED.value
                    ),
                },
            )

            task = self._tasks.get(
                query_id
            )

            if task is not None:
                task.cancel()

            return query

        if (
            query.state
            == QueryState.RUNNING
            and query.backend_thread_id
            is not None
            and query.upstream_turn_id
            is not None
            and runtime is not None
            and not runtime.interrupt_sent
        ):
            await self.broker.interrupt(
                thread_id=(
                    query.backend_thread_id
                ),
                turn_id=(
                    query.upstream_turn_id
                ),
            )

            runtime.interrupt_sent = True

        updated = self.store.get_query(
            query_id
        )

        assert updated is not None
        return updated

    async def aclose(self) -> None:
        self._closing = True

        guard_task = (
            self._restart_guard_expiry_task
        )

        self._restart_guard_expiry_task = None
        self._restart_guard_token = None
        self._execution_allowed.set()

        if guard_task is not None:
            guard_task.cancel()
            await asyncio.gather(
                guard_task,
                return_exceptions=True,
            )

        tasks = list(
            self._tasks.values()
        )

        for task in tasks:
            task.cancel()

        if tasks:
            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

    async def _run(
        self,
        *,
        query_id: str,
    ) -> None:
        query = self.store.get_query(
            query_id
        )

        assert query is not None

        lock = None

        if query.session_name is not None:
            lock = (
                self._session_locks
                .setdefault(
                    query.session_name,
                    asyncio.Lock(),
                )
            )

        try:
            if lock is None:
                await self._execute(
                    query_id
                )

            else:
                async with lock:
                    current = (
                        self.store.get_query(
                            query_id
                        )
                    )

                    if (
                        current is not None
                        and current.state
                        == QueryState.QUEUED
                    ):
                        await self._execute(
                            query_id
                        )

        except asyncio.CancelledError:
            current = self.store.get_query(
                query_id
            )

            if (
                current is not None
                and current.state
                == QueryState.RUNNING
            ):
                await self._mark_indeterminate(
                    query_id,
                    error_type=(
                        "daemon_shutdown"
                    ),
                    error_message=(
                        "query execution was "
                        "interrupted locally"
                    ),
                )

            raise

        finally:
            await self._finish_runtime(
                query_id
            )

    async def _execute(
        self,
        query_id: str,
    ) -> None:
        # The restart guard closes the execution
        # boundary before QUEUED becomes RUNNING.
        # Event.wait() returns without suspension
        # when execution is allowed, so the check
        # and state transition remain atomic on
        # this event loop.
        await self._execution_allowed.wait()

        query = self.store.get_query(
            query_id
        )

        assert query is not None

        if query.state != QueryState.QUEUED:
            return

        query = self.store.transition_query(
            query_id,
            QueryState.RUNNING,
        )

        await self._emit(
            query_id,
            {
                "type": "state",
                "state": (
                    QueryState.RUNNING.value
                ),
            },
        )

        metadata = query.prompt_metadata
        session = None
        thread_id = None
        selected_backend = metadata.get(
            "backend"
        )

        if query.session_name is not None:
            session = self.store.get_session(
                query.session_name
            )

            if session is None:
                await self._fail(
                    query_id,
                    error_type=(
                        "session_missing"
                    ),
                    error_message=(
                        "session disappeared "
                        "before execution"
                    ),
                )
                return

            if session.state in {
                SessionState.ARCHIVED,
                SessionState.BROKEN,
            }:
                await self._fail(
                    query_id,
                    error_type=(
                        "session_unavailable"
                    ),
                    error_message=(
                        "session is "
                        f"{session.state.value}"
                    ),
                )
                return

            bindings = (
                self.store.list_session_backends(
                    query.session_name
                )
            )

            if selected_backend is not None:
                binding = (
                    self.store.get_session_backend(
                        query.session_name,
                        selected_backend,
                    )
                )

                if binding is not None:
                    thread_id = (
                        binding.conversation_id
                    )

            elif len(bindings) == 1:
                binding = bindings[0]
                selected_backend = (
                    binding.backend_name
                )
                thread_id = (
                    binding.conversation_id
                )

            elif len(bindings) > 1:
                await self._fail(
                    query_id,
                    error_type=(
                        "session_backend_ambiguous"
                    ),
                    error_message=(
                        "session has multiple "
                        "backend conversations; "
                        "select a backend"
                    ),
                )
                return

            if thread_id is not None:
                query = (
                    self.store.set_query_backend(
                        query_id,
                        backend_thread_id=(
                            thread_id
                        ),
                    )
                )

        ephemeral = (
            query.session_name is None
        )

        tools_for_request = (
            query.tools_policy
            if thread_id is None
            else None
        )

        terminal_seen = False

        try:
            async for event in (
                self.broker.stream_query(
                    input_text=str(
                        metadata["input"]
                    ),
                    cwd=str(
                        metadata["cwd"]
                    ),
                    model=query.model,
                    reasoning_effort=(
                        query.reasoning_effort
                    ),
                    backend=selected_backend,
                    thread_id=thread_id,
                    tools_policy=(
                        tools_for_request
                    ),
                    ephemeral=ephemeral,
                    sandbox=str(
                        metadata["sandbox"]
                    ),
                )
            ):
                event_type = event.get(
                    "type"
                )

                if event_type == "thread":
                    await self._handle_thread(
                        query_id,
                        event,
                        expected_thread_id=(
                            thread_id
                        ),
                        expected_backend=(
                            selected_backend
                        ),
                    )

                elif event_type == "turn":
                    await self._handle_turn(
                        query_id,
                        event,
                    )

                elif event_type == "server_request":
                    await self._handle_interaction(
                        query_id,
                        event,
                    )

                await self._emit(
                    query_id,
                    {
                        "type": "broker",
                        "event": event,
                    },
                )

                if event_type == "delta":
                    await self._emit(
                        query_id,
                        {
                            "type": "delta",
                            "delta": event.get(
                                "delta",
                                "",
                            ),
                        },
                    )

                if event_type == "completed":
                    terminal_seen = True
                    await self._complete(
                        query_id,
                        event,
                    )
                    break

            if not terminal_seen:
                await self._mark_indeterminate(
                    query_id,
                    error_type=(
                        "stream_ended"
                    ),
                    error_message=(
                        "broker stream ended "
                        "without completion"
                    ),
                )

        except BrokerHTTPError as exc:
            if exc.status_code >= 500:
                await self._mark_indeterminate(
                    query_id,
                    error_type=exc.code,
                    error_message=(
                        exc.message
                    ),
                )
            else:
                await self._fail(
                    query_id,
                    error_type=exc.code,
                    error_message=(
                        exc.message
                    ),
                )

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            await self._mark_indeterminate(
                query_id,
                error_type=(
                    type(exc).__name__
                ),
                error_message=str(exc),
            )

    async def _handle_thread(
        self,
        query_id: str,
        event: dict[str, Any],
        *,
        expected_thread_id: (
            str | None
        ),
        expected_backend: (
            str | None
        ),
    ) -> None:
        thread_id = event.get(
            "thread_id"
        )

        if (
            not isinstance(
                thread_id,
                str,
            )
            or not thread_id
        ):
            raise BrokerProtocolError(
                "thread event missing "
                "thread_id"
            )

        backend = event.get(
            "backend"
        )

        if (
            not isinstance(backend, str)
            or not backend
        ):
            raise BrokerProtocolError(
                "thread event missing backend"
            )

        if (
            expected_thread_id
            is not None
            and thread_id
            != expected_thread_id
        ):
            raise BrokerProtocolError(
                "broker resumed unexpected "
                "thread"
            )

        if (
            expected_backend
            is not None
            and backend
            != expected_backend
        ):
            raise BrokerProtocolError(
                "broker selected unexpected "
                "backend"
            )

        query = self.store.set_query_backend(
            query_id,
            backend_name=backend,
            backend_thread_id=thread_id,
        )

        if query.session_name is None:
            return

        session = self.store.get_session(
            query.session_name
        )

        if session is None:
            raise BrokerProtocolError(
                "query session disappeared"
            )

        binding = (
            self.store.get_session_backend(
                query.session_name,
                backend,
            )
        )

        if binding is None:
            self.store.bind_session_backend(
                query.session_name,
                backend,
                thread_id,
            )

        elif (
            binding.conversation_id
            != thread_id
        ):
            raise BrokerProtocolError(
                "session backend conversation "
                "changed"
            )

    async def _handle_interaction(
        self,
        query_id: str,
        event: dict[str, Any],
    ) -> None:
        interaction_id = event.get(
            "interaction_id"
        )
        method = event.get(
            "method"
        )
        params = event.get(
            "params",
            {},
        )

        if (
            not isinstance(
                interaction_id,
                str,
            )
            or not interaction_id
        ):
            raise BrokerProtocolError(
                "server_request missing "
                "interaction_id"
            )

        if (
            not isinstance(method, str)
            or not method
        ):
            raise BrokerProtocolError(
                "server_request missing method"
            )

        if not isinstance(params, dict):
            raise BrokerProtocolError(
                "server_request params must "
                "be an object"
            )

        query = self.store.get_query(
            query_id
        )

        if query is None:
            raise KeyError(
                query_id
            )

        if query.detached:
            result = (
                _detached_interaction_result(
                    method,
                    params,
                )
            )

            await self.broker.respond_interaction(
                interaction_id=(
                    interaction_id
                ),
                result=result,
            )

            await self._emit(
                query_id,
                {
                    "type": (
                        "interaction_auto_response"
                    ),
                    "interaction_id": (
                        interaction_id
                    ),
                    "method": method,
                    "result": result,
                },
            )

            return

        runtime = self._runtimes[
            query_id
        ]

        if (
            interaction_id
            in runtime.pending_interactions
        ):
            raise BrokerProtocolError(
                "duplicate interaction_id"
            )

        runtime.pending_interactions[
            interaction_id
        ] = method

        await self._emit(
            query_id,
            {
                "type": "interaction",
                "interaction_id": (
                    interaction_id
                ),
                "method": method,
                "params": params,
            },
        )

    async def _handle_turn(
        self,
        query_id: str,
        event: dict[str, Any],
    ) -> None:
        turn_id = event.get(
            "turn_id"
        )

        if (
            not isinstance(
                turn_id,
                str,
            )
            or not turn_id
        ):
            raise BrokerProtocolError(
                "turn event missing turn_id"
            )

        query = self.store.set_query_backend(
            query_id,
            upstream_turn_id=turn_id,
        )

        runtime = self._runtimes[
            query_id
        ]

        if (
            query.cancel_requested_at
            is not None
            and query.backend_thread_id
            is not None
            and not runtime.interrupt_sent
        ):
            await self.broker.interrupt(
                thread_id=(
                    query.backend_thread_id
                ),
                turn_id=turn_id,
            )

            runtime.interrupt_sent = True

    async def _complete(
        self,
        query_id: str,
        event: dict[str, Any],
    ) -> None:
        status = str(
            event.get(
                "status",
                "failed",
            )
        )

        if status == "completed":
            state = QueryState.COMPLETED
            error_type = None
            error_message = None

        elif status in {
            "cancelled",
            "canceled",
            "interrupted",
        }:
            state = QueryState.CANCELLED
            error_type = None
            error_message = None

        else:
            state = QueryState.FAILED
            error_type = (
                f"upstream_{status}"
            )

            raw_error = event.get(
                "error"
            )

            if raw_error is None:
                error_message = status
            elif isinstance(
                raw_error,
                str,
            ):
                error_message = raw_error
            else:
                error_message = (
                    json.dumps(
                        raw_error,
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                )

        current = self.store.get_query(
            query_id
        )

        if (
            current is None
            or current.state
            in _TERMINAL_STATES
        ):
            return

        self.store.transition_query(
            query_id,
            state,
            error_type=error_type,
            error_message=(
                error_message
            ),
        )

        await self._emit(
            query_id,
            {
                "type": "state",
                "state": state.value,
            },
        )

    async def _fail(
        self,
        query_id: str,
        *,
        error_type: str,
        error_message: str,
    ) -> None:
        current = self.store.get_query(
            query_id
        )

        if (
            current is None
            or current.state
            in _TERMINAL_STATES
        ):
            return

        self.store.transition_query(
            query_id,
            QueryState.FAILED,
            error_type=error_type,
            error_message=(
                error_message
            ),
        )

        await self._emit(
            query_id,
            {
                "type": "state",
                "state": (
                    QueryState.FAILED.value
                ),
                "error_type": error_type,
                "error_message": (
                    error_message
                ),
            },
        )

    async def _mark_indeterminate(
        self,
        query_id: str,
        *,
        error_type: str,
        error_message: str,
    ) -> None:
        current = self.store.get_query(
            query_id
        )

        if (
            current is None
            or current.state
            in _TERMINAL_STATES
        ):
            return

        self.store.transition_query(
            query_id,
            QueryState.INDETERMINATE,
            error_type=error_type,
            error_message=(
                error_message
            ),
        )

        if current.session_name is not None:
            self.store.mark_session_broken(
                current.session_name
            )

        await self._emit(
            query_id,
            {
                "type": "state",
                "state": (
                    QueryState.INDETERMINATE.value
                ),
                "error_type": error_type,
                "error_message": (
                    error_message
                ),
            },
        )

    async def _emit(
        self,
        query_id: str,
        event: dict[str, Any],
    ) -> None:
        query = self.store.get_query(
            query_id
        )

        if query is None:
            return

        record = {
            "timestamp": _timestamp(),
            "query_id": query_id,
            **event,
        }

        encoded = (
            json.dumps(
                record,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        )

        append_log_line(
            self.state_dir
            / "q.ndjson",
            encoded,
            rotate_bytes=(
                self.log_rotate_bytes
            ),
            archives=(
                self.log_rotate_archives
            ),
        )

        # Per-query logs are durable records for
        # one query and are never rotated.
        append_log_line(
            Path(query.log_path),
            encoded,
        )

        if (
            query.session_log_path
            is not None
        ):
            append_log_line(
                Path(
                    query.session_log_path
                ),
                encoded,
                rotate_bytes=(
                    self.log_rotate_bytes
                ),
                archives=(
                    self.log_rotate_archives
                ),
            )

        self.store.mark_stream_activity(
            query_id
        )

        runtime = self._runtimes.get(
            query_id
        )

        if runtime is None:
            return

        async with runtime.condition:
            runtime.events.append(
                record
            )
            runtime.condition.notify_all()

    async def _finish_runtime(
        self,
        query_id: str,
    ) -> None:
        runtime = self._runtimes.get(
            query_id
        )

        if runtime is None:
            return

        async with runtime.condition:
            runtime.done = True
            runtime.condition.notify_all()

        # Existing watchers retain their local
        # reference to runtime. New followers use
        # the durable NDJSON log instead.
        self._runtimes.pop(
            query_id,
            None,
        )
