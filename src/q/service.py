from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import datetime
from pathlib import Path
from typing import Any

from .broker import BrokerError
from .config import QConfig
from .engine import QueryEngine
from .ipc import (
    IPCFrame,
    IPCRequest,
)
from .models import (
    QueryRecord,
    QueryState,
    SessionRecord,
    ToolsPolicy,
)
from .store import QStore


_SANDBOXES = {
    "read-only",
    "workspace-write",
    "danger-full-access",
}


def _datetime(
    value: datetime | None,
) -> str | None:
    return (
        value.isoformat()
        if value is not None
        else None
    )


def query_dict(
    query: QueryRecord,
    *,
    include_prompt: bool = False,
) -> dict[str, Any]:
    value = {
        "query_id": query.query_id,
        "session_name": (
            query.session_name
        ),
        "backend_thread_id": (
            query.backend_thread_id
        ),
        "upstream_turn_id": (
            query.upstream_turn_id
        ),
        "model": query.model,
        "reasoning_effort": (
            query.reasoning_effort
        ),
        "tools_policy": (
            query.tools_policy.value
        ),
        "state": query.state.value,
        "created_at": _datetime(
            query.created_at
        ),
        "started_at": _datetime(
            query.started_at
        ),
        "completed_at": _datetime(
            query.completed_at
        ),
        "error_type": query.error_type,
        "error_message": (
            query.error_message
        ),
        "retry_after": query.retry_after,
        "log_path": query.log_path,
        "session_log_path": (
            query.session_log_path
        ),
        "detached": query.detached,
        "stdout_attached": (
            query.stdout_attached
        ),
        "cancel_requested_at": (
            _datetime(
                query.cancel_requested_at
            )
        ),
        "last_stream_at": _datetime(
            query.last_stream_at
        ),
    }

    if include_prompt:
        value["prompt_metadata"] = (
            query.prompt_metadata
        )

    return value


def session_dict(
    session: SessionRecord,
) -> dict[str, Any]:
    return {
        "session_name": (
            session.session_name
        ),
        "backend_thread_id": (
            session.backend_thread_id
        ),
        "model": session.model,
        "reasoning_effort": (
            session.reasoning_effort
        ),
        "tools_policy": (
            session.tools_policy.value
        ),
        "state": session.state.value,
        "created_at": _datetime(
            session.created_at
        ),
        "last_used_at": _datetime(
            session.last_used_at
        ),
        "archived_at": _datetime(
            session.archived_at
        ),
    }


def _required_string(
    params: dict[str, Any],
    name: str,
) -> str:
    value = params.get(name)

    if (
        not isinstance(value, str)
        or not value
    ):
        raise ValueError(
            f"{name} must be a "
            "non-empty string"
        )

    return value


def _optional_string(
    params: dict[str, Any],
    name: str,
    *,
    default: str | None = None,
) -> str | None:
    value = params.get(
        name,
        default,
    )

    if value is None:
        return None

    if (
        not isinstance(value, str)
        or not value
    ):
        raise ValueError(
            f"{name} must be a "
            "non-empty string or null"
        )

    return value


def _boolean(
    params: dict[str, Any],
    name: str,
    *,
    default: bool,
) -> bool:
    value = params.get(
        name,
        default,
    )

    if not isinstance(value, bool):
        raise ValueError(
            f"{name} must be boolean"
        )

    return value


class QDService:
    def __init__(
        self,
        *,
        store: QStore,
        engine: QueryEngine,
        config: QConfig | None = None,
    ):
        self.store = store
        self.engine = engine
        self.config = (
            config
            if config is not None
            else QConfig()
        )

    async def handle(
        self,
        request: IPCRequest,
    ) -> AsyncIterator[IPCFrame]:
        try:
            if request.method == "ping":
                yield IPCFrame.result_frame(
                    request.request_id,
                    {
                        "ok": True,
                    },
                )
                return

            if request.method == "submit":
                async for frame in (
                    self._submit(request)
                ):
                    yield frame
                return

            if request.method == "follow":
                async for frame in (
                    self._follow(request)
                ):
                    yield frame
                return

            if request.method == "logs":
                async for frame in (
                    self._logs(request)
                ):
                    yield frame
                return

            if request.method == "status":
                yield self._status(request)
                return

            if request.method == "show":
                yield self._show(request)
                return

            if request.method == "queries":
                yield self._queries(request)
                return

            if request.method == "sessions":
                yield self._sessions(
                    request
                )
                return

            if request.method == "cancel":
                yield await self._cancel(
                    request
                )
                return

            yield IPCFrame.error_frame(
                request.request_id,
                code="method_not_found",
                message=(
                    "unknown qd method "
                    f"{request.method!r}"
                ),
            )

        except ValueError as exc:
            yield IPCFrame.error_frame(
                request.request_id,
                code="invalid_params",
                message=str(exc),
            )

        except KeyError as exc:
            key = (
                exc.args[0]
                if exc.args
                else None
            )

            yield IPCFrame.error_frame(
                request.request_id,
                code="not_found",
                message=(
                    "requested object "
                    "was not found"
                ),
                details={
                    "key": key,
                },
            )

        except BrokerError as exc:
            yield IPCFrame.error_frame(
                request.request_id,
                code="broker_error",
                message=str(exc),
            )

        except RuntimeError as exc:
            yield IPCFrame.error_frame(
                request.request_id,
                code="conflict",
                message=str(exc),
            )

    async def _submit(
        self,
        request: IPCRequest,
    ) -> AsyncIterator[IPCFrame]:
        params = request.params

        input_text = _required_string(
            params,
            "input",
        )

        cwd = _required_string(
            params,
            "cwd",
        )

        session_name = _optional_string(
            params,
            "session_name",
        )

        model = _optional_string(
            params,
            "model",
            default=self.config.model,
        )

        reasoning_effort = (
            _optional_string(
                params,
                "reasoning_effort",
                default=(
                    self.config.reasoning_effort
                ),
            )
        )

        tools_raw = _optional_string(
            params,
            "tools",
            default=None,
        )

        if tools_raw is None:
            tools_policy = None
        else:
            try:
                tools_policy = ToolsPolicy(
                    tools_raw
                )
            except ValueError as exc:
                raise ValueError(
                    "tools must be "
                    "'enabled' or 'disabled'"
                ) from exc

        sandbox = _optional_string(
            params,
            "sandbox",
            default=self.config.sandbox,
        )

        if sandbox not in _SANDBOXES:
            raise ValueError(
                "invalid sandbox"
            )

        detached = _boolean(
            params,
            "detached",
            default=False,
        )

        stdout_attached = _boolean(
            params,
            "stdout_attached",
            default=not detached,
        )

        assert model is not None
        assert reasoning_effort is not None
        assert sandbox is not None

        query = await self.engine.submit(
            input_text=input_text,
            cwd=cwd,
            session_name=session_name,
            model=model,
            reasoning_effort=(
                reasoning_effort
            ),
            tools_policy=tools_policy,
            sandbox=sandbox,
            detached=detached,
            stdout_attached=(
                stdout_attached
            ),
        )

        if detached:
            yield IPCFrame.result_frame(
                request.request_id,
                query_dict(query),
            )
            return

        async for event in (
            self.engine.watch(
                query.query_id
            )
        ):
            event_name = event.get(
                "type",
                "query",
            )

            if not isinstance(
                event_name,
                str,
            ):
                event_name = "query"

            yield IPCFrame.event_frame(
                request.request_id,
                event_name,
                event,
            )

        final = self.store.get_query(
            query.query_id
        )

        if final is None:
            raise KeyError(
                query.query_id
            )

        yield IPCFrame.result_frame(
            request.request_id,
            query_dict(final),
        )

    async def _follow(
        self,
        request: IPCRequest,
    ) -> AsyncIterator[IPCFrame]:
        query_id = _required_string(
            request.params,
            "query_id",
        )

        query = self.store.get_query(
            query_id
        )

        if query is None:
            raise KeyError(
                query_id
            )

        used_runtime = False

        if (
            query.state
            in {
                QueryState.QUEUED,
                QueryState.RUNNING,
            }
            and self.engine.has_runtime(
                query_id
            )
        ):
            used_runtime = True

            async for event in (
                self.engine.watch(
                    query_id
                )
            ):
                event_name = event.get(
                    "type",
                    "query",
                )

                if not isinstance(
                    event_name,
                    str,
                ):
                    event_name = "query"

                yield IPCFrame.event_frame(
                    request.request_id,
                    event_name,
                    event,
                )

        if not used_runtime:
            log_path = Path(
                query.log_path
            )

            try:
                stream = log_path.open(
                    "r",
                    encoding="utf-8",
                )

            except FileNotFoundError:
                stream = None

            if stream is not None:
                with stream:
                    for line in stream:
                        if not line.strip():
                            continue

                        event = json.loads(
                            line
                        )

                        if (
                            event.get(
                                "query_id"
                            )
                            != query_id
                        ):
                            continue

                        event_name = event.get(
                            "type",
                            "query",
                        )

                        if not isinstance(
                            event_name,
                            str,
                        ):
                            event_name = (
                                "query"
                            )

                        yield (
                            IPCFrame
                            .event_frame(
                                request.request_id,
                                event_name,
                                event,
                            )
                        )

        final = self.store.get_query(
            query_id
        )

        if final is None:
            raise KeyError(
                query_id
            )

        yield IPCFrame.result_frame(
            request.request_id,
            query_dict(final),
        )

    async def _logs(
        self,
        request: IPCRequest,
    ) -> AsyncIterator[IPCFrame]:
        session_name = _optional_string(
            request.params,
            "session_name",
        )

        follow = _boolean(
            request.params,
            "follow",
            default=False,
        )

        if session_name is None:
            log_path = (
                self.engine.state_dir
                / "q.ndjson"
            )

        else:
            session = self.store.get_session(
                session_name
            )

            if session is None:
                raise KeyError(
                    session_name
                )

            log_path = (
                self.engine.state_dir
                / "sessions"
                / f"{session_name}.ndjson"
            )

        offset = 0

        while True:
            try:
                with log_path.open(
                    "r",
                    encoding="utf-8",
                ) as stream:
                    stream.seek(offset)

                    while True:
                        line = stream.readline()

                        if not line:
                            break

                        offset = stream.tell()

                        yield (
                            IPCFrame.event_frame(
                                request.request_id,
                                "log",
                                {
                                    "path": str(
                                        log_path
                                    ),
                                    "line": (
                                        line.rstrip(
                                            "\n"
                                        )
                                    ),
                                },
                            )
                        )

            except FileNotFoundError:
                pass

            if not follow:
                break

            await asyncio.sleep(
                0.2
            )

        yield IPCFrame.result_frame(
            request.request_id,
            {
                "path": str(log_path),
                "follow": follow,
            },
        )

    def _status(
        self,
        request: IPCRequest,
    ) -> IPCFrame:
        query_id = _required_string(
            request.params,
            "query_id",
        )

        query = self.store.get_query(
            query_id
        )

        if query is None:
            raise KeyError(
                query_id
            )

        return IPCFrame.result_frame(
            request.request_id,
            query_dict(query),
        )

    def _show(
        self,
        request: IPCRequest,
    ) -> IPCFrame:
        query_id = _required_string(
            request.params,
            "query_id",
        )

        query = self.store.get_query(
            query_id
        )

        if query is None:
            raise KeyError(
                query_id
            )

        return IPCFrame.result_frame(
            request.request_id,
            query_dict(
                query,
                include_prompt=True,
            ),
        )

    def _queries(
        self,
        request: IPCRequest,
    ) -> IPCFrame:
        session_name = _optional_string(
            request.params,
            "session_name",
        )

        running = _boolean(
            request.params,
            "running",
            default=False,
        )

        queries = self.store.list_queries(
            session_name=session_name,
        )

        if running:
            queries = [
                query
                for query in queries
                if query.state
                in {
                    QueryState.QUEUED,
                    QueryState.RUNNING,
                }
            ]

        return IPCFrame.result_frame(
            request.request_id,
            {
                "queries": [
                    query_dict(query)
                    for query in queries
                ],
            },
        )

    def _sessions(
        self,
        request: IPCRequest,
    ) -> IPCFrame:
        return IPCFrame.result_frame(
            request.request_id,
            {
                "sessions": [
                    session_dict(
                        session
                    )
                    for session
                    in (
                        self.store
                        .list_sessions()
                    )
                ],
            },
        )

    async def _cancel(
        self,
        request: IPCRequest,
    ) -> IPCFrame:
        query_id = _required_string(
            request.params,
            "query_id",
        )

        query = await self.engine.cancel(
            query_id
        )

        return IPCFrame.result_frame(
            request.request_id,
            query_dict(query),
        )
