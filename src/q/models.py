from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any


class QueryState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    INDETERMINATE = "indeterminate"
    CANCELLED = "cancelled"


class SessionState(StrEnum):
    PENDING = "pending"
    ACTIVE = "active"
    ARCHIVED = "archived"
    BROKEN = "broken"


class ToolsPolicy(StrEnum):
    ENABLED = "enabled"
    DISABLED = "disabled"


@dataclass(frozen=True, slots=True)
class SessionRecord:
    session_name: str
    backend_thread_id: str | None
    model: str
    reasoning_effort: str
    tools_policy: ToolsPolicy
    state: SessionState
    created_at: datetime
    last_used_at: datetime
    archived_at: datetime | None


@dataclass(frozen=True, slots=True)
class SessionBackendBinding:
    session_name: str
    backend_name: str
    conversation_id: str
    created_at: datetime
    last_used_at: datetime


@dataclass(frozen=True, slots=True)
class QueryRecord:
    query_id: str
    session_name: str | None
    backend_thread_id: str | None
    upstream_turn_id: str | None
    model: str
    reasoning_effort: str
    tools_policy: ToolsPolicy
    state: QueryState
    prompt_metadata: dict[str, Any]
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    error_type: str | None
    error_message: str | None
    retry_after: str | None
    log_path: str
    session_log_path: str | None
    detached: bool
    stdout_attached: bool
    cancel_requested_at: datetime | None
    last_stream_at: datetime | None
