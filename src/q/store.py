from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import (
    QueryRecord,
    QueryState,
    SessionRecord,
    SessionState,
    ToolsPolicy,
)


SCHEMA_VERSION = 2

_SESSION_NAME = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._-]*$"
)

_TERMINAL_QUERY_STATES = {
    QueryState.COMPLETED,
    QueryState.FAILED,
    QueryState.INDETERMINATE,
    QueryState.CANCELLED,
}

_ALLOWED_QUERY_TRANSITIONS = {
    QueryState.QUEUED: {
        QueryState.RUNNING,
        QueryState.FAILED,
        QueryState.CANCELLED,
    },
    QueryState.RUNNING: {
        QueryState.COMPLETED,
        QueryState.FAILED,
        QueryState.INDETERMINATE,
        QueryState.CANCELLED,
    },
    QueryState.COMPLETED: set(),
    QueryState.FAILED: set(),
    QueryState.INDETERMINATE: set(),
    QueryState.CANCELLED: set(),
}


class StateStoreError(RuntimeError):
    pass


class QueryAlreadyExists(StateStoreError):
    pass


class SessionAlreadyExists(StateStoreError):
    pass


class InvalidQueryTransition(StateStoreError):
    pass


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _dt(value: str | None) -> datetime | None:
    if value is None:
        return None

    return datetime.fromisoformat(value)


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None

    return value.astimezone(
        timezone.utc
    ).isoformat()


class QStore:
    def __init__(
        self,
        path: Path,
    ):
        self.path = path.expanduser()
        self.path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._db = sqlite3.connect(
            self.path
        )
        self._db.row_factory = sqlite3.Row

        self._db.execute(
            "PRAGMA foreign_keys = ON"
        )
        self._db.execute(
            "PRAGMA journal_mode = WAL"
        )

        self._initialize()

    def close(self) -> None:
        self._db.close()

    def _initialize(self) -> None:
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                session_name TEXT PRIMARY KEY,
                backend_thread_id TEXT UNIQUE,
                model TEXT NOT NULL,
                reasoning_effort TEXT NOT NULL,
                tools_policy TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_used_at TEXT NOT NULL,
                archived_at TEXT
            );

            CREATE TABLE IF NOT EXISTS queries (
                query_id TEXT PRIMARY KEY,
                session_name TEXT
                    REFERENCES sessions(session_name)
                    ON UPDATE CASCADE
                    ON DELETE SET NULL,
                backend_thread_id TEXT,
                upstream_turn_id TEXT,
                model TEXT NOT NULL,
                reasoning_effort TEXT NOT NULL,
                tools_policy TEXT NOT NULL,
                state TEXT NOT NULL,
                prompt_metadata TEXT NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                error_type TEXT,
                error_message TEXT,
                retry_after TEXT,
                log_path TEXT NOT NULL,
                session_log_path TEXT,
                detached INTEGER NOT NULL,
                stdout_attached INTEGER NOT NULL,
                cancel_requested_at TEXT,
                last_stream_at TEXT
            );

            CREATE INDEX IF NOT EXISTS
                queries_state_idx
            ON queries(state);

            CREATE INDEX IF NOT EXISTS
                queries_session_created_idx
            ON queries(session_name, created_at);
            """
        )

        row = self._db.execute(
            """
            SELECT value
            FROM metadata
            WHERE key = 'schema_version'
            """
        ).fetchone()

        if row is None:
            self._db.execute(
                """
                INSERT INTO metadata(key, value)
                VALUES ('schema_version', ?)
                """,
                (str(SCHEMA_VERSION),),
            )
            self._db.commit()

        elif int(row["value"]) != SCHEMA_VERSION:
            raise StateStoreError(
                "unsupported q database schema "
                f"version {row['value']}"
            )

    @staticmethod
    def _validate_session_name(
        session_name: str,
    ) -> None:
        if not _SESSION_NAME.fullmatch(
            session_name
        ):
            raise ValueError(
                "invalid session name"
            )

    def create_session(
        self,
        *,
        session_name: str,
        backend_thread_id: str | None = None,
        model: str,
        reasoning_effort: str,
        tools_policy: ToolsPolicy,
    ) -> SessionRecord:
        self._validate_session_name(
            session_name
        )

        now = _now()

        state = (
            SessionState.ACTIVE
            if backend_thread_id is not None
            else SessionState.PENDING
        )

        try:
            with self._db:
                self._db.execute(
                    """
                    INSERT INTO sessions (
                        session_name,
                        backend_thread_id,
                        model,
                        reasoning_effort,
                        tools_policy,
                        state,
                        created_at,
                        last_used_at,
                        archived_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
                    """,
                    (
                        session_name,
                        backend_thread_id,
                        model,
                        reasoning_effort,
                        tools_policy.value,
                        state.value,
                        _iso(now),
                        _iso(now),
                    ),
                )

        except sqlite3.IntegrityError as exc:
            raise SessionAlreadyExists(
                session_name
            ) from exc

        session = self.get_session(
            session_name
        )

        assert session is not None
        return session

    def bind_session_thread(
        self,
        session_name: str,
        backend_thread_id: str,
    ) -> SessionRecord:
        if not backend_thread_id:
            raise ValueError(
                "backend_thread_id must not be empty"
            )

        now = _now()

        try:
            with self._db:
                cursor = self._db.execute(
                    """
                    UPDATE sessions
                    SET
                        backend_thread_id = ?,
                        state = ?,
                        last_used_at = ?,
                        archived_at = NULL
                    WHERE session_name = ?
                      AND backend_thread_id IS NULL
                      AND state = ?
                    """,
                    (
                        backend_thread_id,
                        SessionState.ACTIVE.value,
                        _iso(now),
                        session_name,
                        SessionState.PENDING.value,
                    ),
                )

        except sqlite3.IntegrityError as exc:
            raise StateStoreError(
                "backend thread is already "
                "mapped to another session"
            ) from exc

        if cursor.rowcount != 1:
            current = self.get_session(
                session_name
            )

            if current is None:
                raise KeyError(
                    session_name
                )

            raise StateStoreError(
                "session is not pending "
                "or already has a backend thread"
            )

        session = self.get_session(
            session_name
        )

        assert session is not None
        return session

    def get_session(
        self,
        session_name: str,
    ) -> SessionRecord | None:
        row = self._db.execute(
            """
            SELECT *
            FROM sessions
            WHERE session_name = ?
            """,
            (session_name,),
        ).fetchone()

        if row is None:
            return None

        return self._session(row)

    def list_sessions(
        self,
        *,
        state: SessionState | None = None,
    ) -> list[SessionRecord]:
        if state is None:
            rows = self._db.execute(
                """
                SELECT *
                FROM sessions
                ORDER BY session_name
                """
            ).fetchall()

        else:
            rows = self._db.execute(
                """
                SELECT *
                FROM sessions
                WHERE state = ?
                ORDER BY session_name
                """,
                (state.value,),
            ).fetchall()

        return [
            self._session(row)
            for row in rows
        ]

    def touch_session(
        self,
        session_name: str,
    ) -> None:
        with self._db:
            cursor = self._db.execute(
                """
                UPDATE sessions
                SET last_used_at = ?
                WHERE session_name = ?
                """,
                (
                    _iso(_now()),
                    session_name,
                ),
            )

        if cursor.rowcount != 1:
            raise KeyError(
                session_name
            )

    def rename_session(
        self,
        old_name: str,
        new_name: str,
    ) -> SessionRecord:
        self._validate_session_name(
            new_name
        )

        try:
            with self._db:
                cursor = self._db.execute(
                    """
                    UPDATE sessions
                    SET session_name = ?
                    WHERE session_name = ?
                    """,
                    (
                        new_name,
                        old_name,
                    ),
                )

        except sqlite3.IntegrityError as exc:
            raise SessionAlreadyExists(
                new_name
            ) from exc

        if cursor.rowcount != 1:
            raise KeyError(
                old_name
            )

        session = self.get_session(
            new_name
        )

        assert session is not None
        return session

    def archive_session(
        self,
        session_name: str,
    ) -> SessionRecord:
        now = _now()

        with self._db:
            cursor = self._db.execute(
                """
                UPDATE sessions
                SET
                    state = ?,
                    archived_at = ?,
                    last_used_at = ?
                WHERE session_name = ?
                """,
                (
                    SessionState.ARCHIVED.value,
                    _iso(now),
                    _iso(now),
                    session_name,
                ),
            )

        if cursor.rowcount != 1:
            raise KeyError(
                session_name
            )

        session = self.get_session(
            session_name
        )

        assert session is not None
        return session

    def create_query(
        self,
        *,
        query_id: str,
        session_name: str | None,
        model: str,
        reasoning_effort: str,
        tools_policy: ToolsPolicy,
        prompt_metadata: dict[str, Any],
        log_path: str,
        session_log_path: str | None,
        detached: bool,
        stdout_attached: bool,
    ) -> QueryRecord:
        now = _now()

        try:
            with self._db:
                self._db.execute(
                    """
                    INSERT INTO queries (
                        query_id,
                        session_name,
                        backend_thread_id,
                        upstream_turn_id,
                        model,
                        reasoning_effort,
                        tools_policy,
                        state,
                        prompt_metadata,
                        created_at,
                        started_at,
                        completed_at,
                        error_type,
                        error_message,
                        retry_after,
                        log_path,
                        session_log_path,
                        detached,
                        stdout_attached,
                        cancel_requested_at,
                        last_stream_at
                    )
                    VALUES (
                        ?, ?, NULL, NULL,
                        ?, ?, ?, ?,
                        ?, ?,
                        NULL, NULL,
                        NULL, NULL, NULL,
                        ?, ?,
                        ?, ?,
                        NULL, NULL
                    )
                    """,
                    (
                        query_id,
                        session_name,
                        model,
                        reasoning_effort,
                        tools_policy.value,
                        QueryState.QUEUED.value,
                        json.dumps(
                            prompt_metadata,
                            separators=(",", ":"),
                            sort_keys=True,
                        ),
                        _iso(now),
                        log_path,
                        session_log_path,
                        int(detached),
                        int(stdout_attached),
                    ),
                )

        except sqlite3.IntegrityError as exc:
            raise QueryAlreadyExists(
                query_id
            ) from exc

        query = self.get_query(
            query_id
        )

        assert query is not None
        return query

    def get_query(
        self,
        query_id: str,
    ) -> QueryRecord | None:
        row = self._db.execute(
            """
            SELECT *
            FROM queries
            WHERE query_id = ?
            """,
            (query_id,),
        ).fetchone()

        if row is None:
            return None

        return self._query(row)

    def list_queries(
        self,
        *,
        session_name: str | None = None,
        state: QueryState | None = None,
    ) -> list[QueryRecord]:
        clauses = []
        values = []

        if session_name is not None:
            clauses.append(
                "session_name = ?"
            )
            values.append(
                session_name
            )

        if state is not None:
            clauses.append(
                "state = ?"
            )
            values.append(
                state.value
            )

        where = (
            " WHERE " + " AND ".join(clauses)
            if clauses
            else ""
        )

        rows = self._db.execute(
            f"""
            SELECT *
            FROM queries
            {where}
            ORDER BY created_at, query_id
            """,
            values,
        ).fetchall()

        return [
            self._query(row)
            for row in rows
        ]

    def set_query_backend(
        self,
        query_id: str,
        *,
        backend_thread_id: str | None = None,
        upstream_turn_id: str | None = None,
    ) -> QueryRecord:
        current = self.get_query(
            query_id
        )

        if current is None:
            raise KeyError(
                query_id
            )

        thread_id = (
            backend_thread_id
            if backend_thread_id is not None
            else current.backend_thread_id
        )

        turn_id = (
            upstream_turn_id
            if upstream_turn_id is not None
            else current.upstream_turn_id
        )

        with self._db:
            self._db.execute(
                """
                UPDATE queries
                SET
                    backend_thread_id = ?,
                    upstream_turn_id = ?
                WHERE query_id = ?
                """,
                (
                    thread_id,
                    turn_id,
                    query_id,
                ),
            )

        updated = self.get_query(
            query_id
        )

        assert updated is not None
        return updated

    def transition_query(
        self,
        query_id: str,
        new_state: QueryState,
        *,
        error_type: str | None = None,
        error_message: str | None = None,
        retry_after: str | None = None,
    ) -> QueryRecord:
        current = self.get_query(
            query_id
        )

        if current is None:
            raise KeyError(
                query_id
            )

        if new_state == current.state:
            return current

        if (
            new_state
            not in _ALLOWED_QUERY_TRANSITIONS[
                current.state
            ]
        ):
            raise InvalidQueryTransition(
                f"{current.state.value} -> "
                f"{new_state.value}"
            )

        now = _now()

        started_at = current.started_at
        completed_at = current.completed_at

        if (
            new_state == QueryState.RUNNING
            and started_at is None
        ):
            started_at = now

        if new_state in _TERMINAL_QUERY_STATES:
            completed_at = now

        with self._db:
            self._db.execute(
                """
                UPDATE queries
                SET
                    state = ?,
                    started_at = ?,
                    completed_at = ?,
                    error_type = ?,
                    error_message = ?,
                    retry_after = ?
                WHERE query_id = ?
                """,
                (
                    new_state.value,
                    _iso(started_at),
                    _iso(completed_at),
                    error_type,
                    error_message,
                    retry_after,
                    query_id,
                ),
            )

        updated = self.get_query(
            query_id
        )

        assert updated is not None
        return updated

    def mark_cancel_requested(
        self,
        query_id: str,
    ) -> QueryRecord:
        if self.get_query(query_id) is None:
            raise KeyError(
                query_id
            )

        with self._db:
            self._db.execute(
                """
                UPDATE queries
                SET cancel_requested_at = ?
                WHERE query_id = ?
                """,
                (
                    _iso(_now()),
                    query_id,
                ),
            )

        updated = self.get_query(
            query_id
        )

        assert updated is not None
        return updated

    def mark_stream_activity(
        self,
        query_id: str,
    ) -> QueryRecord:
        if self.get_query(query_id) is None:
            raise KeyError(
                query_id
            )

        with self._db:
            self._db.execute(
                """
                UPDATE queries
                SET last_stream_at = ?
                WHERE query_id = ?
                """,
                (
                    _iso(_now()),
                    query_id,
                ),
            )

        updated = self.get_query(
            query_id
        )

        assert updated is not None
        return updated

    @staticmethod
    def _session(
        row: sqlite3.Row,
    ) -> SessionRecord:
        return SessionRecord(
            session_name=row["session_name"],
            backend_thread_id=(
                row["backend_thread_id"]
            ),
            model=row["model"],
            reasoning_effort=(
                row["reasoning_effort"]
            ),
            tools_policy=ToolsPolicy(
                row["tools_policy"]
            ),
            state=SessionState(
                row["state"]
            ),
            created_at=_dt(
                row["created_at"]
            ),
            last_used_at=_dt(
                row["last_used_at"]
            ),
            archived_at=_dt(
                row["archived_at"]
            ),
        )

    @staticmethod
    def _query(
        row: sqlite3.Row,
    ) -> QueryRecord:
        return QueryRecord(
            query_id=row["query_id"],
            session_name=row["session_name"],
            backend_thread_id=(
                row["backend_thread_id"]
            ),
            upstream_turn_id=(
                row["upstream_turn_id"]
            ),
            model=row["model"],
            reasoning_effort=(
                row["reasoning_effort"]
            ),
            tools_policy=ToolsPolicy(
                row["tools_policy"]
            ),
            state=QueryState(
                row["state"]
            ),
            prompt_metadata=json.loads(
                row["prompt_metadata"]
            ),
            created_at=_dt(
                row["created_at"]
            ),
            started_at=_dt(
                row["started_at"]
            ),
            completed_at=_dt(
                row["completed_at"]
            ),
            error_type=row["error_type"],
            error_message=(
                row["error_message"]
            ),
            retry_after=row["retry_after"],
            log_path=row["log_path"],
            session_log_path=(
                row["session_log_path"]
            ),
            detached=bool(
                row["detached"]
            ),
            stdout_attached=bool(
                row["stdout_attached"]
            ),
            cancel_requested_at=_dt(
                row["cancel_requested_at"]
            ),
            last_stream_at=_dt(
                row["last_stream_at"]
            ),
        )
