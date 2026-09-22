from __future__ import annotations

import json
import os
import re
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .backup_retention import (
    register_managed_backup,
)
from .models import (
    QueryRecord,
    QueryState,
    SessionBackendBinding,
    SessionRecord,
    SessionState,
    ToolsPolicy,
)


MIN_SCHEMA_VERSION = 1
SCHEMA_VERSION = 3

_SESSION_NAME = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._-]*$"
)

_BACKEND_NAME = re.compile(
    r"^[a-z][a-z0-9_-]{0,63}$"
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
            self.path,
            timeout=5.0,
        )
        self._db.row_factory = sqlite3.Row

        # Keep the durability/concurrency policy
        # explicit instead of relying on Python
        # or SQLite connection defaults.
        self._db.execute(
            "PRAGMA foreign_keys = ON"
        )
        self._db.execute(
            "PRAGMA busy_timeout = 5000"
        )
        self._db.execute(
            "PRAGMA journal_mode = WAL"
        )
        self._db.execute(
            "PRAGMA synchronous = FULL"
        )

        try:
            self._initialize()

        except Exception:
            self._db.close()
            raise

    def close(self) -> None:
        self._db.close()

    def quick_check(
        self,
    ) -> dict[str, Any]:
        checks = [
            str(row[0])
            for row in self._db.execute(
                "PRAGMA quick_check"
            ).fetchall()
        ]

        def pragma(name: str):
            row = self._db.execute(
                f"PRAGMA {name}"
            ).fetchone()

            if row is None:
                return None

            return row[0]

        return {
            "ok": checks == ["ok"],
            "quick_check": checks,
            "path": str(self.path),
            "journal_mode": pragma(
                "journal_mode"
            ),
            "synchronous": pragma(
                "synchronous"
            ),
            "foreign_keys": bool(
                pragma("foreign_keys")
            ),
            "busy_timeout": pragma(
                "busy_timeout"
            ),
            "wal_autocheckpoint": pragma(
                "wal_autocheckpoint"
            ),
        }

    def backup(
        self,
        destination: Path | None = None,
        *,
        now: datetime | None = None,
        managed_kind: str | None = None,
    ) -> dict[str, Any]:
        if now is None:
            now = datetime.now(
                timezone.utc
            )

        if destination is None:
            if managed_kind is None:
                managed_kind = "backup"

            backup_dir = (
                self.path.parent
                / "backups"
            )

            backup_dir.mkdir(
                parents=True,
                exist_ok=True,
                mode=0o700,
            )
            backup_dir.chmod(0o700)

            destination = (
                backup_dir
                / (
                    "q-"
                    + now.strftime(
                        "%Y%m%d-%H%M%S"
                    )
                    + ".sqlite3"
                )
            )

        destination = (
            destination
            .expanduser()
            .resolve(strict=False)
        )

        if destination.exists():
            raise StateStoreError(
                "backup destination already "
                f"exists: {destination}"
            )

        destination.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        fd, raw_temp = tempfile.mkstemp(
            dir=destination.parent,
            prefix=(
                f".{destination.name}."
            ),
            suffix=".tmp",
        )
        os.close(fd)

        temp_path = Path(raw_temp)

        try:
            os.chmod(
                temp_path,
                0o600,
            )

            snapshot = sqlite3.connect(
                temp_path,
                timeout=5.0,
            )

            try:
                self._db.backup(
                    snapshot
                )

                # Make the snapshot a standalone
                # SQLite file. QStore will switch
                # it back to WAL if it is restored
                # and opened as the live database.
                snapshot.execute(
                    "PRAGMA journal_mode = DELETE"
                )

                checks = [
                    str(row[0])
                    for row
                    in snapshot.execute(
                        "PRAGMA quick_check"
                    ).fetchall()
                ]

            finally:
                snapshot.close()

            if checks != ["ok"]:
                raise StateStoreError(
                    "backup quick_check failed: "
                    + "; ".join(checks)
                )

            os.chmod(
                temp_path,
                0o600,
            )

            # link() provides an atomic
            # no-overwrite publication step.
            try:
                os.link(
                    temp_path,
                    destination,
                )

            except FileExistsError as exc:
                raise StateStoreError(
                    "backup destination already "
                    f"exists: {destination}"
                ) from exc

            if managed_kind is not None:
                register_managed_backup(
                    destination,
                    kind=managed_kind,
                    created_at=now,
                )

            size = (
                destination.stat().st_size
            )

            return {
                "ok": True,
                "path": str(destination),
                "size_bytes": size,
                "quick_check": checks,
            }

        except (
            OSError,
            sqlite3.Error,
        ) as exc:
            raise StateStoreError(
                f"database backup failed: {exc}"
            ) from exc

        finally:
            for candidate in (
                temp_path,
                Path(
                    str(temp_path)
                    + "-wal"
                ),
                Path(
                    str(temp_path)
                    + "-shm"
                ),
            ):
                candidate.unlink(
                    missing_ok=True
                )

    @staticmethod
    def _validate_snapshot(
        path: Path,
    ) -> dict[str, Any]:
        path = (
            path
            .expanduser()
            .resolve(strict=False)
        )

        if not path.is_file():
            raise StateStoreError(
                "database snapshot does not "
                f"exist: {path}"
            )

        db = sqlite3.connect(
            path,
            timeout=5.0,
        )

        try:
            checks = [
                str(row[0])
                for row in db.execute(
                    "PRAGMA quick_check"
                ).fetchall()
            ]

            if checks != ["ok"]:
                raise StateStoreError(
                    "snapshot quick_check "
                    "failed: "
                    + "; ".join(checks)
                )

            try:
                row = db.execute(
                    """
                    SELECT value
                    FROM metadata
                    WHERE key = 'schema_version'
                    """
                ).fetchone()

            except sqlite3.Error as exc:
                raise StateStoreError(
                    "snapshot does not contain "
                    "a valid q schema"
                ) from exc

            if row is None:
                raise StateStoreError(
                    "snapshot has no "
                    "schema_version"
                )

            try:
                schema_version = int(
                    row[0]
                )

            except (
                TypeError,
                ValueError,
            ) as exc:
                raise StateStoreError(
                    "snapshot has invalid "
                    "schema_version"
                ) from exc

            if (
                schema_version
                < MIN_SCHEMA_VERSION
            ):
                raise StateStoreError(
                    "snapshot schema version "
                    f"{schema_version} is older "
                    "than minimum supported "
                    "version "
                    f"{MIN_SCHEMA_VERSION}"
                )

            if (
                schema_version
                > SCHEMA_VERSION
            ):
                raise StateStoreError(
                    "snapshot schema version "
                    f"{schema_version} is newer "
                    "than supported version "
                    f"{SCHEMA_VERSION}"
                )

            return {
                "quick_check": checks,
                "schema_version": (
                    schema_version
                ),
            }

        finally:
            db.close()

    @staticmethod
    def _failed_restore_path(
        *,
        live_path: Path,
        now: datetime,
    ) -> Path:
        backup_dir = (
            live_path.parent
            / "backups"
        )

        backup_dir.mkdir(
            parents=True,
            exist_ok=True,
            mode=0o700,
        )
        backup_dir.chmod(0o700)

        return (
            backup_dir
            / (
                "failed-restore-"
                + now.strftime(
                    "%Y%m%d-%H%M%S-%f"
                )
                + ".sqlite3"
            )
        )

    @staticmethod
    def _preserve_failed_restore(
        *,
        live_path: Path,
        failed_path: Path,
    ) -> None:
        if failed_path.exists():
            raise StateStoreError(
                "failed-restore diagnostic "
                "destination already exists: "
                f"{failed_path}"
            )

        os.replace(
            live_path,
            failed_path,
        )
        os.chmod(
            failed_path,
            0o600,
        )

        for suffix in (
            "-wal",
            "-shm",
        ):
            source = Path(
                str(live_path)
                + suffix
            )

            if not source.exists():
                continue

            destination = Path(
                str(failed_path)
                + suffix
            )

            if destination.exists():
                raise StateStoreError(
                    "failed-restore diagnostic "
                    "sidecar already exists: "
                    f"{destination}"
                )

            os.replace(
                source,
                destination,
            )
            os.chmod(
                destination,
                0o600,
            )

    @classmethod
    def _restore_rollback_snapshot(
        cls,
        *,
        rollback_path: Path,
        live_path: Path,
    ) -> dict[str, Any]:
        cls._validate_snapshot(
            rollback_path
        )

        fd, raw_temp = tempfile.mkstemp(
            dir=live_path.parent,
            prefix=".q-rollback.",
            suffix=".sqlite3.tmp",
        )
        os.close(fd)

        temp_path = Path(raw_temp)

        try:
            os.chmod(
                temp_path,
                0o600,
            )

            source = sqlite3.connect(
                rollback_path,
                timeout=5.0,
            )
            target = sqlite3.connect(
                temp_path,
                timeout=5.0,
            )

            try:
                source.backup(target)

                target.execute(
                    "PRAGMA journal_mode = DELETE"
                )

            finally:
                target.close()
                source.close()

            cls._validate_snapshot(
                temp_path
            )

            for suffix in (
                "-wal",
                "-shm",
            ):
                Path(
                    str(live_path)
                    + suffix
                ).unlink(
                    missing_ok=True
                )

            os.replace(
                temp_path,
                live_path,
            )
            os.chmod(
                live_path,
                0o600,
            )

            restored = cls(
                live_path
            )

            try:
                health = (
                    restored.quick_check()
                )

            finally:
                restored.close()

            if health["ok"] is not True:
                raise StateStoreError(
                    "automatic rollback "
                    "failed quick_check"
                )

            return health

        finally:
            temp_path.unlink(
                missing_ok=True
            )

    @classmethod
    def restore_backup(
        cls,
        *,
        live_path: Path,
        backup_path: Path,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        if now is None:
            now = datetime.now(
                timezone.utc
            )

        live_path = (
            live_path
            .expanduser()
            .resolve(strict=False)
        )
        backup_path = (
            backup_path
            .expanduser()
            .resolve(strict=False)
        )

        if live_path == backup_path:
            raise StateStoreError(
                "backup path is the live "
                "database path"
            )

        source_info = (
            cls._validate_snapshot(
                backup_path
            )
        )

        live_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        rollback_path = None

        if live_path.exists():
            current = cls(
                live_path
            )

            try:
                rollback_dir = (
                    live_path.parent
                    / "backups"
                )

                rollback_dir.mkdir(
                    parents=True,
                    exist_ok=True,
                    mode=0o700,
                )
                rollback_dir.chmod(0o700)

                rollback_path = (
                    rollback_dir
                    / (
                        "pre-restore-"
                        + now.strftime(
                            "%Y%m%d-%H%M%S-%f"
                        )
                        + ".sqlite3"
                    )
                )

                current.backup(
                    rollback_path,
                    managed_kind=(
                        "pre-restore"
                    ),
                )

                # qd is offline and this is the
                # only live DB connection. Merge
                # any committed WAL content into
                # the main DB before removing
                # stale sidecars.
                current._db.execute(
                    "PRAGMA wal_checkpoint("
                    "TRUNCATE)"
                ).fetchone()

            finally:
                current.close()

        fd, raw_temp = tempfile.mkstemp(
            dir=live_path.parent,
            prefix=".q-restore.",
            suffix=".sqlite3.tmp",
        )
        os.close(fd)

        temp_path = Path(raw_temp)

        try:
            os.chmod(
                temp_path,
                0o600,
            )

            source = sqlite3.connect(
                backup_path,
                timeout=5.0,
            )
            target = sqlite3.connect(
                temp_path,
                timeout=5.0,
            )

            try:
                source.backup(target)

                target.execute(
                    "PRAGMA journal_mode = DELETE"
                )

            finally:
                target.close()
                source.close()

            cls._validate_snapshot(
                temp_path
            )

            os.chmod(
                temp_path,
                0o600,
            )

            # Any old WAL was checkpointed above.
            # Remove stale sidecars before the new
            # main DB becomes live.
            for suffix in (
                "-wal",
                "-shm",
            ):
                Path(
                    str(live_path)
                    + suffix
                ).unlink(
                    missing_ok=True
                )

            os.replace(
                temp_path,
                live_path,
            )
            os.chmod(
                live_path,
                0o600,
            )

            try:
                restored = cls(
                    live_path
                )

                try:
                    health = (
                        restored.quick_check()
                    )
                    restored_schema_version = (
                        restored._schema_version()
                    )

                finally:
                    restored.close()

                if health["ok"] is not True:
                    raise StateStoreError(
                        "restored database failed "
                        "post-restore quick_check: "
                        + "; ".join(
                            str(item)
                            for item
                            in health[
                                "quick_check"
                            ]
                        )
                    )

            except Exception as exc:
                failed_path = (
                    cls._failed_restore_path(
                        live_path=live_path,
                        now=now,
                    )
                )

                preservation_error = None

                try:
                    cls._preserve_failed_restore(
                        live_path=live_path,
                        failed_path=failed_path,
                    )

                except Exception as preserve_exc:
                    preservation_error = (
                        preserve_exc
                    )

                rollback_error = None
                rollback_succeeded = False

                if rollback_path is not None:
                    try:
                        cls._restore_rollback_snapshot(
                            rollback_path=(
                                rollback_path
                            ),
                            live_path=live_path,
                        )

                    except Exception as rollback_exc:
                        rollback_error = (
                            rollback_exc
                        )

                    else:
                        rollback_succeeded = True

                parts = [
                    (
                        "database restore failed "
                        "after live replacement: "
                        f"{exc}"
                    )
                ]

                if failed_path.exists():
                    parts.append(
                        "failed restored image "
                        "preserved at "
                        f"{failed_path}"
                    )

                elif preservation_error is not None:
                    parts.append(
                        "failed-image preservation "
                        "failed: "
                        f"{preservation_error}"
                    )

                if rollback_succeeded:
                    parts.append(
                        "automatic rollback "
                        "succeeded from "
                        f"{rollback_path}"
                    )

                elif rollback_path is None:
                    parts.append(
                        "no pre-restore rollback "
                        "was available"
                    )

                else:
                    parts.append(
                        "automatic rollback also "
                        "failed: "
                        f"{rollback_error}"
                    )

                raise StateStoreError(
                    "; ".join(parts)
                ) from exc

            return {
                "ok": True,
                "path": str(live_path),
                "restored_from": str(
                    backup_path
                ),
                "rollback_backup": (
                    str(rollback_path)
                    if rollback_path
                    is not None
                    else None
                ),
                "source_schema_version": (
                    source_info[
                        "schema_version"
                    ]
                ),
                "schema_version": (
                    restored_schema_version
                ),
                "quick_check": (
                    health["quick_check"]
                ),
                "size_bytes": (
                    live_path
                    .stat()
                    .st_size
                ),
            }

        except (
            OSError,
            sqlite3.Error,
        ) as exc:
            raise StateStoreError(
                f"database restore failed: "
                f"{exc}"
            ) from exc

        finally:
            temp_path.unlink(
                missing_ok=True
            )

    def _ensure_current_schema(
        self,
    ) -> None:
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

            CREATE TABLE IF NOT EXISTS session_backend_bindings (
                session_name TEXT NOT NULL
                    REFERENCES sessions(session_name)
                    ON UPDATE CASCADE
                    ON DELETE CASCADE,
                backend_name TEXT NOT NULL,
                conversation_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_used_at TEXT NOT NULL,
                PRIMARY KEY (
                    session_name,
                    backend_name
                ),
                UNIQUE (
                    backend_name,
                    conversation_id
                )
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

            CREATE INDEX IF NOT EXISTS
                queries_completed_at_idx
            ON queries(completed_at);
            """
        )

    def _schema_version(
        self,
    ) -> int | None:
        row = self._db.execute(
            """
            SELECT value
            FROM metadata
            WHERE key = 'schema_version'
            """
        ).fetchone()

        if row is None:
            return None

        try:
            return int(row["value"])

        except (
            TypeError,
            ValueError,
        ) as exc:
            raise StateStoreError(
                "invalid q database "
                "schema_version"
            ) from exc

    def _backup_before_migration(
        self,
        *,
        from_version: int,
        to_version: int,
    ) -> Path:
        backup_dir = (
            self.path.parent
            / "backups"
        )

        backup_dir.mkdir(
            parents=True,
            exist_ok=True,
            mode=0o700,
        )
        backup_dir.chmod(0o700)

        now = _now()

        destination = (
            backup_dir
            / (
                "pre-migrate-"
                f"v{from_version}-to-v{to_version}-"
                + now.strftime(
                    "%Y%m%d-%H%M%S-%f"
                )
                + ".sqlite3"
            )
        )

        self.backup(
            destination,
            managed_kind=(
                "pre-migrate"
            ),
        )

        return destination

    def _migrate_1_to_2(
        self,
    ) -> int:
        foreign_keys = bool(
            self._db.execute(
                "PRAGMA foreign_keys"
            ).fetchone()[0]
        )

        if foreign_keys:
            self._db.execute(
                "PRAGMA foreign_keys = OFF"
            )

        try:
            self._db.execute(
                "BEGIN IMMEDIATE"
            )

            self._db.execute(
                """
                CREATE TABLE sessions_migrate_v2 (
                    session_name TEXT PRIMARY KEY,
                    backend_thread_id TEXT UNIQUE,
                    model TEXT NOT NULL,
                    reasoning_effort TEXT NOT NULL,
                    tools_policy TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    last_used_at TEXT NOT NULL,
                    archived_at TEXT
                )
                """
            )

            self._db.execute(
                """
                INSERT INTO sessions_migrate_v2 (
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
                SELECT
                    session_name,
                    backend_thread_id,
                    model,
                    reasoning_effort,
                    tools_policy,
                    state,
                    created_at,
                    last_used_at,
                    archived_at
                FROM sessions
                """
            )

            self._db.execute(
                "DROP TABLE sessions"
            )

            self._db.execute(
                """
                ALTER TABLE sessions_migrate_v2
                RENAME TO sessions
                """
            )

            cursor = self._db.execute(
                """
                UPDATE metadata
                SET value = ?
                WHERE key = 'schema_version'
                """,
                ("2",),
            )

            if cursor.rowcount != 1:
                raise StateStoreError(
                    "failed to update "
                    "schema_version during "
                    "migration 1 -> 2"
                )

            violations = (
                self._db.execute(
                    "PRAGMA foreign_key_check"
                ).fetchall()
            )

            if violations:
                raise StateStoreError(
                    "foreign-key validation "
                    "failed during migration "
                    "1 -> 2"
                )

            self._db.commit()

        except StateStoreError:
            self._db.rollback()
            raise

        except sqlite3.Error as exc:
            self._db.rollback()
            raise StateStoreError(
                "database migration "
                f"1 -> 2 failed: {exc}"
            ) from exc

        finally:
            if foreign_keys:
                self._db.execute(
                    "PRAGMA foreign_keys = ON"
                )

        return 2

    def _migrate_2_to_3(
        self,
    ) -> int:
        try:
            self._db.execute(
                "BEGIN IMMEDIATE"
            )

            self._db.execute(
                """
                CREATE TABLE session_backend_bindings (
                    session_name TEXT NOT NULL
                        REFERENCES sessions(session_name)
                        ON UPDATE CASCADE
                        ON DELETE CASCADE,
                    backend_name TEXT NOT NULL,
                    conversation_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    last_used_at TEXT NOT NULL,
                    PRIMARY KEY (
                        session_name,
                        backend_name
                    ),
                    UNIQUE (
                        backend_name,
                        conversation_id
                    )
                )
                """
            )

            # All v2 session bindings were created
            # before q supported multiple backends,
            # so a legacy backend_thread_id is a
            # Codex conversation binding.
            self._db.execute(
                """
                INSERT INTO session_backend_bindings (
                    session_name,
                    backend_name,
                    conversation_id,
                    created_at,
                    last_used_at
                )
                SELECT
                    session_name,
                    'codex',
                    backend_thread_id,
                    created_at,
                    last_used_at
                FROM sessions
                WHERE backend_thread_id IS NOT NULL
                """
            )

            cursor = self._db.execute(
                """
                UPDATE metadata
                SET value = ?
                WHERE key = 'schema_version'
                """,
                ("3",),
            )

            if cursor.rowcount != 1:
                raise StateStoreError(
                    "failed to update "
                    "schema_version during "
                    "migration 2 -> 3"
                )

            violations = self._db.execute(
                "PRAGMA foreign_key_check"
            ).fetchall()

            if violations:
                raise StateStoreError(
                    "foreign-key validation "
                    "failed during migration "
                    "2 -> 3"
                )

            self._db.commit()

        except StateStoreError:
            self._db.rollback()
            raise

        except sqlite3.Error as exc:
            self._db.rollback()
            raise StateStoreError(
                "database migration "
                f"2 -> 3 failed: {exc}"
            ) from exc

        return 3

    def _migrate(
        self,
        version: int,
    ) -> int:
        current = version

        while current < SCHEMA_VERSION:
            if current == 1:
                current = (
                    self._migrate_1_to_2()
                )
                continue

            if current == 2:
                current = (
                    self._migrate_2_to_3()
                )
                continue

            raise StateStoreError(
                "no q database migration "
                f"path from version {current}"
            )

        return current

    def _initialize(self) -> None:
        # metadata must exist before a version
        # can be discovered. No current-version
        # tables are imposed on an older schema
        # until its migration has completed.
        self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        self._db.commit()

        version = self._schema_version()

        if version is None:
            self._ensure_current_schema()

            self._db.execute(
                """
                INSERT INTO metadata(key, value)
                VALUES ('schema_version', ?)
                """,
                (str(SCHEMA_VERSION),),
            )
            self._db.commit()
            return

        if version > SCHEMA_VERSION:
            raise StateStoreError(
                "q database schema version "
                f"{version} is newer than "
                "supported version "
                f"{SCHEMA_VERSION}"
            )

        if version < MIN_SCHEMA_VERSION:
            raise StateStoreError(
                "unsupported q database "
                f"schema version {version}"
            )

        if version < SCHEMA_VERSION:
            self._backup_before_migration(
                from_version=version,
                to_version=(
                    SCHEMA_VERSION
                ),
            )

            version = self._migrate(
                version
            )

        if version != SCHEMA_VERSION:
            raise StateStoreError(
                "unsupported q database "
                f"schema version {version}"
            )

        # Apply idempotent current-schema objects,
        # including indexes added without a schema
        # version bump.
        self._ensure_current_schema()
        self._db.commit()

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

    def bind_session_backend(
        self,
        session_name: str,
        backend_name: str,
        conversation_id: str,
    ) -> SessionBackendBinding:
        if not _BACKEND_NAME.fullmatch(
            backend_name
        ):
            raise ValueError(
                "invalid backend name"
            )

        if not conversation_id:
            raise ValueError(
                "conversation_id must not be empty"
            )

        existing = self.get_session_backend(
            session_name,
            backend_name,
        )

        if existing is not None:
            if (
                existing.conversation_id
                != conversation_id
            ):
                raise StateStoreError(
                    "session backend is already "
                    "bound to another conversation"
                )

            return existing

        now = _now()

        try:
            with self._db:
                self._db.execute(
                    """
                    INSERT INTO session_backend_bindings (
                        session_name,
                        backend_name,
                        conversation_id,
                        created_at,
                        last_used_at
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        session_name,
                        backend_name,
                        conversation_id,
                        _iso(now),
                        _iso(now),
                    ),
                )

        except sqlite3.IntegrityError as exc:
            if (
                self.get_session(
                    session_name
                )
                is None
            ):
                raise KeyError(
                    session_name
                ) from exc

            raise StateStoreError(
                "backend conversation is already "
                "bound to another session"
            ) from exc

        binding = self.get_session_backend(
            session_name,
            backend_name,
        )

        assert binding is not None
        return binding

    def get_session_backend(
        self,
        session_name: str,
        backend_name: str,
    ) -> SessionBackendBinding | None:
        row = self._db.execute(
            """
            SELECT *
            FROM session_backend_bindings
            WHERE session_name = ?
              AND backend_name = ?
            """,
            (
                session_name,
                backend_name,
            ),
        ).fetchone()

        if row is None:
            return None

        return self._session_backend_binding(
            row
        )

    def list_session_backends(
        self,
        session_name: str,
    ) -> list[SessionBackendBinding]:
        rows = self._db.execute(
            """
            SELECT *
            FROM session_backend_bindings
            WHERE session_name = ?
            ORDER BY backend_name
            """,
            (session_name,),
        ).fetchall()

        return [
            self._session_backend_binding(
                row
            )
            for row in rows
        ]

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

    def mark_session_broken(
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
                    last_used_at = ?
                WHERE session_name = ?
                """,
                (
                    SessionState.BROKEN.value,
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

    def mark_session_broken(
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
                    last_used_at = ?
                WHERE session_name = ?
                """,
                (
                    SessionState.BROKEN.value,
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

    def list_queries_completed_before(
        self,
        cutoff: datetime,
    ) -> list[QueryRecord]:
        terminal_states = (
            QueryState.COMPLETED.value,
            QueryState.FAILED.value,
            QueryState.INDETERMINATE.value,
            QueryState.CANCELLED.value,
        )

        rows = self._db.execute(
            """
            SELECT *
            FROM queries
            WHERE completed_at IS NOT NULL
              AND completed_at < ?
              AND state IN (?, ?, ?, ?)
            ORDER BY completed_at, query_id
            """,
            (
                _iso(cutoff),
                *terminal_states,
            ),
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
    def _session_backend_binding(
        row: sqlite3.Row,
    ) -> SessionBackendBinding:
        return SessionBackendBinding(
            session_name=row["session_name"],
            backend_name=row["backend_name"],
            conversation_id=(
                row["conversation_id"]
            ),
            created_at=_dt(
                row["created_at"]
            ),
            last_used_at=_dt(
                row["last_used_at"]
            ),
        )

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


# Session recovery deliberately uses an explicit broken state.
# A session whose upstream state is uncertain must not silently
# accept another turn until qd reconciles it.


# Session recovery deliberately uses an explicit broken state.
# A session whose upstream state is uncertain must not silently
# accept another turn until qd reconciles it.
