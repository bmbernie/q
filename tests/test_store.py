import sqlite3
import os
import subprocess
import sys
from pathlib import Path

import pytest

from q.models import (
    QueryState,
    SessionState,
    ToolsPolicy,
)
from q.store import (
    InvalidQueryTransition,
    QStore,
    QueryAlreadyExists,
)


def make_store(tmp_path):
    return QStore(
        tmp_path / "q.sqlite3"
    )


def test_session_mapping_persists(
    tmp_path,
):
    path = tmp_path / "q.sqlite3"

    store = QStore(path)

    created = store.create_session(
        session_name="l2walker",
        backend_thread_id="thread-123",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
    )

    assert (
        created.session_name
        == "l2walker"
    )
    assert (
        created.backend_thread_id
        == "thread-123"
    )

    store.close()

    reopened = QStore(path)

    loaded = reopened.get_session(
        "l2walker"
    )

    assert loaded is not None
    assert (
        loaded.backend_thread_id
        == "thread-123"
    )
    assert (
        loaded.reasoning_effort
        == "xhigh"
    )

    reopened.close()


def test_session_rename_updates_queries(
    tmp_path,
):
    store = make_store(tmp_path)

    store.create_session(
        session_name="old",
        backend_thread_id="thread-1",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
    )

    store.create_query(
        query_id="q-1",
        session_name="old",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
        prompt_metadata={"kind": "text"},
        log_path="/tmp/q-1.log",
        session_log_path="/tmp/old.log",
        detached=False,
        stdout_attached=True,
    )

    store.rename_session(
        "old",
        "new",
    )

    query = store.get_query("q-1")

    assert query is not None
    assert query.session_name == "new"

    store.close()


def test_query_lifecycle_and_backend_ids(
    tmp_path,
):
    store = make_store(tmp_path)

    query = store.create_query(
        query_id="q-1",
        session_name=None,
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
        prompt_metadata={
            "source": "cli",
        },
        log_path="/tmp/q-1.log",
        session_log_path=None,
        detached=False,
        stdout_attached=True,
    )

    assert query.state == QueryState.QUEUED

    running = store.transition_query(
        "q-1",
        QueryState.RUNNING,
    )

    assert running.started_at is not None

    running = store.set_query_backend(
        "q-1",
        backend_thread_id="thread-1",
        upstream_turn_id="turn-1",
    )

    assert (
        running.backend_thread_id
        == "thread-1"
    )
    assert (
        running.upstream_turn_id
        == "turn-1"
    )

    completed = store.transition_query(
        "q-1",
        QueryState.COMPLETED,
    )

    assert (
        completed.state
        == QueryState.COMPLETED
    )
    assert completed.completed_at is not None

    store.close()


def test_invalid_terminal_transition_rejected(
    tmp_path,
):
    store = make_store(tmp_path)

    store.create_query(
        query_id="q-1",
        session_name=None,
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
        prompt_metadata={},
        log_path="/tmp/q-1.log",
        session_log_path=None,
        detached=True,
        stdout_attached=False,
    )

    store.transition_query(
        "q-1",
        QueryState.RUNNING,
    )

    store.transition_query(
        "q-1",
        QueryState.INDETERMINATE,
    )

    with pytest.raises(
        InvalidQueryTransition
    ):
        store.transition_query(
            "q-1",
            QueryState.RUNNING,
        )

    store.close()


def test_duplicate_query_id_rejected(
    tmp_path,
):
    store = make_store(tmp_path)

    kwargs = {
        "query_id": "q-duplicate",
        "session_name": None,
        "model": "gpt-5.6-luna",
        "reasoning_effort": "xhigh",
        "tools_policy": ToolsPolicy.ENABLED,
        "prompt_metadata": {},
        "log_path": "/tmp/query.log",
        "session_log_path": None,
        "detached": False,
        "stdout_attached": True,
    }

    store.create_query(**kwargs)

    with pytest.raises(
        QueryAlreadyExists
    ):
        store.create_query(**kwargs)

    store.close()


def test_query_filters_and_session_archive(
    tmp_path,
):
    store = make_store(tmp_path)

    store.create_session(
        session_name="alpha",
        backend_thread_id="thread-a",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.DISABLED,
    )

    for query_id in (
        "q-1",
        "q-2",
    ):
        store.create_query(
            query_id=query_id,
            session_name="alpha",
            model="gpt-5.6-luna",
            reasoning_effort="xhigh",
            tools_policy=(
                ToolsPolicy.DISABLED
            ),
            prompt_metadata={},
            log_path=(
                f"/tmp/{query_id}.log"
            ),
            session_log_path=(
                "/tmp/alpha.log"
            ),
            detached=False,
            stdout_attached=True,
        )

    store.transition_query(
        "q-1",
        QueryState.RUNNING,
    )

    running = store.list_queries(
        session_name="alpha",
        state=QueryState.RUNNING,
    )

    assert [
        item.query_id
        for item in running
    ] == ["q-1"]

    archived = store.archive_session(
        "alpha"
    )

    assert (
        archived.state
        == SessionState.ARCHIVED
    )
    assert archived.archived_at is not None

    store.close()



def test_pending_session_can_own_query_before_thread_exists(
    tmp_path,
):
    store = make_store(tmp_path)

    session = store.create_session(
        session_name="new-session",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
    )

    assert (
        session.state
        == SessionState.PENDING
    )
    assert (
        session.backend_thread_id
        is None
    )

    query = store.create_query(
        query_id="q-pending",
        session_name="new-session",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
        prompt_metadata={},
        log_path="/tmp/q-pending.log",
        session_log_path=(
            "/tmp/new-session.log"
        ),
        detached=False,
        stdout_attached=True,
    )

    assert (
        query.session_name
        == "new-session"
    )
    assert (
        query.backend_thread_id
        is None
    )

    bound = store.bind_session_thread(
        "new-session",
        "thread-new",
    )

    assert (
        bound.state
        == SessionState.ACTIVE
    )
    assert (
        bound.backend_thread_id
        == "thread-new"
    )

    store.close()


def test_active_session_cannot_be_rebound(
    tmp_path,
):
    store = make_store(tmp_path)

    store.create_session(
        session_name="fixed",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
    )

    store.bind_session_thread(
        "fixed",
        "thread-a",
    )

    from q.store import StateStoreError

    with pytest.raises(
        StateStoreError
    ):
        store.bind_session_thread(
            "fixed",
            "thread-b",
        )

    session = store.get_session(
        "fixed"
    )

    assert session is not None
    assert (
        session.backend_thread_id
        == "thread-a"
    )

    store.close()



def test_store_uses_explicit_durability_pragmas(
    tmp_path,
):
    store = make_store(tmp_path)

    expected = {
        "journal_mode": "wal",
        "synchronous": 2,
        "foreign_keys": 1,
        "busy_timeout": 5000,
    }

    for name, value in expected.items():
        actual = store._db.execute(
            f"PRAGMA {name}"
        ).fetchone()[0]

        assert actual == value

    store.close()


def test_quick_check_reports_database_health(
    tmp_path,
):
    store = make_store(tmp_path)

    result = store.quick_check()

    assert result["ok"] is True
    assert result["quick_check"] == [
        "ok"
    ]
    assert result["journal_mode"] == "wal"
    assert result["synchronous"] == 2
    assert result["foreign_keys"] is True
    assert result["busy_timeout"] == 5000
    assert (
        result["wal_autocheckpoint"]
        == 1000
    )

    store.close()


def test_wal_recovers_committed_state_after_abrupt_exit(
    tmp_path,
):
    database = (
        tmp_path / "crash.sqlite3"
    )

    source_root = (
        Path(__file__)
        .resolve()
        .parents[1]
        / "src"
    )

    code = r"""
import os
from pathlib import Path

from q.models import ToolsPolicy
from q.store import QStore

store = QStore(
    Path(os.environ["Q_CRASH_DB"])
)

store.create_session(
    session_name="crash-proof",
    backend_thread_id=(
        "thread-crash-proof"
    ),
    model="gpt-5.6-luna",
    reasoning_effort="xhigh",
    tools_policy=ToolsPolicy.ENABLED,
)

# Deliberately bypass normal Python cleanup
# and QStore.close().
os._exit(73)
"""

    environment = dict(os.environ)
    environment["Q_CRASH_DB"] = str(
        database
    )

    existing_pythonpath = (
        environment.get(
            "PYTHONPATH",
            "",
        )
    )

    environment["PYTHONPATH"] = (
        str(source_root)
        + (
            os.pathsep
            + existing_pythonpath
            if existing_pythonpath
            else ""
        )
    )

    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            code,
        ],
        env=environment,
        check=False,
    )

    assert completed.returncode == 73

    reopened = QStore(database)

    session = reopened.get_session(
        "crash-proof"
    )

    assert session is not None
    assert (
        session.backend_thread_id
        == "thread-crash-proof"
    )
    assert (
        reopened.quick_check()["ok"]
        is True
    )

    reopened.close()


def test_database_backup_is_consistent_and_standalone(
    tmp_path,
):
    store = make_store(tmp_path)

    store.create_session(
        session_name="backup-proof",
        backend_thread_id=(
            "thread-backup-proof"
        ),
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
    )

    destination = (
        tmp_path
        / "backup.sqlite3"
    )

    result = store.backup(
        destination
    )

    assert result["ok"] is True
    assert result["path"] == str(
        destination.resolve()
    )
    assert result["size_bytes"] > 0
    assert result["quick_check"] == [
        "ok"
    ]

    assert destination.exists()

    mode = (
        destination.stat().st_mode
        & 0o777
    )

    assert mode == 0o600

    assert not Path(
        str(destination) + "-wal"
    ).exists()
    assert not Path(
        str(destination) + "-shm"
    ).exists()

    snapshot = sqlite3.connect(
        destination
    )

    row = snapshot.execute(
        """
        SELECT backend_thread_id
        FROM sessions
        WHERE session_name = ?
        """,
        ("backup-proof",),
    ).fetchone()

    assert row is not None
    assert row[0] == (
        "thread-backup-proof"
    )

    assert (
        snapshot.execute(
            "PRAGMA quick_check"
        ).fetchone()[0]
        == "ok"
    )

    snapshot.close()
    store.close()


def test_database_backup_refuses_overwrite(
    tmp_path,
):
    from q.store import StateStoreError

    store = make_store(tmp_path)

    destination = (
        tmp_path
        / "existing.sqlite3"
    )

    destination.write_text(
        "do not overwrite",
        encoding="utf-8",
    )

    with pytest.raises(
        StateStoreError
    ):
        store.backup(destination)

    assert destination.read_text(
        encoding="utf-8"
    ) == "do not overwrite"

    store.close()


def test_default_database_backup_directory(
    tmp_path,
):
    from datetime import (
        datetime,
        timezone,
    )

    store = make_store(tmp_path)

    result = store.backup(
        now=datetime(
            2026,
            9,
            22,
            4,
            30,
            15,
            tzinfo=timezone.utc,
        )
    )

    expected = (
        tmp_path
        / "backups"
        / "q-20260922-043015.sqlite3"
    )

    assert result["path"] == str(
        expected.resolve()
    )
    assert expected.exists()

    assert (
        expected.parent.stat().st_mode
        & 0o777
    ) == 0o700

    store.close()


def test_restore_backup_replaces_live_database_and_preserves_rollback(
    tmp_path,
):
    live_path = tmp_path / "q.sqlite3"
    source_path = tmp_path / "source.sqlite3"

    live = QStore(live_path)

    live.create_session(
        session_name="live-only",
        backend_thread_id="thread-live",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
    )

    live.close()

    source = QStore(source_path)

    source.create_session(
        session_name="source-only",
        backend_thread_id="thread-source",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
    )

    source.close()

    result = QStore.restore_backup(
        live_path=live_path,
        backup_path=source_path,
    )

    assert result["ok"] is True
    assert result["quick_check"] == ["ok"]
    assert (
        result["source_schema_version"]
        == 2
    )
    assert result["schema_version"] == 2

    rollback = Path(
        result["rollback_backup"]
    )

    assert rollback.exists()

    restored = QStore(live_path)

    assert restored.get_session(
        "source-only"
    ) is not None

    assert restored.get_session(
        "live-only"
    ) is None

    restored.close()

    previous = QStore(rollback)

    assert previous.get_session(
        "live-only"
    ) is not None

    previous.close()


def test_restore_rejects_invalid_schema_version(
    tmp_path,
):
    from q.store import StateStoreError

    live_path = tmp_path / "q.sqlite3"
    source_path = tmp_path / "source.sqlite3"

    live = QStore(live_path)

    live.create_session(
        session_name="keep-live",
        backend_thread_id="thread-keep-live",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
    )

    live.close()

    source = QStore(source_path)

    with source._db:
        source._db.execute(
            """
            UPDATE metadata
            SET value = 'not-a-version'
            WHERE key = 'schema_version'
            """
        )

    source.close()

    with pytest.raises(
        StateStoreError,
        match="invalid schema_version",
    ):
        QStore.restore_backup(
            live_path=live_path,
            backup_path=source_path,
        )

    live = QStore(live_path)

    assert (
        live.get_session(
            "keep-live"
        )
        is not None
    )

    live.close()

    assert not (
        tmp_path / "backups"
    ).exists()


def test_restore_rejects_live_database_as_source(
    tmp_path,
):
    from q.store import StateStoreError

    live_path = tmp_path / "q.sqlite3"

    live = QStore(live_path)
    live.close()

    with pytest.raises(StateStoreError):
        QStore.restore_backup(
            live_path=live_path,
            backup_path=live_path,
        )


def _create_v1_database(
    path,
):
    db = sqlite3.connect(path)

    db.executescript(
        """
        CREATE TABLE metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE sessions (
            session_name TEXT PRIMARY KEY,
            backend_thread_id TEXT NOT NULL UNIQUE,
            model TEXT NOT NULL,
            reasoning_effort TEXT NOT NULL,
            tools_policy TEXT NOT NULL,
            state TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_used_at TEXT NOT NULL,
            archived_at TEXT
        );

        CREATE TABLE queries (
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

        CREATE INDEX queries_state_idx
        ON queries(state);

        CREATE INDEX queries_session_created_idx
        ON queries(session_name, created_at);
        """
    )

    timestamp = (
        "2026-09-01T00:00:00+00:00"
    )

    db.execute(
        """
        INSERT INTO metadata(key, value)
        VALUES ('schema_version', '1')
        """
    )

    db.execute(
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
            "legacy",
            "thread-legacy",
            "gpt-5.6-luna",
            "xhigh",
            "enabled",
            "active",
            timestamp,
            timestamp,
        ),
    )

    db.execute(
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
            ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, NULL, NULL, NULL,
            ?, ?, 0, 1, NULL, ?
        )
        """,
        (
            "q-legacy",
            "legacy",
            "thread-legacy",
            "turn-legacy",
            "gpt-5.6-luna",
            "xhigh",
            "enabled",
            "completed",
            "{}",
            timestamp,
            timestamp,
            timestamp,
            "/tmp/q-legacy.ndjson",
            "/tmp/legacy.ndjson",
            timestamp,
        ),
    )

    db.commit()
    db.close()


def test_v1_database_migrates_to_v2_without_data_loss(
    tmp_path,
):
    path = tmp_path / "q.sqlite3"

    _create_v1_database(path)

    store = QStore(path)

    version = store._db.execute(
        """
        SELECT value
        FROM metadata
        WHERE key = 'schema_version'
        """
    ).fetchone()[0]

    assert version == "2"

    legacy = store.get_session(
        "legacy"
    )

    assert legacy is not None
    assert (
        legacy.backend_thread_id
        == "thread-legacy"
    )

    query = store.get_query(
        "q-legacy"
    )

    assert query is not None
    assert query.session_name == "legacy"
    assert (
        query.backend_thread_id
        == "thread-legacy"
    )

    # The v2 capability that required the
    # migration: pending sessions may have no
    # backend thread yet.
    pending = store.create_session(
        session_name="pending-after-migrate",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
    )

    assert (
        pending.backend_thread_id
        is None
    )
    assert (
        pending.state
        == SessionState.PENDING
    )

    columns = {
        row["name"]: row
        for row in store._db.execute(
            "PRAGMA table_info(sessions)"
        ).fetchall()
    }

    assert (
        columns[
            "backend_thread_id"
        ]["notnull"]
        == 0
    )

    indexes = {
        row["name"]
        for row in store._db.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'index'
            """
        ).fetchall()
    }

    assert (
        "queries_completed_at_idx"
        in indexes
    )

    assert (
        store.quick_check()["ok"]
        is True
    )

    store.close()

    backups = list(
        (
            tmp_path
            / "backups"
        ).glob(
            "pre-migrate-v1-to-v2-"
            "*.sqlite3"
        )
    )

    assert len(backups) == 1

    backup = sqlite3.connect(
        backups[0]
    )

    try:
        backup_version = (
            backup.execute(
                """
                SELECT value
                FROM metadata
                WHERE key = 'schema_version'
                """
            ).fetchone()[0]
        )

        assert backup_version == "1"

        assert (
            backup.execute(
                "PRAGMA quick_check"
            ).fetchone()[0]
            == "ok"
        )

    finally:
        backup.close()


def test_current_v2_database_does_not_create_migration_backup(
    tmp_path,
):
    path = tmp_path / "q.sqlite3"

    store = QStore(path)
    store.close()

    reopened = QStore(path)
    reopened.close()

    backup_dir = (
        tmp_path / "backups"
    )

    assert (
        not backup_dir.exists()
        or not list(
            backup_dir.glob(
                "pre-migrate-*.sqlite3"
            )
        )
    )


def test_newer_schema_is_refused_without_mutation(
    tmp_path,
):
    from q.store import StateStoreError

    path = tmp_path / "q.sqlite3"

    store = QStore(path)

    with store._db:
        store._db.execute(
            """
            UPDATE metadata
            SET value = '999'
            WHERE key = 'schema_version'
            """
        )

    store.close()

    with pytest.raises(
        StateStoreError,
        match="newer than supported",
    ):
        QStore(path)

    raw = sqlite3.connect(path)

    try:
        version = raw.execute(
            """
            SELECT value
            FROM metadata
            WHERE key = 'schema_version'
            """
        ).fetchone()[0]

        assert version == "999"

    finally:
        raw.close()


def test_default_backup_is_marked_managed(
    tmp_path,
):
    from q.backup_retention import (
        list_managed_backups,
    )

    store = make_store(tmp_path)

    result = store.backup()

    items = list_managed_backups(
        tmp_path / "backups"
    )

    assert len(items) == 1
    assert items[0].kind == "backup"
    assert str(items[0].path) == (
        result["path"]
    )

    store.close()


def test_custom_backup_is_not_marked_managed(
    tmp_path,
):
    from q.backup_retention import (
        list_managed_backups,
    )

    store = make_store(tmp_path)

    backup_dir = (
        tmp_path / "backups"
    )
    backup_dir.mkdir(
        parents=True
    )

    destination = (
        backup_dir
        / "q-20990101-000000.sqlite3"
    )

    store.backup(
        destination
    )

    assert (
        list_managed_backups(
            backup_dir
        )
        == []
    )

    store.close()


def test_restore_v1_backup_migrates_to_current_schema(
    tmp_path,
):
    live_path = tmp_path / "q.sqlite3"
    source_path = (
        tmp_path / "source-v1.sqlite3"
    )

    live = QStore(live_path)

    live.create_session(
        session_name="live-before-restore",
        backend_thread_id="thread-live-before",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
    )

    live.close()

    _create_v1_database(
        source_path
    )

    result = QStore.restore_backup(
        live_path=live_path,
        backup_path=source_path,
    )

    assert result["ok"] is True
    assert (
        result["source_schema_version"]
        == 1
    )
    assert result["schema_version"] == 2
    assert result["quick_check"] == ["ok"]

    restored = QStore(live_path)

    assert (
        restored._schema_version()
        == 2
    )

    assert (
        restored.get_session(
            "legacy"
        )
        is not None
    )

    assert (
        restored.get_query(
            "q-legacy"
        )
        is not None
    )

    assert (
        restored.get_session(
            "live-before-restore"
        )
        is None
    )

    # Prove the v1 -> v2 change actually
    # occurred: backend_thread_id is now
    # nullable for pending sessions.
    columns = {
        row["name"]: row
        for row in restored._db.execute(
            "PRAGMA table_info(sessions)"
        ).fetchall()
    }

    assert (
        columns[
            "backend_thread_id"
        ]["notnull"]
        == 0
    )

    restored.close()

    rollback = Path(
        result["rollback_backup"]
    )

    assert rollback.exists()

    previous = QStore(rollback)

    assert (
        previous.get_session(
            "live-before-restore"
        )
        is not None
    )

    previous.close()

    migration_backups = list(
        (
            tmp_path / "backups"
        ).glob(
            "pre-migrate-v1-to-v2-"
            "*.sqlite3"
        )
    )

    assert len(migration_backups) == 1


def test_restore_rejects_schema_older_than_supported_without_replacing_live(
    tmp_path,
):
    from q.store import StateStoreError

    live_path = tmp_path / "q.sqlite3"
    source_path = (
        tmp_path / "source-old.sqlite3"
    )

    live = QStore(live_path)

    live.create_session(
        session_name="keep-live",
        backend_thread_id="thread-keep-live",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
    )

    live.close()

    source = QStore(source_path)

    with source._db:
        source._db.execute(
            """
            UPDATE metadata
            SET value = '0'
            WHERE key = 'schema_version'
            """
        )

    source.close()

    with pytest.raises(
        StateStoreError,
        match=(
            "older than minimum "
            "supported version"
        ),
    ):
        QStore.restore_backup(
            live_path=live_path,
            backup_path=source_path,
        )

    live = QStore(live_path)

    assert (
        live.get_session(
            "keep-live"
        )
        is not None
    )

    live.close()

    assert not (
        tmp_path / "backups"
    ).exists()


def test_restore_rejects_newer_schema_without_replacing_live(
    tmp_path,
):
    from q.store import StateStoreError

    live_path = tmp_path / "q.sqlite3"
    source_path = (
        tmp_path / "source-new.sqlite3"
    )

    live = QStore(live_path)

    live.create_session(
        session_name="keep-live",
        backend_thread_id="thread-keep-live",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
    )

    live.close()

    source = QStore(source_path)

    with source._db:
        source._db.execute(
            """
            UPDATE metadata
            SET value = '999'
            WHERE key = 'schema_version'
            """
        )

    source.close()

    with pytest.raises(
        StateStoreError,
        match=(
            "newer than supported "
            "version"
        ),
    ):
        QStore.restore_backup(
            live_path=live_path,
            backup_path=source_path,
        )

    live = QStore(live_path)

    assert (
        live.get_session(
            "keep-live"
        )
        is not None
    )

    live.close()

    assert not (
        tmp_path / "backups"
    ).exists()


def test_restore_post_replace_failure_rolls_back_and_preserves_failed_image(
    tmp_path,
    monkeypatch,
):
    from q.store import StateStoreError

    live_path = tmp_path / "q.sqlite3"
    source_path = tmp_path / "source.sqlite3"

    live = QStore(live_path)
    live.create_session(
        session_name="live-only",
        backend_thread_id="thread-live",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
    )
    live.close()

    source = QStore(source_path)
    source.create_session(
        session_name="source-only",
        backend_thread_id="thread-source",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
    )
    source.close()

    original_quick_check = (
        QStore.quick_check
    )

    def forced_quick_check(self):
        result = original_quick_check(
            self
        )

        if (
            self.get_session(
                "source-only"
            )
            is not None
        ):
            return {
                **result,
                "ok": False,
                "quick_check": [
                    "forced failure"
                ],
            }

        return result

    monkeypatch.setattr(
        QStore,
        "quick_check",
        forced_quick_check,
    )

    with pytest.raises(
        StateStoreError,
        match=(
            "automatic rollback "
            "succeeded"
        ),
    ):
        QStore.restore_backup(
            live_path=live_path,
            backup_path=source_path,
        )

    restored = QStore(live_path)

    assert (
        restored.get_session(
            "live-only"
        )
        is not None
    )
    assert (
        restored.get_session(
            "source-only"
        )
        is None
    )

    restored.close()

    failed = list(
        (
            tmp_path / "backups"
        ).glob(
            "failed-restore-*.sqlite3"
        )
    )

    assert len(failed) == 1

    db = sqlite3.connect(
        failed[0]
    )

    try:
        names = {
            row[0]
            for row in db.execute(
                """
                SELECT session_name
                FROM sessions
                """
            ).fetchall()
        }

    finally:
        db.close()

    assert "source-only" in names


def test_restore_migration_failure_rolls_back(
    tmp_path,
    monkeypatch,
):
    from q.store import StateStoreError

    live_path = tmp_path / "q.sqlite3"
    source_path = (
        tmp_path
        / "source-v1.sqlite3"
    )

    live = QStore(live_path)
    live.create_session(
        session_name="live-only",
        backend_thread_id="thread-live",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
    )
    live.close()

    _create_v1_database(
        source_path
    )

    def fail_migration(self):
        raise StateStoreError(
            "forced migration failure"
        )

    monkeypatch.setattr(
        QStore,
        "_migrate_1_to_2",
        fail_migration,
    )

    with pytest.raises(
        StateStoreError,
        match=(
            "automatic rollback "
            "succeeded"
        ),
    ):
        QStore.restore_backup(
            live_path=live_path,
            backup_path=source_path,
        )

    restored = QStore(live_path)

    assert (
        restored.get_session(
            "live-only"
        )
        is not None
    )

    restored.close()

    assert len(
        list(
            (
                tmp_path
                / "backups"
            ).glob(
                "failed-restore-"
                "*.sqlite3"
            )
        )
    ) == 1


def test_restore_reports_rollback_failure_and_keeps_diagnostic(
    tmp_path,
    monkeypatch,
):
    from q.store import StateStoreError

    live_path = tmp_path / "q.sqlite3"
    source_path = tmp_path / "source.sqlite3"

    live = QStore(live_path)
    live.create_session(
        session_name="live-only",
        backend_thread_id="thread-live",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
    )
    live.close()

    source = QStore(source_path)
    source.create_session(
        session_name="source-only",
        backend_thread_id="thread-source",
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=ToolsPolicy.ENABLED,
    )
    source.close()

    original_quick_check = (
        QStore.quick_check
    )

    def forced_quick_check(self):
        result = original_quick_check(
            self
        )

        if (
            self.get_session(
                "source-only"
            )
            is not None
        ):
            return {
                **result,
                "ok": False,
                "quick_check": [
                    "forced failure"
                ],
            }

        return result

    monkeypatch.setattr(
        QStore,
        "quick_check",
        forced_quick_check,
    )

    def fail_rollback(
        cls,
        *,
        rollback_path,
        live_path,
    ):
        raise StateStoreError(
            "forced rollback failure"
        )

    monkeypatch.setattr(
        QStore,
        "_restore_rollback_snapshot",
        classmethod(
            fail_rollback
        ),
    )

    with pytest.raises(
        StateStoreError,
    ) as caught:
        QStore.restore_backup(
            live_path=live_path,
            backup_path=source_path,
        )

    message = str(
        caught.value
    )

    assert (
        "forced failure"
        in message
    )
    assert (
        "automatic rollback also failed"
        in message
    )
    assert (
        "forced rollback failure"
        in message
    )
    assert (
        "failed restored image "
        "preserved at"
        in message
    )

    failed = list(
        (
            tmp_path / "backups"
        ).glob(
            "failed-restore-*.sqlite3"
        )
    )

    assert len(failed) == 1

    rollback = list(
        (
            tmp_path / "backups"
        ).glob(
            "pre-restore-*.sqlite3"
        )
    )

    assert len(rollback) == 1
