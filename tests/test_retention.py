from datetime import timedelta

from q.models import (
    QueryState,
    ToolsPolicy,
)
from q.retention import cleanup_query_logs
from q.store import QStore


def create_query(
    *,
    store,
    state_dir,
    query_id,
):
    path = (
        state_dir
        / "queries"
        / f"{query_id}.ndjson"
    )

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    path.write_text(
        "retained\n",
        encoding="utf-8",
    )

    store.create_query(
        query_id=query_id,
        session_name=None,
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=(
            ToolsPolicy.ENABLED
        ),
        prompt_metadata={
            "input": "test",
            "cwd": "/tmp",
            "sandbox": "read-only",
        },
        log_path=str(path),
        session_log_path=None,
        detached=False,
        stdout_attached=True,
    )

    return path


def complete_query(
    store,
    query_id,
):
    store.transition_query(
        query_id,
        QueryState.RUNNING,
    )

    return store.transition_query(
        query_id,
        QueryState.COMPLETED,
    )


def test_old_terminal_query_log_is_deleted(
    tmp_path,
):
    store = QStore(
        tmp_path / "q.sqlite3"
    )
    state_dir = tmp_path / "state"

    path = create_query(
        store=store,
        state_dir=state_dir,
        query_id="q-old",
    )

    query = complete_query(
        store,
        "q-old",
    )

    assert (
        query.completed_at
        is not None
    )

    result = cleanup_query_logs(
        store=store,
        state_dir=state_dir,
        retention_days=30,
        now=(
            query.completed_at
            + timedelta(days=31)
        ),
    )

    assert result.deleted == 1
    assert not path.exists()

    # Durable query metadata is preserved.
    saved = store.get_query(
        "q-old"
    )

    assert saved is not None
    assert (
        saved.state
        == QueryState.COMPLETED
    )

    store.close()


def test_recent_terminal_log_is_retained(
    tmp_path,
):
    store = QStore(
        tmp_path / "q.sqlite3"
    )
    state_dir = tmp_path / "state"

    path = create_query(
        store=store,
        state_dir=state_dir,
        query_id="q-recent",
    )

    query = complete_query(
        store,
        "q-recent",
    )

    assert (
        query.completed_at
        is not None
    )

    result = cleanup_query_logs(
        store=store,
        state_dir=state_dir,
        retention_days=30,
        now=(
            query.completed_at
            + timedelta(days=29)
        ),
    )

    assert result.deleted == 0
    assert path.exists()

    store.close()


def test_running_query_log_is_never_deleted(
    tmp_path,
):
    store = QStore(
        tmp_path / "q.sqlite3"
    )
    state_dir = tmp_path / "state"

    path = create_query(
        store=store,
        state_dir=state_dir,
        query_id="q-running",
    )

    query = store.transition_query(
        "q-running",
        QueryState.RUNNING,
    )

    result = cleanup_query_logs(
        store=store,
        state_dir=state_dir,
        retention_days=1,
        now=(
            query.started_at
            + timedelta(days=365)
        ),
    )

    assert result.deleted == 0
    assert path.exists()

    store.close()


def test_missing_old_log_is_idempotent(
    tmp_path,
):
    store = QStore(
        tmp_path / "q.sqlite3"
    )
    state_dir = tmp_path / "state"

    path = create_query(
        store=store,
        state_dir=state_dir,
        query_id="q-missing",
    )

    query = complete_query(
        store,
        "q-missing",
    )

    path.unlink()

    result = cleanup_query_logs(
        store=store,
        state_dir=state_dir,
        retention_days=30,
        now=(
            query.completed_at
            + timedelta(days=31)
        ),
    )

    assert result.missing == 1
    assert result.errors == 0

    store.close()


def test_cleanup_rejects_unsafe_log_path(
    tmp_path,
):
    store = QStore(
        tmp_path / "q.sqlite3"
    )
    state_dir = tmp_path / "state"

    outside = (
        tmp_path / "outside.ndjson"
    )

    outside.write_text(
        "do not delete\n",
        encoding="utf-8",
    )

    store.create_query(
        query_id="q-unsafe",
        session_name=None,
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        tools_policy=(
            ToolsPolicy.ENABLED
        ),
        prompt_metadata={
            "input": "test",
            "cwd": "/tmp",
            "sandbox": "read-only",
        },
        log_path=str(outside),
        session_log_path=None,
        detached=False,
        stdout_attached=True,
    )

    query = complete_query(
        store,
        "q-unsafe",
    )

    result = cleanup_query_logs(
        store=store,
        state_dir=state_dir,
        retention_days=30,
        now=(
            query.completed_at
            + timedelta(days=31)
        ),
    )

    assert (
        result.skipped_unsafe
        == 1
    )
    assert outside.exists()

    store.close()


def test_zero_retention_disables_cleanup(
    tmp_path,
):
    store = QStore(
        tmp_path / "q.sqlite3"
    )
    state_dir = tmp_path / "state"

    path = create_query(
        store=store,
        state_dir=state_dir,
        query_id="q-disabled",
    )

    complete_query(
        store,
        "q-disabled",
    )

    result = cleanup_query_logs(
        store=store,
        state_dir=state_dir,
        retention_days=0,
    )

    assert result.deleted == 0
    assert path.exists()

    store.close()
