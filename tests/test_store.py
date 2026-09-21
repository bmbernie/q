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
