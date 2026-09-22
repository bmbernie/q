from __future__ import annotations

from dataclasses import dataclass
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from pathlib import Path

from .store import QStore


@dataclass(
    frozen=True,
    slots=True,
)
class QueryLogCleanupResult:
    deleted: int = 0
    missing: int = 0
    skipped_unsafe: int = 0
    errors: int = 0


def cleanup_query_logs(
    *,
    store: QStore,
    state_dir: Path,
    retention_days: int,
    now: datetime | None = None,
) -> QueryLogCleanupResult:
    if retention_days <= 0:
        return QueryLogCleanupResult()

    if now is None:
        now = datetime.now(
            timezone.utc
        )

    cutoff = now - timedelta(
        days=retention_days
    )

    query_dir = (
        state_dir.expanduser()
        / "queries"
    ).resolve(
        strict=False
    )

    deleted = 0
    missing = 0
    skipped_unsafe = 0
    errors = 0

    for query in (
        store.list_queries_completed_before(
            cutoff
        )
    ):
        path = Path(
            query.log_path
        ).expanduser()

        # Never trust a durable path blindly.
        # Retention is permitted to unlink only
        # the canonical per-query file.
        try:
            parent = path.parent.resolve(
                strict=False
            )

        except OSError:
            skipped_unsafe += 1
            continue

        if (
            parent != query_dir
            or path.name
            != f"{query.query_id}.ndjson"
        ):
            skipped_unsafe += 1
            continue

        try:
            path.unlink()

        except FileNotFoundError:
            missing += 1

        except OSError:
            errors += 1

        else:
            deleted += 1

    return QueryLogCleanupResult(
        deleted=deleted,
        missing=missing,
        skipped_unsafe=skipped_unsafe,
        errors=errors,
    )
