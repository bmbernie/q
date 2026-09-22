from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from pathlib import Path
from typing import Any


_MANAGED_KINDS = {
    "backup",
    "pre-restore",
    "pre-migrate",
}


@dataclass(
    frozen=True,
    slots=True,
)
class ManagedBackup:
    path: Path
    marker_path: Path
    kind: str
    created_at: datetime


@dataclass(
    frozen=True,
    slots=True,
)
class BackupCleanupResult:
    deleted: int = 0
    stale_markers_removed: int = 0
    protected: int = 0
    errors: int = 0


def _marker_path(
    backup_path: Path,
) -> Path:
    return backup_path.with_name(
        ".q-managed."
        + backup_path.name
        + ".json"
    )


def _timestamp(
    value: datetime,
) -> str:
    return value.astimezone(
        timezone.utc
    ).isoformat()


def register_managed_backup(
    backup_path: Path,
    *,
    kind: str,
    created_at: datetime,
) -> Path:
    if kind not in _MANAGED_KINDS:
        raise ValueError(
            f"invalid managed backup kind: "
            f"{kind}"
        )

    backup_path = (
        backup_path
        .expanduser()
        .resolve(strict=False)
    )

    if not backup_path.is_file():
        raise FileNotFoundError(
            backup_path
        )

    backup_dir = backup_path.parent

    backup_dir.mkdir(
        parents=True,
        exist_ok=True,
        mode=0o700,
    )
    backup_dir.chmod(0o700)

    marker = _marker_path(
        backup_path
    )

    payload = {
        "version": 1,
        "filename": (
            backup_path.name
        ),
        "kind": kind,
        "created_at": _timestamp(
            created_at
        ),
    }

    fd, raw_temp = tempfile.mkstemp(
        dir=backup_dir,
        prefix=".q-managed.",
        suffix=".tmp",
    )
    os.close(fd)

    temp_path = Path(raw_temp)

    try:
        os.chmod(
            temp_path,
            0o600,
        )

        temp_path.write_text(
            json.dumps(
                payload,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        os.chmod(
            temp_path,
            0o600,
        )

        os.replace(
            temp_path,
            marker,
        )

        os.chmod(
            marker,
            0o600,
        )

    finally:
        temp_path.unlink(
            missing_ok=True
        )

    return marker


def _load_marker(
    marker: Path,
) -> ManagedBackup | None:
    try:
        raw = json.loads(
            marker.read_text(
                encoding="utf-8"
            )
        )

    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ):
        return None

    if not isinstance(raw, dict):
        return None

    if raw.get("version") != 1:
        return None

    filename = raw.get(
        "filename"
    )
    kind = raw.get("kind")
    created_raw = raw.get(
        "created_at"
    )

    if (
        not isinstance(
            filename,
            str,
        )
        or not filename
        or "/" in filename
        or filename in {
            ".",
            "..",
        }
    ):
        return None

    if kind not in _MANAGED_KINDS:
        return None

    if not isinstance(
        created_raw,
        str,
    ):
        return None

    try:
        created_at = (
            datetime.fromisoformat(
                created_raw
            )
        )

    except ValueError:
        return None

    if (
        created_at.tzinfo
        is None
    ):
        return None

    backup_path = (
        marker.parent
        / filename
    ).resolve(
        strict=False
    )

    expected_marker = (
        _marker_path(
            backup_path
        )
    )

    if expected_marker != marker:
        return None

    return ManagedBackup(
        path=backup_path,
        marker_path=marker,
        kind=kind,
        created_at=(
            created_at.astimezone(
                timezone.utc
            )
        ),
    )


def list_managed_backups(
    backup_dir: Path,
) -> list[ManagedBackup]:
    backup_dir = (
        backup_dir
        .expanduser()
        .resolve(strict=False)
    )

    if not backup_dir.exists():
        return []

    items = []

    for marker in backup_dir.glob(
        ".q-managed.*.json"
    ):
        item = _load_marker(
            marker
        )

        if item is not None:
            items.append(item)

    return sorted(
        items,
        key=lambda item: (
            item.created_at,
            item.path.name,
        ),
    )


def cleanup_managed_backups(
    *,
    backup_dir: Path,
    retention_count: int,
    retention_days: int,
    now: datetime | None = None,
) -> BackupCleanupResult:
    if (
        retention_count <= 0
        and retention_days <= 0
    ):
        return BackupCleanupResult()

    if now is None:
        now = datetime.now(
            timezone.utc
        )

    now = now.astimezone(
        timezone.utc
    )

    items = list_managed_backups(
        backup_dir
    )

    stale_markers_removed = 0
    errors = 0

    existing = []

    for item in items:
        if item.path.is_file():
            existing.append(item)
            continue

        try:
            item.marker_path.unlink(
                missing_ok=True
            )

        except OSError:
            errors += 1

        else:
            stale_markers_removed += 1

    # Always preserve the newest recovery
    # point in each managed class.
    protected_paths: set[Path] = set()

    for kind in _MANAGED_KINDS:
        matches = [
            item
            for item in existing
            if item.kind == kind
        ]

        if matches:
            protected_paths.add(
                max(
                    matches,
                    key=lambda item: (
                        item.created_at,
                        item.path.name,
                    ),
                ).path
            )

    delete_paths: set[Path] = set()

    if retention_days > 0:
        cutoff = (
            now
            - timedelta(
                days=retention_days
            )
        )

        for item in existing:
            if (
                item.path
                not in protected_paths
                and item.created_at
                < cutoff
            ):
                delete_paths.add(
                    item.path
                )

    survivors = [
        item
        for item in existing
        if item.path
        not in delete_paths
    ]

    if (
        retention_count > 0
        and len(survivors)
        > retention_count
    ):
        deletable = sorted(
            (
                item
                for item
                in survivors
                if item.path
                not in protected_paths
            ),
            key=lambda item: (
                item.created_at,
                item.path.name,
            ),
        )

        excess = (
            len(survivors)
            - retention_count
        )

        for item in deletable[
            :excess
        ]:
            delete_paths.add(
                item.path
            )

    by_path = {
        item.path: item
        for item in existing
    }

    deleted = 0

    for path in sorted(
        delete_paths,
        key=str,
    ):
        item = by_path[path]

        try:
            path.unlink()

        except FileNotFoundError:
            pass

        except OSError:
            errors += 1
            continue

        try:
            item.marker_path.unlink(
                missing_ok=True
            )

        except OSError:
            errors += 1

        deleted += 1

    return BackupCleanupResult(
        deleted=deleted,
        stale_markers_removed=(
            stale_markers_removed
        ),
        protected=len(
            protected_paths
        ),
        errors=errors,
    )
