from datetime import (
    datetime,
    timedelta,
    timezone,
)

from q.backup_retention import (
    cleanup_managed_backups,
    list_managed_backups,
    register_managed_backup,
)


def create_managed(
    backup_dir,
    *,
    name,
    kind,
    created_at,
):
    backup_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    path = backup_dir / name

    path.write_bytes(
        b"sqlite snapshot"
    )

    register_managed_backup(
        path,
        kind=kind,
        created_at=created_at,
    )

    return path


def test_custom_unmarked_backup_is_never_deleted(
    tmp_path,
):
    backup_dir = (
        tmp_path / "backups"
    )
    backup_dir.mkdir()

    custom = (
        backup_dir
        / "q-20000101-000000.sqlite3"
    )

    custom.write_bytes(
        b"custom"
    )

    result = cleanup_managed_backups(
        backup_dir=backup_dir,
        retention_count=1,
        retention_days=1,
        now=datetime(
            2026,
            9,
            22,
            tzinfo=timezone.utc,
        ),
    )

    assert result.deleted == 0
    assert custom.exists()


def test_age_cleanup_preserves_newest_of_each_kind(
    tmp_path,
):
    backup_dir = (
        tmp_path / "backups"
    )
    now = datetime(
        2026,
        9,
        22,
        tzinfo=timezone.utc,
    )

    paths = []

    for kind in (
        "backup",
        "pre-restore",
        "pre-migrate",
    ):
        old = create_managed(
            backup_dir,
            name=(
                f"{kind}-old.sqlite3"
            ),
            kind=kind,
            created_at=(
                now
                - timedelta(days=200)
            ),
        )

        newest = create_managed(
            backup_dir,
            name=(
                f"{kind}-new.sqlite3"
            ),
            kind=kind,
            created_at=(
                now
                - timedelta(days=100)
            ),
        )

        paths.append(
            (old, newest)
        )

    result = cleanup_managed_backups(
        backup_dir=backup_dir,
        retention_count=0,
        retention_days=90,
        now=now,
    )

    assert result.deleted == 3
    assert result.protected == 3

    for old, newest in paths:
        assert not old.exists()
        assert newest.exists()


def test_count_cleanup_keeps_newest_total_set(
    tmp_path,
):
    backup_dir = (
        tmp_path / "backups"
    )
    now = datetime(
        2026,
        9,
        22,
        tzinfo=timezone.utc,
    )

    paths = []

    for index in range(6):
        path = create_managed(
            backup_dir,
            name=(
                "q-test-"
                f"{index}.sqlite3"
            ),
            kind="backup",
            created_at=(
                now
                - timedelta(
                    hours=(6 - index)
                )
            ),
        )
        paths.append(path)

    result = cleanup_managed_backups(
        backup_dir=backup_dir,
        retention_count=3,
        retention_days=0,
        now=now,
    )

    assert result.deleted == 3

    assert not paths[0].exists()
    assert not paths[1].exists()
    assert not paths[2].exists()

    assert paths[3].exists()
    assert paths[4].exists()
    assert paths[5].exists()


def test_stale_marker_is_removed(
    tmp_path,
):
    backup_dir = (
        tmp_path / "backups"
    )

    path = create_managed(
        backup_dir,
        name="q-stale.sqlite3",
        kind="backup",
        created_at=datetime(
            2026,
            9,
            1,
            tzinfo=timezone.utc,
        ),
    )

    path.unlink()

    result = cleanup_managed_backups(
        backup_dir=backup_dir,
        retention_count=20,
        retention_days=90,
    )

    assert (
        result.stale_markers_removed
        == 1
    )

    assert (
        list_managed_backups(
            backup_dir
        )
        == []
    )


def test_both_zero_disables_cleanup(
    tmp_path,
):
    backup_dir = (
        tmp_path / "backups"
    )

    path = create_managed(
        backup_dir,
        name="q-disabled.sqlite3",
        kind="backup",
        created_at=datetime(
            2000,
            1,
            1,
            tzinfo=timezone.utc,
        ),
    )

    result = cleanup_managed_backups(
        backup_dir=backup_dir,
        retention_count=0,
        retention_days=0,
    )

    assert result.deleted == 0
    assert path.exists()
