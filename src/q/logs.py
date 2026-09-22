from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from pathlib import Path


def archive_path(
    path: Path,
    index: int,
) -> Path:
    if index < 1:
        raise ValueError(
            "archive index must be >= 1"
        )

    return path.with_name(
        f"{path.name}.{index}"
    )


def history_paths(
    path: Path,
    *,
    archives: int,
) -> list[Path]:
    paths = []

    for index in range(
        archives,
        0,
        -1,
    ):
        candidate = archive_path(
            path,
            index,
        )

        if candidate.exists():
            paths.append(candidate)

    if path.exists():
        paths.append(path)

    return paths


def rotate_if_needed(
    path: Path,
    *,
    incoming_bytes: int,
    rotate_bytes: int,
    archives: int,
) -> None:
    if (
        rotate_bytes <= 0
        or archives <= 0
    ):
        return

    try:
        current_size = (
            path.stat().st_size
        )

    except FileNotFoundError:
        return

    if (
        current_size == 0
        or current_size
        + incoming_bytes
        <= rotate_bytes
    ):
        return

    oldest = archive_path(
        path,
        archives,
    )

    oldest.unlink(
        missing_ok=True
    )

    for index in range(
        archives - 1,
        0,
        -1,
    ):
        source = archive_path(
            path,
            index,
        )

        if source.exists():
            source.replace(
                archive_path(
                    path,
                    index + 1,
                )
            )

    path.replace(
        archive_path(
            path,
            1,
        )
    )


def append_log_line(
    path: Path,
    line: str,
    *,
    rotate_bytes: int = 0,
    archives: int = 0,
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    encoded_size = len(
        line.encode("utf-8")
    )

    rotate_if_needed(
        path,
        incoming_bytes=encoded_size,
        rotate_bytes=rotate_bytes,
        archives=archives,
    )

    with path.open(
        "a",
        encoding="utf-8",
    ) as stream:
        stream.write(line)


async def stream_log_lines(
    path: Path,
    *,
    archives: int,
    follow: bool,
    poll_interval: float = 0.2,
) -> AsyncIterator[str]:
    current_offset = 0
    current_identity = None

    for history_path in history_paths(
        path,
        archives=archives,
    ):
        try:
            stream = history_path.open(
                "r",
                encoding="utf-8",
            )

        except FileNotFoundError:
            continue

        with stream:
            stat = os.fstat(
                stream.fileno()
            )

            is_current = (
                history_path == path
            )

            if is_current:
                current_identity = (
                    stat.st_dev,
                    stat.st_ino,
                )

            while True:
                line = stream.readline()

                if not line:
                    break

                if is_current:
                    current_offset = (
                        stream.tell()
                    )

                yield line.rstrip(
                    "\n"
                )

            if is_current:
                current_offset = (
                    stream.tell()
                )

    if not follow:
        return

    while True:
        await asyncio.sleep(
            poll_interval
        )

        try:
            stream = path.open(
                "r",
                encoding="utf-8",
            )

        except FileNotFoundError:
            continue

        with stream:
            stat = os.fstat(
                stream.fileno()
            )

            identity = (
                stat.st_dev,
                stat.st_ino,
            )

            if (
                current_identity
                != identity
                or stat.st_size
                < current_offset
            ):
                current_offset = 0

            current_identity = identity

            stream.seek(
                current_offset
            )

            while True:
                line = stream.readline()

                if not line:
                    break

                current_offset = (
                    stream.tell()
                )

                yield line.rstrip(
                    "\n"
                )
