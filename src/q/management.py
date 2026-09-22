from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys
from typing import TextIO

from . import __version__ as CLIENT_VERSION
from .doctor import local_doctor
from .ipc import (
    IPCClient,
    IPCRemoteError,
    default_socket_path,
)
from .paths import (
    QPaths,
    resolve_paths,
)


DOCTOR_IPC_TIMEOUT_SECONDS = 5.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qd",
        description=(
            "Inspect and control the q daemon."
        ),
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    status = subparsers.add_parser(
        "status",
        help="show query status",
    )
    status.add_argument(
        "query_id",
    )

    show = subparsers.add_parser(
        "show",
        help="show complete query metadata",
    )
    show.add_argument(
        "query_id",
    )

    queries = subparsers.add_parser(
        "queries",
        help="list queries",
    )
    queries.add_argument(
        "-s",
        "--session",
        dest="session_name",
    )
    queries.add_argument(
        "--running",
        action="store_true",
    )

    subparsers.add_parser(
        "sessions",
        help="list named sessions",
    )

    subparsers.add_parser(
        "db-check",
        help="check q state database health",
    )

    subparsers.add_parser(
        "doctor",
        help=(
            "run q operational diagnostics"
        ),
    )

    restore = subparsers.add_parser(
        "db-restore",
        help=(
            "restore q state while qd "
            "is stopped"
        ),
    )
    restore.add_argument(
        "backup",
    )

    db_backup = subparsers.add_parser(
        "db-backup",
        help="create a consistent q database backup",
    )
    db_backup.add_argument(
        "path",
        nargs="?",
        help=(
            "destination path; defaults to "
            "~/.local/share/q/backups/"
        ),
    )

    logs = subparsers.add_parser(
        "logs",
        help="show durable q event logs",
    )
    logs.add_argument(
        "-f",
        "--follow",
        action="store_true",
        help="follow appended log events",
    )
    logs.add_argument(
        "-s",
        "--session",
        dest="session_name",
        help="show one named session log",
    )

    follow = subparsers.add_parser(
        "follow",
        help="follow query output",
    )
    follow.add_argument(
        "query_id",
    )

    cancel = subparsers.add_parser(
        "cancel",
        help="cancel a query",
    )
    cancel.add_argument(
        "query_id",
    )

    return parser


def _json(
    stdout: TextIO,
    value,
) -> None:
    json.dump(
        value,
        stdout,
        indent=2,
        sort_keys=True,
    )
    stdout.write("\n")
    stdout.flush()


async def run_management(
    *,
    args: argparse.Namespace,
    client: IPCClient,
    stdout: TextIO,
    paths: QPaths | None = None,
) -> int:
    command = args.command

    if command == "status":
        result = await client.request(
            "status",
            {
                "query_id": args.query_id,
            },
        )
        _json(stdout, result)
        return 0

    if command == "show":
        result = await client.request(
            "show",
            {
                "query_id": args.query_id,
            },
        )
        _json(stdout, result)
        return 0

    if command == "queries":
        params = {
            "running": args.running,
        }

        if args.session_name is not None:
            params["session_name"] = (
                args.session_name
            )

        result = await client.request(
            "queries",
            params,
        )
        _json(stdout, result)
        return 0

    if command == "sessions":
        result = await client.request(
            "sessions"
        )
        _json(stdout, result)
        return 0

    if command == "doctor":
        if paths is None:
            paths = resolve_paths()

        local = local_doctor(
            paths
        )

        ipc: dict = {
            "ok": False,
        }

        remote = None

        try:
            remote = await asyncio.wait_for(
                client.request(
                    "doctor"
                ),
                timeout=(
                    DOCTOR_IPC_TIMEOUT_SECONDS
                ),
            )

        except TimeoutError:
            ipc.update(
                {
                    "timed_out": True,
                    "timeout_seconds": (
                        DOCTOR_IPC_TIMEOUT_SECONDS
                    ),
                    "error": (
                        "daemon doctor IPC timed "
                        "out after "
                        f"{DOCTOR_IPC_TIMEOUT_SECONDS:g} "
                        "seconds"
                    ),
                }
            )

        except Exception as exc:
            ipc["error"] = (
                f"{type(exc).__name__}: "
                f"{exc}"
            )

        else:
            ipc["ok"] = True
            ipc["timed_out"] = False
            ipc["timeout_seconds"] = (
                DOCTOR_IPC_TIMEOUT_SECONDS
            )

        if isinstance(
            remote,
            dict,
        ):
            database = remote.get(
                "database"
            )
            broker = remote.get(
                "broker"
            )
            remote_version = (
                remote.get(
                    "version"
                )
            )

        else:
            database = None
            broker = None
            remote_version = None

        if not isinstance(
            database,
            dict,
        ):
            database = {
                "ok": False,
                "available": False,
                "error": (
                    "database diagnostics "
                    "unavailable"
                ),
            }

        if not isinstance(
            broker,
            dict,
        ):
            broker = {
                "ok": False,
                "available": False,
                "error": (
                    "broker diagnostics "
                    "unavailable"
                ),
            }

        daemon_version = None

        if isinstance(
            remote_version,
            dict,
        ):
            candidate = (
                remote_version.get(
                    "daemon"
                )
            )

            if isinstance(
                candidate,
                str,
            ) and candidate:
                daemon_version = (
                    candidate
                )

        version = {
            "ok": (
                daemon_version
                == CLIENT_VERSION
            ),
            "client": CLIENT_VERSION,
            "daemon": daemon_version,
            "match": (
                daemon_version
                == CLIENT_VERSION
            ),
        }

        if daemon_version is None:
            version["error"] = (
                "daemon version unavailable"
            )

        result = {
            "ok": (
                local["ok"]
                and ipc["ok"]
                and version["ok"]
                and database.get(
                    "ok"
                )
                is True
                and broker.get(
                    "ok"
                )
                is True
            ),
            "version": version,
            "configuration": (
                local[
                    "configuration"
                ]
            ),
            "filesystem": (
                local["filesystem"]
            ),
            "daemon": {
                "socket": (
                    local["socket"]
                ),
                "ipc": ipc,
            },
            "database": database,
            "broker": broker,
        }

        _json(
            stdout,
            result,
        )

        return (
            0
            if result["ok"]
            else 1
        )

    if command == "db-check":
        result = await client.request(
            "db_check"
        )
        _json(stdout, result)
        return (
            0
            if result.get("ok") is True
            else 1
        )

    if command == "db-backup":
        params = {}

        if args.path is not None:
            params["path"] = str(
                Path(args.path)
                .expanduser()
                .resolve(strict=False)
            )

        result = await client.request(
            "db_backup",
            params,
        )
        _json(stdout, result)
        return (
            0
            if result.get("ok") is True
            else 1
        )

    if command == "logs":
        params = {
            "follow": args.follow,
        }

        if args.session_name is not None:
            params["session_name"] = (
                args.session_name
            )

        async for frame in client.frames(
            "logs",
            params,
        ):
            if (
                frame.type == "event"
                and frame.event == "log"
            ):
                line = frame.data.get(
                    "line",
                    "",
                )

                if isinstance(line, str):
                    stdout.write(
                        line + "\n"
                    )
                    stdout.flush()

        return 0

    if command == "cancel":
        result = await client.request(
            "cancel",
            {
                "query_id": args.query_id,
            },
        )
        _json(stdout, result)
        return 0

    if command == "follow":
        last_delta = ""
        wrote = False
        notice = None

        async for frame in client.frames(
            "follow",
            {
                "query_id": args.query_id,
            },
        ):
            if (
                frame.type == "event"
                and frame.event == "delta"
            ):
                delta = frame.data.get(
                    "delta",
                    "",
                )

                if isinstance(delta, str):
                    stdout.write(delta)
                    stdout.flush()
                    last_delta = delta
                    wrote = True

            elif (
                frame.type == "event"
                and frame.event == "notice"
            ):
                message = frame.data.get(
                    "message"
                )

                if isinstance(
                    message,
                    str,
                ):
                    notice = message

        if (
            wrote
            and not last_delta.endswith(
                "\n"
            )
        ):
            stdout.write("\n")
            stdout.flush()

        elif (
            not wrote
            and notice is not None
        ):
            stdout.write(
                f"qd: {notice}\n"
            )
            stdout.flush()

        return 0

    raise RuntimeError(
        f"unsupported command {command!r}"
    )


async def _async_main(
    args: argparse.Namespace,
) -> int:
    client = IPCClient(
        default_socket_path()
    )

    return await run_management(
        args=args,
        client=client,
        stdout=sys.stdout,
        paths=resolve_paths(),
    )


def main(
    argv: list[str] | None = None,
) -> None:
    parser = build_parser()
    args = parser.parse_args(
        argv
    )

    try:
        status = asyncio.run(
            _async_main(args)
        )

    except IPCRemoteError as exc:
        print(
            (
                f"qd: {exc.code}: "
                f"{exc.message}"
            ),
            file=sys.stderr,
        )
        raise SystemExit(1) from exc

    except (
        FileNotFoundError,
        ConnectionRefusedError,
    ) as exc:
        print(
            "qd: daemon is not running",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc

    except OSError as exc:
        print(
            f"qd: IPC failure: {exc}",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc

    raise SystemExit(status)
