from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import TextIO

from .ipc import (
    IPCClient,
    IPCRemoteError,
    default_socket_path,
)


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

        if (
            wrote
            and not last_delta.endswith(
                "\n"
            )
        ):
            stdout.write("\n")
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
