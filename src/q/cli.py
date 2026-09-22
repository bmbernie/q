from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import TextIO

from .ipc import (
    IPCClient,
    IPCRemoteError,
    default_socket_path,
)


class CLIError(RuntimeError):
    pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="q",
        description=(
            "Submit a query to the local qd daemon."
        ),
    )

    parser.add_argument(
        "prompt",
        nargs="*",
        help="query text",
    )

    parser.add_argument(
        "-s",
        "--session",
        dest="session_name",
        help="named persistent session",
    )

    parser.add_argument(
        "-r",
        "--reasoning",
        dest="reasoning_effort",
        help="reasoning effort, e.g. high or xhigh",
    )

    parser.add_argument(
        "-m",
        "--model",
        help="Codex model override",
    )

    parser.add_argument(
        "-f",
        "--file",
        dest="files",
        action="append",
        default=[],
        metavar="PATH",
        help=(
            "include a UTF-8 text file; "
            "may be specified more than once"
        ),
    )

    parser.add_argument(
        "-q",
        dest="quiet",
        action="count",
        default=0,
        help=(
            "detach and print query ID; "
            "-qq detaches silently"
        ),
    )

    parser.add_argument(
        "--no-stream",
        action="store_true",
        help=(
            "buffer the answer and print it "
            "after completion"
        ),
    )

    parser.add_argument(
        "--cwd",
        default=None,
        help=(
            "working directory sent to Codex; "
            "defaults to current directory"
        ),
    )

    parser.add_argument(
        "--sandbox",
        choices=(
            "read-only",
            "workspace-write",
            "danger-full-access",
        ),
        default=None,
    )

    tools = parser.add_mutually_exclusive_group()

    tools.add_argument(
        "--tools",
        dest="tools",
        action="store_const",
        const="enabled",
        help="force tools enabled",
    )

    tools.add_argument(
        "--no-tools",
        dest="tools",
        action="store_const",
        const="disabled",
        help="force tools disabled",
    )

    return parser


def _read_text_file(
    raw_path: str,
) -> tuple[str, str]:
    path = Path(
        raw_path
    ).expanduser()

    try:
        resolved = path.resolve(
            strict=True
        )
    except FileNotFoundError as exc:
        raise CLIError(
            f"file not found: {path}"
        ) from exc

    if not resolved.is_file():
        raise CLIError(
            f"not a regular file: {resolved}"
        )

    try:
        content = resolved.read_text(
            encoding="utf-8"
        )
    except UnicodeDecodeError as exc:
        raise CLIError(
            f"file is not UTF-8 text: {resolved}"
        ) from exc

    except OSError as exc:
        raise CLIError(
            f"cannot read {resolved}: {exc}"
        ) from exc

    return str(resolved), content


def build_input(
    *,
    prompt_parts: list[str],
    file_paths: list[str],
    stdin: TextIO,
) -> str:
    prompt = " ".join(
        prompt_parts
    ).strip()

    stdin_text = ""

    if not stdin.isatty():
        stdin_text = stdin.read()

    files = [
        _read_text_file(path)
        for path in file_paths
    ]

    # Preserve input exactly when there is only one source.
    if (
        prompt
        and not stdin_text
        and not files
    ):
        return prompt

    if (
        stdin_text
        and not prompt
        and not files
    ):
        return stdin_text

    if (
        len(files) == 1
        and not prompt
        and not stdin_text
    ):
        return files[0][1]

    sections: list[str] = []

    if prompt:
        sections.append(prompt)

    if stdin_text:
        sections.append(
            "----- stdin -----\n"
            + stdin_text
        )

    for path, content in files:
        sections.append(
            f"----- file: {path} -----\n"
            + content
        )

    return "\n\n".join(
        sections
    )


def _write_answer(
    stream: TextIO,
    text: str,
) -> None:
    stream.write(text)

    if text and not text.endswith(
        "\n"
    ):
        stream.write("\n")

    stream.flush()


async def run_query(
    *,
    args: argparse.Namespace,
    client: IPCClient,
    stdin: TextIO,
    stdout: TextIO,
) -> int:
    input_text = build_input(
        prompt_parts=args.prompt,
        file_paths=args.files,
        stdin=stdin,
    )

    if not input_text:
        raise CLIError(
            "no query input provided"
        )

    cwd = Path(
        args.cwd
        if args.cwd is not None
        else os.getcwd()
    ).expanduser()

    try:
        cwd = cwd.resolve(
            strict=True
        )
    except FileNotFoundError as exc:
        raise CLIError(
            f"working directory does not exist: {cwd}"
        ) from exc

    if not cwd.is_dir():
        raise CLIError(
            f"working directory is not a directory: {cwd}"
        )

    detached = (
        args.quiet >= 1
    )

    params = {
        "input": input_text,
        "cwd": str(cwd),
        "detached": detached,
        "stdout_attached": (
            not detached
        ),
    }

    if args.session_name is not None:
        params["session_name"] = (
            args.session_name
        )

    if args.reasoning_effort is not None:
        params["reasoning_effort"] = (
            args.reasoning_effort
        )

    if args.model is not None:
        params["model"] = args.model

    if args.tools is not None:
        params["tools"] = args.tools

    if args.sandbox is not None:
        params["sandbox"] = (
            args.sandbox
        )

    if detached:
        result = await client.request(
            "submit",
            params,
        )

        if args.quiet == 1:
            stdout.write(
                result["query_id"]
                + "\n"
            )
            stdout.flush()

        return 0

    deltas: list[str] = []
    final = None
    wrote_stream = False
    last_delta = ""

    async for frame in client.frames(
        "submit",
        params,
    ):
        if (
            frame.type == "event"
            and frame.event == "delta"
        ):
            delta = frame.data.get(
                "delta",
                ""
            )

            if not isinstance(
                delta,
                str,
            ):
                continue

            deltas.append(delta)
            last_delta = delta

            if not args.no_stream:
                stdout.write(delta)
                stdout.flush()
                wrote_stream = True

        elif frame.type == "result":
            final = frame.result

    if final is None:
        raise CLIError(
            "qd returned no final result"
        )

    if args.no_stream:
        _write_answer(
            stdout,
            "".join(deltas),
        )

    elif (
        wrote_stream
        and not last_delta.endswith(
            "\n"
        )
    ):
        stdout.write("\n")
        stdout.flush()

    state = final.get("state")

    if state != "completed":
        raise CLIError(
            "query ended in state "
            f"{state!r}"
        )

    return 0


async def _async_main(
    args: argparse.Namespace,
) -> int:
    client = IPCClient(
        default_socket_path()
    )

    return await run_query(
        args=args,
        client=client,
        stdin=sys.stdin,
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

    except CLIError as exc:
        print(
            f"q: {exc}",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc

    except IPCRemoteError as exc:
        print(
            (
                f"q: {exc.code}: "
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
            "q: qd is not running",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc

    except OSError as exc:
        print(
            f"q: IPC failure: {exc}",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc

    raise SystemExit(status)
