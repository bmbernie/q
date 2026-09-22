from __future__ import annotations

import argparse
import getpass
import json
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


def _safe_interaction_result(
    method: str,
    params: dict,
) -> dict:
    if method in {
        "item/commandExecution/requestApproval",
        "item/fileChange/requestApproval",
    }:
        return {
            "decision": "decline",
        }

    if method == "mcpServer/elicitation/request":
        return {
            "action": "decline",
        }

    if method == "item/permissions/requestApproval":
        return {
            "permissions": {},
            "scope": "turn",
        }

    if method == "item/tool/requestUserInput":
        answers = {}

        questions = params.get(
            "questions",
            [],
        )

        if isinstance(
            questions,
            list,
        ):
            for question in questions:
                if not isinstance(
                    question,
                    dict,
                ):
                    continue

                question_id = question.get(
                    "id"
                )

                if isinstance(
                    question_id,
                    str,
                ) and question_id:
                    answers[
                        question_id
                    ] = {
                        "answers": [],
                    }

        return {
            "answers": answers,
        }

    raise CLIError(
        "unsupported interactive request "
        f"{method!r}"
    )


def _prompt_line(
    input_stream: TextIO,
    output_stream: TextIO,
    prompt: str,
    *,
    secret: bool = False,
) -> str:
    if secret:
        # getpass uses /dev/tty when available.
        return getpass.getpass(
            prompt,
            stream=output_stream,
        )

    output_stream.write(prompt)
    output_stream.flush()

    line = input_stream.readline()

    if line == "":
        raise CLIError(
            "interactive input closed"
        )

    return line.rstrip("\n")


def _approval_result(
    *,
    input_stream: TextIO,
    output_stream: TextIO,
    title: str,
    details: list[tuple[str, object]],
) -> dict:
    output_stream.write(
        f"\n[q interaction] {title}\n"
    )

    for label, value in details:
        if value is None:
            continue

        output_stream.write(
            f"  {label}: {value}\n"
        )

    output_stream.write(
        "  [y] accept once\n"
        "  [s] accept for session\n"
        "  [n] decline\n"
        "  [c] cancel\n"
    )
    output_stream.flush()

    while True:
        choice = _prompt_line(
            input_stream,
            output_stream,
            "choice [n]: ",
        ).strip().lower()

        if choice in {
            "",
            "n",
            "no",
            "decline",
        }:
            return {
                "decision": "decline",
            }

        if choice in {
            "y",
            "yes",
            "accept",
        }:
            return {
                "decision": "accept",
            }

        if choice in {
            "s",
            "session",
        }:
            return {
                "decision": (
                    "acceptForSession"
                ),
            }

        if choice in {
            "c",
            "cancel",
        }:
            return {
                "decision": "cancel",
            }

        output_stream.write(
            "invalid choice\n"
        )


def _tool_input_result(
    *,
    input_stream: TextIO,
    output_stream: TextIO,
    params: dict,
) -> dict:
    questions = params.get(
        "questions",
        [],
    )

    if not isinstance(
        questions,
        list,
    ):
        raise CLIError(
            "tool input questions are invalid"
        )

    answers = {}

    output_stream.write(
        "\n[q interaction] input requested\n"
    )
    output_stream.flush()

    for question in questions:
        if not isinstance(
            question,
            dict,
        ):
            continue

        question_id = question.get(
            "id"
        )

        if (
            not isinstance(
                question_id,
                str,
            )
            or not question_id
        ):
            raise CLIError(
                "tool input question "
                "is missing id"
            )

        header = question.get(
            "header",
            question_id,
        )
        prompt_text = question.get(
            "question",
            question_id,
        )
        options = question.get(
            "options"
        )

        output_stream.write(
            f"\n{header}\n"
            f"{prompt_text}\n"
        )

        option_labels = []

        if isinstance(
            options,
            list,
        ) and options:
            for index, option in enumerate(
                options,
                start=1,
            ):
                if not isinstance(
                    option,
                    dict,
                ):
                    continue

                label = str(
                    option.get(
                        "label",
                        index,
                    )
                )
                description = str(
                    option.get(
                        "description",
                        "",
                    )
                )

                option_labels.append(label)

                output_stream.write(
                    f"  {index}. {label}"
                )

                if description:
                    output_stream.write(
                        f" — {description}"
                    )

                output_stream.write(
                    "\n"
                )

        output_stream.flush()

        raw = _prompt_line(
            input_stream,
            output_stream,
            "answer: ",
            secret=bool(
                question.get(
                    "isSecret",
                    False,
                )
            ),
        ).strip()

        selected = raw

        if (
            option_labels
            and raw.isdigit()
        ):
            index = int(raw)

            if (
                1
                <= index
                <= len(option_labels)
            ):
                selected = option_labels[
                    index - 1
                ]

        values = (
            []
            if selected == ""
            else [selected]
        )

        answers[
            question_id
        ] = {
            "answers": values,
        }

    return {
        "answers": answers,
    }


def _mcp_elicitation_result(
    *,
    input_stream: TextIO,
    output_stream: TextIO,
    params: dict,
) -> dict:
    message = params.get(
        "message",
        "MCP server requests input",
    )
    mode = params.get(
        "mode"
    )
    url = params.get(
        "url"
    )

    output_stream.write(
        "\n[q interaction] MCP elicitation\n"
        f"  message: {message}\n"
    )

    if mode is not None:
        output_stream.write(
            f"  mode: {mode}\n"
        )

    if url is not None:
        output_stream.write(
            f"  url: {url}\n"
        )

    output_stream.write(
        "  [a] accept\n"
        "  [d] decline\n"
        "  [c] cancel\n"
    )
    output_stream.flush()

    while True:
        choice = _prompt_line(
            input_stream,
            output_stream,
            "choice [d]: ",
        ).strip().lower()

        if choice in {
            "",
            "d",
            "decline",
            "n",
            "no",
        }:
            return {
                "action": "decline",
            }

        if choice in {
            "c",
            "cancel",
        }:
            return {
                "action": "cancel",
            }

        if choice in {
            "a",
            "accept",
            "y",
            "yes",
        }:
            result = {
                "action": "accept",
            }

            if mode in {
                "form",
                "openai/form",
                "openaiForm",
            }:
                raw = _prompt_line(
                    input_stream,
                    output_stream,
                    (
                        "content JSON object "
                        "[{}]: "
                    ),
                ).strip()

                if raw == "":
                    content = {}
                else:
                    try:
                        content = json.loads(
                            raw
                        )
                    except json.JSONDecodeError as exc:
                        raise CLIError(
                            "invalid elicitation "
                            "content JSON"
                        ) from exc

                    if not isinstance(
                        content,
                        dict,
                    ):
                        raise CLIError(
                            "elicitation content "
                            "must be a JSON object"
                        )

                result["content"] = (
                    content
                )

            return result

        output_stream.write(
            "invalid choice\n"
        )


def _permissions_result(
    *,
    input_stream: TextIO,
    output_stream: TextIO,
    params: dict,
) -> dict:
    requested = params.get(
        "permissions",
        {},
    )

    if not isinstance(
        requested,
        dict,
    ):
        raise CLIError(
            "permission request is invalid"
        )

    output_stream.write(
        "\n[q interaction] additional permissions requested\n"
    )

    reason = params.get(
        "reason"
    )

    if reason:
        output_stream.write(
            f"  reason: {reason}\n"
        )

    cwd = params.get(
        "cwd"
    )

    if cwd:
        output_stream.write(
            f"  cwd: {cwd}\n"
        )

    output_stream.write(
        json.dumps(
            requested,
            indent=2,
            sort_keys=True,
        )
        + "\n"
        "  [t] grant for turn\n"
        "  [s] grant for session\n"
        "  [n] deny\n"
    )
    output_stream.flush()

    while True:
        choice = _prompt_line(
            input_stream,
            output_stream,
            "choice [n]: ",
        ).strip().lower()

        if choice in {
            "",
            "n",
            "no",
            "deny",
            "decline",
        }:
            return {
                "permissions": {},
                "scope": "turn",
            }

        if choice in {
            "t",
            "turn",
            "y",
            "yes",
        }:
            return {
                "permissions": requested,
                "scope": "turn",
            }

        if choice in {
            "s",
            "session",
        }:
            return {
                "permissions": requested,
                "scope": "session",
            }

        output_stream.write(
            "invalid choice\n"
        )


def _interactive_result(
    *,
    method: str,
    params: dict,
    input_stream: TextIO,
    output_stream: TextIO,
) -> dict:
    if method == (
        "item/commandExecution/"
        "requestApproval"
    ):
        return _approval_result(
            input_stream=input_stream,
            output_stream=output_stream,
            title="command approval",
            details=[
                (
                    "command",
                    params.get(
                        "command"
                    ),
                ),
                (
                    "cwd",
                    params.get(
                        "cwd"
                    ),
                ),
                (
                    "reason",
                    params.get(
                        "reason"
                    ),
                ),
            ],
        )

    if method == (
        "item/fileChange/"
        "requestApproval"
    ):
        return _approval_result(
            input_stream=input_stream,
            output_stream=output_stream,
            title="file change approval",
            details=[
                (
                    "grant root",
                    params.get(
                        "grantRoot"
                    ),
                ),
                (
                    "reason",
                    params.get(
                        "reason"
                    ),
                ),
            ],
        )

    if method == "item/tool/requestUserInput":
        return _tool_input_result(
            input_stream=input_stream,
            output_stream=output_stream,
            params=params,
        )

    if method == "mcpServer/elicitation/request":
        return _mcp_elicitation_result(
            input_stream=input_stream,
            output_stream=output_stream,
            params=params,
        )

    if method == "item/permissions/requestApproval":
        return _permissions_result(
            input_stream=input_stream,
            output_stream=output_stream,
            params=params,
        )

    raise CLIError(
        "unsupported interactive request "
        f"{method!r}"
    )


def _interaction_input(
    stdin: TextIO,
):
    if stdin.isatty():
        return stdin, None

    try:
        tty = open(
            "/dev/tty",
            "r",
            encoding="utf-8",
        )
    except OSError:
        return None, None

    return tty, tty


async def run_query(
    *,
    args: argparse.Namespace,
    client: IPCClient,
    stdin: TextIO,
    stdout: TextIO,
    stderr: TextIO | None = None,
) -> int:
    prompt_output = (
        stderr
        if stderr is not None
        else sys.stderr
    )

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

        elif (
            frame.type == "event"
            and frame.event
            == "interaction"
        ):
            data = frame.data

            if not isinstance(
                data,
                dict,
            ):
                raise CLIError(
                    "invalid interaction event"
                )

            query_id = data.get(
                "query_id"
            )
            interaction_id = data.get(
                "interaction_id"
            )
            method = data.get(
                "method"
            )
            interaction_params = (
                data.get(
                    "params",
                    {},
                )
            )

            if (
                not isinstance(
                    query_id,
                    str,
                )
                or not query_id
                or not isinstance(
                    interaction_id,
                    str,
                )
                or not interaction_id
                or not isinstance(
                    method,
                    str,
                )
                or not method
                or not isinstance(
                    interaction_params,
                    dict,
                )
            ):
                raise CLIError(
                    "invalid interaction event"
                )

            (
                interaction_input,
                close_stream,
            ) = _interaction_input(
                stdin
            )

            try:
                if interaction_input is None:
                    prompt_output.write(
                        "\nq: no interactive TTY; "
                        "declining request\n"
                    )
                    prompt_output.flush()

                    result = (
                        _safe_interaction_result(
                            method,
                            interaction_params,
                        )
                    )

                else:
                    result = (
                        _interactive_result(
                            method=method,
                            params=(
                                interaction_params
                            ),
                            input_stream=(
                                interaction_input
                            ),
                            output_stream=(
                                prompt_output
                            ),
                        )
                    )

                await client.request(
                    "respond_interaction",
                    {
                        "query_id": query_id,
                        "interaction_id": (
                            interaction_id
                        ),
                        "result": result,
                    },
                )

            finally:
                if close_stream is not None:
                    close_stream.close()

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
        stderr=sys.stderr,
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
