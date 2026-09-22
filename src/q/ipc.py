from __future__ import annotations

import asyncio
import json
import os
import stat
import uuid
from collections.abc import (
    AsyncIterator,
    Callable,
)
from dataclasses import dataclass
from pathlib import Path
from typing import Any


MAX_MESSAGE_BYTES = 64 * 1024 * 1024


class IPCError(RuntimeError):
    pass


class IPCProtocolError(IPCError):
    pass


class IPCRemoteError(IPCError):
    def __init__(
        self,
        *,
        code: str,
        message: str,
        details: Any = None,
    ):
        self.code = code
        self.message = message
        self.details = details

        super().__init__(
            f"{code}: {message}"
        )


@dataclass(frozen=True, slots=True)
class IPCRequest:
    request_id: str
    method: str
    params: dict[str, Any]

    @classmethod
    def from_wire(
        cls,
        value: Any,
    ) -> "IPCRequest":
        if not isinstance(value, dict):
            raise IPCProtocolError(
                "request must be an object"
            )

        request_id = value.get(
            "request_id"
        )
        method = value.get("method")
        params = value.get(
            "params",
            {},
        )

        if (
            not isinstance(
                request_id,
                str,
            )
            or not request_id
        ):
            raise IPCProtocolError(
                "request_id must be a non-empty string"
            )

        if (
            not isinstance(method, str)
            or not method
        ):
            raise IPCProtocolError(
                "method must be a non-empty string"
            )

        if not isinstance(params, dict):
            raise IPCProtocolError(
                "params must be an object"
            )

        return cls(
            request_id=request_id,
            method=method,
            params=params,
        )

    def to_wire(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "method": self.method,
            "params": self.params,
        }


@dataclass(frozen=True, slots=True)
class IPCFrame:
    request_id: str
    type: str
    event: str | None = None
    data: Any = None
    result: Any = None
    error: dict[str, Any] | None = None

    @classmethod
    def event_frame(
        cls,
        request_id: str,
        event: str,
        data: Any,
    ) -> "IPCFrame":
        return cls(
            request_id=request_id,
            type="event",
            event=event,
            data=data,
        )

    @classmethod
    def result_frame(
        cls,
        request_id: str,
        result: Any,
    ) -> "IPCFrame":
        return cls(
            request_id=request_id,
            type="result",
            result=result,
        )

    @classmethod
    def error_frame(
        cls,
        request_id: str,
        *,
        code: str,
        message: str,
        details: Any = None,
    ) -> "IPCFrame":
        error = {
            "code": code,
            "message": message,
        }

        if details is not None:
            error["details"] = details

        return cls(
            request_id=request_id,
            type="error",
            error=error,
        )

    @property
    def terminal(self) -> bool:
        return self.type in {
            "result",
            "error",
        }

    def to_wire(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "request_id": self.request_id,
            "type": self.type,
        }

        if self.type == "event":
            value["event"] = self.event
            value["data"] = self.data

        elif self.type == "result":
            value["result"] = self.result

        elif self.type == "error":
            value["error"] = self.error

        else:
            raise IPCProtocolError(
                f"unknown frame type {self.type!r}"
            )

        return value

    @classmethod
    def from_wire(
        cls,
        value: Any,
    ) -> "IPCFrame":
        if not isinstance(value, dict):
            raise IPCProtocolError(
                "frame must be an object"
            )

        request_id = value.get(
            "request_id"
        )
        frame_type = value.get("type")

        if (
            not isinstance(
                request_id,
                str,
            )
            or not request_id
        ):
            raise IPCProtocolError(
                "frame request_id must be "
                "a non-empty string"
            )

        if frame_type == "event":
            event = value.get("event")

            if (
                not isinstance(
                    event,
                    str,
                )
                or not event
            ):
                raise IPCProtocolError(
                    "event frame requires "
                    "a non-empty event name"
                )

            return cls(
                request_id=request_id,
                type="event",
                event=event,
                data=value.get("data"),
            )

        if frame_type == "result":
            return cls(
                request_id=request_id,
                type="result",
                result=value.get(
                    "result"
                ),
            )

        if frame_type == "error":
            error = value.get("error")

            if not isinstance(
                error,
                dict,
            ):
                raise IPCProtocolError(
                    "error frame requires "
                    "an error object"
                )

            code = error.get("code")
            message = error.get(
                "message"
            )

            if (
                not isinstance(
                    code,
                    str,
                )
                or not code
                or not isinstance(
                    message,
                    str,
                )
            ):
                raise IPCProtocolError(
                    "error frame requires "
                    "string code and message"
                )

            return cls(
                request_id=request_id,
                type="error",
                error=error,
            )

        raise IPCProtocolError(
            f"unknown frame type "
            f"{frame_type!r}"
        )


IPCHandler = Callable[
    [IPCRequest],
    AsyncIterator[IPCFrame],
]


def default_socket_path(
    *,
    environ: dict[str, str]
    | None = None,
) -> Path:
    env = (
        os.environ
        if environ is None
        else environ
    )

    runtime_dir = env.get(
        "XDG_RUNTIME_DIR",
        "",
    ).strip()

    if runtime_dir:
        base = Path(runtime_dir)
    else:
        base = Path(
            f"/run/user/{os.getuid()}"
        )

    return base / "q" / "qd.sock"


def _encode(
    value: Any,
) -> bytes:
    payload = (
        json.dumps(
            value,
            separators=(",", ":"),
        )
        + "\n"
    ).encode()

    if len(payload) > MAX_MESSAGE_BYTES:
        raise IPCProtocolError(
            "IPC message exceeds maximum size"
        )

    return payload


async def _write(
    writer: asyncio.StreamWriter,
    value: Any,
) -> None:
    writer.write(
        _encode(value)
    )
    await writer.drain()


async def _read(
    reader: asyncio.StreamReader,
) -> Any:
    try:
        line = await reader.readline()

    except ValueError as exc:
        raise IPCProtocolError(
            "IPC message exceeds maximum size"
        ) from exc

    if not line:
        raise EOFError(
            "IPC peer closed connection"
        )

    if len(line) > MAX_MESSAGE_BYTES:
        raise IPCProtocolError(
            "IPC message exceeds maximum size"
        )

    try:
        return json.loads(line)

    except json.JSONDecodeError as exc:
        raise IPCProtocolError(
            "IPC message is not valid JSON"
        ) from exc


class IPCServer:
    def __init__(
        self,
        socket_path: Path,
        handler: IPCHandler,
    ):
        self.socket_path = (
            socket_path.expanduser()
        )
        self._handler = handler
        self._server: (
            asyncio.AbstractServer
            | None
        ) = None

    @property
    def running(self) -> bool:
        return self._server is not None

    async def start(self) -> None:
        if self._server is not None:
            raise RuntimeError(
                "IPC server is already running"
            )

        self.socket_path.parent.mkdir(
            parents=True,
            exist_ok=True,
            mode=0o700,
        )

        self.socket_path.parent.chmod(
            0o700
        )

        if self.socket_path.exists():
            mode = (
                self.socket_path
                .lstat()
                .st_mode
            )

            if not stat.S_ISSOCK(mode):
                raise IPCError(
                    "refusing to remove "
                    f"non-socket path "
                    f"{self.socket_path}"
                )

            self.socket_path.unlink()

        self._server = (
            await asyncio.start_unix_server(
                self._handle_client,
                path=str(
                    self.socket_path
                ),
                limit=(
                    MAX_MESSAGE_BYTES + 1
                ),
            )
        )

        self.socket_path.chmod(
            0o600
        )

    async def aclose(self) -> None:
        server = self._server
        self._server = None

        if server is not None:
            server.close()
            await server.wait_closed()

        try:
            mode = (
                self.socket_path
                .lstat()
                .st_mode
            )

        except FileNotFoundError:
            return

        if stat.S_ISSOCK(mode):
            self.socket_path.unlink()

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        request_id = "_invalid"

        try:
            raw = await _read(reader)

            if isinstance(raw, dict):
                candidate = raw.get(
                    "request_id"
                )

                if (
                    isinstance(
                        candidate,
                        str,
                    )
                    and candidate
                ):
                    request_id = (
                        candidate
                    )

            request = (
                IPCRequest.from_wire(
                    raw
                )
            )

            request_id = (
                request.request_id
            )
            terminal_sent = False

            try:
                async for frame in (
                    self._handler(
                        request
                    )
                ):
                    if (
                        frame.request_id
                        != request_id
                    ):
                        raise IPCProtocolError(
                            "handler emitted "
                            "mismatched request_id"
                        )

                    await _write(
                        writer,
                        frame.to_wire(),
                    )

                    if frame.terminal:
                        terminal_sent = True
                        break

            except Exception as exc:
                if not terminal_sent:
                    await _write(
                        writer,
                        IPCFrame.error_frame(
                            request_id,
                            code=(
                                "internal_error"
                            ),
                            message=(
                                "qd request "
                                "handler failed"
                            ),
                            details={
                                "exception": (
                                    type(
                                        exc
                                    ).__name__
                                ),
                            },
                        ).to_wire(),
                    )

                    terminal_sent = True

            if not terminal_sent:
                await _write(
                    writer,
                    IPCFrame.error_frame(
                        request_id,
                        code=(
                            "protocol_error"
                        ),
                        message=(
                            "handler completed "
                            "without a terminal "
                            "frame"
                        ),
                    ).to_wire(),
                )

        except IPCProtocolError as exc:
            try:
                await _write(
                    writer,
                    IPCFrame.error_frame(
                        request_id,
                        code=(
                            "invalid_request"
                        ),
                        message=str(exc),
                    ).to_wire(),
                )
            except (
                BrokenPipeError,
                ConnectionResetError,
            ):
                pass

        except EOFError:
            pass

        except (
            BrokenPipeError,
            ConnectionResetError,
        ):
            pass

        finally:
            writer.close()

            try:
                await writer.wait_closed()
            except (
                BrokenPipeError,
                ConnectionResetError,
            ):
                pass


class IPCClient:
    def __init__(
        self,
        socket_path: Path,
    ):
        self.socket_path = (
            socket_path.expanduser()
        )

    async def frames(
        self,
        method: str,
        params: dict[str, Any]
        | None = None,
        *,
        request_id: str | None = None,
    ) -> AsyncIterator[IPCFrame]:
        actual_request_id = (
            request_id
            or str(uuid.uuid4())
        )

        request = IPCRequest(
            request_id=(
                actual_request_id
            ),
            method=method,
            params=params or {},
        )

        reader, writer = (
            await asyncio.open_unix_connection(
                path=str(
                    self.socket_path
                ),
                limit=(
                    MAX_MESSAGE_BYTES + 1
                ),
            )
        )

        try:
            await _write(
                writer,
                request.to_wire(),
            )

            terminal_seen = False

            while not terminal_seen:
                try:
                    raw = await _read(
                        reader
                    )

                except EOFError as exc:
                    raise IPCProtocolError(
                        "qd closed IPC "
                        "connection before "
                        "terminal frame"
                    ) from exc

                frame = (
                    IPCFrame.from_wire(
                        raw
                    )
                )

                if (
                    frame.request_id
                    != actual_request_id
                ):
                    raise IPCProtocolError(
                        "qd returned "
                        "mismatched request_id"
                    )

                terminal_seen = (
                    frame.terminal
                )

                yield frame

        finally:
            writer.close()

            try:
                await writer.wait_closed()
            except (
                BrokenPipeError,
                ConnectionResetError,
            ):
                pass

    async def request(
        self,
        method: str,
        params: dict[str, Any]
        | None = None,
        *,
        request_id: str | None = None,
    ) -> Any:
        result_seen = False
        result = None

        async for frame in self.frames(
            method,
            params,
            request_id=request_id,
        ):
            if frame.type == "event":
                continue

            if frame.type == "error":
                assert (
                    frame.error
                    is not None
                )

                raise IPCRemoteError(
                    code=(
                        frame.error["code"]
                    ),
                    message=(
                        frame.error[
                            "message"
                        ]
                    ),
                    details=(
                        frame.error.get(
                            "details"
                        )
                    ),
                )

            if frame.type == "result":
                result_seen = True
                result = frame.result

        if not result_seen:
            raise IPCProtocolError(
                "request completed without "
                "a result frame"
            )

        return result
