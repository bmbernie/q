from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any, Protocol

import httpx

from .models import ToolsPolicy


class BrokerError(RuntimeError):
    pass


class BrokerProtocolError(BrokerError):
    pass


class BrokerTransportError(BrokerError):
    pass


class BrokerHTTPError(BrokerError):
    def __init__(
        self,
        *,
        status_code: int,
        code: str,
        message: str,
        details: Any = None,
    ):
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details

        super().__init__(
            f"{status_code} {code}: {message}"
        )


class Broker(Protocol):
    def stream_query(
        self,
        *,
        input_text: str,
        cwd: str,
        model: str,
        reasoning_effort: str,
        thread_id: str | None,
        tools_policy: ToolsPolicy | None,
        ephemeral: bool,
        sandbox: str,
    ) -> AsyncIterator[dict[str, Any]]:
        ...

    async def respond_interaction(
        self,
        *,
        interaction_id: str,
        result: dict[str, Any],
    ) -> None:
        ...


    async def interrupt(
        self,
        *,
        thread_id: str,
        turn_id: str,
    ) -> None:
        ...


class HTTPBroker:
    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8792",
        *,
        client: httpx.AsyncClient | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self._owns_client = client is None

        self._client = (
            client
            if client is not None
            else httpx.AsyncClient(
                timeout=httpx.Timeout(
                    connect=10.0,
                    read=None,
                    write=60.0,
                    pool=60.0,
                )
            )
        )

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    def stream_query(
        self,
        *,
        input_text: str,
        cwd: str,
        model: str,
        reasoning_effort: str,
        thread_id: str | None,
        tools_policy: ToolsPolicy | None,
        ephemeral: bool,
        sandbox: str,
    ) -> AsyncIterator[dict[str, Any]]:
        return self._stream_query(
            input_text=input_text,
            cwd=cwd,
            model=model,
            reasoning_effort=reasoning_effort,
            thread_id=thread_id,
            tools_policy=tools_policy,
            ephemeral=ephemeral,
            sandbox=sandbox,
        )

    async def _stream_query(
        self,
        *,
        input_text: str,
        cwd: str,
        model: str,
        reasoning_effort: str,
        thread_id: str | None,
        tools_policy: ToolsPolicy | None,
        ephemeral: bool,
        sandbox: str,
    ) -> AsyncIterator[dict[str, Any]]:
        payload: dict[str, Any] = {
            "input": input_text,
            "cwd": cwd,
            "model": model,
            "reasoning_effort": reasoning_effort,
            "ephemeral": ephemeral,
            "sandbox": sandbox,
        }

        if thread_id is not None:
            payload["thread_id"] = thread_id

        elif tools_policy is not None:
            payload["tools"] = (
                tools_policy.value
            )

        try:
            async with self._client.stream(
                "POST",
                f"{self.base_url}/v1/query",
                json=payload,
            ) as response:
                if response.status_code >= 400:
                    body = await response.aread()

                    try:
                        parsed = json.loads(
                            body
                        )
                    except (
                        json.JSONDecodeError,
                        UnicodeDecodeError,
                    ):
                        parsed = None

                    detail = (
                        parsed.get("detail")
                        if isinstance(
                            parsed,
                            dict,
                        )
                        else None
                    )

                    if isinstance(
                        detail,
                        dict,
                    ):
                        code = str(
                            detail.get(
                                "error",
                                "broker_error",
                            )
                        )
                        message = str(
                            detail.get(
                                "message",
                                code,
                            )
                        )
                        details = detail

                    else:
                        code = "broker_error"
                        message = (
                            str(detail)
                            if detail is not None
                            else (
                                response.reason_phrase
                                or "broker request failed"
                            )
                        )
                        details = parsed

                    raise BrokerHTTPError(
                        status_code=(
                            response.status_code
                        ),
                        code=code,
                        message=message,
                        details=details,
                    )

                async for line in (
                    response.aiter_lines()
                ):
                    if not line.strip():
                        continue

                    try:
                        event = json.loads(
                            line
                        )
                    except json.JSONDecodeError as exc:
                        raise BrokerProtocolError(
                            "broker emitted invalid "
                            "NDJSON"
                        ) from exc

                    if not isinstance(
                        event,
                        dict,
                    ):
                        raise BrokerProtocolError(
                            "broker event must be "
                            "an object"
                        )

                    yield event

        except BrokerError:
            raise

        except httpx.HTTPError as exc:
            raise BrokerTransportError(
                str(exc)
            ) from exc

    async def respond_interaction(
        self,
        *,
        interaction_id: str,
        result: dict[str, Any],
    ) -> None:
        try:
            response = await self._client.post(
                (
                    f"{self.base_url}/v1/"
                    "interactions/"
                    f"{interaction_id}/respond"
                ),
                json={
                    "result": result,
                },
            )

        except httpx.HTTPError as exc:
            raise BrokerTransportError(
                str(exc)
            ) from exc

        if response.status_code >= 400:
            try:
                parsed = response.json()
            except (
                json.JSONDecodeError,
                UnicodeDecodeError,
            ):
                parsed = None

            detail = (
                parsed.get("detail")
                if isinstance(
                    parsed,
                    dict,
                )
                else None
            )

            if isinstance(detail, dict):
                code = str(
                    detail.get(
                        "error",
                        "broker_error",
                    )
                )
                message = str(
                    detail.get(
                        "message",
                        code,
                    )
                )
            else:
                code = "broker_error"
                message = (
                    str(detail)
                    if detail is not None
                    else (
                        response.reason_phrase
                        or (
                            "interaction response "
                            "failed"
                        )
                    )
                )

            raise BrokerHTTPError(
                status_code=(
                    response.status_code
                ),
                code=code,
                message=message,
                details=parsed,
            )

    async def interrupt(
        self,
        *,
        thread_id: str,
        turn_id: str,
    ) -> None:
        try:
            response = await self._client.post(
                (
                    f"{self.base_url}/v1/threads/"
                    f"{thread_id}/turns/"
                    f"{turn_id}/interrupt"
                )
            )

        except httpx.HTTPError as exc:
            raise BrokerTransportError(
                str(exc)
            ) from exc

        if response.status_code >= 400:
            try:
                parsed = response.json()
            except (
                json.JSONDecodeError,
                UnicodeDecodeError,
            ):
                parsed = None

            detail = (
                parsed.get("detail")
                if isinstance(
                    parsed,
                    dict,
                )
                else None
            )

            if isinstance(detail, dict):
                code = str(
                    detail.get(
                        "error",
                        "broker_error",
                    )
                )
                message = str(
                    detail.get(
                        "message",
                        code,
                    )
                )
            else:
                code = "broker_error"
                message = (
                    str(detail)
                    if detail is not None
                    else (
                        response.reason_phrase
                        or "interrupt failed"
                    )
                )

            raise BrokerHTTPError(
                status_code=response.status_code,
                code=code,
                message=message,
                details=parsed,
            )
