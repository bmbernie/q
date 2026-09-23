import asyncio
import json

import httpx
import pytest

from q.broker import (
    BrokerHTTPError,
    HTTPBroker,
)


def test_http_broker_responds_to_interaction():
    async def run():
        requests = []

        async def handler(
            request: httpx.Request,
        ):
            requests.append(request)

            return httpx.Response(
                200,
                json={
                    "responded": True,
                },
            )

        client = httpx.AsyncClient(
            transport=httpx.MockTransport(
                handler
            )
        )

        try:
            broker = HTTPBroker(
                "http://broker.test",
                client=client,
            )

            await broker.respond_interaction(
                interaction_id="interaction-1",
                result={
                    "decision": "accept",
                },
            )

        finally:
            await client.aclose()

        assert len(requests) == 1

        request = requests[0]

        assert request.method == "POST"
        assert (
            request.url.path
            == (
                "/v1/interactions/"
                "interaction-1/respond"
            )
        )

        assert json.loads(
            request.content
        ) == {
            "result": {
                "decision": "accept",
            },
        }

    asyncio.run(run())


def test_http_broker_maps_interaction_error():
    async def run():
        async def handler(
            request: httpx.Request,
        ):
            return httpx.Response(
                404,
                json={
                    "detail": {
                        "error": (
                            "interaction_not_found"
                        ),
                    },
                },
            )

        client = httpx.AsyncClient(
            transport=httpx.MockTransport(
                handler
            )
        )

        try:
            broker = HTTPBroker(
                "http://broker.test",
                client=client,
            )

            with pytest.raises(
                BrokerHTTPError
            ) as caught:
                await broker.respond_interaction(
                    interaction_id=(
                        "missing"
                    ),
                    result={
                        "decision": "accept",
                    },
                )

            assert (
                caught.value.status_code
                == 404
            )
            assert (
                caught.value.code
                == "interaction_not_found"
            )

        finally:
            await client.aclose()

    asyncio.run(run())


def test_http_broker_sends_backend_selection():
    async def run():
        requests = []

        async def handler(
            request: httpx.Request,
        ):
            requests.append(request)

            return httpx.Response(
                200,
                content=(
                    b'{"type":"thread",'
                    b'"thread_id":"qb1:web:abc"}\n'
                    b'{"type":"turn",'
                    b'"thread_id":"qb1:web:abc",'
                    b'"turn_id":"qb1:web:def"}\n'
                    b'{"type":"completed",'
                    b'"thread_id":"qb1:web:abc",'
                    b'"turn_id":"qb1:web:def",'
                    b'"status":"completed"}\n'
                ),
                headers={
                    "content-type": (
                        "application/x-ndjson"
                    ),
                },
            )

        client = httpx.AsyncClient(
            transport=httpx.MockTransport(
                handler
            )
        )

        try:
            broker = HTTPBroker(
                "http://broker.test",
                client=client,
            )

            events = [
                event
                async for event in broker.stream_query(
                    input_text="hello",
                    cwd="/tmp",
                    model="gpt-test",
                    reasoning_effort="high",
                    attachments=[
                        {
                            "kind": "image",
                            "path": "/tmp/example.png",
                        },
                    ],
                    backend="web",
                    thread_id=None,
                    tools_policy=None,
                    ephemeral=True,
                    sandbox="read-only",
                )
            ]

        finally:
            await client.aclose()

        assert len(requests) == 1

        request = requests[0]

        assert request.method == "POST"
        assert (
            request.url.path
            == "/v1/query"
        )

        payload = json.loads(
            request.content
        )

        assert payload["backend"] == "web"
        assert payload["input"] == "hello"
        assert payload["model"] == "gpt-test"

        assert [
            event["type"]
            for event in events
        ] == [
            "thread",
            "turn",
            "completed",
        ]

    asyncio.run(run())
