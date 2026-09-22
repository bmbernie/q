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
