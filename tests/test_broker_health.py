import asyncio

import httpx
import pytest

from q.broker import (
    BrokerHTTPError,
    BrokerProtocolError,
    HTTPBroker,
)


def test_broker_health():
    async def run():
        async def handler(request):
            assert (
                request.url.path
                == "/health"
            )

            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "workers": 5,
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

            result = (
                await broker.health()
            )

            assert result == {
                "ok": True,
                "workers": 5,
            }

        finally:
            await client.aclose()

    asyncio.run(run())


def test_broker_health_rejects_unhealthy_body():
    async def run():
        async def handler(request):
            return httpx.Response(
                200,
                json={
                    "ok": False,
                    "workers": 0,
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
                BrokerProtocolError,
                match="not healthy",
            ):
                await broker.health()

        finally:
            await client.aclose()

    asyncio.run(run())


def test_broker_health_reports_http_failure():
    async def run():
        async def handler(request):
            return httpx.Response(
                503,
                text="unavailable",
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
                await broker.health()

            assert (
                caught.value.status_code
                == 503
            )

        finally:
            await client.aclose()

    asyncio.run(run())
