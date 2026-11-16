import asyncio

import httpx
import pytest

import worker_client
from worker_client import (
    WorkerClient,
    WorkerError,
    WorkerRejected,
    WorkerUnreachable,
    _normalize_worker,
)


def w(wid="worker1", address="http://worker1:8000"):
    return {"worker_id": wid, "address": address}


def run(coro):
    return asyncio.run(coro)


def test_normalize_worker_variants():
    assert _normalize_worker({"worker_id": "w1", "address": "http://w1:8000"}) == {
        "worker_id": "w1", "address": "http://w1:8000"
    }
    assert _normalize_worker({"worker_id": "w1"}) == {
        "worker_id": "w1", "address": "http://w1:8000"
    }
    assert _normalize_worker("http://w2:9000") == {
        "worker_id": "w2:9000", "address": "http://w2:9000"
    }

    class Obj:
        worker_id = "w3"
        address = "http://w3:8000"

    assert _normalize_worker(Obj()) == {"worker_id": "w3", "address": "http://w3:8000"}


def test_put_chunk_200_and_201_succeed_and_send_headers():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, str(request.url), dict(request.headers), request.content))
        code = 201 if len(seen) == 1 else 200
        return httpx.Response(code, json={"chunk_id": "c1"})

    async def scenario():
        client = WorkerClient(transport=httpx.MockTransport(handler))
        await client.start()
        try:
            await client.put_chunk(w(), "c1", b"hello", "deadbeef")
            await client.put_chunk(w(), "c1", b"hello", "deadbeef")
        finally:
            await client.close()

    run(scenario())

    assert len(seen) == 2
    method, url, headers, content = seen[0]
    assert method == "PUT"
    assert url == "http://worker1:8000/internal/chunks/c1"
    assert headers["x-chunk-checksum"] == "deadbeef"
    assert content == b"hello"


@pytest.mark.parametrize("status", [400, 404, 409, 422])
def test_put_chunk_4xx_raises_worker_rejected(status):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json={"error": "BAD_CHUNK", "message": "nope"})

    async def scenario():
        client = WorkerClient(transport=httpx.MockTransport(handler))
        await client.start()
        try:
            with pytest.raises(WorkerRejected) as exc:
                await client.put_chunk(w(), "c1", b"hello", "abc")
            assert exc.value.status == status
            assert exc.value.worker_id == "worker1"
            assert "BAD_CHUNK" in exc.value.message
        finally:
            await client.close()

    run(scenario())


@pytest.mark.parametrize("status", [500, 502, 503])
def test_put_chunk_5xx_raises_worker_error(status):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, text="boom")

    async def scenario():
        client = WorkerClient(transport=httpx.MockTransport(handler))
        await client.start()
        try:
            with pytest.raises(WorkerError) as exc:
                await client.put_chunk(w(), "c1", b"hello", "abc")
            assert exc.value.status == status
        finally:
            await client.close()

    run(scenario())


@pytest.mark.parametrize(
    "exc_factory",
    [
        lambda req: httpx.ConnectError("refused", request=req),
        lambda req: httpx.ReadTimeout("timed out", request=req),
        lambda req: httpx.RemoteProtocolError("disconnected", request=req),
    ],
)
def test_network_failures_raise_worker_unreachable(exc_factory):
    def handler(request: httpx.Request) -> httpx.Response:
        raise exc_factory(request)

    async def scenario():
        client = WorkerClient(transport=httpx.MockTransport(handler))
        await client.start()
        try:
            with pytest.raises(WorkerUnreachable):
                await client.put_chunk(w(), "c1", b"hello", "abc")
            with pytest.raises(WorkerUnreachable):
                await client.get_chunk(w(), "c1")
            with pytest.raises(WorkerUnreachable):
                await client.delete_chunk(w(), "c1")
        finally:
            await client.close()

    run(scenario())


def test_get_chunk_success_and_errors():
    mode = {"status": 200, "body": b"\x00\x01\x02raw"}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        if mode["status"] == 200:
            return httpx.Response(200, content=mode["body"])
        return httpx.Response(mode["status"], json={"error": "ERR"})

    async def scenario():
        client = WorkerClient(transport=httpx.MockTransport(handler))
        await client.start()
        try:
            assert await client.get_chunk(w(), "c1") == b"\x00\x01\x02raw"
            mode["status"] = 404
            with pytest.raises(WorkerRejected):
                await client.get_chunk(w(), "c1")
            mode["status"] = 409
            with pytest.raises(WorkerRejected):
                await client.get_chunk(w(), "c1")
            mode["status"] = 500
            with pytest.raises(WorkerError):
                await client.get_chunk(w(), "c1")
        finally:
            await client.close()

    run(scenario())


def test_delete_chunk_treats_200_204_404_as_success():
    statuses = iter([200, 204, 404, 400, 503])

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "DELETE"
        return httpx.Response(next(statuses))

    async def scenario():
        client = WorkerClient(transport=httpx.MockTransport(handler))
        await client.start()
        try:
            await client.delete_chunk(w(), "c1")  # 200
            await client.delete_chunk(w(), "c1")  # 204
            await client.delete_chunk(w(), "c1")  # 404 (already gone)
            with pytest.raises(WorkerRejected):
                await client.delete_chunk(w(), "c1")  # 400
            with pytest.raises(WorkerError):
                await client.delete_chunk(w(), "c1")  # 503
        finally:
            await client.close()

    run(scenario())


def test_requires_start_before_use():
    async def scenario():
        client = WorkerClient()
        with pytest.raises(RuntimeError):
            await client.get_chunk(w(), "c1")
        await worker_client.close_default_client()

    run(scenario())
