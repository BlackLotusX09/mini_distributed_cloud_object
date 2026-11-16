"""The controller's ONLY network interface to worker nodes (shared pooled httpx client)."""
from typing import Any, Dict, Optional
from urllib.parse import quote

import httpx

from settings import UPLOAD_WORKER_TIMEOUT


class WorkerClientError(Exception):
    def __init__(self, worker_id: str, message: str, status: Optional[int] = None):
        super().__init__(f"{worker_id}: {message}")
        self.worker_id = worker_id
        self.status = status
        self.message = message


class WorkerUnreachable(WorkerClientError):
    """Connection failure, DNS error or timeout."""


class WorkerRejected(WorkerClientError):
    """Worker answered 4xx (bad checksum, bad id, chunk missing/corrupt ...)."""


class WorkerError(WorkerClientError):
    """Worker answered 5xx (or an unexpected status)."""


def _describe(resp: httpx.Response) -> str:
    try:
        body = resp.json()
        if isinstance(body, dict) and body.get("error"):
            return f"HTTP {resp.status_code} {body['error']}"
    except ValueError:
        pass
    return f"HTTP {resp.status_code}"


def _normalize_worker(worker: Any) -> Dict[str, str]:
    """Accept a dict, an object with .address/.worker_id, or a raw URL string."""
    if isinstance(worker, str):
        addr = worker
        wid = addr.split("://", 1)[-1].split("/", 1)[0] or addr
        return {"worker_id": wid, "address": addr}
    if isinstance(worker, dict):
        addr = worker.get("address")
        wid = worker.get("worker_id") or worker.get("id")
        if not addr and wid:
            addr = f"http://{wid}:8000"
        if not addr:
            raise ValueError("worker dict must include 'address' or 'worker_id'")
        return {"worker_id": str(wid or addr), "address": str(addr)}
    addr = getattr(worker, "address", None)
    wid = getattr(worker, "worker_id", None) or getattr(worker, "id", None)
    if not addr and wid:
        addr = f"http://{wid}:8000"
    if not addr:
        raise ValueError("worker object must define 'address' or 'worker_id'")
    return {"worker_id": str(wid or addr), "address": str(addr)}


class WorkerClient:
    """Shared connection-pooled HTTP client for worker communication."""

    def __init__(self, timeout: float = UPLOAD_WORKER_TIMEOUT,
                 transport: Optional[httpx.AsyncBaseTransport] = None,
                 max_connections: int = 100):
        self._timeout = timeout
        self._transport = transport
        self._max_connections = max_connections
        self._client: Optional[httpx.AsyncClient] = None

    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self._timeout),
                limits=httpx.Limits(
                    max_connections=self._max_connections,
                    max_keepalive_connections=max(1, self._max_connections // 5),
                ),
                transport=self._transport,
            )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _request(self, worker: Any, method: str, chunk_id: str, **kwargs) -> httpx.Response:
        if self._client is None:
            raise RuntimeError("WorkerClient not started")
        w = _normalize_worker(worker)
        url = f"{w['address'].rstrip('/')}/internal/chunks/{quote(chunk_id, safe='')}"
        try:
            return await self._client.request(method, url, **kwargs)
        except (httpx.HTTPError, httpx.InvalidURL) as e:  # connect, DNS, timeout, protocol
            raise WorkerUnreachable(w["worker_id"], f"{type(e).__name__}: {e}") from e

    @staticmethod
    def _raise(worker: Any, resp: httpx.Response):
        w = _normalize_worker(worker)
        msg = _describe(resp)
        if 400 <= resp.status_code < 500:
            raise WorkerRejected(w["worker_id"], msg, resp.status_code)
        raise WorkerError(w["worker_id"], msg, resp.status_code)

    async def put_chunk(self, worker: Any, chunk_id: str, data: bytes, checksum: str) -> None:
        resp = await self._request(
            worker, "PUT", chunk_id, content=data,
            headers={"X-Chunk-Checksum": checksum, "Content-Type": "application/octet-stream"},
        )
        if resp.status_code not in (200, 201):
            self._raise(worker, resp)

    async def get_chunk(self, worker: Any, chunk_id: str) -> bytes:
        resp = await self._request(worker, "GET", chunk_id)
        if resp.status_code != 200:
            self._raise(worker, resp)
        return resp.content

    async def delete_chunk(self, worker: Any, chunk_id: str) -> None:
        resp = await self._request(worker, "DELETE", chunk_id)
        if resp.status_code not in (200, 204, 404):  # 404 = already gone
            self._raise(worker, resp)


_default_client: Optional[WorkerClient] = None


async def _get_default_client() -> WorkerClient:
    global _default_client
    if _default_client is None or _default_client._client is None:
        _default_client = WorkerClient()
        await _default_client.start()
    return _default_client


async def close_default_client() -> None:
    global _default_client
    if _default_client is not None:
        await _default_client.close()
        _default_client = None


async def put_chunk(worker: Any, chunk_id: str, data: bytes, checksum: str) -> None:
    client = await _get_default_client()
    await client.put_chunk(worker, chunk_id, data, checksum)


async def get_chunk(worker: Any, chunk_id: str) -> bytes:
    client = await _get_default_client()
    return await client.get_chunk(worker, chunk_id)


async def delete_chunk(worker: Any, chunk_id: str) -> None:
    client = await _get_default_client()
    await client.delete_chunk(worker, chunk_id)
