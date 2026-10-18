"""pyLoad 0.5 API client.

Authenticates with the X-API-Key header only (there is no /api/login). The
API key is a secret: it is never logged or put into exception text.
"""

import logging
from dataclasses import dataclass

import httpx

logger = logging.getLogger("websharr.pyload")

FINISHED = 0
FAILED_STATES = frozenset({1, 4, 6, 8, 9})
TIMEOUT = 30.0


class PyloadError(Exception):
    """Any pyLoad communication or authentication failure."""


class PyloadPackageGone(PyloadError):
    """The package no longer exists in pyLoad."""


@dataclass
class PyloadFile:
    fid: int
    name: str
    size: int
    status: int
    statusmsg: str
    error: str


class PyloadClient:
    def __init__(self, url: str, api_key: str,
                 transport: httpx.AsyncBaseTransport | None = None):
        self._client = httpx.AsyncClient(
            base_url=url.rstrip("/"),
            headers={"X-API-Key": api_key},
            timeout=TIMEOUT,
            transport=transport,
        )

    async def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        try:
            resp = await self._client.request(method, path, **kwargs)
        except httpx.HTTPError as e:
            raise PyloadError(f"pyLoad unreachable: {type(e).__name__}") from None
        if resp.status_code in (401, 403):
            raise PyloadError("pyLoad rejected the API key")
        return resp

    @staticmethod
    def _check(resp: httpx.Response) -> None:
        if resp.status_code >= 400:
            raise PyloadError(f"pyLoad returned HTTP {resp.status_code}")

    async def add_package(self, name: str, link: str) -> int:
        resp = await self._request(
            "POST", "/api/add_package",
            json={"name": name, "links": [link], "dest": 1})
        self._check(resp)
        return int(resp.json())

    async def package_files(self, pid: int) -> list[PyloadFile]:
        resp = await self._request(
            "GET", "/api/get_package_data", params={"package_id": pid})
        if resp.status_code == 500:
            raise PyloadPackageGone(f"pyLoad package {pid} not found")
        self._check(resp)
        return [
            PyloadFile(
                fid=int(f["fid"]),
                name=f.get("name", ""),
                size=int(f.get("size") or 0),
                status=int(f.get("status", 0)),
                statusmsg=f.get("statusmsg", ""),
                error=f.get("error", "") or "",
            )
            for f in resp.json().get("links", [])
        ]

    async def progress(self, pid: int) -> tuple[int, float]:
        resp = await self._request("GET", "/api/status_downloads")
        self._check(resp)
        done = 0
        speed = 0.0
        for row in resp.json() or []:
            if row.get("package_id") == pid:
                done += int((row.get("size") or 0) * (row.get("percent") or 0) / 100)
                speed += float(row.get("speed") or 0)
        return done, speed

    async def delete_package(self, pid: int) -> None:
        resp = await self._request(
            "POST", "/api/delete_packages", json={"package_ids": [pid]})
        self._check(resp)

    async def close(self) -> None:
        await self._client.aclose()
