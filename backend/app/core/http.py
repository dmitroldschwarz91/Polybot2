"""HTTP helpers — async (aiohttp) and sync (requests) sessions."""

from __future__ import annotations

import asyncio
from functools import partial as fp

import aiohttp
import requests
import urllib3
from requests.adapters import HTTPAdapter

from ..config import Settings


def create_sync_session(settings: Settings) -> requests.Session:
    s = requests.Session()
    retry = urllib3.Retry(
        total=settings.http_retries, backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


class AsyncHTTP:
    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None

    async def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=20, limit_per_host=10, keepalive_timeout=30)
            )
        return self._session

    async def get(self, url: str, params=None):
        try:
            sess = await self.session()
            timeout = aiohttp.ClientTimeout(total=settings_total, connect=5)
            async with sess.get(url, params=params, timeout=timeout) as r:
                r.raise_for_status()
                return await r.json()
        except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError):
            return None

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            await asyncio.sleep(0.25)


settings_total = 15  # default total timeout; overridden per-call site if needed


async def run_sync(fn, *args):
    return await asyncio.get_running_loop().run_in_executor(None, fp(fn, *args))
