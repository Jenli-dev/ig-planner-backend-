import asyncio
import random
import httpx

# Единый дефолтный timeout для всех запросов
DEFAULT_TIMEOUT = httpx.Timeout(
    connect=10,
    read=30,
    write=30,
    pool=30,
)


class RetryClient(httpx.AsyncClient):
    """
    Async HTTP client с retry + exponential backoff.
    Используется для Graph API, Cloudinary и т.п.
    """

    async def request(
        self,
        method: str,
        url: str,
        *args,
        retries: int = 3,
        backoff: float = 0.5,
        **kwargs,
    ):
        attempt = 0

        while True:
            try:
                return await super().request(
                    method,
                    url,
                    *args,
                    timeout=kwargs.pop("timeout", DEFAULT_TIMEOUT),
                    **kwargs,
                )

            except (
                httpx.ConnectError,
                httpx.ReadTimeout,
                httpx.WriteError,
                httpx.RemoteProtocolError,
            ) as e:
                attempt += 1
                if attempt > retries:
                    raise e

                # exponential backoff + jitter
                await asyncio.sleep(
                    backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.2)
                )

    async def get(self, url: str, *, retries: int = 3, backoff: float = 0.5, **kwargs):
        return await self.request(
            "GET", url, retries=retries, backoff=backoff, **kwargs
        )

    async def post(self, url: str, *, retries: int = 3, backoff: float = 0.5, **kwargs):
        return await self.request(
            "POST", url, retries=retries, backoff=backoff, **kwargs
        )

    async def put(self, url: str, *, retries: int = 3, backoff: float = 0.5, **kwargs):
        return await self.request(
            "PUT", url, retries=retries, backoff=backoff, **kwargs
        )

    async def patch(self, url: str, *, retries: int = 3, backoff: float = 0.5, **kwargs):
        return await self.request(
            "PATCH", url, retries=retries, backoff=backoff, **kwargs
        )

    async def delete(
        self, url: str, *, retries: int = 3, backoff: float = 0.5, **kwargs
    ):
        return await self.request(
            "DELETE", url, retries=retries, backoff=backoff, **kwargs
        )
