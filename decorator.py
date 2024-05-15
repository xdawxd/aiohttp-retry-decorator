import asyncio
import logging
from functools import wraps
from typing import Callable, Generator

from aiohttp import ClientError
from aiohttp.web_exceptions import HTTPException


class _Retry:
    def __init__(
            self,
            func: Callable = None,
            total: int = 1,
            backoff_factor: int | None = None,
            retry_in_seconds: int | None = None,
            log_occurred_exception: bool = False,
            retry_on_exceptions: tuple = (ClientError, HTTPException),
    ):
        self._func = func
        self._total = total
        self._backoff_factor = backoff_factor
        self._retry_in_seconds = retry_in_seconds
        self._log_occurred_exception = log_occurred_exception
        self._retry_on_exceptions = retry_on_exceptions

        self.args: tuple = ()
        self.kwargs: dict = {}

    async def handle_request(self, *args, **kwargs):
        self.args, self.kwargs = args, kwargs
        await self._make_request()

    async def _make_request(self):
        try:
            await self._func(*self.args, **self.kwargs)
        except self._retry_on_exceptions as exception:
            await self._handle_exception(exception)

    async def _handle_exception(self, exception: Exception) -> None:
        if self._log_occurred_exception:
            logging.exception(exception)

        while self._total > 0:
            self._total -= 1
            if self._backoff_factor:
                await asyncio.sleep(next(self._backoff))
            if self._retry_in_seconds:
                await asyncio.sleep(self._retry_in_seconds)

            await self._make_request()

        if self._total == 0:
            raise exception

    @property
    def _backoff(self) -> Generator[int, None, None]:
        yield self._backoff_factor * (2 ** (self._total - 1))


def retry(
    total: int = 1,
    backoff_factor: int | None = None,
    retry_in_seconds: int | None = None,
    log_occurred_exception: bool = False,
    retry_on_exceptions: tuple = (ClientError, HTTPException),
):
    def inner_retry(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retry_obj = _Retry(func, total, backoff_factor, retry_in_seconds, log_occurred_exception, retry_on_exceptions)
            await retry_obj.handle_request(*args, **kwargs)

        return wrapper
    return inner_retry


@retry(total=1, log_occurred_exception=True)
async def test_retry():
    raise ClientError


async def main():
    await test_retry()


if __name__ == "__main__":
    asyncio.run(main())
