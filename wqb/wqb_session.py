import json
import os
import asyncio
import datetime
import itertools
import logging
from collections.abc import Awaitable, Callable, Coroutine, Generator, Iterable, Sized
import time
from typing import Any, Dict, List, Type, Optional, Tuple
import concurrent
from requests import RequestException, Response
import requests
from requests.auth import HTTPBasicAuth

from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from . import (
    GET,
    POST,
    LOCATION,
    RETRY_AFTER,
    EQUITY,
    Null,
    Alpha,
    MultiAlpha,
    Region,
    Delay,
    Universe,
    InstrumentType,
    DataCategory,
    FieldType,
    DatasetsOrder,
    FieldsOrder,
    Status,
    AlphaType,
    AlphaCategory,
    Language,
    Color,
    Neutralization,
    UnitHandling,
    NanHandling,
    Pasteurization,
    AlphasOrder,
)
from .auto_auth_session import AutoAuthSession
from .filter_range import FilterRange
from .wqb_urls import (
    URL_ALPHAS,
    URL_ALPHAS_ALPHAID,
    URL_ALPHAS_ALPHAID_CHECK,
    URL_ALPHAS_ALPHAID_SUBMIT,
    URL_AUTHENTICATION,
    URL_DATAFIELDS,
    URL_DATAFIELDS_FIELDID,
    URL_DATASETS,
    URL_DATASETS_DATASETID,
    URL_OPERATORS,
    URL_SIMULATIONS,
    WQB_API_URL,
    URL_USERS_SELF_ALPHAS,
)

__all__ = ['print', 'wqb_logger', 'to_multi_alphas', 'concurrent_await', 'WQBSession']


_print = print


def print(
    *args,
    **kwargs,
) -> None:
    """
    Prints, and then flushes instantly.

    The usage is the same as the built-in `print`.

    Parameters
    ----------
    See also the built-in `print`.

    Returns
    -------
    None

    Notes
    -----
    `args` and `kwargs` are passed to the built-in `print`. `flush` is
    overridden to True no matter what.
    """
    kwargs['flush'] = True
    _print(*args, **kwargs)


def wqb_logger(
    *,
    name: str | None = None,
    log_dir: str = "logs",
) -> logging.Logger:
    """
    Returns a pre-configured `logging.Logger` object.

    INFO logs are written to both the .log file and the console.

    WARNING logs are written to the console only.

    Parameters
    ----------
    name: str | None = None
        `logging.Logger.name`. If *None*, it is set to 'wqb' followed by
        the current datetime. The filename of the .log file is set to
        `name` followed by '.log'.

    Returns
    -------
    logging.Logger
        A pre-configured `logging.Logger` object.
    """
    if name is None:
        name = 'wqb' + datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    logger = logging.getLogger(name=name)
    logger.setLevel(logging.INFO)
    handler1 = logging.FileHandler(f"{logger.name}.log")
    handler1.setLevel(logging.INFO)
    handler1.setFormatter(
        logging.Formatter(fmt='# %(levelname)s %(asctime)s\n%(message)s\n')
    )
    logger.addHandler(handler1)
    handler2 = logging.StreamHandler()
    handler2.setLevel(logging.WARNING)
     # 1. 创建日志文件夹 (如果不存在)
    if log_dir:  # 确保 log_dir 不是 None 或空字符串
        os.makedirs(log_dir, exist_ok=True)  # exist_ok=True: 文件夹已存在时不会报错

    # 2. 构建完整的日志文件路径
    log_file_path = os.path.join(
        log_dir, f"{logger.name}.log"
    )  # 使用 os.path.join 拼接路径

    # print("log_file_path", log_file_path)
    handler1 = logging.FileHandler(
        log_file_path, encoding="utf-8"
    )  # 使用完整的日志文件路径
    handler2.setFormatter(
        logging.Formatter(fmt='# %(levelname)s %(asctime)s\n%(message)s\n')
    )
    logger.addHandler(handler2)
    return logger


def to_multi_alphas(
    alphas: Iterable[Alpha],
    multiple: int | Iterable[Any],
) -> Generator[MultiAlpha, None, None]:
    """
    Converts an iterable series of `Alpha` objects to an iterable series
    of `MultiAlpha` objects.

    Parameters
    ----------
    alphas: Iterable[Alpha]
        The iterable series of `Alpha` objects.
    multiple: int | Iterable[Any]
        The number of `Alpha` objects to be grouped into a `MultiAlpha`
        object. If *int*, the `Alpha` objects are grouped by it. If
        *Iterable[Any]*, the `Alpha` objects are grouped by its length.

    Returns
    -------
    Iterable[MultiAlpha]
        An iterable series of `MultiAlpha` objects.

    Examples
    --------
    >>> alphas = [{...} for _ in range(6)]
    >>> alphas
    [{...}, {...}, {...}, {...}, {...}, {...}]
    >>> multi_alphas = list(wqb.to_multi_alphas(alphas, 3))
    >>> multi_alphas
    [[{...}, {...}, {...}], [{...}, {...}, {...}]]
    """
    alphas = iter(alphas)
    multiple = range(multiple) if isinstance(multiple, int) else tuple(multiple)
    try:
        while True:
            multi_alpha = []
            for _ in multiple:
                multi_alpha.append(next(alphas))
            yield multi_alpha
    except StopIteration as e:
        if 0 < len(multi_alpha):
            yield multi_alpha


async def concurrent_await(
    awaitables: Iterable[Awaitable[Any]],
    *,
    concurrency: int | asyncio.Semaphore | None = None,
    return_exceptions: bool = False,
) -> Coroutine[None, None, list[Any | BaseException]]:
    """
    Returns a `Coroutine` object that awaits an iterable series of
    `Awaitable` objects with a concurrency limit that controls the
    maximum number of `Awaitable` objects that can be awaited at the
    same time.

    Parameters
    ----------
    awaitables: Iterable[Awaitable[Any]]
        The iterable series of `Awaitable` objects.
    concurrency: int | asyncio.Semaphore | None = None
        The maximum number of `Awaitable` objects that can be awaited at
        the same time. If *int | asyncio.Semaphore*, the concurrency
        limit is set to it. If *None*, there is no concurrency limit.
    return_exceptions: bool = False
        Whether to return exceptions instead of raising them.

    Returns
    -------
    Coroutine[None, None, list[Any | BaseException]]
        A `Coroutine` object that awaits `Awaitable` objects
        concurrently.
    """
    if concurrency is None:
        return await asyncio.gather(*awaitables)
    if isinstance(concurrency, int):
        concurrency = asyncio.Semaphore(value=concurrency)

    async def semaphore_wrapper(
        awaitable: Awaitable[Any],
    ) -> Coroutine[None, None, Any]:
        """
        Wraps an `Awaitable` object with `concurrency`.

        Parameters
        ----------
        awaitable: Awaitable[Any]
            The `Awaitable` object to be wrapped.

        Returns
        -------
        Coroutine[None, None, Any]
            A `Coroutine` object that awaits the wrapped `Awaitable`
            object.
        """
        async with concurrency:
            result = await awaitable
        return result

    return await asyncio.gather(
        *(semaphore_wrapper(awaitable) for awaitable in awaitables),
        return_exceptions=return_exceptions,
    )


class WQBSession(AutoAuthSession):
    """
    A class that implements common APIs of WorldQuant BRAIN platform.
    """

    def __init__(
        self,
        wqb_auth: tuple[str, str] | HTTPBasicAuth,
        *,
        logger: logging.Logger = logging.root,
        **kwargs,
    ) -> None:
        """
        Initializes a `WQBSession` object.

        Parameters
        ----------
        wqb_auth: tuple[str, str] | HTTPBasicAuth
            The authentication credentials that consist of email and
            password.
        logger: logging.Logger = logging.root
            The `logging.Logger` object to log requests.

        Returns
        -------
        None

        Notes
        -----
        No `args` are accepted, while `kwargs` are passed to
        `AutoAuthSession.__init__`.

        Examples
        --------
        Without setting `logger`:

        >>> wqbs = wqb.WQBSession(('<email>', '<password>'))

        With setting `logger` (Recommended):

        >>> logger = wqb.wqb_logger()
        >>> wqbs = wqb.WQBSession(
        ...     ('<email>', '<password>'),
        ...     logger=logger,
        ... )
        """
        if not isinstance(wqb_auth, HTTPBasicAuth):
            wqb_auth = HTTPBasicAuth(*wqb_auth)
        kwargs['auth'] = wqb_auth
        super().__init__(
            POST,
            URL_AUTHENTICATION,
            auth_expected=lambda resp: 201 == resp.status_code,
            expected=lambda resp: resp.status_code not in (204, 401, 429),
            logger=logger,
            **kwargs,
        )
        self.expected_location = (
            lambda resp: self.expected(resp) and LOCATION in resp.headers
        )

    def __repr__(
        self,
    ) -> str:
        """
        Returns a string representation of the `WQBSession` object.

        Returns
        -------
        str
            A string representation of the `WQBSession` object.
        """
        return f"<WQBSession [{repr(self.wqb_auth.username)}]>"

    @property
    def wqb_auth(
        self,
    ) -> HTTPBasicAuth:
        """
        `wqb_auth`
        """
        return self.kwargs['auth']

    @wqb_auth.setter
    def wqb_auth(
        self,
        wqb_auth: tuple[str, str] | HTTPBasicAuth,
    ) -> None:
        """
        `wqb_auth`
        """
        if not isinstance(wqb_auth, HTTPBasicAuth):
            wqb_auth = HTTPBasicAuth(*wqb_auth)
        self.kwargs['auth'] = wqb_auth

    def get_authentication(
        self,
        *args,
        log: str | None = '',
        **kwargs,
    ) -> Response:
        """
        Sends a GET request to `URL_AUTHENTICATION`.

        Parameters
        ----------
        log: str | None = ''
            The message to be appended. If *None*, logging is disabled.

        Returns
        -------
        Response
            A `Response` object.

        Notes
        -----
        `args` and `kwargs` are passed to `Session.get`.

        Examples
        --------
        >>> wqbs = wqb.WQBSession(('<email>', '<password>'))
        >>> resp = wqbs.get_authentication()
        >>> resp.ok
        True
        """
        url = URL_AUTHENTICATION
        resp = self.get(url, *args, **kwargs)
        if log is not None:
            self.logger.info(
                '\n'.join(
                    (
                        f"{self}.get_authentication(...) [",
                        f"    {url}",
                        f"]: {log}",
                    )
                )
            )
        return resp

    def post_authentication(
        self,
        *args,
        log: str | None = '',
        **kwargs,
    ) -> Response:
        """
        Sends a POST request to `URL_AUTHENTICATION`.

        Parameters
        ----------
        log: str | None = ''
            The message to be appended. If *None*, logging is disabled.

        Returns
        -------
        Response
            A `Response` object.

        Notes
        -----
        `args` and `kwargs` are passed to `Session.post`.

        Examples
        --------
        >>> wqbs = wqb.WQBSession(('<email>', '<password>'))
        >>> resp = wqbs.post_authentication(auth=wqbs.wqb_auth)
        >>> resp.ok
        True
        """
        url = URL_AUTHENTICATION
        resp = self.post(url, *args, auth=self.wqb_auth, **kwargs)
        if log is not None:
            self.logger.info(
                '\n'.join(
                    (
                        f"{self}.post_authentication(...) [",
                        f"    {url}",
                        f"]: {log}",
                    )
                )
            )
        return resp

    def delete_authentication(
        self,
        *args,
        log: str | None = '',
        **kwargs,
    ) -> Response:
        """
        Sends a DELETE request to `URL_AUTHENTICATION`.

        Parameters
        ----------
        log: str | None = ''
            The message to be appended. If *None*, logging is disabled.

        Returns
        -------
        Response
            A `Response` object.

        Notes
        -----
        `args` and `kwargs` are passed to `Session.delete`.

        Examples
        --------
        >>> wqbs = wqb.WQBSession(('<email>', '<password>'))
        >>> resp = wqbs.delete_authentication()
        >>> resp.ok
        True
        """
        url = URL_AUTHENTICATION
        resp = self.delete(url, *args, **kwargs)
        if log is not None:
            self.logger.info(
                '\n'.join(
                    (
                        f"{self}.delete_authentication(...) [",
                        f"    {url}",
                        f"]: {log}",
                    )
                )
            )
        return resp

    def head_authentication(
        self,
        *args,
        log: str | None = '',
        **kwargs,
    ) -> Response:
        """
        Sends a HEAD request to `URL_AUTHENTICATION`.

        Parameters
        ----------
        log: str | None = ''
            The message to be appended. If *None*, logging is disabled.

        Returns
        -------
        Response
            A `Response` object.

        Notes
        -----
        `args` and `kwargs` are passed to `Session.head`.

        Examples
        --------
        >>> wqbs = wqb.WQBSession(('<email>', '<password>'))
        >>> resp = wqbs.head_authentication()
        >>> resp.ok
        True
        """
        url = URL_AUTHENTICATION
        resp = self.head(url, *args, **kwargs)
        if log is not None:
            self.logger.info(
                '\n'.join(
                    (
                        f"{self}.head_authentication(...) [",
                        f"    {url}",
                        f"]: {log}",
                    )
                )
            )
        return resp

    def search_operators(
        self,
        *args,
        log: str | None = '',
        **kwargs,
    ) -> Response:
        """
        Sends a GET request to `URL_OPERATORS`.

        Parameters
        ----------
        log: str | None = ''
            The message to be appended. If *None*, logging is disabled.

        Returns
        -------
        Response
            A `Response` object.

        Notes
        -----
        `args` and `kwargs` are passed to `Session.get`.

        Examples
        --------
        >>> wqbs = wqb.WQBSession(('<email>', '<password>'))
        >>> resp = wqbs.search_operators()
        >>> resp.ok
        True
        """
        url = URL_OPERATORS
        resp = self.get(url, *args, **kwargs)
        if log is not None:
            self.logger.info(
                '\n'.join(
                    (
                        f"{self}.search_operators(...) [",
                        f"    {url}",
                        f"]: {log}",
                    )
                )
            )
        return resp

    def locate_dataset(
        self,
        dataset_id: str,
        *args,
        log: str | None = '',
        **kwargs,
    ) -> Response:
        """
        Sends a GET request to
        `URL_DATASETS_DATASETID.format(dataset_id)`.

        Parameters
        ----------
        dataset_id: str
            The dataset ID.
        log: str | None = ''
            The message to be appended. If *None*, logging is disabled.

        Returns
        -------
        Response
            A `Response` object.

        Notes
        -----
        `args` and `kwargs` are passed to `Session.get`.

        Examples
        --------
        >>> wqbs = wqb.WQBSession(('<email>', '<password>'))
        >>> resp = wqbs.locate_dataset('pv1')
        >>> resp.ok
        True
        """
        url = URL_DATASETS_DATASETID.format(dataset_id)
        resp = self.get(url, *args, **kwargs)
        if log is not None:
            self.logger.info(
                '\n'.join(
                    (
                        f"{self}.locate_dataset(...) [",
                        f"    {url}",
                        f"]: {log}",
                    )
                )
            )
        return resp

    def search_datasets_limited(
        self,
        region: Region,
        delay: Delay,
        universe: Universe,
        *args,
        instrument_type: InstrumentType = EQUITY,
        search: str | None = None,
        category: DataCategory | None = None,
        theme: bool | None = None,
        coverage: FilterRange | None = None,
        value_score: FilterRange | None = None,
        alpha_count: FilterRange | None = None,
        user_count: FilterRange | None = None,
        order: DatasetsOrder | None = None,
        limit: int = 50,
        offset: int = 0,
        others: Iterable[str] | None = None,
        log: str | None = '',
        **kwargs,
    ) -> Response:
        limit = min(max(limit, 1), 50)
        offset = min(max(offset, 0), 10000 - limit)
        params = [
            f"region={region}",
            f"delay={delay}",
            f"universe={universe}",
        ]
        params.append(f"instrumentType={instrument_type}")
        if search is not None:
            params.append(f"search={search}")
        if category is not None:
            params.append(f"category={category}")
        if theme is not None:
            params.append(f"theme={'true' if theme else 'false'}")
        if coverage is not None:
            params.append(coverage.to_params('coverage'))
        if value_score is not None:
            params.append(value_score.to_params('valueScore'))
        if alpha_count is not None:
            params.append(alpha_count.to_params('alphaCount'))
        if user_count is not None:
            params.append(user_count.to_params('userCount'))
        if order is not None:
            params.append(f"order={order}")
        params.append(f"limit={limit}")
        params.append(f"offset={offset}")
        if others is not None:
            params.extend(others)
        url = URL_DATASETS + '?' + '&'.join(params)
        resp = self.get(url, *args, **kwargs)
        if log is not None:
            self.logger.info(
                '\n'.join(
                    (
                        f"{self}.search_datasets_limited(...) [",
                        f"    {url}",
                        f"]: {log}",
                    )
                )
            )
        return resp

    def search_datasets(
        self,
        region: Region,
        delay: Delay,
        universe: Universe,
        *args,
        limit: int = 50,
        offset: int = 0,
        log: str | None = '',
        log_gap: int = 100,
        **kwargs,
    ) -> Generator[Response, None, None]:
        if log is None:
            log_gap = 0
        count = self.search_datasets_limited(
            region, delay, universe, *args, limit=1, offset=offset, log=log, **kwargs
        ).json()['count']
        offsets = range(offset, count, limit)
        if log is not None:
            self.logger.info(f"{self}.search_datasets(...) [start {offsets}]: {log}")
        total = len(offsets)
        for idx, offset in enumerate(offsets, start=1):
            yield self.search_datasets_limited(
                region,
                delay,
                universe,
                *args,
                limit=limit,
                offset=offset,
                log=(
                    f"{idx}/{total} = {int(100*idx/total)}%"
                    if 0 != log_gap and 0 == idx % log_gap
                    else None
                ),
                **kwargs,
            )
        if log is not None:
            self.logger.info(f"{self}.search_datasets(...) [finish {offsets}]: {log}")

    def locate_field(
        self,
        field_id: str,
        *args,
        log: str | None = '',
        **kwargs,
    ) -> Response:
        url = URL_DATAFIELDS_FIELDID.format(field_id)
        resp = self.get(url, *args, **kwargs)
        if log is not None:
            self.logger.info(
                '\n'.join(
                    (
                        f"{self}.locate_field(...) [",
                        f"    {url}",
                        f"]: {log}",
                    )
                )
            )
        return resp

    def search_fields_limited(
        self,
        region: Region,
        delay: Delay,
        universe: Universe,
        *args,
        instrument_type: InstrumentType = EQUITY,
        dataset_id: str | None = None,
        search: str | None = None,
        category: DataCategory | None = None,
        theme: bool | None = None,
        coverage: FilterRange | None = None,
        type: FieldType | None = None,
        alpha_count: FilterRange | None = None,
        user_count: FilterRange | None = None,
        order: FieldsOrder | None = None,
        limit: int = 50,
        offset: int = 0,
        others: Iterable[str] | None = None,
        log: str | None = '',
        **kwargs,
    ) -> Response:
        limit = min(max(limit, 1), 50)
        offset = min(max(offset, 0), 10000 - limit)
        params = [
            f"region={region}",
            f"delay={delay}",
            f"universe={universe}",
        ]
        params.append(f"instrumentType={instrument_type}")
        if dataset_id is not None:
            params.append(f"dataset.id={dataset_id}")
        if search is not None:
            params.append(f"search={search}")
        if category is not None:
            params.append(f"category={category}")
        if theme is not None:
            params.append(f"theme={'true' if theme else 'false'}")
        if coverage is not None:
            params.append(coverage.to_params('coverage'))
        if type is not None:
            params.append(f"type={type}")
        if alpha_count is not None:
            params.append(alpha_count.to_params('alphaCount'))
        if user_count is not None:
            params.append(user_count.to_params('userCount'))
        if order is not None:
            params.append(f"order={order}")
        params.append(f"limit={limit}")
        params.append(f"offset={offset}")
        if others is not None:
            params.extend(others)
        url = URL_DATAFIELDS + '?' + '&'.join(params)
        resp = self.get(url, *args, **kwargs)
        if log is not None:
            self.logger.info(
                '\n'.join(
                    (
                        f"{self}.search_fields_limited(...) [",
                        f"    {url}",
                        f"]: {log}",
                    )
                )
            )
        return resp

    def search_fields(
        self,
        region: Region,
        delay: Delay,
        universe: Universe,
        *args,
        limit: int = 50,
        offset: int = 0,
        log: str | None = '',
        log_gap: int = 100,
        **kwargs,
    ) -> Generator[Response, None, None]:
        if log is None:
            log_gap = 0
        count = self.search_fields_limited(
            region, delay, universe, *args, limit=1, offset=offset, log=log, **kwargs
        ).json()['count']
        offsets = range(offset, count, limit)
        if log is not None:
            self.logger.info(f"{self}.search_fields(...) [start {offsets}]: {log}")
        total = len(offsets)
        for idx, offset in enumerate(offsets, start=1):
            yield self.search_fields_limited(
                region,
                delay,
                universe,
                *args,
                limit=limit,
                offset=offset,
                log=(
                    f"{idx}/{total} = {int(100*idx/total)}%"
                    if 0 != log_gap and 0 == idx % log_gap
                    else None
                ),
                **kwargs,
            )
        if log is not None:
            self.logger.info(f"{self}.search_fields(...) [finish {offsets}]: {log}")

    async def locate_alpha(  # 改为 async def
        self,
        alpha_id: str,
        *args,
        log: str | None = "",  # 用户提供的日志上下文信息
        # 新增的重试参数 (设为仅关键字参数以增加清晰度)
        max_retries: int = 5,
        initial_retry_delay: float = 1.0,
        backoff_factor: float = 2.0,
        retry_status_codes: Iterable[int] = (
            429,
            500,
            502,
            503,
            504,
        ),  # 可重试的HTTP状态码
        retry_exceptions: tuple[Type[BaseException], ...] = (  # 可重试的异常类型
            requests.exceptions.ConnectionError,  # 假设 requests.exceptions 已被导入
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ),
        **kwargs,
    ) -> "Response | None":  # 使用字符串避免在方法签名处立即需要 Response 定义
        # 假设 URL_ALPHAS_ALPHAID 是类变量或全局变量
        url = URL_ALPHAS_ALPHAID.format(alpha_id)

        current_delay = initial_retry_delay
        final_resp: "Response | None" = None

        for attempt in range(max_retries + 1):  # 0次重试意味着总共尝试1次
            try:
                # 如果 self.get 是同步方法，使用 asyncio.to_thread 在线程池中运行
                # 假设 self.get 是同步的，模仿 patch_properties 的做法
                final_resp = await asyncio.to_thread(self.get, url, *args, **kwargs)

                if final_resp.status_code < 400:  # 例如 2xx 表示成功
                    if (
                        log is not None and log != "" and attempt != 0
                    ):  # 只有重试才输出日志
                        self.logger.info(
                            f"{self}.locate_alpha(alpha_id={alpha_id}) successful on attempt {attempt + 1}. "
                            f"Status: {final_resp.status_code}. Context: {log}"
                        )
                        print(
                            f"{self}.locate_alpha(alpha_id={alpha_id}) successful on attempt {attempt + 1}. "
                            f"Status: {final_resp.status_code}. Context: {log}"
                        )
                    return final_resp

                if final_resp.status_code in retry_status_codes:
                    if attempt < max_retries:
                        self.logger.warning(
                            f"{self}.locate_alpha(alpha_id={alpha_id}) attempt {attempt + 1}/{max_retries + 1} failed with status {final_resp.status_code}. "
                            f"Retrying in {current_delay:.2f}s. URL: {url}. Context: {log if log else 'No specific context'}"
                        )
                        print(
                            f"{self}.locate_alpha(alpha_id={alpha_id}) attempt {attempt + 1}/{max_retries + 1} failed with status {final_resp.status_code}. "
                            f"Retrying in {current_delay:.2f}s. URL: {url}. Context: {log if log else 'No specific context'}"
                        )
                        await asyncio.sleep(current_delay)  # 使用 asyncio.sleep
                        current_delay *= backoff_factor
                    else:
                        self.logger.error(
                            f"{self}.locate_alpha(alpha_id={alpha_id}) failed after {max_retries + 1} attempts. "
                            f"Last status: {final_resp.status_code}. URL: {url}. Response: {final_resp.text[:200]}. Context: {log if log else 'No specific context'}"
                        )
                        print(
                            f"{self}.locate_alpha(alpha_id={alpha_id}) failed after {max_retries + 1} attempts. "
                            f"Last status: {final_resp.status_code}. URL: {url}. Response: {final_resp.text[:200]}. Context: {log if log else 'No specific context'}"
                        )
                        break
                else:
                    self.logger.error(
                        f"{self}.locate_alpha(alpha_id={alpha_id}) failed with non-retryable status {final_resp.status_code}. "
                        f"URL: {url}. Response: {final_resp.text[:200]}. Context: {log if log else 'No specific context'}"
                    )
                    print(
                        f"{self}.locate_alpha(alpha_id={alpha_id}) failed with non-retryable status {final_resp.status_code}. "
                        f"URL: {url}. Response: {final_resp.text[:200]}. Context: {log if log else 'No specific context'}"
                    )
                    return final_resp

            except retry_exceptions as e:
                if attempt < max_retries:
                    self.logger.warning(
                        f"{self}.locate_alpha(alpha_id={alpha_id}) attempt {attempt + 1}/{max_retries + 1} failed with {type(e).__name__}: {e}. "
                        f"Retrying in {current_delay:.2f}s. URL: {url}. Context: {log if log else 'No specific context'}"
                    )
                    print(
                        f"{self}.locate_alpha(alpha_id={alpha_id}) attempt {attempt + 1}/{max_retries + 1} failed with {type(e).__name__}: {e}. "
                        f"Retrying in {current_delay:.2f}s. URL: {url}. Context: {log if log else 'No specific context'}"
                    )
                    await asyncio.sleep(current_delay)  # 使用 asyncio.sleep
                    current_delay *= backoff_factor
                else:
                    self.logger.error(
                        f"{self}.locate_alpha(alpha_id={alpha_id}) failed after {max_retries + 1} attempts due to {type(e).__name__}: {e}. "
                        f"URL: {url}. Context: {log if log else 'No specific context'}"
                    )
                    print(
                        f"{self}.locate_alpha(alpha_id={alpha_id}) failed after {max_retries + 1} attempts due to {type(e).__name__}: {e}. "
                        f"URL: {url}. Context: {log if log else 'No specific context'}"
                    )
                    final_resp = None
                    break
            except Exception as e:
                self.logger.exception(
                    f"{self}.locate_alpha(alpha_id={alpha_id}) encountered an unexpected error on attempt {attempt + 1}. "
                    f"URL: {url}. Context: {log if log else 'No specific context'}"
                )
                print(
                    f"{self}.locate_alpha(alpha_id={alpha_id}) encountered an unexpected error on attempt {attempt + 1}. "
                    f"URL: {url}. Context: {log if log else 'No specific context'}"
                )
                final_resp = None
                break

        # 最终失败的补充日志，模仿 patch_properties
        if final_resp and final_resp.status_code >= 400 and attempt == max_retries:
            if log is not None and log != "":
                self.logger.error(
                    "\n".join(
                        (
                            f"{self}.locate_alpha(...) [FAILED after all retries for HTTP error]",
                            f"    alpha_id: {alpha_id}",
                            f"    url: {url}",
                            f"    last_status: {final_resp.status_code}",
                            f"    last_response_text: {final_resp.text[:200]}",
                            f"] Original Context: {log}",
                        )
                    )
                )
                print(
                    "\n".join(
                        (
                            f"{self}.locate_alpha(...) [FAILED after all retries for HTTP error]",
                            f"    alpha_id: {alpha_id}",
                            f"    url: {url}",
                            f"    last_status: {final_resp.status_code}",
                            f"    last_response_text: {final_resp.text[:200]}",
                            f"] Original Context: {log}",
                        )
                    )
                )
        elif not final_resp and attempt == max_retries:
            if log is not None and log != "":
                self.logger.error(
                    "\n".join(
                        (
                            f"{self}.locate_alpha(...) [FAILED after all retries due to exception]",
                            f"    alpha_id: {alpha_id}",
                            f"    url: {url}",
                            f"] Original Context: {log}",
                        )
                    )
                )
                print(
                    "\n".join(
                        (
                            f"{self}.locate_alpha(...) [FAILED after all retries due to exception]",
                            f"    alpha_id: {alpha_id}",
                            f"    url: {url}",
                            f"] Original Context: {log}",
                        )
                    )
                )
        return final_resp

    def filter_alphas_limited(
        self,
        *args,
        name: str | None = None,
        competition: bool | None = None,
        type: AlphaType | None = None,
        language: Language | None = None,
        date_created: FilterRange | None = None,
        favorite: bool | None = None,
        date_submitted: FilterRange | None = None,
        start_date: FilterRange | None = None,
        status: Status | None = None,
        category: AlphaCategory | None = None,
        color: Color | None = None,
        tag: str | None = None,
        hidden: bool | None = None,
        region: Region | None = None,
        instrument_type: InstrumentType | None = None,
        universe: Universe | None = None,
        delay: Delay | None = None,
        decay: FilterRange | None = None,
        neutralization: Neutralization | None = None,
        truncation: FilterRange | None = None,
        unit_handling: UnitHandling | None = None,
        nan_handling: NanHandling | None = None,
        pasteurization: Pasteurization | None = None,
        sharpe: FilterRange | None = None,
        returns: FilterRange | None = None,
        pnl: FilterRange | None = None,
        turnover: FilterRange | None = None,
        drawdown: FilterRange | None = None,
        margin: FilterRange | None = None,
        fitness: FilterRange | None = None,
        book_size: FilterRange | None = None,
        long_count: FilterRange | None = None,
        short_count: FilterRange | None = None,
        sharpe60: FilterRange | None = None,
        sharpe125: FilterRange | None = None,
        sharpe250: FilterRange | None = None,
        sharpe500: FilterRange | None = None,
        os_is_sharpe_ratio: FilterRange | None = None,
        pre_close_sharpe: FilterRange | None = None,
        pre_close_sharpe_ratio: FilterRange | None = None,
        self_correlation: FilterRange | None = None,
        prod_correlation: FilterRange | None = None,
        order: AlphasOrder | None = None,
        limit: int = 100,
        offset: int = 0,
        others: Iterable[str] | None = None,
        log: str | None = '',
        **kwargs,
    ) -> Response:
        limit = min(max(limit, 1), 100)
        offset = min(max(offset, 0), 10000 - limit)
        params = []
        if name is not None:
            params.append(f"name{name if name[0] in '~=' else '~' + name}")
        if competition is not None:
            params.append(f"competition={'true' if competition else 'false'}")
        if type is not None:
            params.append(f"type={type}")
        if language is not None:
            params.append(f"settings.language={language}")
        if date_created is not None:
            params.append(date_created.to_params('dateCreated'))
        if favorite is not None:
            params.append(f"favorite={'true' if favorite else 'false'}")
        if date_submitted is not None:
            params.append(date_submitted.to_params('dateSubmitted'))
        if start_date is not None:
            params.append(start_date.to_params('os.startDate'))
        if status is not None:
            params.append(f"status={status}")
        if category is not None:
            params.append(f"category={category}")
        if color is not None:
            params.append(f"color={color}")
        if tag is not None:
            params.append(f"tag={tag}")
        if hidden is not None:
            params.append(f"hidden={'true' if hidden else 'false'}")
        if region is not None:
            params.append(f"settings.region={region}")
        if instrument_type is not None:
            params.append(f"settings.instrumentType={instrument_type}")
        if universe is not None:
            params.append(f"settings.universe={universe}")
        if delay is not None:
            params.append(f"settings.delay={delay}")
        if decay is not None:
            params.append(decay.to_params('settings.decay'))
        if neutralization is not None:
            params.append(f"settings.neutralization={neutralization}")
        if truncation is not None:
            params.append(truncation.to_params('settings.truncation'))
        if unit_handling is not None:
            params.append(f"settings.unitHandling={unit_handling}")
        if nan_handling is not None:
            params.append(f"settings.nanHandling={nan_handling}")
        if pasteurization is not None:
            params.append(f"settings.pasteurization={pasteurization}")
        if sharpe is not None:
            params.append(sharpe.to_params('is.sharpe'))
        if returns is not None:
            params.append(returns.to_params('is.returns'))
        if pnl is not None:
            params.append(pnl.to_params('is.pnl'))
        if turnover is not None:
            params.append(turnover.to_params('is.turnover'))
        if drawdown is not None:
            params.append(drawdown.to_params('is.drawdown'))
        if margin is not None:
            params.append(margin.to_params('is.margin'))
        if fitness is not None:
            params.append(fitness.to_params('is.fitness'))
        if book_size is not None:
            params.append(book_size.to_params('is.bookSize'))
        if long_count is not None:
            params.append(long_count.to_params('is.longCount'))
        if short_count is not None:
            params.append(short_count.to_params('is.shortCount'))
        if sharpe60 is not None:
            params.append(sharpe60.to_params('os.sharpe60'))
        if sharpe125 is not None:
            params.append(sharpe125.to_params('os.sharpe125'))
        if sharpe250 is not None:
            params.append(sharpe250.to_params('os.sharpe250'))
        if sharpe500 is not None:
            params.append(sharpe500.to_params('os.sharpe500'))
        if os_is_sharpe_ratio is not None:
            params.append(os_is_sharpe_ratio.to_params('os.osISSharpeRatio'))
        if pre_close_sharpe is not None:
            params.append(pre_close_sharpe.to_params('os.preCloseSharpe'))
        if pre_close_sharpe_ratio is not None:
            params.append(pre_close_sharpe_ratio.to_params('os.preCloseSharpeRatio'))
        if self_correlation is not None:
            params.append(self_correlation.to_params('is.selfCorrelation'))
        if prod_correlation is not None:
            params.append(prod_correlation.to_params('is.prodCorrelation'))
        if order is not None:
            params.append(f"order={order}")
        params.append(f"limit={limit}")
        params.append(f"offset={offset}")
        if others is not None:
            params.extend(others)
        url = URL_USERS_SELF_ALPHAS + '?' + '&'.join(params)
        url = url.replace('+', '%2B')  # TODO: Can be improved.
        resp = self.get(url, *args, **kwargs)
        if log is not None:
            self.logger.info(
                '\n'.join(
                    (
                        f"{self}.filter_alphas_limited(...) [",
                        f"    {url}",
                        f"]: {log}",
                    )
                )
            )
        return resp

    def filter_alphas(
        self,
        *args,
        limit: int = 100,
        offset: int = 0,
        log: str | None = '',
        log_gap: int = 100,
        **kwargs,
    ) -> Generator[Response, None, None]:
        if log is None:
            log_gap = 0
        count = self.filter_alphas_limited(
            *args, limit=1, offset=offset, log=log, **kwargs
        ).json()['count']
        offsets = range(offset, count, limit)
        if log is not None:
            self.logger.info(f"{self}.filter_alphas(...) [start {offsets}]: {log}")
        total = len(offsets)
        for idx, offset in enumerate(offsets, start=1):
            yield self.filter_alphas_limited(
                *args,
                limit=limit,
                offset=offset,
                log=(
                    f"{idx}/{total} = {int(100*idx/total)}%"
                    if 0 != log_gap and 0 == idx % log_gap
                    else None
                ),
                **kwargs,
            )
        if log is not None:
            self.logger.info(f"{self}.filter_alphas(...) [finish {offsets}]: {log}")

    def patch_properties(
        self,
        alpha_id: str,
        *args,
        favorite: bool | None = None,
        hidden: bool | None = None,
        name: str | Null | None = None,
        category: AlphaCategory | Null | None = None,
        tags: str | Iterable[str] | Null | None = None,
        color: Color | Null | None = None,
        regular_description: str | Null | None = None,
        log: str | None = '',
        **kwargs,
    ) -> Response:
        url = URL_ALPHAS_ALPHAID.format(alpha_id)
        properties = {}
        if favorite is not None:
            properties['favorite'] = favorite
        if hidden is not None:
            properties['hidden'] = hidden
        if name is not None:
            properties['name'] = None if isinstance(name, Null) else name
        if category is not None:
            properties['category'] = None if isinstance(category, Null) else category
        if tags is not None:
            properties['tags'] = (
                []
                if isinstance(tags, Null)
                else [tags] if isinstance(tags, str) else list(tags)
            )
        if color is not None:
            properties['color'] = None if isinstance(color, Null) else color
        if regular_description is not None:
            properties['regular'] = {}
            properties['regular']['description'] = (
                None if isinstance(regular_description, Null) else regular_description
            )
        resp = self.patch(url, json=properties, *args, **kwargs)
        if log is not None:
            self.logger.info(
                '\n'.join(
                    (
                        f"{self}.patch_properties(...) [",
                        f"    {url}",
                        f"    {properties}",
                        f"]: {log}",
                    )
                )
            )
        return resp

    async def retry(
        self,
        method: str,
        url: str,
        *args,
        max_tries: int | Iterable[Any] = itertools.repeat(None),
        max_key_errors: int = 1,
        max_value_errors: int = 1,
        delay_key_error: float = 2.0,
        delay_value_error: float = 2.0,
        on_start: Callable[[dict[str, Any]], None] | None = None,
        on_finish: Callable[[dict[str, Any]], None] | None = None,
        on_success: Callable[[dict[str, Any]], None] | None = None,
        on_failure: Callable[[dict[str, Any]], None] | None = None,
        log: str | None = '',
        **kwargs,
    ) -> Coroutine[None, None, Response | None]:
        if isinstance(max_tries, int):
            max_tries = range(max_tries)
        tries = 0
        resp = None
        key_errors = 0
        value_errors = 0
        if log is not None:
            self.logger.info(f"{self}.retry(...) [start {max_tries}]: {log}")
        if on_start is not None:
            on_start(locals())
        for tries, _ in enumerate(max_tries, start=1):
            resp = self.request(method, url, *args, **kwargs)
            try:
                await asyncio.sleep(float(resp.headers[RETRY_AFTER]))
            except KeyError as e:
                key_errors += 1
                if max_key_errors <= key_errors:
                    if log is not None:
                        self.logger.info(
                            f"{self}.retry(...) [{key_errors} key_errors]: {log}"
                        )
                    if on_success is not None:
                        on_success(locals())
                    break
                await asyncio.sleep(delay_key_error)
            except ValueError as e:
                value_errors += 1
                if max_value_errors <= value_errors:
                    if log is not None:
                        self.logger.info(
                            f"{self}.retry(...) [{value_errors} value_errors]: {log}"
                        )
                    if on_success is not None:
                        on_success(locals())
                    break
                await asyncio.sleep(delay_value_error)
        else:
            self.logger.warning(
                '\n'.join(
                    (
                        f"{self}.retry(...) [max {tries} tries ran out]",
                        f"self.request(method, url, *args, **kwargs):",
                        f"    method: {method}",
                        f"    url: {url}",
                        f"    args: {args}",
                        f"    kwargs: {kwargs}",
                        f"{resp}:",
                        f"    status_code: {resp.status_code}",
                        f"    reason: {resp.reason}",
                        f"    url: {resp.url}",
                        f"    elapsed: {resp.elapsed}",
                        f"    headers: {resp.headers}",
                        f"    text: {resp.text}",
                    )
                )
            )
            if on_failure is not None:
                on_failure(locals())
        if log is not None:
            self.logger.info(f"{self}.retry(...) [finish {tries} tries]: {log}")
        if on_finish is not None:
            on_finish(locals())
        return resp

    async def simulate(
        self,
        target: Alpha | MultiAlpha,
        *args,
        max_tries: int | Iterable[Any] = range(600),
        on_nolocation: Callable[[dict[str, Any]], None] | None = None,
        log: str | None = '',
        retry_log: str | None = None,
        **kwargs,
    ) -> Coroutine[None, None, Response | None]:
        resp = self.post(
            URL_SIMULATIONS,
            json=target,
            expected=self.expected_location,
            max_tries=60,
            delay_unexpected=5.0,
        )
        try:
            url = resp.headers[LOCATION]
        except KeyError as e:
            self.logger.warning(
                '\n'.join(
                    (
                        f"{self}.simulate(...) [",
                        f"    {repr(e)}",
                        f"    {target}",
                        f"]:",
                        f"{resp}:",
                        f"    status_code: {resp.status_code}",
                        f"    reason: {resp.reason}",
                        f"    url: {resp.url}",
                        f"    elapsed: {resp.elapsed}",
                        f"    headers: {resp.headers}",
                        f"    text: {resp.text}",
                    )
                )
            )
            if on_nolocation is not None:
                on_nolocation(locals())
            return None
        resp = await self.retry(
            GET, url, *args, max_tries=max_tries, log=retry_log, **kwargs
        )
        if log is not None:
            self.logger.info(
                '\n'.join(
                    (
                        f"{self}.simulate(...) [",
                        f"    {url}",
                        # f"    {target}",
                        f"]: {log}",
                    )
                )
            )
        return resp

    async def concurrent_simulate(
        self,
        targets: Iterable[Alpha | MultiAlpha],
        concurrency: int | asyncio.Semaphore,
        *args,
        return_exceptions: bool = False,
        log: str | None = '',
        log_gap: int = 100,
        **kwargs,
    ) -> Coroutine[None, None, list[Response | BaseException]]:
        if not isinstance(targets, Sized):
            targets = list(targets)
        if log is None:
            log_gap = 0
        if isinstance(concurrency, int):
            concurrency = asyncio.Semaphore(value=concurrency)
        total = len(targets)
        if log is not None:
            self.logger.info(
                f"{self}.concurrent_simulate(...) [start {total}, {concurrency._value}]: {log}"
            )
        resp = await concurrent_await(
            (
                self.simulate(
                    target,
                    *args,
                    log=(
                        f"{idx}/{total} = {int(100*idx/total)}%"
                        if 0 != log_gap and 0 == idx % log_gap
                        else None
                    ),
                    **kwargs,
                )
                for idx, target in enumerate(targets, start=1)
            ),
            concurrency=concurrency,
            return_exceptions=return_exceptions,
        )
        if log is not None:
            self.logger.info(
                f"{self}.concurrent_simulate(...) [finish {total}, {concurrency._value}]: {log}"
            )
        return resp

    async def check(
        self,
        alpha_id: str,
        *args,
        max_tries: int | Iterable[Any] = range(600),
        log: str | None = '',
        retry_log: str | None = None,
        **kwargs,
    ) -> Coroutine[None, None, Response | None]:
        url = URL_ALPHAS_ALPHAID_CHECK.format(alpha_id)
        resp = await self.retry(
            GET, url, *args, max_tries=max_tries, log=retry_log, **kwargs
        )
        if log is not None:
            self.logger.info(
                '\n'.join(
                    (
                        f"{self}.check(...) [",
                        f"    {url}",
                        f"]: {log}",
                    )
                )
            )
        return resp

    async def concurrent_check(
        self,
        alpha_ids: Iterable[str],
        concurrency: int | asyncio.Semaphore,
        *args,
        return_exceptions: bool = False,
        log: str | None = '',
        log_gap: int = 100,
        **kwargs,
    ) -> Coroutine[None, None, list[Response | BaseException]]:
        if not isinstance(alpha_ids, Sized):
            alpha_ids = list(alpha_ids)
        if log is None:
            log_gap = 0
        if isinstance(concurrency, int):
            concurrency = asyncio.Semaphore(value=concurrency)
        total = len(alpha_ids)
        if log is not None:
            self.logger.info(
                f"{self}.concurrent_check(...) [start {total}, {concurrency._value}]: {log}"
            )
        resp = await concurrent_await(
            (
                self.check(
                    alpha_id,
                    *args,
                    log=(
                        f"{idx}/{total} = {int(100*idx/total)}%"
                        if 0 != log_gap and 0 == idx % log_gap
                        else None
                    ),
                    **kwargs,
                )
                for idx, alpha_id in enumerate(alpha_ids, start=1)
            ),
            concurrency=concurrency,
            return_exceptions=return_exceptions,
        )
        if log is not None:
            self.logger.info(
                f"{self}.concurrent_check(...) [finish {total}, {concurrency._value}]: {log}"
            )
        return resp

    async def submit(
        self,
        alpha_id: str,
        got_201: List[bool],  # 用来传递是否获得201响应的信息
        *args,
        # http_max_tries 用于 self.retry 的 HTTP POST 请求重试
        http_max_tries: int | Iterable[Any] = range(600),
        log: str | None = "",  # 用于此方法整体的日志上下文
        retry_log: str | None = None,  # 用于 self.retry 方法的日志上下文
        # sim_count: int = 0, # 假设 sim_count 也可能相关，与 check 方法保持一致
        # 新增的应用层面重试参数 (针对 submit 操作本身)
        app_max_retries: int = 2,  # 例如，submit 操作本身额外重试2次 (总共3次尝试调用 self.retry)
        app_initial_delay: float = 5.0,  # 第一次 submit 重试前的延迟
        app_backoff_factor: float = 2.0,  # submit 重试延迟的指数退避因子
        # 期望的HTTP成功状态码，对于POST通常是200, 201, 202等
        # 如果 self.retry 内部已经处理了期望的状态码，这里可以更宽松或不设
        expected_http_status_codes: tuple[int, ...] = (200, 201, 202, 204),
        **kwargs,
    ) -> Coroutine[None, None, Response | None]:
        final_resp: Response | None = None
        url = URL_ALPHAS_ALPHAID_SUBMIT.format(alpha_id)
        app_current_delay = app_initial_delay

        log_prefix = f"{self}.submit(alpha_id={alpha_id})"

        for app_attempt in range(app_max_retries + 1):
            self.logger.info(
                f"{log_prefix} app_attempt {app_attempt + 1}/{app_max_retries + 1}..."
            )

            current_attempt_resp = await self.retry(
                POST,
                url,
                *args,
                max_tries=http_max_tries,  # 传递给 self.retry
                log=retry_log,  # 传递给 self.retry
                alpha_id=alpha_id,  # 传递给 self.retry
                # sim_count=sim_count, # 传递给 self.retry (如果 retry 方法使用它)
                is_submit=True,
                got_201=got_201,
                **kwargs,
            )

            final_resp = current_attempt_resp  # 存储当前尝试的响应

            if not final_resp:
                # self.logger.warning(f"{log_prefix} app_attempt {app_attempt + 1}: self.retry returned None (POST failed after its retries).")
                if app_attempt < app_max_retries:
                    self.logger.info(
                        f"{log_prefix} Will sleep for {app_current_delay:.2f}s and retry submit operation."
                    )
                    await asyncio.sleep(app_current_delay)
                    app_current_delay *= app_backoff_factor
                    continue
                else:
                    self.logger.error(
                        f"{log_prefix} Submit operation failed: self.retry returned None after all app_retries."
                    )
                    # 原始日志记录点（在所有尝试失败后）
                    if log is not None and log != "":
                        self.logger.info(
                            "\n".join(
                                (
                                    f"{log_prefix} [FAILED after all app_retries, self.retry was None]",
                                    f"    URL: {url}",
                                    f"] {log}",
                                )
                            )
                        )
                    return None  # 所有 submit 尝试后仍无响应

            # 检查HTTP状态码是否符合预期
            # 注意：self.retry 可能已经处理了部分重试（如429），这里是上层检查
            if final_resp.status_code not in expected_http_status_codes:
                self.logger.warning(
                    f"{log_prefix} app_attempt {app_attempt + 1}: Received unexpected HTTP status {final_resp.status_code} "
                    f"(expected one of {expected_http_status_codes}). Response: {final_resp.text[:200]}"
                )
                # 根据具体业务，判断哪些非预期状态码值得上层重试
                # 例如，5xx 错误可能值得重试
                if (
                    500 <= final_resp.status_code < 600
                    and app_attempt < app_max_retries
                ):
                    self.logger.info(
                        f"{log_prefix} Server error. Will sleep for {app_current_delay:.2f}s and retry submit operation."
                    )
                    await asyncio.sleep(app_current_delay)
                    app_current_delay *= app_backoff_factor
                    continue
                else:  # 对于其他错误或已达最大重试次数
                    self.logger.error(
                        f"{log_prefix} Submit failed with status {final_resp.status_code} or max app_retries reached."
                    )
                    # 原始日志记录点（在操作失败后）
                    if log is not None and log != "":
                        self.logger.info(
                            "\n".join(
                                (
                                    f"{log_prefix} [FAILED with status {final_resp.status_code}]",
                                    f"    URL: {url}",
                                    f"] {log}",
                                )
                            )
                        )
                    return final_resp  # 返回这个非预期的响应

            # 如果状态码符合预期，认为本次提交尝试成功
            self.logger.info(
                f"{log_prefix} app_attempt {app_attempt + 1} successful with status {final_resp.status_code}."
            )
            # 原始日志记录点（在操作成功后）
            if log is not None and log != "":
                self.logger.info(
                    "\n".join(
                        (
                            f"{log_prefix} [SUCCESS]",
                            f"    URL: {url}",
                            f"] {log}",
                        )
                    )
                )
            return final_resp  # 成功，直接返回响应

        # 如果循环正常结束 (即所有 app_attempt 都执行完毕且未成功返回)
        # 这通常意味着每次都进入了 continue，最后一次尝试后没有成功
        self.logger.error(
            f"{log_prefix} Submit operation failed after all {app_max_retries + 1} app_attempts."
        )
        # 原始日志记录点（在所有尝试失败后）
        if log is not None and log != "":
            log_status = (
                f"Last status: {final_resp.status_code}"
                if final_resp
                else "self.retry was None"
            )
            self.logger.info(
                "\n".join(
                    (
                        f"{log_prefix} [FAILED after all app_retries, {log_status}]",
                        f"    URL: {url}",
                        f"] {log}",
                    )
                )
            )
        return final_resp  # 返回最后一次尝试的响应（可能是None或错误状态）

    # ==============================================================================================
    # ====   下面是自定义方法集合  ===================================================================

    def _wait_get_response(
        self, url: str, max_retries: int = 10, timeout: int = 60
    ) -> requests.Response:
        retries = 0
        last_exception = None
        while retries < max_retries:
            try:
                response = self.get(url, timeout=timeout)  # 使用 session 的 get 方法
                retry_after = response.headers.get("Retry-After")
                if response.status_code == 429 or retry_after:
                    # sleep_time = float(retry_after) if retry_after else (2 ** retries)
                    sleep_time = min(2**retries, 64)
                    # self.logger.info(f"Rate limit for {url}. Retrying in {sleep_time:.1f}s ({retries+1}/{max_retries})")
                    # print(f"Rate limit for {url}. Retrying in {sleep_time:.1f}s ({retries+1}/{max_retries})")
                    time.sleep(sleep_time)
                    retries += 1
                    continue
                response.raise_for_status()
                if retries > 0:
                    # self.logger.info(f"Successfully retried for {url}. ({retries+1}/{max_retries})")
                    # print(f"Successfully retried for {url}. ({retries+1}/{max_retries})")
                    pass
                return response
            except requests.exceptions.Timeout as e:
                self.logger.warning(
                    f"Timeout for {url}. Retrying ({retries+1}/{max_retries})"
                )
                print(f"Timeout for {url}. Retrying ({retries+1}/{max_retries})")
                last_exception = e
                retries += 1
                time.sleep(1 + 2**retries)
            except requests.exceptions.RequestException as e:
                self.logger.error(
                    f"Request failed for {url}: {e}. Retrying ({retries+1}/{max_retries})"
                )
                print(
                    f"Request failed for {url}: {e}. Retrying ({retries+1}/{max_retries})"
                )
                last_exception = e
                retries += 1
                time.sleep(2**retries)
            except Exception as e:
                self.logger.error(f"Unexpected error for {url}: {e}")
                print(f"Unexpected error for {url}: {e}")
                last_exception = e
                break
        self.logger.error(f"Failed to get {url} after {max_retries} retries.")
        print(f"Failed to get {url} after {max_retries} retries.")
        if last_exception:
            raise last_exception
        raise requests.exceptions.RequestException(
            f"Failed {url} after {max_retries} retries."
        )

    def get_alpha_details(self, alpha_id: str) -> Optional[Dict[str, Any]]:
        # url = f"{URL_ALPHAS}/{alpha_id}" # 使用你定义的 URL_ALPHAS
        url = f"{WQB_API_URL}/alphas/{alpha_id}"  # 或者直接构造
        try:
            response = self._wait_get_response(url)
            return response.json()
        except Exception as e:
            self.logger.error(f"Failed to get details for alpha {alpha_id}: {e}")
            print(f"Failed to get details for alpha {alpha_id}: {e}")
            return None

    def get_alpha_pnl(self, alpha_id: str) -> Optional[pd.DataFrame]:
        # pnl_url = f"{URL_ALPHAS}/{alpha_id}/recordsets/pnl" # 使用你定义的 URL_ALPHAS
        pnl_url = f"{WQB_API_URL}/alphas/{alpha_id}/recordsets/pnl"  # 或者直接构造
        try:
            response = self._wait_get_response(pnl_url)
            pnl_data = response.json()

            if (
                "records" in pnl_data
                and isinstance(pnl_data["records"], list)
                and "schema" in pnl_data
                and "properties" in pnl_data["schema"]
            ):
                columns = [item["name"] for item in pnl_data["schema"]["properties"]]
                df = pd.DataFrame(pnl_data["records"], columns=columns)

                if df.empty:
                    return None

                rename_map = {}
                if "date" in df.columns:
                    rename_map["date"] = "Date"
                if "pnl" in df.columns:
                    rename_map["pnl"] = alpha_id
                else:
                    for col in df.columns:
                        if col.lower() not in [
                            "date",
                            "id",
                            "alphaid",
                        ]:  # 避免常用非pnl列
                            rename_map[col] = alpha_id
                            break
                df = df.rename(columns=rename_map)

                if "Date" not in df.columns or alpha_id not in df.columns:
                    self.logger.error(
                        f"PNL rename failed for {alpha_id}. Cols: {df.columns}"
                    )
                    print(f"PNL rename failed for {alpha_id}. Cols: {df.columns}")
                    return None

                df = df[["Date", alpha_id]]
                df["Date"] = pd.to_datetime(df["Date"])
                df[alpha_id] = pd.to_numeric(df[alpha_id], errors="coerce")

                return df if not df.empty else None
            else:
                self.logger.error(f"Unexpected PNL JSON for {alpha_id}")
                print(f"Unexpected PNL JSON for {alpha_id}")
                return None
        except Exception as e:
            self.logger.error(f"Failed to fetch/process PNL for {alpha_id}: {e}")
            print(f"Failed to fetch/process PNL for {alpha_id}: {e}")
            return None

    def get_os_alphas(
        self, limit: int = 100, get_first: bool = False, stage: str = "OS"
    ) -> List[Dict]:
        fetched_alphas = []
        offset = 0
        total_alphas_count = -1
        # base_api_url = WQB_API_URL # 来自你的配置

        while True:
            url = f"{WQB_API_URL}/users/self/alphas?stage={stage}&limit={limit}&offset={offset}&order=-dateSubmitted"
            try:
                response = self._wait_get_response(url)
                res_json = response.json()
            except Exception as e:
                self.logger.error(f"Failed to fetch alpha list {url}: {e}")
                print(f"Failed to fetch alpha list {url}: {e}")
                break

            if offset == 0 and "count" in res_json:
                total_alphas_count = res_json["count"]
            current_batch = res_json.get("results", [])
            if not current_batch:
                break

            fetched_alphas.extend(current_batch)

            if (
                get_first
                or len(current_batch) < limit
                or (
                    total_alphas_count != -1
                    and len(fetched_alphas) >= total_alphas_count
                )
            ):
                break
            offset += limit

        return (
            fetched_alphas[:total_alphas_count]
            if total_alphas_count != -1 and len(fetched_alphas) > total_alphas_count
            else fetched_alphas
        )

    def get_alpha_pnls_bulk(
        self, alphas_metadata_list: list[dict], max_workers: int = 10
    ) -> pd.DataFrame:
        all_pnls_list = []

        fetch_pnl_func = lambda alpha_meta: (
            alpha_meta["id"],
            self.get_alpha_pnl(alpha_meta["id"]),
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results_iter = executor.map(fetch_pnl_func, alphas_metadata_list)
            for alpha_id, pnl_df in results_iter:
                if pnl_df is not None and not pnl_df.empty:
                    pnl_df_indexed = pnl_df.set_index("Date")
                    all_pnls_list.append(pnl_df_indexed)
                else:
                    self.logger.warning(
                        f"PNL for {alpha_id} empty/failed, skipping in bulk."
                    )
                    print(f"PNL for {alpha_id} empty/failed, skipping in bulk.")

        if not all_pnls_list:
            return pd.DataFrame()
        combined_pnls_df = pd.concat(all_pnls_list, axis=1)
        combined_pnls_df = combined_pnls_df.loc[
            :, ~combined_pnls_df.columns.duplicated(keep="first")
        ]
        combined_pnls_df.sort_index(inplace=True)
        return combined_pnls_df

    # --- 原有的 update_local_pnl_storage 和 save_pnl ---
    # 这些方法在新自相关流程中不直接管理OS Alpha的整体PNL缓存（后者使用pickle），
    # 但可能用于其他目的或管理单个（如候选）Alpha的PNL（如果需要）。
    # 为了最小改动，这里原样保留。
    def update_local_pnl_storage(
        self,
        submitted_alphas_list: List[Dict[str, Any]],
        storage_base_dir: str,
        max_workers: int = 10,
        force_to_update_all_pnl=False,  # 变量名统一
    ) -> int:
        new_pnl_count = 0
        alphas_to_fetch = []

        for alpha_data in submitted_alphas_list:
            alpha_id = alpha_data.get("id")
            settings = alpha_data.get("settings", {})
            region = settings.get("region")

            if not alpha_id or not region:
                self.logger.warning(
                    f"Skipping PNL update for alpha due to missing ID/region: {alpha_data}"
                )
                print(
                    f"Skipping PNL update for alpha due to missing ID/region: {alpha_data}"
                )
                continue

            pnl_file_path = os.path.join(
                storage_base_dir, region, f"{alpha_id}.parquet"
            )
            if not os.path.exists(pnl_file_path) or force_to_update_all_pnl:
                alphas_to_fetch.append({"id": alpha_id, "region": region})

        if not alphas_to_fetch:
            # self.logger.info("Local PNL storage is up-to-date (old method).")
            return 0

        # self.logger.info(f"{'Forcing update for' if force_to_update_all_pnl else 'Found'} {len(alphas_to_fetch)} alphas for PNL download (old method).")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_alpha = {
                executor.submit(self.get_alpha_pnl, alpha_info["id"]): alpha_info
                for alpha_info in alphas_to_fetch
            }
            for future in concurrent.futures.as_completed(future_to_alpha):
                alpha_details = future_to_alpha[future]
                alpha_id = alpha_details["id"]
                region = alpha_details["region"]
                try:
                    pnl_df_single = future.result()  # get_alpha_pnl 返回Date为列的DF
                    if pnl_df_single is not None and not pnl_df_single.empty:
                        # save_pnl 期望Date为索引
                        pnl_df_to_save = pnl_df_single.set_index("Date")
                        self.save_pnl(
                            pnl_df_to_save, alpha_id, region, storage_base_dir
                        )
                        new_pnl_count += 1
                except Exception as exc:
                    self.logger.error(
                        f"Error processing alpha {alpha_id} (old PNL update): {exc}"
                    )
        # self.logger.info(f"Saved {new_pnl_count} new PNL files (old method).")
        return new_pnl_count

    def save_pnl(
        self, pnl_df: pd.DataFrame, alpha_id: str, region: str, storage_base_dir: str
    ):
        if pnl_df is None or pnl_df.empty:
            return

        region_dir = os.path.join(storage_base_dir, region)
        os.makedirs(region_dir, exist_ok=True)
        file_path = os.path.join(region_dir, f"{alpha_id}.parquet")
        try:
            # 确保pnl_df的列名是alpha_id，如果它是从get_alpha_pnl然后set_index来的，列名就是alpha_id
            # 如果pnl_df只有一个数据列但名字不是alpha_id，需要重命名
            if len(pnl_df.columns) == 1 and pnl_df.columns[0] != alpha_id:
                pnl_df = pnl_df.rename(columns={pnl_df.columns[0]: alpha_id})

            pnl_df[[alpha_id]].to_parquet(
                file_path, engine="pyarrow"
            )  # 只保存目标alpha的列
        except Exception as e:
            self.logger.error(
                f"Error saving PNL for {alpha_id} (Region: {region}) to {file_path}: {e}"
            )

    def get_performance(self, alpha_id:str, iqc_id:str = 'IQC2025S2'):
        """获取alpha的performance"""
        resp = self._wait_get_response(f'{WQB_API_URL}/competitions/{iqc_id}/alphas/{alpha_id}/performance', max_retries=100)
        if resp.ok:
            score = resp.json()['score']
            return  score['after'] - score['before']
        self.logger.error(f'获取alpha {alpha_id}的performance失败')
        return None