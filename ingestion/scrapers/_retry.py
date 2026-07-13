"""
ingestion/scrapers/_retry.py

Phase: 0.4 (Data Ingestion Scrapers)
Owner: Platform / Ingestion
Consumers: ingestion/scrapers/*.py

Shared tenacity-based retry helper, replacing the ~4 hand-rolled `_retry()`
helpers and standalone `for attempt in range(1, retries + 1): try/except`
loops that used to be independently copy-pasted across screener.py,
tijori.py, trendlyne.py, macro.py and bhavcopy.py.

Two distinct retry semantics existed before this consolidation and are both
preserved here via the `wait_seconds` parameter (do not silently collapse
them without checking call sites):

  - screener.py / tijori.py / trendlyne.py: DEFAULT_RETRY_COUNT (3) attempts,
    NO sleep between attempts, catches only requests.RequestException.
  - macro.py / bhavcopy.py: 3 attempts, fixed 2-second sleep between
    attempts, catches requests.RequestException (bhavcopy also catches
    pandas.errors.ParserError).

`retry_call` mirrors the previous helpers' external contract: it calls a
zero-arg callable, logs a warning on every failed attempt, and raises
ConnectionError (not tenacity's own RetryError) after the final failed
attempt so existing `except ConnectionError` call sites keep working
unchanged.
"""

import logging
from typing import Callable, Optional, Sequence, Type, TypeVar

from tenacity import (
    RetryError,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Single source of truth for the fixed inter-attempt delay used by the
# scrapers that sleep between retries (macro.py, bhavcopy.py). Scrapers that
# never slept between retries (screener.py, tijori.py, trendlyne.py) pass
# wait_seconds=0 explicitly instead of relying on this default.
RETRY_DELAY_SECONDS = 2


def retry_call(
    fn: Callable[[], T],
    *,
    retries: int,
    label: str,
    wait_seconds: float = 0,
    exceptions: Sequence[Type[BaseException]] = (Exception,),
) -> T:
    """
    Call fn() up to `retries` times, retrying only on `exceptions`.

    Parameters
    ----------
    fn : Callable[[], T]
        Zero-arg callable to invoke, e.g. `lambda: session.get(url, timeout=30)`.
    retries : int
        Maximum number of attempts (matches the old `retries` param name).
    label : str
        Used in log messages and the final ConnectionError, e.g.
        "screener.in request" or "India VIX".
    wait_seconds : float, default 0
        Fixed delay between attempts. 0 preserves screener.py/tijori.py/
        trendlyne.py's original no-sleep behaviour; pass 2 (or
        RETRY_DELAY_SECONDS) to match macro.py/bhavcopy.py's original
        behaviour.
    exceptions : Sequence[Type[BaseException]]
        Exception types that trigger a retry; anything else propagates
        immediately, same as the old `except requests.RequestException`
        (or `except (requests.RequestException, pd.errors.ParserError)`
        for bhavcopy) clauses.

    Returns
    -------
    T
        fn()'s return value on the first successful attempt.

    Raises
    ------
    ConnectionError
        After `retries` failed attempts, wrapping the last exception —
        same behaviour and message shape as the original hand-rolled loops.
    """
    last_exc: Optional[BaseException] = None

    def _after_attempt(retry_state) -> None:
        nonlocal last_exc
        outcome = retry_state.outcome
        if outcome is not None and outcome.failed:
            last_exc = outcome.exception()
            logger.warning(
                f"{label} failed (attempt {retry_state.attempt_number}/{retries}): {last_exc}"
            )

    retryer = Retrying(
        stop=stop_after_attempt(retries),
        wait=wait_fixed(wait_seconds),
        retry=retry_if_exception_type(tuple(exceptions)),
        after=_after_attempt,
        reraise=False,
    )

    try:
        return retryer(fn)
    except RetryError as exc:
        final_exc = last_exc if last_exc is not None else exc
        raise ConnectionError(f"{label} failed after {retries} attempts: {final_exc}") from exc
