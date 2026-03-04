from queue import Queue, Empty  # ← was wrongly importing from multiprocessing
import time
import random
import logging
import pandas as pd
import requests
from threading import Semaphore
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from plugins import db

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/119.0.0.0 Safari/537.36"
    )
}

# ---------------------------------------------------------------- #
# Rate Limiter
# ---------------------------------------------------------------- #

class RateLimiter:
    def __init__(self, max_workers: int = 5, min_delay: float = 0.3):
        self.semaphore  = Semaphore(max_workers)
        self.min_delay  = min_delay
        self._last_time = 0.0

    def acquire(self):
        self.semaphore.acquire()
        elapsed = time.time() - self._last_time
        wait = self.min_delay - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_time = time.time()

    def release(self):
        self.semaphore.release()


_rate_limiter = RateLimiter(max_workers=5, min_delay=0.3)


# ---------------------------------------------------------------- #
# HTTP Client
# ---------------------------------------------------------------- #

def get_tiki_api(url: str, params: dict, retries: int = 4) -> Optional[dict]:
    _rate_limiter.acquire()
    try:
        for attempt in range(retries):
            wait = None
            try:
                session = requests.Session()
                session.max_redirects = 10
                response = session.get(
                    url, params=params, headers=HEADERS,
                    timeout=10,
                )

                if response.status_code == 200 and not response.text.strip():
                    wait = (2 ** attempt) + random.uniform(1, 3)
                    logger.warning(
                        "Empty body (soft rate limit). Waiting %.1fs (attempt %d/%d) — %s",
                        wait, attempt + 1, retries, params
                    )
                    time.sleep(wait)
                    continue

                if response.status_code == 200:
                    try:
                        return response.json()
                    except ValueError:
                        wait = (2 ** attempt) + random.uniform(1, 2)
                        logger.warning(
                            "Invalid JSON body. Waiting %.1fs (attempt %d/%d) — %s",
                            wait, attempt + 1, retries, params
                        )
                        time.sleep(wait)
                        continue

                if response.status_code == 429:
                    wait = (2 ** attempt) * 2 + random.uniform(2, 5)
                    logger.warning(
                        "Rate limited (429). Waiting %.1fs (attempt %d/%d)",
                        wait, attempt + 1, retries
                    )
                    time.sleep(wait)
                    continue

                if response.status_code in (403, 503):
                    wait = (2 ** attempt) * 3 + random.uniform(3, 8)
                    logger.warning(
                        "Blocked (HTTP %s). Waiting %.1fs (attempt %d/%d)",
                        response.status_code, wait, attempt + 1, retries
                    )
                    time.sleep(wait)
                    continue

                logger.warning("HTTP %s — skipping %s", response.status_code, params)
                return None

            except requests.exceptions.TooManyRedirects:
                wait = (2 ** attempt) * 3 + random.uniform(5, 10)
                logger.warning(
                    "Too many redirects. Waiting %.1fs (attempt %d/%d) — %s",
                    wait, attempt + 1, retries, params
                )
                time.sleep(wait)

            except requests.exceptions.Timeout:
                wait = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(
                    "Timeout. Waiting %.1fs (attempt %d/%d)",
                    wait, attempt + 1, retries
                )
                time.sleep(wait)

            except requests.exceptions.ConnectionError as e:
                wait = (2 ** attempt) + random.uniform(0, 1)
                logger.warning("Connection error: %s. Waiting %.1fs", e, wait)
                time.sleep(wait)

        logger.error("All %d retries failed — %s", retries, params)
        return None

    finally:
        _rate_limiter.release()


# ---------------------------------------------------------------- #
# Concurrent fetch helper
# ---------------------------------------------------------------- #

def fetch_concurrent(
    tasks: List[dict],
    fetch_fn,
    max_workers: int = 5,
    desc: str = "Fetching",
    accumulate: bool = True,   # ← set False when fetch_fn already pushes to a queue
) -> List:
    """
    Run fetch_fn over tasks concurrently.

    Parameters
    ----------
    accumulate : bool
        When True (default) results are collected and returned as a list — suitable
        for small result sets (e.g. categories, sellers, probe-page-1 results).

        Set to False for high-volume fetches where fetch_fn pushes results directly
        into a Queue/DB.  This avoids accumulating tens of thousands of large JSON
        blobs in memory, which previously caused OOM (SIGKILL / return code -9).
    """
    results = [] if accumulate else None
    total   = len(tasks)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(fetch_fn, task): task
            for task in tasks
        }
        for i, future in enumerate(as_completed(future_to_task), 1):
            task = future_to_task[future]
            try:
                result = future.result()
                if accumulate and result is not None:
                    results.append(result)
                if i % 50 == 0 or i == total:
                    logger.info("%s: %d/%d done", desc, i, total)
            except Exception as e:
                logger.error("Task %s failed: %s", task, e)

    return results if accumulate else []


def generic_db_writer(
    queue: Queue,
    batch_size: int,
    total_tasks: int,
    table_name: str,
    primary_key: List[str],
):
    """Thread that drains a Queue and pushes rows to the DB in batches."""
    buffer = []
    pushed = 0

    while True:
        try:
            item = queue.get(timeout=30)
        except Empty:
            break

        if item is None:   # poison pill
            break

        buffer.append(item)

        if len(buffer) >= batch_size:
            df = pd.DataFrame(buffer)
            db.push_df_to_db(df, table_name, schema="raw", primary_key=primary_key)
            pushed += len(buffer)
            logger.info("DB writer pushed %d / %d records", pushed, total_tasks)
            buffer.clear()

    if buffer:
        df = pd.DataFrame(buffer)
        db.push_df_to_db(df, table_name, schema="raw", primary_key=primary_key)
        pushed += len(buffer)
        logger.info("DB writer final flush: %d records total", pushed)