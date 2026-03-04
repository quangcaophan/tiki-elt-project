import os
import sys
import logging
from datetime import datetime
from queue import Queue
from threading import Thread

root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if root_path not in sys.path:
    sys.path.append(root_path)

import plugins.db as db
import plugins.utils as util

logger = logging.getLogger(__name__)

BASE_URL = "https://tiki.vn/api/v2/reviews"
QUERY    = "SELECT product_id, spid, seller_id FROM cleaned.products WHERE review_count != 0"

# ---------------------------------------------------------------- #

def _fetch_one_page(task: dict):
    params = {
        "page":       task["page"],
        "spid":       task["spid"],
        "product_id": task["product_id"],
        "seller_id":  task["seller_id"],
    }
    response_json = util.get_tiki_api(BASE_URL, params)
    if not response_json or not response_json.get("data"):
        return None
    return {
        "spid":         task["spid"],
        "product_id":   task["product_id"],
        "seller_id":    task["seller_id"],
        "page":         task["page"],
        "extract_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "raw_response": response_json,
    }


def fetch_reviews(batch_size: int = 200, max_workers: int = 5):
    """
    Crawl reviews with a producer-consumer pattern.

    Fix (OOM / SIGKILL at return code -9):
    ----------------------------------------
    Same root cause as raw_products.py — fetch_and_enqueue was returning the full
    result dict, so fetch_concurrent accumulated every review page's JSON in its
    internal `results` list while it was also sitting in the queue.  For tens of
    thousands of pages this caused RAM to grow until the OOM killer fired (SIGKILL,
    return code -9).

    Now both probe_and_store and fetch_and_enqueue return None, and fetch_concurrent
    is called with accumulate=False so it never builds an in-memory list.
    """
    data = db.query_db(QUERY)
    if data.empty:
        logger.warning("No products found, aborting.")
        return

    logger.info("Found %d products to crawl reviews", len(data))

    probe_queue = Queue(maxsize=500)

    # Start DB writer immediately
    writer_thread = Thread(
        target=util.generic_db_writer,
        args=(probe_queue, batch_size, len(data), "raw_reviews", ["spid", "page"]),
        daemon=True,
    )
    writer_thread.start()

    # ── Probe page 1 ─────────────────────────────────────────────────────────
    probe_tasks = [
        {
            "product_id": row["product_id"],
            "spid":       row["spid"],
            "seller_id":  row["seller_id"],
            "page":       1,
        }
        for _, row in data.iterrows()
    ]

    # Only keep the fields needed to build remaining page tasks.
    probe_meta = []   # list of {"spid":..., "product_id":..., "seller_id":..., "last_page":...}

    def probe_and_store(task):
        result = _fetch_one_page(task)
        if result:
            probe_queue.put(result)
            # Store only lightweight metadata, not the full payload
            probe_meta.append({
                "product_id": result["product_id"],
                "spid":       result["spid"],
                "seller_id":  result["seller_id"],
                "last_page":  result["raw_response"].get("paging", {}).get("last_page", 1),
            })
        return None   # ← do NOT return result to fetch_concurrent

    logger.info("Probing %d products (page 1)...", len(probe_tasks))
    util.fetch_concurrent(
        probe_tasks, probe_and_store,
        max_workers=max_workers,
        desc="Probing page 1",
        accumulate=False,
    )

    # ── Build remaining tasks from lightweight metadata ────────────────────────
    remaining_tasks = [
        {
            "product_id": meta["product_id"],
            "spid":       meta["spid"],
            "seller_id":  meta["seller_id"],
            "page":       page,
        }
        for meta in probe_meta
        for page in range(2, meta["last_page"] + 1)
    ]

    total_tasks = len(probe_tasks) + len(remaining_tasks)
    logger.info(
        "Total pages to fetch: %d (probe: %d + remaining: %d)",
        total_tasks, len(probe_tasks), len(remaining_tasks),
    )

    # ── Fetch remaining pages ─────────────────────────────────────────────────
    def fetch_and_enqueue(task):
        result = _fetch_one_page(task)
        if result:
            probe_queue.put(result)
        return None   # ← critical: returning the result caused the OOM

    util.fetch_concurrent(
        remaining_tasks, fetch_and_enqueue,
        max_workers=max_workers,
        desc="Fetching remaining review pages",
        accumulate=False,
    )

    probe_queue.put(None)   # poison pill
    writer_thread.join()
    logger.info("All done.")