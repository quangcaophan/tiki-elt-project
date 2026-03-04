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

BASE_URL = "https://tiki.vn/api/personalish/v1/blocks/listings"

SPECIFIC_CATE_QUERY = """
    SELECT distinct category_id as id 
    FROM cleaned.categories
"""

# ---------------------------------------------------------------- #

def _fetch_one_page(task: dict):
    params = {"sort": "top_seller", "page": task["page"], "category": task["category_id"]}
    response_json = util.get_tiki_api(BASE_URL, params)
    if not response_json or not response_json.get("data"):
        return None
    return {
        "category_id":  task["category_id"],
        "page":         task["page"],
        "extract_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "raw_response": response_json
    }


def fetch_products(batch_size: int = 200, max_workers: int = 5):
    """
    Crawl products with a producer-consumer pattern.

    Fix (OOM / SIGKILL at return code -9):
    ----------------------------------------
    Previously fetch_and_enqueue both pushed to the queue AND returned the full
    result dict, causing fetch_concurrent to accumulate ~20k large JSON blobs in
    its internal `results` list simultaneously — in addition to the queue buffer.
    This doubled RAM usage and triggered the OOM killer mid-run.

    Now:
    - probe_and_enqueue stores only lightweight metadata (category_id + last_page),
      not the full raw_response.
    - fetch_and_enqueue returns None so fetch_concurrent (accumulate=False) keeps
      zero results in memory; data flows only through the Queue → DB writer thread.
    """
    categories_id = db.query_db(SPECIFIC_CATE_QUERY)
    if categories_id.empty:
        logger.warning("No categories found, aborting.")
        return

    logger.info("Found %d categories to crawl", len(categories_id))

    result_queue = Queue(maxsize=500)

    # Start DB writer thread immediately so the queue never backs up
    writer_thread = Thread(
        target=util.generic_db_writer,
        args=(result_queue, batch_size, len(categories_id), "raw_product_listings", ["category_id", "page"]),
        daemon=True,
    )
    writer_thread.start()

    # ── Probe page 1 to discover last_page per category ──────────────────────
    probe_tasks = [
        {"category_id": row["id"], "page": 1}
        for _, row in categories_id.iterrows()
    ]

    # Only store the two fields we need to build remaining tasks.
    # Storing full raw_response here was the second source of unbounded RAM usage.
    probe_meta = []   # list of {"category_id": ..., "last_page": ...}

    def probe_and_enqueue(task):
        result = _fetch_one_page(task)
        if result:
            result_queue.put(result)
            # Keep only lightweight pagination metadata, not the full payload
            probe_meta.append({
                "category_id": result["category_id"],
                "last_page":   result["raw_response"].get("paging", {}).get("last_page", 1),
            })
        return None   # ← do NOT return result; fetch_concurrent must not accumulate it

    util.fetch_concurrent(
        probe_tasks, probe_and_enqueue,
        max_workers=max_workers,
        desc="Probing page 1",
        accumulate=False,   # no in-memory list needed
    )

    # ── Build remaining page tasks from lightweight metadata ──────────────────
    remaining_tasks = [
        {"category_id": meta["category_id"], "page": page}
        for meta in probe_meta
        for page in range(2, meta["last_page"] + 1)
    ]
    logger.info("Probe done. %d additional pages to fetch", len(remaining_tasks))

    # ── Fetch remaining pages — data flows Queue → DB writer only ─────────────
    def fetch_and_enqueue(task):
        result = _fetch_one_page(task)
        if result:
            result_queue.put(result)
        return None   # ← critical: returning the result caused the OOM

    util.fetch_concurrent(
        remaining_tasks, fetch_and_enqueue,
        max_workers=max_workers,
        desc="Fetching remaining pages",
        accumulate=False,   # 20k JSON blobs must NOT accumulate in RAM
    )

    result_queue.put(None)   # poison pill — signals writer to flush and exit
    writer_thread.join()
    logger.info("All done.")