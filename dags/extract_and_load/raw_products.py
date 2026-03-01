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
WITH RECURSIVE CategoryTree AS (
    SELECT category_id, parent_id, name, level
    FROM cleaned.categories
    WHERE name = 'Sách tiếng Việt'
    UNION ALL
    SELECT c.category_id, c.parent_id, c.name, c.level
    FROM cleaned.categories c
    INNER JOIN CategoryTree ct ON c.parent_id = ct.category_id
)
SELECT category_id AS id FROM CategoryTree
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
    Crawl products với producer-consumer pattern.
    Kết quả được push DB liên tục thay vì tích lũy hết trong RAM.
    """
    categories_id = db.query_db(SPECIFIC_CATE_QUERY)
    if categories_id.empty:
        logger.warning("No categories found, aborting.")
        return

    logger.info("Found %d categories to crawl", len(categories_id))

    result_queue = Queue(maxsize=500)

    # Start DB writer thread
    writer_thread = Thread(
        target=util.generic_db_writer,
        args=(result_queue, batch_size, len(categories_id), "raw_product_listings", ["category_id", "page"]),
        daemon=True
    )
    writer_thread.start()

    # ── Probe page 1 để biết total pages ──
    probe_tasks = [
        {"category_id": row["id"], "page": 1}
        for _, row in categories_id.iterrows()
    ]
    probe_results_store = []

    def probe_and_enqueue(task):
        result = _fetch_one_page(task)
        if result:
            result_queue.put(result)
            probe_results_store.append(result)
        return result

    util.fetch_concurrent(probe_tasks, probe_and_enqueue, max_workers=max_workers, desc="Probing page 1")

    # ── Build remaining tasks ──
    remaining_tasks = []
    for result in probe_results_store:
        last_page = result["raw_response"].get("paging", {}).get("last_page", 1)
        for page in range(2, last_page + 1):
            remaining_tasks.append({"category_id": result["category_id"], "page": page})

    logger.info("Probe done. %d additional pages to fetch", len(remaining_tasks))

    # ── Fetch remaining, kết quả chảy thẳng vào queue ──
    def fetch_and_enqueue(task):
        result = _fetch_one_page(task)
        if result:
            result_queue.put(result)
        return result

    util.fetch_concurrent(
        remaining_tasks, fetch_and_enqueue,
        max_workers=max_workers,
        desc="Fetching remaining pages"
    )

    result_queue.put(None)  # poison pill
    writer_thread.join()
    logger.info("All done.")