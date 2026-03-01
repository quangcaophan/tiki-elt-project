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
        "raw_response": response_json
    }


def fetch_reviews(batch_size: int = 200, max_workers: int = 5):
    """
    Crawl reviews với producer-consumer pattern:
    - Fetcher threads (concurrent) → Queue → DB writer thread
    - Không tích lũy toàn bộ kết quả trong RAM
    - Push DB liên tục trong lúc fetch → an toàn với dataset lớn
    """
    data = db.query_db(QUERY)
    if data.empty:
        logger.warning("No products found, aborting.")
        return

    logger.info("Found %d products to crawl reviews", len(data))

    # ── Bước 1: Probe page 1 song song để biết total pages ──
    probe_tasks = [
        {"product_id": row["product_id"], "spid": row["spid"],
         "seller_id": row["seller_id"], "page": 1}
        for _, row in data.iterrows()
    ]

    # Queue để probe results chảy thẳng vào DB writer
    probe_queue = Queue(maxsize=500)
    probe_results_store = []

    def probe_and_store(task):
        result = _fetch_one_page(task)
        if result:
            probe_queue.put(result)
            probe_results_store.append(result)
        return result

    # Start DB writer ngay từ đầu
    writer_thread = Thread(
        target=util.generic_db_writer,
        args=(probe_queue, batch_size, len(probe_tasks), "raw_reviews", ["spid", "page"]),
        daemon=True
    )
    writer_thread.start()

    logger.info("Probing %d products (page 1)...", len(probe_tasks))
    util.fetch_concurrent(probe_tasks, probe_and_store, max_workers=max_workers, desc="Probing page 1")

    # ── Bước 2: Build remaining tasks từ last_page ──
    remaining_tasks = []
    for result in probe_results_store:
        last_page = result["raw_response"].get("paging", {}).get("last_page", 1)
        for page in range(2, last_page + 1):
            remaining_tasks.append({
                "product_id": result["product_id"],
                "spid":       result["spid"],
                "seller_id":  result["seller_id"],
                "page":       page,
            })

    total_tasks = len(probe_tasks) + len(remaining_tasks)
    logger.info("Total pages to fetch: %d (probe: %d + remaining: %d)",
                total_tasks, len(probe_tasks), len(remaining_tasks))

    # ── Bước 3: Fetch remaining pages, kết quả chảy thẳng vào queue ──
    def fetch_and_enqueue(task):
        result = _fetch_one_page(task)
        if result:
            probe_queue.put(result)
        return result

    util.fetch_concurrent(
        remaining_tasks, fetch_and_enqueue,
        max_workers=max_workers,
        desc="Fetching remaining review pages"
    )

    probe_queue.put(None)
    writer_thread.join()
    logger.info("All done.")