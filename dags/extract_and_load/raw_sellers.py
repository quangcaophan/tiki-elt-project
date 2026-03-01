import os
import sys
import pandas as pd
import logging
from datetime import datetime

root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if root_path not in sys.path:
    sys.path.append(root_path)

import plugins.db as db
import plugins.utils as util

logger = logging.getLogger(__name__)

BASE_URL = "https://api.tiki.vn/product-detail/v2/widgets/seller"
QUERY    = "SELECT DISTINCT seller_id FROM cleaned.products"

# ---------------------------------------------------------------- #

def _fetch_one_seller(task: dict):
    """Fetch info của 1 seller."""
    seller_id     = task["seller_id"]
    response_json = util.get_tiki_api(BASE_URL, {"seller_id": seller_id})

    if not response_json or not response_json.get("data"):
        logger.warning("No data for seller %s", seller_id)
        return None

    return {
        "seller_id":    seller_id,
        "extract_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "raw_response": response_json
    }


def fetch_seller(batch_size: int = 100, max_workers: int = 5):
    """
    Crawl info tất cả sellers, chạy concurrent.
    Sellers ít hơn products/reviews nên max_workers thấp hơn để an toàn.
    """
    list_store = db.query_db(QUERY)  # lazy query
    if list_store.empty:
        logger.warning("No sellers found, aborting.")
        return

    logger.info("Found %d sellers to crawl", len(list_store))

    tasks = [{"seller_id": row["seller_id"]} for _, row in list_store.iterrows()]

    all_results = util.fetch_concurrent(
        tasks, _fetch_one_seller,
        max_workers=max_workers,
        desc="Fetching sellers"
    )

    logger.info("Total sellers fetched: %d / %d", len(all_results), len(tasks))

    # Push theo batch
    for i in range(0, len(all_results), batch_size):
        batch = all_results[i : i + batch_size]
        df = pd.DataFrame(batch)
        db.push_df_to_db(
            df, "raw_sellers",
            schema="raw",
            primary_key="seller_id"
        )
        logger.info("Pushed batch %d-%d / %d", i, i + len(batch), len(all_results))