import os
import sys
import pandas as pd
import logging
from datetime import datetime

root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if root_path not in sys.path:
    sys.path.append(root_path)

import plugins.utils as util

logger = logging.getLogger(__name__)

BASE_URL = "https://tiki.vn/api/v2/categories"
ROOT_ID  = "1"

# ---------------------------------------------------------------- #

def _fetch_one_category(task: dict):
    """Fetch children của 1 parent_id."""
    parent_id     = task["parent_id"]
    response_data = util.get_tiki_api(BASE_URL, {"parent_id": parent_id})

    if not response_data or "data" not in response_data:
        return None

    data = response_data.get("data", [])
    if not data:
        return None

    return {
        "categories_id": parent_id,
        "extract_time":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "raw_response":  data,
        "children_ids":  [item.get("id") for item in data]  # dùng để build next level tasks
    }


def fetch_categories(root_id: str = ROOT_ID, max_workers: int = 5):
    """
    Crawl toàn bộ category tree theo từng level (BFS), chạy concurrent.

    Strategy: thay vì đệ quy tuần tự (DFS), dùng BFS từng level:
    - Level 1: fetch children của root → biết các level-2 ids
    - Level 2: fetch children của tất cả level-2 nodes song song
    - Level 3: fetch children của tất cả level-3 nodes song song
    - ...tiếp tục cho đến khi không còn children nào

    Returns:
        list các dict {category_id, extract_time, raw_response}
        (đã bỏ cột children_ids trước khi return)
    """
    all_results  = []
    current_level_ids = [root_id]

    level = 1
    while current_level_ids:
        logger.info("Fetching level %d — %d nodes", level, len(current_level_ids))

        tasks = [{"parent_id": pid} for pid in current_level_ids]

        results = util.fetch_concurrent(
            tasks, _fetch_one_category,
            max_workers=max_workers,
            desc=f"Level {level}"
        )

        # Collect ids cho level tiếp theo
        next_level_ids = []
        for r in results:
            next_level_ids.extend(r.pop("children_ids", []))  # pop để không lưu vào DB
            all_results.append(r)

        current_level_ids = next_level_ids
        level += 1

    logger.info("Done. Total category nodes fetched: %d", len(all_results))
    return all_results