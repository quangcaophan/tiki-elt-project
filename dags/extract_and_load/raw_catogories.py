import os 
import sys
import pandas as pd
import time
import random
from datetime import datetime
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if root_path not in sys.path:
    sys.path.append(root_path)

import plugins.db as db
import plugins.utils as util
# ---------------------------------------------------------------- #

BASE_URL = "https://tiki.vn/api/v2/categories"
ROOT_ID = "8322" 

def fetch_categories(parent_id, level=1):
    print(f"Fetching categories for parent_id: {parent_id} at level {level}")
    results = []
    response_data = util.get_tiki_api(BASE_URL, {"parent_id": parent_id})
    
    if not response_data or "data" not in response_data:
        return results

    data = response_data.get("data", [])
    results.append({
        'categories_id': parent_id,
        'extract_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'raw_response': data
    })

    for item in data:
        time.sleep(random.uniform(0.3, 0.8))
        results.extend(fetch_categories(item.get("id"), level + 1))
    
    return results

# if __name__ == '__main__':
#     print(f"Starting crawl from Root ID: {ROOT_ID}")
#     raw_json_list = fetch_categories(ROOT_ID)
#     df_raw = pd.DataFrame(raw_json_list)
    # db.push_df_to_db(df_raw, "raw_categories", schema="raw", primary_key="categories_id")