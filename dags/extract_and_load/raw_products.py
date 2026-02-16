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

raw_logs_product_listing_list = [] 
BASE_URL = "https://tiki.vn/api/personalish/v1/blocks/listings"

# query = 'select distinct category_id as id from cleaned.categories'

# Only crawl for Vietnamese book to reduce wait time
specific_cate = """
WITH RECURSIVE CategoryTree AS (
    SELECT category_id, parent_id, name, level
    FROM cleaned.categories
    WHERE name = 'Sách tiếng Việt'
        UNION ALL
    SELECT c.category_id, c.parent_id, c.name, c.level
    FROM cleaned.categories c
    INNER JOIN CategoryTree ct ON c.parent_id = ct.category_id
)
select category_id as id from categorytree
"""

categories_id = db.query_db(specific_cate)

def fetch_products(batch_size=50):
    global raw_logs_product_listing_list 
    total_categories = len(categories_id)
    
    for index, row in categories_id.iterrows(): 
        cat_id = row['id']
        print(f"[{index + 1}/{total_categories}] Crawling Category ID: {cat_id}")
        
        page = 1
        while True:
            params = {"sort": "top_seller", "page": page, "category": cat_id}
            try:
                response_json = util.get_tiki_api(BASE_URL, params)

                if not response_json or not response_json.get("data") or len(response_json.get("data")) == 0:
                    break

                raw_logs_product_listing_list.append({
                    "category_id": cat_id,
                    "page": page,
                    "extract_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "raw_response": response_json
                })

                print(f"   Scraped page {page} ({len(response_json.get('data'))} items)")

                if len(raw_logs_product_listing_list) >= batch_size:
                    print(f"   [Batch Push] Pushing {len(raw_logs_product_listing_list)} logs to DB...")
                    df_to_push = pd.DataFrame(raw_logs_product_listing_list)
                    db.push_df_to_db(df_to_push, "raw_product_listings", schema="raw", primary_key=["category_id", "page"])
                    raw_logs_product_listing_list.clear()
                    del df_to_push

                page += 1
                time.sleep(random.uniform(0.3, 0.8))

            except Exception as e:
                print(f"   [Error] Cat {cat_id} Page {page}: {e}")
                break
    
    if raw_logs_product_listing_list:
        print(f"[Final Push] Pushing remaining {len(raw_logs_product_listing_list)} logs...")
        db.push_df_to_db(pd.DataFrame(raw_logs_product_listing_list), "raw_product_listings", schema="raw", primary_key=["category_id", "page"])
        raw_logs_product_listing_list.clear()

# if __name__ == '__main__':
#     fetch_products(batch_size = 100)