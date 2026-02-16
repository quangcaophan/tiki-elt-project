import os 
import sys
import pandas as pd
import time
from datetime import datetime
import random
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if root_path not in sys.path:
    sys.path.append(root_path)

import plugins.db as db
import plugins.utils as util
# ---------------------------------------------------------------- #

raw_logs_reviews_list = [] 
BASE_URL = "https://tiki.vn/api/v2/reviews"

query = 'select product_id,spid,seller_id from cleaned.products where review_count != 0'
data = db.query_db(query)

def fetch_reviews(batch_size=50):
    total_id = len(data)
    global raw_logs_reviews_list 
    
    for index, row in data.iterrows(): 
        product_id = row['product_id']
        spid = row['spid']
        seller_id = row['seller_id']
        print(f"{index + 1}/{total_id} Started crawling review for product {product_id}")
        
        page = 1
        while True:
            params = {
                'page': page,
                'spid': spid,
                'product_id': product_id,
                'seller_id': seller_id
            }
            try:
                response_json = util.get_tiki_api(BASE_URL, params)

                if not response_json or not response_json.get("data") or len(response_json.get("data")) == 0:
                    break

                raw_logs_reviews_list.append({
                    "spid": spid,
                    "product_id": product_id,
                    "seller_id": seller_id,
                    "page": page,
                    "extract_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "raw_response": response_json
                })

                print(f"   Scraped page {page} ({len(response_json.get('data'))} items)")

                if len(raw_logs_reviews_list) >= batch_size:
                    print(f"   [Batch Push] Pushing {len(raw_logs_reviews_list)} logs to DB...")
                    df_to_push = pd.DataFrame(raw_logs_reviews_list)
                    db.push_df_to_db(df_to_push, "raw_reviews", schema="raw", primary_key=["spid", "page"])
                    raw_logs_reviews_list.clear()
                    del df_to_push

                page += 1
                time.sleep(random.uniform(0.3, 0.8))

            except Exception as e:
                print(f"   [Error] Product {product_id} Page {page}: {e}")
                break
    
    if raw_logs_reviews_list:
        print(f"[Final Push] Pushing remaining {len(raw_logs_reviews_list)} logs...")
        db.push_df_to_db(pd.DataFrame(raw_logs_reviews_list), "raw_reviews", schema="raw", primary_key=["spid", "page"])
        raw_logs_reviews_list.clear()

# if __name__ == '__main__':
#     fetch_reviews(batch_size = 100)