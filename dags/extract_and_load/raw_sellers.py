import pandas as pd
import time
import random
from datetime import datetime
import os 
import sys

root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if root_path not in sys.path:
    sys.path.append(root_path)
    
import plugins.db as db
import plugins.utils as util

# ---------------------------------------------------------------- #

raw_logs_sellers_list = [] 
BASE_URL = "https://api.tiki.vn/product-detail/v2/widgets/seller"


query = 'select distinct seller_id from cleaned.products'
list_store = db.query_db(query)

def fetch_seller():
    global raw_logs_sellers_list 
    
    total_sellers = len(list_store)
    print(f"Total sellers to crawl: {total_sellers}")
    
    for index, row in list_store.iterrows(): 
        store_id = row['seller_id']
        print(f"[{index + 1}/{total_sellers}] Starting crawl from Seller ID: {store_id}")
        
        params = {'seller_id': store_id}

        try:
            response_json = util.get_tiki_api(BASE_URL, params)

            if response_json:
                raw_logs_sellers_list.append({
                    "seller_id": store_id,
                    "extract_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "raw_response": response_json
                })
            
            if not response_json or not response_json.get("data"):
                print(f"   -> Dont have data or error at Seller {store_id}")
                continue

            if len(raw_logs_sellers_list) >= 100:
                print(f"\n[Batch Update] Reached 100 raw logs. Pushing to DB...")
                
                df_raw_batch = pd.DataFrame(raw_logs_sellers_list)
                db.push_df_to_db(df_raw_batch, "raw_sellers",schema='raw', primary_key="seller_id")
                raw_logs_sellers_list.clear()
                del df_raw_batch
            
            time.sleep(random.uniform(0.5, 1.2)) 

        except Exception as e:
            print(f"Exception Error at Seller {store_id}: {e}")
            continue

# if __name__ == '__main__':
#     fetch_seller()
    
#     if raw_logs_sellers_list:
#         print("[Final Update] Pushing remaining raw logs...")
#         db.push_df_to_db(pd.DataFrame(raw_logs_sellers_list), "raw_sellers",schema='raw', primary_key="seller_id")
