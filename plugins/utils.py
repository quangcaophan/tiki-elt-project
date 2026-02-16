import requests
import time
import random

HEADERS={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"}

def get_tiki_api(url, params, retries=3):
    for i in range(retries):
        try:
            response = requests.get(url, params=params, headers=HEADERS, timeout=10)

            if response.status_code == 200:
                return response.json()
            
            print(f"  [!] Retry {i+1} time: HTTP {response.status_code} at {url}")
            time.sleep(random.uniform(1, 3))
            
        except Exception as e:
            print(f"  [!] Connection error {i+1} time: {e}")
            time.sleep(2)
            
    return None