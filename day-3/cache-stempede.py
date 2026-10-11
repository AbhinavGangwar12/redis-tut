import redis 
import time 
import json 
from db import db_get_product

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def get_product_safe(pid):
    key = f"cache:product:{pid}"
    lock = f"lock:product:{pid}"
    for _ in range(10):
        cached = r.get(key)
        if cached is not None:
            print("Cache hit!")
            return json.loads(cached)
        if r.set(lock, "1", nx=True, ex=10):
            try:
                product = db_get_product(pid)
                print("LOCK acquired!")
                r.set(key, json.dumps(product), ex=60)
                return product
            finally:
                print("Lock Released!")
                r.delete(lock)
    time.sleep(0.1)
    print("Cache is Locked, so directly fetching DB!")
    return db_get_product(pid) # we are returning the product because in-case when a key is being set using the locks, so other request should not wait

"""
Honest limitation: r.delete(lock) is unsafe in one case. If the rebuild takes longer than the 10-second lock, the lock expires, another request takes it, and your DEL then deletes their lock. Fixing this properly needs a token check, which is Day 4. For now, keep the lock’s TTL longer than your database read.
"""

if __name__ == '__main__':
    _  = get_product_safe(1)