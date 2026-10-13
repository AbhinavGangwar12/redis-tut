import json 
import redis 
import sqlite3
import time
import secrets
import random
from db import db_get_product 



# Just a harness
from concurrent.futures import ThreadPoolExecutor

def run_concurrent(fn, pid, n=10):
    with ThreadPoolExecutor(max_workers=n) as pool:
        return list(pool.map(lambda _: fn(pid), range(n)))

def jittered_ttl(base):
    return base + random.randint(0, base // 10)

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
POSITIVE_TTL = 60
NEGATIVE_TTL = 15
SESSION_TTL = 1800
SESSION_MAX_AGE = 86400

def counted_db_product(pid):
    r.incr("stats:db:reads")
    return db_get_product(pid)

def get_product(pid):
    key = f"cache:product:{pid}"
    cached = r.get(key)
    status = None 
    if cached is not None:
        r.incr("stats:cache:hits")
        status = "HIT"
        return json.loads(cached), status 
    r.incr("stats:cache:misses")
    product = counted_db_product(pid)
    if product:
        status = "MISS"
        r.set(key, json.dumps(product), ex=jittered_ttl(POSITIVE_TTL))
    else:
        status = None 
        r.set(key, "null", ex=NEGATIVE_TTL)

    return product, status

def get_product_safe(pid):
    key = f"cache:product:{pid}"
    lock = f"lock:product:{pid}"
    for _ in range(10):
        cached = r.get(key)
        if cached is not None:
            return json.loads(cached)
        if r.set(lock, 1 , ex=10, nx=True):
            try:
                product = counted_db_product(pid)
                if product:
                    r.set(key, json.dumps(product), ex=POSITIVE_TTL)
                else:
                    r.set(key, "null", ex=NEGATIVE_TTL)
                return product
            finally:
                r.delete(lock)
        time.sleep(0.1)
    return counted_db_product(pid)


# Delete on write 
def update_price(pid, new_price):
    con = sqlite3.connect("shop.db")
    cur = con.execute("UPDATE products SET price = ? WHERE id = ?", (new_price, pid))
    is_updated = cur.rowcount > 0
    con.commit()
    con.close()
    if is_updated:
        r.delete(f"cache:product:{pid}")
    return is_updated


def cache_stats() -> dict:
    pipe = r.pipeline()
    pipe.get("stats:cache:hits")
    pipe.get("stats:cache:misses")
    hits, misses = pipe.execute()
    hits = int(hits or 0)
    misses = int(misses or 0)
    total = hits + misses
    return {"hits": hits, "misses": misses, "hit_ratio": round(hits / total, 2) if total else 0.0}


def reset_stats():
    pipe = r.pipeline()
    pipe.delete("stats:cache:hits")
    pipe.delete("stats:cache:misses")
    pipe.execute()

def create_session(user_id: int) -> str:
    token = secrets.token_urlsafe(32)
    key = f"session:{token}"
    pipe = r.pipeline()
    pipe.hset(key, mapping={
        "user_id" : user_id, "created_at" : int(time.time())
    })
    pipe.expire(key, SESSION_TTL)
    pipe.execute()
    return token 

def get_session(token:str) -> dict | None:
    key = f"session:{token}"
    # pipe = r.pipeline()
    # pipe.hgetall(key)
    # pipe.expire(key, SESSION_TTL)
    # data, _ = pipe.execute()
    # if not data:
    #     return None 
    data = r.hgetall(key)
    if not data:
        return None 
    remaining = SESSION_MAX_AGE - (int(time.time() - int(data["created_at"])))
    if remaining <= 0:
        r.delete(key)
        return None 
    r.expire(key, min(remaining, SESSION_TTL))
    return {"user_id": int(data["user_id"]), "created_at": int(data["created_at"])}

def logout(token: str) -> bool:
    key = f"session:{token}"
    return r.delete(key) > 0

def get_products_many(ids: list[int]) -> list[dict | None]:
    keys = [f"cache:product:{id}" for id in ids]
    data = r.mget(keys)
    results = []
    pipe = r.pipeline()
    misses = 0
    hits = 0
    for pid, cached_str in zip(ids, data):
        if cached_str is None:
            product = counted_db_product(pid)
            if product:
                pipe.set(f"cache:product:{pid}", json.dumps(product), ex=jittered_ttl(POSITIVE_TTL))
            else:
                pipe.set(f"cache:product:{pid}", "null", ex=NEGATIVE_TTL)
            results.append(product)
            misses += 1
        else:
            hits += 1
            results.append(json.loads(cached_str))
    if misses > 0:
        pipe.execute()
    if hits > 0 or misses > 0:
        stat_pipe = r.pipeline()
        if hits > 0:
            stat_pipe.incr("stats:cache:hits", hits)
        if misses > 0:
            stat_pipe.incr("stats:cache:misses", misses)
        stat_pipe.execute()
    return results
                

    
if __name__ == '__main__':
    print("ANSWER 3 \n\n")
    r.delete("cache:product:1")
    print(get_product(1)["price"])
    print(update_price(1, 55.0))
    print(r.exists("cache:product:1"))
    print(get_product(1)["price"])
    print(update_price(99, 10.0))
    update_price(1, 49.5)
    

    print("ANSWER 4 \n\n")
    r.delete("cache:product:99")
    reset_stats()
    t = time.time(); print(get_product(99)); print(round(time.time() - t, 1))
    t = time.time(); print(get_product(99)); print(round(time.time() - t, 1))
    print(r.get("cache:product:99"))
    print(0 < r.ttl("cache:product:99") <= 15)
    print(cache_stats())

    print("ANSWER 5 \n\n")
    for fn in (get_product, get_product_safe):
        r.delete("cache:product:3", "stats:db:reads", "lock:product:3")
        results = run_concurrent(fn, 3)
        print(fn.__name__, r.get("stats:db:reads"), all(x == results[0] for x in results))


    print("ANSWER 6 \n\n")
    token = create_session(42)
    print(get_session(token)["user_id"])
    time.sleep(3); print(get_session(token) is not None)
    time.sleep(3); print(get_session(token) is not None)
    time.sleep(6); print(get_session(token))
    print(logout(token))
    t2 = create_session(7); print(logout(t2)); print(get_session(t2))

    print("ANSWER 7 \n\n")
    token = create_session(42)
    time.sleep(3); print(get_session(token) is not None)
    time.sleep(3); print(get_session(token) is not None)
    time.sleep(3); print(get_session(token) is not None)
    print(r.ttl(f"session:{token}") <= 3)
    time.sleep(4); print(get_session(token))
    print(r.exists(f"session:{token}"))

    print("ANSWER 8 \n\n")
    r.delete("cache:product:1", "cache:product:2", "cache:product:99", "stats:db:reads")
    reset_stats()
    print(get_products_many([1, 2, 99]))
    print(get_products_many([1, 2, 99]))
    print(cache_stats(), r.get("stats:db:reads"))