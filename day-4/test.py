import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import redis
import requests

BASE = "http://127.0.0.1:8000"
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def hit_full(path, method="GET", client="u1"):
    headers = {"X-Client-Id": client} if client else {}
    return requests.request(method, BASE + path, headers=headers, timeout=15)

def hit(path, method="GET", client="u1"):
    return hit_full(path, method, client).status_code

def burst(n, *args, **kwargs):
    with ThreadPoolExecutor(max_workers=n) as pool:
        return Counter(pool.map(lambda _: hit(*args, **kwargs), range(n)))

def step(title):
    print(f"\n--- {title} ---")

r.delete("rl:sw:u1", "rl:sw:u2", "lock:report:sales", "lock:report:finance")

step("1. burst of 20 /ping from u1")
print(burst(20, "/ping"))

step("2. a different client is unaffected")
print(burst(1, "/ping", client="u2"))

step("3. headers of a 429 for u1")
resp = hit_full("/ping")
retry_after = int(resp.headers["Retry-After"])
print(resp.status_code, "Retry-After =", retry_after,
      "X-RateLimit-Remaining =", resp.headers["X-RateLimit-Remaining"])
print("Retry-After in range:", 1 <= retry_after <= 10)

step("4. after waiting out the window")
time.sleep(10.5)
print(burst(1, "/ping"))

step("5. five concurrent report requests")
with ThreadPoolExecutor(max_workers=1) as bg:
    future = bg.submit(burst, 5, "/reports/sales", method="POST")
    time.sleep(1)
    print("lock token while running:", r.get("lock:report:sales"),
          "| ttl:", r.ttl("lock:report:sales"))
    print(future.result())
print("lock exists afterwards:", r.exists("lock:report:sales"))

step("6. a lock held by someone else")
r.set("lock:report:finance", "foo", ex=5)
print(hit("/reports/finance", method="POST"))
time.sleep(5.5)
print(hit("/reports/finance", method="POST"))

step("7. validation (422 cases)")
print(hit("/ping", client=None))
print(hit("/reports/" + "a" * 31, method="POST"))