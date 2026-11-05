import redis 

r = redis.Redis(host="localhost", port=6380, decode_responses=True)

r.flushdb()
r.config_set("maxmemory", "2mb")
r.config_set("maxmemory-policy", "allkeys-lru")

for i in range(5000):
    r.set(f"fill:{i}", "x" * 1000)
print(r.dbsize() < 5000, r.info("stats")["evicted_keys"] > 0)
print(r.exists("fill:4999"), r.exists("fill:0"))

r.config_set("maxmemory-policy", "noeviction")
try:
    for i in range(5000, 10000):
        r.set(f"fill:{i}", "x" * 1000)
except redis.exceptions.OutOfMemoryError as exc:
    print("write rejected:", type(exc).__name__)

