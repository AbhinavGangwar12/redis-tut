from cache_aside import get_product 
import redis 

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

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
    
if __name__ == '__main__':
    reset_stats()
    r.delete("cache:product:1")
    for _ in range(4):
        get_product(1)
    print(cache_stats())