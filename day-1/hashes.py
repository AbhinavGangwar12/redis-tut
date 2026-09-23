import redis 
import hashlib 
import time

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def get_query_hash(prompt):
    # Creates a consistent, short, safe key from a long prompt
    return hashlib.sha256(prompt.lower().strip().encode()).hexdigest()

def get_ai_response_pro(prompt):
    query_hash = get_query_hash(prompt=prompt)
    cache_key = f"llm:cache:{query_hash}"

    r.hincrby("llm:stats", "total_requests", 1)
    cached_response = r.get(cache_key)
    if cached_response:
        r.hincrby("llm:stats", "cache_hits", 1)
        return f"[HIT] {cached_response}"

    r.hincrby("llm:stats", "cache_misses", 1)
    response = f"LLM generated answer for: {prompt}"
    time.sleep(2)
    r.set(cache_key, response, ex=3600)
    return f"[MISS] {response}"

def invalid_cache(prompt):
    """Force evict a specific prompt from the cache."""
    query_hash = get_query_hash(prompt)
    r.delete(f"llm:cache:{query_hash}")
    print(f"Invalidated cache for hash: {query_hash}")

def print_stats():
    stats = r.hgetall("llm:stats")
    print(f"Cache Stats: {stats}")
    return

if __name__ == '__main__':
    print(get_ai_response_pro("Explain vector embeddings"))
    print(get_ai_response_pro("Explain vector embeddings"))
    print_stats()