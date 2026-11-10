import redis 
import time 
import uuid
import re, hashlib 

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def rate_limiter(client_id: str, limit: int, window_sec: int):
    script = """
    local key = KEYS[1]
    local limit = tonumber(ARGV[1])
    local window = tonumber(ARGV[2])
    local now = tonumber(ARGV[3])
    local member = ARGV[4]
    redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
    if redis.call('ZCARD', key) < limit then
        redis.call('ZADD' , key, now, member)
        redis.call('PEXPIRE', key, window)
        return 1
    end 
    return 0
    """
    lua = r.register_script(script=script)
    now = int(time.time() * 1000)
    member = f"{now}-{uuid.uuid4().hex[:8]}"

    return lua(keys=[f"rl:sw:agent:{client_id}"], args=[limit, window_sec * 1000, now, member]) == 1

def run_once(key: str,fn ,ttl: int = 3600):
    if r.set(key, 1, ex=ttl, nx=True):
        fn()
        return True
    return False

def normalize(text: str) -> str:
    """Normalizes the topic for consistent caching."""
    return re.sub(r"\s+", " ", text.strip().lower())

def exact_key(question: str):
    digest = hashlib.sha256(normalize(question).encode("utf-8")).hexdigest()
    return f"cache:tool:{digest}"

def exact_cache_get(question: str):
    key = exact_key(question=question)
    ans = r.get(key)
    return ans if ans is not None else None

def exact_cache_set(question: str, ans: str):
    key = exact_key(question=question)
    r.set(key, ans, ex=300)


