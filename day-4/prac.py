import threading
import redis 
import time
import uuid
from concurrent.futures import ThreadPoolExecutor


r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def run_threads(fn, n_threads=20, n_loops=50):
    def worker():
        for _ in range(n_loops):
            fn()

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

def unsafe_decrement():
    key = "race:stock"
    count = int(r.get(key))
    time.sleep(0.001)
    r.set(key, count - 1)

def safe_decrement():
    return r.decr("race:stock")

def acquire_lock(name: str , ttl: int = 10) -> str | None:
    token = uuid.uuid4().hex
    if r.set(f"lock:{name}", token, nx=True, ex=ttl):
        return token
    return None 

script = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""
def release_lock(name: str, token: str) -> bool:
    exec_script = r.register_script(script)
    return exec_script(keys=[f"lock:{name}"], args=[token]) == 1

def withdraw_watch(key: str, amount: int) -> bool:
    with r.pipeline() as pipe:
        while True:
            try:
                pipe.watch(key)
                val = pipe.get(key)
                if val is None:
                    pipe.unwatch()
                    return False 
                balance = int(val)
                if balance < amount:
                    pipe.unwatch()
                    return False 
                pipe.multi()
                pipe.set(key, balance - amount)
                pipe.execute()
                return True 
            except redis.WatchError:
                continue
    return False # If the key is not found

def withdraw_lua(key: str, amount: int) -> bool:
    script = """
    local v = redis.call('GET', KEYS[1])
    if not v then return 0 end
    local balance = tonumber(v)
    local amount = tonumber(ARGV[1])
    if balance < amount then
        return 0
    end
    redis.call('SET', KEYS[1], balance - amount)
    return 1
    """

    lua_script = r.register_script(script)
    if r.exists(key):
        return lua_script(keys=[key], args=[amount]) == 1
    return False

def fixed_window_allow(client: str, limit: int, window: int) -> tuple[bool, int]:
    bucket = int(time.time()) // window
    key = f"rl:fw:{client}:{bucket}"
    pipe = r.pipeline()
    pipe.incr(key)
    pipe.expire(key, window, nx=True)
    count, _ = pipe.execute()
    return count <= limit , max(limit - count, 0)

def sliding_window_allow(client: str, limit: int, windows_seconds: int) -> bool:
    script = """
    local time = redis.call('TIME')
    local seconds = tonumber(time[1])
    local microseconds = tonumber(time[2])
    local milliseconds = math.floor((seconds * 1000) + (microseconds/1000))
    local key = KEYS[1]
    local window = tonumber(ARGV[1]) * 1000
    local limit = tonumber(ARGV[2])
    local member = ARGV[3]
    redis.call('ZREMRANGEBYSCORE', key, 0, milliseconds - window)
    if redis.call('ZCARD', key) < limit then
        redis.call('ZADD', key, milliseconds, member)
        redis.call('PEXPIRE', key, window)
        return 1
    end
    return 0
    """
    member = uuid.uuid4().hex
    lua = r.register_script(script)
    return lua(keys=[f"rl:sw:{client}"], args=[windows_seconds, limit, member]) == 1


def sliding_window_unsafe(client, limit, window_seconds):
    key = f"rl:sw:{client}"
    now_ms = int(time.time() * 1000)
    r.zremrangebyscore(key, 0, now_ms - window_seconds * 1000)
    if r.zcard(key) < limit:
        time.sleep(0.005)
        r.zadd(key, {f"{now_ms}-{uuid.uuid4().hex[:8]}": now_ms})
        return True
    return False

def sliding_window_info(client: str, limit: int, window_seconds: int) -> dict:
    script = """
    local time = redis.call('TIME')
    local now = math.floor((tonumber(time[1]) * 1000) + (tonumber(time[2]) / 1000))
    local window = tonumber(ARGV[1]) * 1000
    local limit = tonumber(ARGV[2])
    local key = KEYS[1]
    local member = ARGV[3]
    redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
    local count = tonumber(redis.call('ZCARD', key))
    if count < limit then
        redis.call('ZADD', key, now, member)
        redis.call('PEXPIRE', key, window)
        return { 1 , limit - count , 0 }
    end
    local res = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local oldest = tonumber(res[2])
    return { 0 , 0 , oldest + window - now }
    """
    lua = r.register_script(script)
    member = uuid.uuid4().hex 
    allowed , remaining , retry = lua(keys=[f"rl:sw:{client}"], args=[window_seconds, limit, member])
    return {"allowed": bool(allowed), "remaining": remaining, "retry_after_ms": retry}

if __name__ == '__main__':
    # r.set("race:stock", 1000); run_threads(unsafe_decrement); print(r.get("race:stock"))
    # r.set("race:stock", 1000); run_threads(safe_decrement); print(r.get("race:stock"))

    # r.delete("lock:demo")
    # a = acquire_lock("demo", ttl=10)
    # print(bool(a))
    # print(acquire_lock("demo"))
    # print(0 < r.ttl("lock:demo") <= 10)
    # print(release_lock("demo", "wrong-token"))
    # print(release_lock("demo", a))
    # print(r.exists("lock:demo"))

    # fn = withdraw_watch
    # r.set("acct:t", 100)
    # with ThreadPoolExecutor(max_workers=20) as pool:
    #     results = list(pool.map(lambda _: fn("acct:t", 10), range(20)))
    # print(results.count(True), r.get("acct:t"))
    # r.set("acct:t", 5); print(fn("acct:t", 10), r.get("acct:t"))
    # r.delete("acct:t"); print(fn("acct:t", 10))

    # time.sleep(10 - time.time() % 10)
    # out = [fixed_window_allow("u1", 5, 10) for _ in range(7)]
    # print([a for a, _ in out]); print([rem for _, rem in out])
    # key = f"rl:fw:u1:{int(time.time()) // 10}"
    # print(0 < r.ttl(key) <= 10)


    # r.delete("rl:sw:u1")
    # print([sliding_window_allow("u1", 3, 2) for _ in range(5)])
    # time.sleep(2.1)
    # print(sliding_window_allow("u1", 3, 2))
    # print(0 < r.pttl("rl:sw:u1") <= 2000)

    # for name, fn in (("unsafe", sliding_window_unsafe), ("lua", sliding_window_allow)):
    #     r.delete("rl:sw:race")
    #     with ThreadPoolExecutor(max_workers=50) as pool:
    #         results = list(pool.map(lambda _: fn("race", 10, 5), range(50)))
    #     print(name, results.count(True))


    r.delete("rl:sw:u2")
    out = [sliding_window_info("u2", 3, 2) for _ in range(4)]
    print([o["allowed"] for o in out]); print([o["remaining"] for o in out])
    print(out[3]["retry_after_ms"] > 0 and out[3]["retry_after_ms"] <= 2000)
    time.sleep(out[3]["retry_after_ms"] / 1000 + 0.05)
    print(sliding_window_info("u2", 3, 2)["allowed"])