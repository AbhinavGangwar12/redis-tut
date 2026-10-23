import redis 
import time 
import uuid 


r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def fixed_window(client, limit, window):
    window_id = int(time.time()) // window 
    key = f"rl:fw:{client}:{window_id}"
    pipe = r.pipeline()
    pipe.incr(key)
    pipe.expire(key, window, nx=True)
    count, _ = pipe.execute()
    return count <= limit

SLIDING_WINDOW_LUA = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
if redis.call('ZCARD', key) < limit then
    redis.call('ZADD', key, now, member)
    redis.call('PEXPIRE', key, window)
    return 1
end
return 0
"""
sliding_script = r.register_script(SLIDING_WINDOW_LUA)
def sliding_window(client, limit, window_seconds):
    now_ms = int(time.time() * 1000)
    member = f"{now_ms}-{uuid.uuid4().hex[:8]}"
    result = sliding_script(
        keys=[f"rl:sw:{client}"], args=[now_ms, window_seconds * 1000, limit, member]
    )
    return result == 1
if __name__ == '__main__':
    # print([fixed_window("u1", 5, 10) for _ in range(7)])
    r.delete("rl:sw:u1")
    print([sliding_window("u1", 3, 2) for _ in range(5)])
    print("Pausing for 2.1 seconds (dummy)")
    time.sleep(2.1)
    print(sliding_window("u1", 3, 2))