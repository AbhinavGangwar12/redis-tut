import redis 
import uuid
from agent_lab_1 import build, re, RedisSaver

r = redis.Redis(host="localhost", port=6380, decode_responses=True)
def health(r : redis.Redis):
    mem = r.info('memory')
    stats = r.info('stats')
    slowlog = r.slowlog_len()
    hits, misses = stats["keyspace_hits"], stats["keyspace_misses"]

    if hits and misses:
        ratio = round((int(hits) / (int(hits) + int(misses))),2)
    else:
        ratio = None
    return {
        "used_mb" : mem["used_memory_human"],
        "hit_ratio" : ratio,
        "evicted_keys" : int(stats["evicted_keys"]),
        "slowlog_len" : int(slowlog)
    }

def memory_status(info: dict, warn_ratio: float = 0.8) -> str:
    used, mx = info["used_memory"], info["maxmemory"]
    if mx == 0:
        return "unbounded"
    if used >= mx:
        return "full"
    if round((used/mx), 1) >= warn_ratio:
        return "warn"
    return "ok"

def run_once(key: str, fn, ttl: int = 3600) -> bool:
    if r.set(key, 1, ex=ttl, nx=True):
        fn()
        return True 
    return False

def run_with_ttl(thread: str, minutes: int) -> list[str]:
    re.set("lab:crash" , 0)
    ttl = {"default_ttl" : minutes}
    with RedisSaver.from_conn_string(redis_url="redis://localhost:6379", ttl=ttl) as checkpointer:
        checkpointer.setup()
        graph = build(checkpointer=checkpointer)
        config = {"configurable" : {"thread_id" : thread}}
        graph.invoke({"task" : "demo_task", "log" : []}, config=config)
    return re.keys(f"checkpoint:{thread}:*")


if __name__ == '__main__':

    print("----     TEST 2      ----")
    rl = redis.Redis(host="localhost", port=6380, decode_responses=True)
    rl.config_resetstat()
    rl.set("a", 1); rl.get("a"); rl.get("missing")
    h = health(rl)
    print(sorted(h), h["hit_ratio"], h["evicted_keys"])
    
    
    print("----     TEST 3      ----")
    for used, mx in [(50, 100), (85, 100), (80, 100), (100, 100), (50, 0)]:
        print(memory_status({"used_memory": used, "maxmemory": mx}))

    
    print("----     TEST 4      ----")
    r.delete("once:t1")
    calls = []
    print(run_once("once:t1", lambda: calls.append(1)))
    print(run_once("once:t1", lambda: calls.append(1)))
    print(len(calls))


    print("----     TEST 5      ----")
    keys = run_with_ttl(f"ttl-{uuid.uuid4().hex[:6]}", 60)
    print(len(keys) > 0, all(0 < r.ttl(k) <= 3600 for k in keys))
    keys = run_with_ttl(f"ttl-{uuid.uuid4().hex[:6]}", 1)
    print(all(0 < r.ttl(k) <= 60 for k in keys))