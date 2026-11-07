import redis 
import uuid
import time
import json
import operator 
import hashlib, re
from collections import Counter

from langgraph.checkpoint.redis import RedisSaver 
from langgraph.graph import START, END, StateGraph 
from typing import TypedDict, Annotated 

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



class ChargeState(TypedDict):
    order: str 
    logs: Annotated[list[str], operator.add]

def build_charge_graph(checkpointer, use_markers:bool):
    def prepare(state: ChargeState):
        return {"log" : ["prepare"]}
    
    def charge(state: ChargeState):
        order = state["order"]

        def do_charge():
            r.rpush("lab:charges", order)

        if use_markers:
            run_once(f"done:charge:{order}", do_charge)
        else:
            do_charge()

        if r.get("lab:crash") == "1":
            raise RuntimeError("simulated crash in charge")
        return {"log" : ["charged"]}
    def finish(state: ChargeState):
        return {"log" : ["finish"]}

    g = StateGraph(ChargeState)
    g.add_node("prepare", prepare)
    g.add_node("charge", charge)
    g.add_node("finish", finish)
    
    g.add_edge(START, "prepare")
    g.add_edge("prepare", "charge")
    g.add_edge("charge", "finish")
    g.add_edge("finish", END)
    
    return g.compile(checkpointer=checkpointer)


def sliding_window_allow(client_id: str, limit: int, window_sec: int):
    SCRIPT = """
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
    lua = r.register_script(script=SCRIPT)
    now_ms = int(time.time() * 1000)
    mem = f"{now_ms}-{uuid.uuid4().hex[:8]}"
    return lua(keys=[f"rl:sw:{client_id}"], args=[now_ms, window_sec * 1000, limit, mem]) == 1

def exact_key(name: str, args: dict):
    raw_string = f"{name}:{json.dumps(args, sort_keys=True)}"
    digest = hashlib.sha256(raw_string.encode("utf-8")).hexdigest()
    return f"cache:tool:{digest}"
def exact_cache_set(name: str, args:dict,answer: str ,ttl: int = 3600) -> None:
    key = exact_key(name=name, args=args)
    pipe = r.pipeline()
    pipe.set(key, answer, ex=ttl)
    pipe.execute()

def exact_cache_get(name: str, args: dict) -> str | None:
    key = exact_key(name=name, args=args)
    ans = r.get(key)
    if ans is not None:
        return ans 
    return None 

def call_tool(name: str, args: dict, client_id: str, tools: dict) -> dict:
    allowed = sliding_window_allow(client_id=client_id, limit=5, window_sec=10)
    if allowed:
        ans = exact_cache_get(name=name, args=args)
        if ans is not None:
            return {"status":"cached", "result" : ans}
        ans = tools[name](**args)
        exact_cache_set(name=name, args=args, answer=ans)
        return {"status" : "ok", "result" : ans}
    return {"status" : "rate_limited", "result" : None}

def write_durable(client, key: str, value: str, replicas: int = 1, timeout_ms: int = 100) -> bool:
    pipe = client.pipeline()
    pipe.set(key, value)
    pipe.wait(replicas, timeout_ms)
    _, count = pipe.execute()
    return int(count) >= replicas 


if __name__ == '__main__':

    # print("----     TEST 2      ----")
    # rl = redis.Redis(host="localhost", port=6380, decode_responses=True)
    # rl.config_resetstat()
    # rl.set("a", 1); rl.get("a"); rl.get("missing")
    # h = health(rl)
    # print(sorted(h), h["hit_ratio"], h["evicted_keys"])
    
    
    # print("----     TEST 3      ----")
    # for used, mx in [(50, 100), (85, 100), (80, 100), (100, 100), (50, 0)]:
    #     print(memory_status({"used_memory": used, "maxmemory": mx}))

    
    # print("----     TEST 4      ----")
    # r.delete("once:t1")
    # calls = []
    # print(run_once("once:t1", lambda: calls.append(1)))
    # print(run_once("once:t1", lambda: calls.append(1)))
    # print(len(calls))


    # print("----     TEST 5      ----")
    # keys = run_with_ttl(f"ttl-{uuid.uuid4().hex[:6]}", 60)
    # print(len(keys) > 0, all(0 < re.ttl(k) <= 3600 for k in keys))
    # keys = run_with_ttl(f"ttl-{uuid.uuid4().hex[:6]}", 1)
    # print(all(0 < re.ttl(k) <= 60 for k in keys))


    # print("----     TEST 6      ----")
    # for use_marker in (False, True):
    #     r.delete("lab:charges")
    #     order = f"order-{uuid.uuid4().hex[:6]}"
    #     with RedisSaver.from_conn_string("redis://localhost:6379") as cp:
    #         cp.setup()
    #         graph = build_charge_graph(cp, use_marker)
    #         config = {"configurable": {"thread_id": order}}
    #         r.set("lab:crash", "1")
    #         try:
    #             graph.invoke({"order": order, "log": []}, config)
    #         except RuntimeError:
    #             pass
    #         r.set("lab:crash", "0")
    #         graph.invoke(None, config)
    #     print(use_marker, r.llen("lab:charges"))
    
    
    # print("----     TEST 7      ----")
    # calls = []
    # TOOLS = {"lookup": lambda q: (calls.append(q), f"result:{q}")[1]}
    # for k in list(r.scan_iter("cache:tool:*")) + ["rl:sw:c1", "rl:sw:other"]:
    #     r.delete(k)
    # print(call_tool("lookup", {"q": "a"}, "c1", TOOLS))
    # print(call_tool("lookup", {"q": "a"}, "c1", TOOLS))
    # print(len(calls))
    # statuses = [call_tool("lookup", {"q": f"x{i}"}, "c1", TOOLS)["status"] for i in range(10)]
    # print(Counter(statuses), len(calls))
    # print(call_tool("lookup", {"q": "a"}, "other", TOOLS))
    
    
    print("----     TEST 8      ----")
    p = redis.Redis(host="localhost", port=6382, decode_responses=True)
    # print(write_durable(p, "k1", "v1"))

    # print(write_durable(p, "k2", "v2"))
    # print(p.get("k2"))

    print(write_durable(p, "k3", "v3"))