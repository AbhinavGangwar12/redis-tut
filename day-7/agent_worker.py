import redis 
import time 
import sys 
import json 
from langgraph.checkpoint.redis import RedisSaver


from agent_graph import build

STREAM = "agent:jobs"
GROUP = "agents"
CLAIM_IDLE_MS = 8000
MAX_ATTEMPTS = 3
BLOCK_MS = 1000

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def ensure_group():
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise 

def get_delivery_count(msg_id: str) -> int:
    """Fetch the exact number of times this message was delivered."""
    pend_info = r.xpending_range(STREAM, GROUP, msg_id, msg_id, 1)
    return pend_info[0]['times_delivered'] if pend_info else 1

def process_job(name: str, graph, msg_id: str, job_id: str, topic: str):
    attempts = get_delivery_count(msg_id=msg_id)
    print(f"[{name}] Picked up job {job_id} | Attempt: {attempts}")

    r.hset(f"agent:job:{job_id}", mapping={
        "status" : "running", 
        "attempts" : attempts
    })

    config = {"configurable" : {"thread_id" : job_id}}

    try:
        snap = graph.get_state(config)
        if snap.next:
            print(f"[{name}] Resuming job {job_id} from checkpoint...")
            out = graph.invoke(None, config)
        elif snap.values:
            print(f"[{name}] Job {job_id} finished previously, pulling saved state.")
            out = snap.values
        else:
            print(f"[{name}] Starting job {job_id} from scratch.")
            out = graph.invoke({
                "job_id": job_id, 
                "topic": topic, 
                "facts": "", 
                "draft": "", 
                "result": "", 
                "log": []
            }, config)
        pipe = r.pipeline()
        pipe.hset(f"agent:job:{job_id}", mapping={
            "status": "done",
            "result": out.get("result", ""),
            "steps": json.dumps(out.get("log", []))
        })
        pipe.xack(STREAM, GROUP, msg_id)
        pipe.xdel(STREAM, msg_id)
        pipe.execute()
        
        print(f"[{name}] Job {job_id} successfully completed!")
    except Exception as e:
        if attempts < MAX_ATTEMPTS:
            r.hset(f"agent:job:{job_id}", mapping={"status": "queued", "error": str(e)})
            print(f"[{name}] Job {job_id} failed (Attempt {attempts}/{MAX_ATTEMPTS}): {e}")
        else:
            pipe = r.pipeline()
            pipe.xadd("agent:dead", {"job_id": job_id, "topic": topic, "error": str(e), "msg_id": msg_id})
            pipe.hset(f"agent:job:{job_id}", mapping={"status": "dead", "error": str(e)})
            pipe.xack(STREAM, GROUP, msg_id)
            pipe.xdel(STREAM, msg_id)
            pipe.execute()
            
            print(f"[{name}] Job {job_id} moved to dead-letter queue after {attempts} attempts.")

def main(name: str):
    ensure_group()
    with RedisSaver.from_conn_string("redis://localhost:6379", ttl={"default_ttl": 120}) as cp:
        cp.setup()
        graph = build(cp)  
        
        print(f"[{name}] Checking for own pending (crashed) messages...")
        res = r.xreadgroup(GROUP, name, {STREAM: "0"}, count=1)
        if res:
            for _, messages in res:
                for msg_id, fields in messages:
                    process_job(name, graph, msg_id, fields["job_id"], fields["topic"])
                    
        print(f"[{name}] Listening for new or abandoned jobs...")
        while True:
            claim_res = r.xautoclaim(STREAM, GROUP, name, CLAIM_IDLE_MS, start_id="0", count=1)
            messages = claim_res[1]
            if messages:
                for msg_id, fields in messages:
                    print(f"[{name}] Auto-claimed idle message {msg_id}")
                    process_job(name, graph, msg_id, fields["job_id"], fields["topic"])
                continue
                
            res = r.xreadgroup(GROUP, name, {STREAM: ">"}, count=1, block=BLOCK_MS)
            if res:
                for _, messages in res:
                    for msg_id, fields in messages:
                        process_job(name, graph, msg_id, fields["job_id"], fields["topic"])

if __name__ == '__main__':
    worker_name = sys.argv[1] if len(sys.argv) > 1 else "worker-1"
    main(worker_name)