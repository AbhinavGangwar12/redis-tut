import redis 
import time 
import math
import json
import uuid
from fastapi import FastAPI, Header, status, HTTPException
from typing import Annotated
from pydantic import BaseModel , Field 

from yard import rate_limiter

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
app = FastAPI(title="agent-service")

class PostTask(BaseModel):
    topic: str = Field(..., min_length=3, max_length=100)

@app.post("/tasks", status_code=status.HTTP_202_ACCEPTED)
def post_tasks(topic: PostTask, x_client_id: Annotated[str , Header(min_length=1, max_length=50)]):
    window_sec = 10
    allowed = rate_limiter(x_client_id, limit=5, window_sec=window_sec)
    if allowed:
        job_id = uuid.uuid4().hex[:8]
        pipe = r.pipeline()
        pipe.hset(f"agent:job:{job_id}", mapping={
            "topic" : topic.topic,
            "status" : "queued",
            "current" : "",
            "attempts" : 0,
            "steps" : "",
            "result" : "",
            "error" : ""
        })
        pipe.expire(f"agent:job:{job_id}", 86400)
        pipe.xadd("agent:jobs" , {"job_id" : job_id, "topic" : topic.topic})
        pipe.execute()
        return {"job_id": job_id, "status" : "queued"}
    oldest_candidate = r.zrange(f"rl:sw:agent:{x_client_id}", 0, 0, withscores=True)
    wait_sec = 1
    if oldest_candidate:
        oldest_score = oldest_candidate[0][1]
        now_ms = int(time.time() * 1000)
        wait_ms = (oldest_score + (window_sec * 1000)) - now_ms
        wait_sec = max(1, math.ceil(wait_ms / 1000))

    raise HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail="Rate limit exceeded",
        headers={"Retry-After": str(wait_sec)}
    )


@app.get("/tasks/{job_id}", status_code=status.HTTP_200_OK)
def get_tasks(job_id: str):
    key = f"agent:job:{job_id}"
    details = r.hgetall(key)
    if not details:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found"
        )
    raw_steps = details.get("steps", "")
    try:
        steps_list = json.loads(raw_steps) if raw_steps else []
    except json.JSONDecodeError:
        steps_list = []

    return {
        "job_id": job_id, 
        "topic": details.get("topic", ""), 
        "status": details.get("status", ""), 
        "current": details.get("current", ""), 
        "attempts": int(details.get("attempts", 0)), 
        "steps": steps_list, 
        "result": details.get("result", "")
    }


@app.get("/health")
def health():
    mem = r.info("memory")
    stats = r.info("stats")
    slow = r.slowlog_len()
    hits, misses = int(stats.get("keyspace_hits", 0)), int(stats.get("keyspace_misses", 0))

    ratio = round(hits / (hits + misses), 2) if (hits + misses) > 0 else None

    length = r.xlen("agent:jobs")
    dead = r.xlen("agent:dead")

    try:
        pending_info = r.xpending("agent:jobs", "workers")
        pending = pending_info["pending"]
    except redis.exceptions.ResponseError:
        pending = 0
    
    return {
        "used_md" : mem["used_memory_human"],
        "hit_ratio" : ratio,
        "evicted_keys" : int(stats["evicted_keys"]),
        "slowlog_len" : int(slow),
        "queue" : {"lenght" : length, "dead" : dead, "pending" : pending}
    }

