import redis 
import time
import math 
import uuid
from fastapi import FastAPI, Header, Response, status, Path, HTTPException
from fastapi.responses import JSONResponse
from typing import Annotated

app = FastAPI(title="guarded-api")
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def check(client: str, limit: int, window: int) -> dict:
    script = """
    local time = redis.call('TIME')
    local now = math.floor( (tonumber(time[1]) * 1000) + (tonumber(time[2] / 1000) ) )
    local key = KEYS[1]
    local window = tonumber(ARGV[1]) * 1000
    local limit = tonumber(ARGV[2])
    local member = ARGV[3]

    redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
    local count = tonumber(redis.call('ZCARD', key))
    if count < limit then
        redis.call('ZADD', key, now, member)
        redis.call('PEXPIRE', key, window)
        return {1, limit - count, 0}
    end
    local oldest_candidate = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    return {0, 0, oldest_candidate[2] + window - now}
    """
    lua = r.register_script(script)
    member = uuid.uuid4().hex
    allowed, remaining, retry = lua(keys=[f"rl:sw:{client}"], args=[window, limit, member])
    return {"ping" : bool(allowed), "remaining" : remaining, "retry" : retry}

@app.get("/ping")
def ping(x_client_id: Annotated[str, Header(min_length=1, max_length=50)]):
    payload = check(client=x_client_id, limit=5, window=10)
    if payload["ping"]:
        return JSONResponse(
            content={"pong": True},
            headers={"X-RateLimit-Limit": "5", "X-RateLimit-Remaining": str(payload["remaining"])},
        )
    
    retry_seconds = max(1, math.ceil(payload["retry"] / 1000))
    raise HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail="Rate limit exceeded",
        headers={
            "Retry-After": str(retry_seconds),
            "X-RateLimit-Remaining": "0"
        }
    )

def acquire_lock(name: str, ttl: int = 10) -> tuple[str, int] | None:
    key = f"lock:report:{name}"
    token = uuid.uuid4().hex
    if r.set(key, token, nx=True, ex=ttl):
        return token, int(time.time())
    return None 

def release_lock(name: str, token: str):
    script = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        redis.call('DEL', KEYS[1])
        return 1
    end 
    return 0
    """
    lua = r.register_script(script)
    return lua(keys=[f"lock:report:{name}"],args=[token]) == 1

@app.post("/reports/{name}")
def reports(name: Annotated[str, Path(min_length=1, max_length=30)]):
    lock_result = acquire_lock(name=name)
    if lock_result is None:
        return JSONResponse(
            content={"detail": "Report is already being generated"},
            status_code=status.HTTP_409_CONFLICT
        )
    token, generated_at = lock_result
    try:
        time.sleep(3)
        return {
            "name": name,
            "status": "generated",
            "generated_at": generated_at
        }
    finally:
        release_lock(name=name, token=token)

