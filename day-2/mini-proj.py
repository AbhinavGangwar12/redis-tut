import redis 
import datetime 
from fastapi import FastAPI, Request, HTTPException, Depends, status
from pydantic import BaseModel
from typing import List, Annotated

app = FastAPI("prompt_library")
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

class RequestBody(BaseModel):
    title : str 
    body : str 
    tags: List[str]

def check_body(request : RequestBody):
    title, body, tag = len(request.title), len(request.body), len(request.tags)
    if title <=0 or title > 100 or body < 1 or body > 5000 or tag < 1 or tag > 10:
        raise HTTPException(status_code=422, detail="Input Violations")
    return request

@app.post("/prompts", status_code=status.HTTP_201_OK)
def add_prompt(req : Annotated[RequestBody, Depends(check_body)], request: Request):
    id = r.incr("prompt:next_id")
    tags = [t.lower() for t in req.tags]
    pipe = r.pipeline()
    pipe.hset(f"prompt:{id}", mapping={
        "title" : req.title, "body" : req.body, "tags" : ",".join(tags), "uses" : 0
    })
    for t in tags:
        pipe.sadd(f"tag:{id}", t)
    pipe.execute()

@app.get("/prompts/{id}?user_id=u1", status_code=status.HTTP_200_OK)
def get_prompt(id : int):
    if r.hexists(f"prompt:{id}", "uses") is None:
        raise HTTPException(status_code=404, detail="Prompt not found")
    today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    pipe = r.pipeline()
    pipe.hincrby(f"prompt:{id}", "uses", 1)
    pipe.zincrby(f"trending:{today}", 1, id)
    pipe.expire(f"trending:{id}", 604800, nx=True)
    pipe.lrem(f"recent:", 0, id)
    pipe.lpush(f"recent:", id)
    pipe.ltrim(f"recent:", 0, 4)
    pipe.execute()
    data = r.hgetall(f"prompt:{id}")
    return {
        "title" : data["title"],
        "body" : data["body"],
        "tags" : data["tags"],
        "uses" : int(data["uses"])
    }

@app.get("/prompts?tags=python,ai&mode=all", status_code=status.HTTP_200_OK)
def find_by_tags(tags: List[str], mode: str):
    if mode not in ("all", "any"):
        raise HTTPException(status_code=422)
    keys = [t.lower() for t in tags]
    if not keys:
        return []
    ids = r.sinter(keys) if mode == "all" else r.sunion(keys)
    prompt = [r.hgetall(f"prompt:{id}") for id in sorted(ids)]
    return [p for p in prompt if p is not None]

