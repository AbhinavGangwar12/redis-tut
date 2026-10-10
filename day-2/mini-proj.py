import redis 
import datetime 
from fastapi import FastAPI, Request, HTTPException, Depends, status, Query
from pydantic import BaseModel
from typing import List, Annotated

app = FastAPI("prompt-library")
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

@app.post("/prompts", status_code=status.HTTP_201_CREATED)
def add_prompt(req : Annotated[RequestBody, Depends(check_body)], request: Request):
    id = r.incr("prompt:next_id")
    tags = [t.lower() for t in req.tags]
    pipe = r.pipeline()
    pipe.hset(f"prompt:{id}", mapping={
        "title" : req.title, "body" : req.body, "tags" : ",".join(tags), "uses" : 0
    })
    for t in tags:
        pipe.sadd(f"tag:{t}", id)
    pipe.execute()
    return {"id" : id, "title" : req.title, "body" : req.body, "tags" : ",".join(tags), "uses" : 0}

@app.get("/prompts/{id}", status_code=status.HTTP_200_OK)
def get_prompt(id : int, user_id: str = Query(..., min_length=1, max_length=50)):
    if r.hexists(f"prompt:{id}", "uses") is None:
        raise HTTPException(status_code=404, detail="Prompt not found")
    today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    pipe = r.pipeline()
    pipe.hincrby(f"prompt:{id}", "uses", 1)
    pipe.zincrby(f"trending:{today}", 1, id)
    pipe.expire(f"trending:{today}", 604800, nx=True)
    pipe.lrem(f"recent:{user_id}", 0, id)
    pipe.lpush(f"recent:{user_id}", id)
    pipe.ltrim(f"recent:{user_id}", 0, 4)
    pipe.execute()
    data = r.hgetall(f"prompt:{id}")
    return {
        "title" : data["title"],
        "body" : data["body"],
        "tags" : data["tags"],
        "uses" : int(data["uses"])
    }

@app.get("/prompts", status_code=status.HTTP_200_OK)
def find_by_tags(tags: str = Query(..., min_length=1), mode: str = "all"):
    if mode not in ("all", "any"):
        raise HTTPException(status_code=422)
    tag_list = [t.strip().lower() for t in tags.split(",") if t.strip()]
    keys = [f"tag:{t.lower()}" for t in tag_list]
    if not keys:
        return []
    ids = r.sinter(keys) if mode == "all" else r.sunion(keys)
    prompt = [r.hgetall(f"prompt:{id}") for id in sorted(ids, key=int)]
    return [p for p in prompt if p]

@app.get("/trending")
def get_trending(limit: int = Query(..., gt=0, le=50), period: str = "today"):
    if period.lower() not in ("today", "week"):
        raise HTTPException(status_code=422, detail="Invalid request")
    if period.lower() == "today":
        today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
        ids = r.zrange(f"trending:{today}", 0, limit-1, desc=True, withscores=True)
        if ids is None:
            return []
        titles = [r.hget(f"prompt:{id}", "title") for id, _ in ids]
        ret = []
        for i in range(len(ids)):
            ret.append({
                "id" : int(ids[i][0]),
                "title" : titles[i],
                "score" : int(ids[i][1])
            })
        return ret 
    else:
        pass # because I didn't understood whether we have to return the list of last 7 days or just the Sum of scores as you mentioned to store the sum into a cache.

@app.get("/users/{user_id}/recent", status_code=status.HTTP_200_OK)
def get_prompts(user_id: str):
    user_ids = r.lrange(f"recent:{user_id}", 0, -1)
    if not user_ids:
        raise HTTPException(status_code=404)
    users = []
    for user in user_ids:
        data = r.hgetall(f"prompt:{user}")
        if data:
            users.append(data)
    return users 

@app.delete("/prompt/{id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_prompt(id):
    if not r.exists(f"prompt:{id}"):
        raise HTTPException(status_code=404, detail="Prompt not found!")
    data = r.hgetall(f"prompt:{id}")
    tags = data["tags"].split(",")
    today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    # i forget the logic to create the list of previous 6 days list 
    pipe = r.pipeline()
    pipe.delete(f"prompt:{id}")
    for t in tags:
        pipe.srem(f"tag:{t}", id)
    pipe.zrem(f"trending:{today}", id)
    pipe.execute()
    return data
    


