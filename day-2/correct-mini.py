import datetime
from typing import Literal

import redis
from fastapi import FastAPI, HTTPException, Path, Query, Response, status
from pydantic import BaseModel, Field, field_validator

app = FastAPI(title="prompt-library")
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

TRENDING_TTL = 7 * 24 * 3600
WEEK_CACHE_KEY = "cache:trending:week"
WEEK_CACHE_TTL = 60
RECENT_MAX = 5

def utc_today() -> datetime.date:
    return datetime.datetime.now(datetime.timezone.utc).date()


def last_7_trending_keys() -> list[str]:
    today = utc_today()
    return [f"trending:{(today - datetime.timedelta(days=i)).isoformat()}" for i in range(7)]


def to_prompt(prompt_id: int, data: dict) -> dict:
    return {
        "id": prompt_id,
        "title": data["title"],
        "body": data["body"],
        "tags": data["tags"].split(",") if data["tags"] else [],
        "uses": int(data["uses"]),
    }

class PromptIn(BaseModel):
    title: str = Field(min_length=1, max_length=100)
    body: str = Field(min_length=1, max_length=5000)
    tags: list[str] = Field(min_length=1, max_length=10)

    @field_validator("tags")
    @classmethod
    def clean_tags(cls, tags: list[str]) -> list[str]:
        cleaned: list[str] = []
        for tag in tags:
            tag = tag.strip().lower()
            if not 1 <= len(tag) <= 30 or "," in tag:
                raise ValueError("each tag must be 1-30 characters and contain no comma")
            if tag not in cleaned:
                cleaned.append(tag)
        return cleaned

@app.post("/prompts", status_code=status.HTTP_201_CREATED)
def create_prompt(req: PromptIn):
    prompt_id = r.incr("prompt:next_id")
    pipe = r.pipeline()
    pipe.hset(f"prompt:{prompt_id}", mapping={
        "title": req.title,
        "body": req.body,
        "tags": ",".join(req.tags),
        "uses": 0,
    })
    for tag in req.tags:
        pipe.sadd(f"tag:{tag}", prompt_id)
    pipe.execute()
    return {"id": prompt_id, "title": req.title, "body": req.body, "tags": req.tags, "uses": 0}

@app.get("/prompts/{prompt_id}")
def view_prompt(prompt_id: int, user_id: str = Query(..., min_length=1, max_length=50)):
    key = f"prompt:{prompt_id}"
    if not r.exists(key):
        raise HTTPException(status_code=404, detail="Prompt not found")

    daily = f"trending:{utc_today().isoformat()}"
    recent = f"recent:{user_id}"
    pipe = r.pipeline()
    pipe.hincrby(key, "uses", 1)
    pipe.zincrby(daily, 1, prompt_id)
    pipe.expire(daily, TRENDING_TTL, nx=True)
    pipe.lrem(recent, 0, prompt_id)
    pipe.lpush(recent, prompt_id)
    pipe.ltrim(recent, 0, RECENT_MAX - 1)
    pipe.hgetall(key)
    results = pipe.execute()
    return to_prompt(prompt_id, results[-1])

@app.get("/prompts")
def find_prompts(
    tags: str = Query(..., min_length=1),
    mode: Literal["all", "any"] = "all",
):
    tag_list = [t.strip().lower() for t in tags.split(",") if t.strip()]
    if not 1 <= len(tag_list) <= 10:
        raise HTTPException(status_code=422, detail="tags must contain 1-10 items")

    keys = [f"tag:{t}" for t in tag_list]
    ids = r.sinter(keys) if mode == "all" else r.sunion(keys)
    ids = sorted(ids, key=int)
    if not ids:
        return []

    pipe = r.pipeline()
    for pid in ids:
        pipe.hgetall(f"prompt:{pid}")
    rows = pipe.execute()
    return [to_prompt(int(pid), data) for pid, data in zip(ids, rows) if data]

@app.get("/trending")
def trending(
    limit: int = Query(5, ge=1, le=50),
    period: Literal["today", "week"] = "today",
):
    if period == "today":
        key = f"trending:{utc_today().isoformat()}"
    else:
        key = WEEK_CACHE_KEY
        if not r.exists(key):
            pipe = r.pipeline()
            pipe.zunionstore(key, last_7_trending_keys())
            pipe.expire(key, WEEK_CACHE_TTL)
            pipe.execute()

    ranked = r.zrange(key, 0, limit - 1, desc=True, withscores=True)
    if not ranked:
        return []

    pipe = r.pipeline()
    for pid, _ in ranked:
        pipe.hget(f"prompt:{pid}", "title")
    titles = pipe.execute()
    return [
        {"id": int(pid), "title": title, "score": int(score)}
        for (pid, score), title in zip(ranked, titles)
        if title is not None
    ]

@app.get("/users/{user_id}/recent")
def recent_prompts(user_id: str = Path(..., min_length=1, max_length=50)):
    ids = r.lrange(f"recent:{user_id}", 0, RECENT_MAX - 1)
    if not ids:
        return []

    pipe = r.pipeline()
    for pid in ids:
        pipe.hgetall(f"prompt:{pid}")
    rows = pipe.execute()
    return [to_prompt(int(pid), data) for pid, data in zip(ids, rows) if data]

@app.delete("/prompts/{prompt_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_prompt(prompt_id: int):
    key = f"prompt:{prompt_id}"
    tags_csv = r.hget(key, "tags")
    if tags_csv is None:
        raise HTTPException(status_code=404, detail="Prompt not found")

    pipe = r.pipeline()
    pipe.delete(key)
    for tag in tags_csv.split(","):
        if tag:
            pipe.srem(f"tag:{tag}", prompt_id)
    for day_key in last_7_trending_keys():
        pipe.zrem(day_key, prompt_id)
    pipe.execute()
    return Response(status_code=status.HTTP_204_NO_CONTENT)

