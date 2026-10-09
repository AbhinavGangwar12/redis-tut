import redis 
import secrets
from fastapi import FastAPI, HTTPException, Request 
from pydantic import BaseModel, Field, HttpUrl
from fastapi.responses import RedirectResponse

app = FastAPI("url-shortner")
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
MAX_TTL = 30 * 24 * 3600

class ShortenRequest(BaseModel):
    url: HttpUrl
    ttl_seconds : int = Field(default=3600, gt=0, le=MAX_TTL)

def create_code(url: str, ttl: int) -> str:
    for _ in range(5):
        code = secrets.token_urlsafe(4)
        key = f"url:{code}"
        pipe = r.pipeline()
        pipe.hsetnx(key, "url", url)
        pipe.hsetnx(key, "clicks", 0)
        pipe.expire(key, ttl, nx=True)
        created, _, _ = pipe.execute()
        if created:
            return code
    raise RuntimeError("could not generate a unique code after 5 attempts")

@app.post("/shorten")
def shorten(req: ShortenRequest, request: Request):
    try:
        code = create_code(req.url, req.ttl_seconds)
    except RuntimeError:
        raise HTTPException(status_code=503, detail="try again")

    return {
        "code": code,
        "short_url": f"{request.base_url}{code}",
        "expires_in": req.ttl_seconds,
    }

@app.get("/{code}")
def redirect_to_url(code: str):
    key = f"url:{code}"
    url = r.hget(key, "url")
    if url is None:
        raise HTTPException(status_code=404, detail="URL not found or expired")
    r.hincrby(key, "clicks", 1)
    return RedirectResponse(url=url, status_code=307)

@app.get("/stats/{code}")
def stats(code: str):
    key = f"url:{code}"
    pipe = r.pipeline()
    pipe.hgetall(key)
    pipe.ttl(key)
    data, ttl = pipe.execute()
    if "url" not in data:
        raise HTTPException(status_code=404, detail="URL not found or expired")
    return {"code": code, "url": data["url"], "clicks": int(data["clicks"]),
            "ttl_remaining": ttl}

