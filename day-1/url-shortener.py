import secrets
import redis
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field, HttpUrl

app = FastAPI(title="url-shortener")
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
MAX_TTL = 30 * 24 * 3600

class ShortenRequest(BaseModel):
    url: HttpUrl
    ttl_seconds: int = Field(default=3600, gt=0, le=MAX_TTL)

def create_code(url: str, ttl: int) -> str:
    for _ in range(5):
        code = secrets.token_urlsafe(4)
        if r.set(f"url:{code}", url, ex=ttl, nx=True):
            r.set(f"clicks:{code}", 0, ex=ttl)
            return code
    raise RuntimeError("could not generate a unique code after 5 attempts")

@app.post("/shorten")
def shorten(req: ShortenRequest, request: Request):
    try:
        code = create_code(str(req.url), req.ttl_seconds)
    except RuntimeError:
        raise HTTPException(status_code=503, detail="try again")
    return {
        "code": code,
        "short_url": f"{request.base_url}{code}",
        "expires_in": req.ttl_seconds,
    }

@app.get("/{code}")
def redirect_to_url(code: str):
    url = r.get(f"url:{code}")
    if url is None:
        raise HTTPException(status_code=404, detail="URL not found or expired")
    r.incr(f"clicks:{code}")
    return RedirectResponse(url=url, status_code=307)

@app.get("/stats/{code}")
def stats(code: str):
    pipe = r.pipeline()
    pipe.ttl(f"url:{code}")
    pipe.get(f"url:{code}")
    pipe.get(f"clicks:{code}")
    ttl, url, clicks = pipe.execute()
    if url is None:
        raise HTTPException(status_code=404, detail="URL not found or expired")
    return {"code": code, "url": url, "clicks": int(clicks or 0), "ttl_remaining": ttl}

