import redis 
import sqlite3
from db import init_db
from cache_aside import get_product, cache_stats, create_session, get_session, logout
from fastapi import FastAPI, status, HTTPException, Header, Response
from fastapi.responses import JSONResponse
from typing import Annotated
from pydantic import BaseModel, Field

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
app = FastAPI(title="cached-shop")

try:
    init_db()
except sqlite3.Error as e:
    raise RuntimeError("Failed to initialize the DB") from e

class PriceStock(BaseModel):
    price: float = Field(gt=0, le=1_000_000)
    stock: int = Field(ge=0)

class LoginIn(BaseModel):
    user_id: int = Field(ge=1)

@app.get("/products/{id}")
def fget_product(id: int):
    product, cache_status = get_product(id)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return JSONResponse(content=product, headers={"X-Cache": cache_status})

@app.put("/products/{id}")
def fput_product(id: int, body: PriceStock):
    con = sqlite3.connect("shop.db")
    cur = con.execute("UPDATE products SET price = ?, stock = ? WHERE id = ?",
                      (body.price, body.stock, id))
    updated = cur.rowcount > 0
    con.commit()
    con.close()
    if not updated:
        raise HTTPException(status_code=404, detail="Product not found")
    r.delete(f"cache:product:{id}")
    return {"id": id, "price": body.price, "stock": body.stock}

@app.get("/stats")
def fstats():
    payload = cache_stats()
    payload["db_reads"] = int(r.get("stats:db:reads") or 0)
    return payload 

@app.post("/login")
def flogin(body: LoginIn):
    return {"token": create_session(body.user_id), "expires_in": 1800}

@app.get("/me")
def fme(x_session_token: str | None = Header(default=None)):
    session = get_session(x_session_token) if x_session_token else None
    if session is None:
        raise HTTPException(status_code=401, detail="Invalid or expired session")
    return session

@app.post("/logout")
def flogout(x_session_token: str | None = Header(default=None)):
    if not x_session_token or not logout(x_session_token):
        raise HTTPException(status_code=401, detail="Invalid or expired session")
    return Response(status_code=204)