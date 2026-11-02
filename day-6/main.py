import redis 
import uuid
import time
import json
from redis.commands.search.field import VectorField, TagField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query 

from fastapi import FastAPI, status
from typing import Literal, Optional
from pydantic import BaseModel, Field
from fastapi import Query as FQuery
from fastapi.responses import JSONResponse, Response

from embed import embed
from sample_docs import SAMPLE_DOCS
from vector_lab import exact_cache_get, semantic_cache_lookup, semantic_cache_store, exact_cache_set, create_cache_index

rb = redis.Redis(host="localhost", port=6379, decode_responses=False) 
r = redis.Redis(host="localhost", port=6379, decode_responses=True) 
app = FastAPI(title="rag-service")

class PostRequest(BaseModel):
    question: str = Field(min_length=3, max_length=300)
    category: Optional[Literal["account", "billing", "shipping"]] = None
    session_id: Optional[str] = Field(default=None,min_length=1, max_length=50)

RAG_MAX_DISTANCE = 0.7

@app.post("/ingest")
def ingest():
    try:
        rb.ft("idx:rag").dropindex(delete_documents=True)
    except redis.ResponseError:
        pass 

    schema = (
        TextField("doc_id"),
        TagField("category"),
        TextField("text"),
        VectorField("embedding", "FLAT", {"TYPE" : "FLOAT32", "DIM" : 384, "DISTANCE_METRIC" : "COSINE"})
    )
    rb.ft("idx:rag").create_index(schema, definition=IndexDefinition(prefix=["rag:chunk:"], index_type=IndexType.HASH))
    pipe = rb.pipeline()
    count = 0
    for doc in SAMPLE_DOCS:
        doc_id = doc.get("id", uuid.uuid4().hex)
        category = doc.get("category", "all")
        text = doc.get("text", "")
        vector = embed(text).tobytes()

        key = f"rag:chunk:{doc_id}"
        pipe.hset(key, mapping={
            "doc_id": doc_id,
            "category" : category,
            "text" : text,
            "embedding" : vector 
        })
        count += 1
    pipe.execute()
    create_cache_index()
    return JSONResponse(
        content={"ingested" : count},
        status_code=status.HTTP_200_OK
    )

@app.delete("/cache")
def delete():
    keys_to_delete = []
    for key in r.scan_iter(match="cache:*"):
        keys_to_delete.append(key)
    stats_keys = [
        "stats:rag:exact_hits", 
        "stats:rag:semantic_hits", 
        "stats:rag:misses", 
        "stats:llm:calls"
    ]
    keys_to_delete.extend(stats_keys)
    if keys_to_delete:
        rb.unlink(*keys_to_delete)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


def generate(question, passages):
    r.incr("stats:llm:calls")
    time.sleep(1)
    return "Based on our docs: " + passages[0]["text"]

def log_chat_history(session_id: str | None, question: str, answer: str):
    """Appends interaction, keeps last 10 messages, and refreshes the 1-hour sliding TTL."""
    if session_id:
        key = f"chat:{session_id}"
        pipe = r.pipeline()
        pipe.rpush(key, json.dumps({"role": "user", "content": question}))
        pipe.rpush(key, json.dumps({"role": "assistant", "content": answer}))
        pipe.ltrim(key, -10, -1)
        pipe.expire(key, 3600)
        pipe.execute()
    
def rag_search(question: str, category: str | None, k: int = 3) -> list[dict]:
    """Raw Redis vector search matching the exact spec for idx:rag."""
    base_query = f"@category:{{{category}}}" if category else "*"
    q_str = f"{base_query}=>[KNN {k} @embedding $vec AS dist]"
    query = (Query(q_str)
            .sort_by("dist")
            .return_fields("doc_id", "text", "dist")
            .dialect(2)
            )
    res = rb.ft("idx:rag").search(query, query_params={"vec" : embed(question).tobytes()})
    results = []
    for doc in res.docs:
        dist = float(doc.dist)
        if dist <= RAG_MAX_DISTANCE:
            doc_id = doc.doc_id.decode("utf-8") if isinstance(doc.doc_id, bytes) else doc.doc_id
            text = doc.text.decode("utf-8") if isinstance(doc.text, bytes) else doc.text
            results.append({"id": doc_id, "text": text, "distance": dist})
    return results
@app.post("/ask", status_code=status.HTTP_200_OK)
def post(request: PostRequest):
    q = request.question
    scope = request.category or "all"
    sess = request.session_id
    exact = exact_cache_get(question=q, scope=scope)
    
    if exact:
        rb.incr("stats:rag:exact_hits")
        log_chat_history(sess, q, exact)
        return {
            "answer": exact,
            "source": "exact_cache",
            "sources": [],
            "distance": None 
        }
    semantic = semantic_cache_lookup(question=q, category=scope, threshold=0.2)
    if semantic:
        ans, dist = semantic
        rb.incr("stats:rag:semantic_hits")
        log_chat_history(sess, q, ans)
        return {
            "answer": ans,
            "source": "semantic_cache",
            "sources": [],
            "distance": round(dist, 2)
        }
    
    rb.incr("stats:rag:misses")
    docs = rag_search(question=q, category=request.category, k=3)
    if not docs:
        ans = "I don't know based on the documents."
        log_chat_history(sess, q, ans)
        
        return {
            "answer": ans,
            "source": "rag",
            "sources": [],
            "distance": None
        }
    # passages = [d["text"] for d in docs]
    ans = generate(question=q, passages=docs)
    exact_cache_set(question=q, answer=ans, scope=scope)
    semantic_cache_store(question=q, answer=ans, category=scope)
    log_chat_history(sess, q, ans)
    
    return {
        "answer": ans,
        "source": "rag",
        "sources": [d["id"] for d in docs],
        "distance": round(docs[0]["distance"], 2)
    }

@app.get("/history/{session_id}")
def history(session_id: str):
    msgs = r.lrange(f"chat:{session_id}", 0, -1)
    return [json.loads(m) for m in msgs]
    

@app.get("/stats")
def stats():
    response = r.mget(["stats:rag:exact_hits", "stats:rag:semantic_hits", "stats:rag:misses", "stats:llm:calls"])
    return {
        "exact_hits" : int(response[0]) if response[0] else 0,
        "semantic_hits" : int(response[1]) if response[1] else 0,
        "misses" : int(response[2]) if response[2] else 0,
        "llm_calls" : int(response[3]) if response[3] else 0,
    }

@app.get("/")
def welcome():
    return {"message" : "welcome to the mini-project"}