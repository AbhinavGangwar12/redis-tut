import redis 
import json
import uuid
import time
import hashlib, re
import numpy as np
from redis.commands.search.field import TextField, TagField, VectorField 
from redis.commands.search.index_definition import IndexDefinition , IndexType
from redis.commands.search.query import Query
from langchain_redis import RedisConfig, RedisVectorStore
from redisvl.query.filter import Tag
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_core.documents import Document
from embed import embed 
from sample_docs import SAMPLE_DOCS

rb = redis.Redis(host="localhost", port=6379)
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
lc_embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")

def create_index():
    try:
        rb.ft("idx:docs").dropindex(delete_documents=True)
    except redis.ResponseError:
        pass 

    schema = (
        TextField("title"),
        TagField("category"),
        VectorField("embedding", "FLAT", {"TYPE" : "FLOAT32", "DIM" : 384, "DISTANCE_METRIC" : "COSINE"})
    )
    rb.ft("idx:docs").create_index(schema, definition=IndexDefinition(prefix=["doc:"], index_type=IndexType.HASH))

DOCS = [
    ("How to reset a forgotten password", "account"),
    ("Update your billing address", "billing"),
    ("Refund policy for annual plans", "billing"),
    ("Two-factor authentication setup guide", "account"),
    ("Best pizza toppings in Italy", "food"),
]

def add_docs():
    pipe = rb.pipeline()
    for i, (title, category) in enumerate(DOCS):
        pipe.hset(f"doc:{i}", mapping={
            "title" : title,
            "category" : category,
            "embedding" : embed(title).tobytes(),
        })
    pipe.execute()

def knn(text, k: int = 3):
    q = (Query(f"*=>[KNN {k} @embedding $vec as dist]").sort_by("dist").return_fields("title", "category", "dist").dialect(2))
    res = rb.ft("idx:docs").search(q, query_params={"vec" : embed(text).tobytes()})
    return [(d.title, d.category, round(float(d.dist), 2)) for d in res.docs]

def bruteforce_top(text: str, k: int = 3) -> list[str]:
    query = embed(text)
    embeddings = np.empty(shape=(len(DOCS), query.shape[0]), dtype=np.float32)
    for i, (title, _) in enumerate(DOCS):
        embeddings[i] = embed(title)

    res = np.dot(query, embeddings.T)
    top_indices = np.argsort(res)[-k:][::-1]
    return [DOCS[i][0] for i in top_indices]

def add_message(session_id: str, role: str, content: str, max_messages: int = 20, ttl: int = 3600) -> None:
    key = f"chat:{session_id}"
    pipe = r.pipeline()
    pipe.rpush(key, json.dumps({"role" : role, "content" : content}))
    pipe.ltrim(key, -max_messages, -1)
    pipe.expire(key, ttl)
    pipe.execute()

def get_history(session_id: str) -> list[dict]:
    return [json.loads(m) for m in r.lrange(f"chat:{session_id}", 0, -1)]

def build_query(k, category=None, text=None):
    parts = []
    if category:
        parts.append("@category:{" + category + "}")
    if text:
        parts.append(f"@title:({text})")
    base = " ".join(parts) if parts else "*"
    return base+f"=>[KNN {k} @embedding $vec AS dist]"

def knn_filtered(text: str, k: int = 3, category: None | str = None, max_distance: float | None = None) -> list[dict]:
    valid_categories = ("account", "billing", "food")
    if category is not None and category not in valid_categories:
        raise ValueError(f"Category must be one of {valid_categories}")
    q = (Query(build_query(k, category=category))
        .sort_by("dist")
        .return_fields("title", "category", "dist").dialect(2)
        )
    res = rb.ft("idx:docs").search(q, query_params={"vec" : embed(text).tobytes()})
    out = [{"title" : d.title, "category" : d.category, "distance" : float(d.dist)} for d in res.docs]
    return [d for d in out if max_distance is None or d["distance"] <= max_distance]

    
def hybrid_search(text: str, must_contain: str, k: int = 3) -> list[dict]:
    if len(must_contain.split()) > 1:
        raise ValueError("More than one word!")
    query = (Query(build_query(k, text=must_contain))
            .sort_by("dist")
            .return_field("title").dialect(2) 
            )
    res = rb.ft("idx:docs").search(query=query, query_params={"vec" : embed(text).tobytes()})
    return [d.title for d in res.docs]

def normalize(question: str) -> str:
    question = re.sub(r"\s+", " ", question.strip().lower())
    return question.rstrip("?. ")

def exact_key(question: str, scope: str = "all"):
    digest = hashlib.sha256(f"{normalize(question=question)}|{scope}".encode("utf-8")).hexdigest()
    return f"cache:exact:{digest}"

def exact_cache_set(question: str, answer: str, scope: str = "all", ttl: int = 300) -> None:
    key = exact_key(question=question, scope=scope)
    pipe = r.pipeline()
    pipe.set(key, answer, ex=ttl)
    pipe.execute()

def exact_cache_get(question: str, scope: str = "all") -> str | None:
    key = exact_key(question=question, scope=scope)
    answer = r.get(key)
    if answer is not None:
        return answer
    return None

def create_cache_index() -> None:
    try:
        rb.ft("idx:cache").dropindex(delete_documents=True)
    except redis.ResponseError:
        pass 

    schema = (
        TextField("question"),
        TextField("answer"),
        TagField("category"),
        VectorField("embedding", "FLAT", {
            "TYPE" : "FLOAT32",
            "DIM" : 384,
            "DISTANCE_METRIC" : "COSINE"
        })
    )
    rb.ft("idx:cache").create_index(schema, definition=IndexDefinition(prefix=["cache:sem:"], index_type=IndexType.HASH))

def semantic_cache_store(question: str, answer: str, category: str = "all", ttl: int = 300) -> None:
    encoded = embed(question).tobytes()
    key = f"cache:sem:{uuid.uuid4().hex}"
    pipe = rb.pipeline()
    pipe.hset(key, mapping={"question" : question, "answer" : answer, "category" : category, "embedding" : encoded})
    pipe.expire(key, ttl)
    pipe.execute()

def semantic_cache_lookup(question: str, category: str = "all", threshold: float = 0.2) -> tuple[str, float] | None:
    query = (Query(build_query(k=1, category=category))
            .sort_by("dist")
            .return_fields("question", "answer", "category", "dist")
            .dialect(2)
            )
    res = rb.ft("idx:cache").search(query=query, query_params={"vec" : embed(question).tobytes()})
    if not res.docs:
        return None
    if float(res.docs[0].dist) <= threshold:
        ans = res.docs[0].answer
        if isinstance(ans, bytes):
            ans = ans.decode("utf-8")
        return (res.docs[0].answer, float(res.docs[0].dist))
    return None 

def get_lc_store() -> RedisVectorStore:
    """Helper to initialize the LangChain Redis Vector Store consistently."""
    config = RedisConfig(
        index_name="idx:lc", 
        redis_url="redis://localhost:6379", 
        key_prefix="lc",
        metadata_schema=[{"name": "category", "type": "tag"}]
    )
    return RedisVectorStore(lc_embeddings, config=config)

def build_lc_store() -> None:
    try:
        r.ft("idx:lc").dropindex(delete_documents=True)
    except redis.ResponseError:
        pass 

    store = get_lc_store()
    documents = [
        Document(page_content=doc["text"], metadata={"category": doc["category"], "id" : doc["id"]})
        for doc in SAMPLE_DOCS 
    ]

    store.add_documents(documents=documents)

def lc_search(question: str, category: str | None = None, k: int = 3) -> list[tuple[str, float]]:
    store = get_lc_store()
    if category:
        results = store.similarity_search_with_score(question, k=k, filter=Tag("category") == category)
    else:
        results = store.similarity_search_with_score(question, k=k)
    
    return [(doc.page_content, float(score), doc.metadata.get("id", "")) for doc, score in results]


if __name__ == "__main__":
    # create_index()
    # add_docs()
    # print(knn("I can't log in to my account"))
    # print(knn("how do I get my money back", k=2))
    # print(knn("what should I put on a pizza", k=1))

    # print(np.argsort([3,1,4,5,2])[-3:][::-1])

    # q = "I can't log in to my account"
    # print([t for t, _, _ in knn(q, 3)] == bruteforce_top(q, 3))
    # q = "how do I get my money back"
    # print([t for t, _, _ in knn(q, 2)] == bruteforce_top(q, 2))

    # r.delete("chat:s1")
    # for i in range(12):
    #     add_message("s1", "user" if i % 2 == 0 else "assistant", f"m{i}", max_messages=10)
    # h = get_history("s1")
    # print(len(h), h[0]["content"], h[-1]["content"])
    # print(0 < r.ttl("chat:s1") <= 3600)
    # print(get_history("nobody"))

    # res = knn_filtered("update my details", k=3, category="billing")
    # print(len(res) > 0 and all(d["category"] == "billing" for d in res))
    # print(knn_filtered("update my details", k=3, max_distance=0.0001))
    # print(knn_filtered("what should I put on a pizza", k=3, max_distance=0.6)[0]["title"])
    # try:
    #     knn_filtered("x", category="billing}|*")
    # except ValueError:
    #     print("rejected")

    # print(hybrid_search("how do I get my money back", "refund"))
    # print(hybrid_search("how do I get my money back", "pizza"))
    # print(hybrid_search("how do I get my money back", "unicorn"))

    # for k in r.scan_iter("cache:exact:*"):
    #     r.delete(k)
    # exact_cache_set("How do I reset my password?", "ANS1")
    # print(exact_cache_get("  how do I   reset my password "))
    # print(exact_cache_get("How can I reset my password"))
    # print(exact_cache_get("How do I reset my password?", scope="billing"))
    # key = exact_key("How do I reset my password?")
    # print(0 < r.ttl(key) <= 300)

    # create_cache_index()
    # semantic_cache_store("How do I reset my password?", "ANS1")
    # hit = semantic_cache_lookup("how can I reset my password")
    # print(hit[0], hit[1] <= 0.2)
    # print(semantic_cache_lookup("What is the refund policy?"))
    # semantic_cache_store("How do I change my card?", "ANS2", category="billing")
    # print(semantic_cache_lookup("How do I change my card?", category="account"))
    # print(semantic_cache_lookup("How do I change my card?", category="billing")[0])
    # semantic_cache_store("Short lived question", "TMP", ttl=1)
    # time.sleep(1.5)
    # print(semantic_cache_lookup("Short lived question"))
    # print([0 < rb.ttl(k) <= 300 for k in rb.scan_iter("cache:sem:*")].count(False))
    # build_lc_store()
    # print(lc_search("how do I get my money back", k=1)[0][0])
    # print(all(t in ("How to reset a forgotten password", "Two-factor authentication setup guide")
    #           for t, _ in lc_search("I can't log in", category="account", k=3)))
    # print(round(lc_search("Best pizza toppings in Italy", k=1)[0][1], 1))
    pass