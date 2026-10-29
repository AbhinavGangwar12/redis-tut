import redis 
from redis.commands.search.field import TextField, TagField, VectorField 
from redis.commands.search.index_definition import IndexDefinition , IndexType
from redis.commands.search.query import Query
from embed import embed 

rb = redis.Redis(host="localhost", port=6379)

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

if __name__ == "__main__":
    create_index()
    add_docs()
    print(knn("I can't log in to my account"))
    print(knn("how do I get my money back", k=2))
    print(knn("what should I put on a pizza", k=1))