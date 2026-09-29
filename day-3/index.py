import redis
from redis.commands.search.field import TextField, VectorField
from redis.commands.search.index_definition import IndexDefinition, IndexType

r = redis.Redis(host="localhost", port=6379, decode_responses=False)

index_name = "idx:documents"

schema = (
    TextField("title", weight=5.0),
    TextField("category"),
    VectorField(
        "content_vector",
        "FLAT", # Or HNSW 
        {
            "TYPE" : "FLOAT32",
            "DIM" : 3, 
            "DISTANCE_METRIC" : "COSINE"
        }
    )
)

try:
    r.ft(index_name=index_name).create_index(
        schema,
        definition=IndexDefinition(prefix=["doc:"], index_type=IndexType.HASH)
    )
    print("Index created!")
except redis.exceptions.ResponseError:
    print("Index already exists.")