import numpy as np
from index import r 
docs = [
    {"id": "doc:1", "title": "Cat Care", "category": "pets", "vector": [0.1, 0.8, 0.1]},
    {"id": "doc:2", "title": "Dog Training", "category": "pets", "vector": [0.2, 0.9, 0.1]},
    {"id": "doc:3", "title": "Index Funds", "category": "finance", "vector": [0.9, 0.1, 0.2]}
]

pipe = r.pipeline()
for doc in docs:
    vector_bytes = np.array(doc["vector"], dtype=np.float32).tobytes()
    pipe.hset(doc["id"], mapping={
        "title" : doc["title"],
        "category" : doc["category"],
        "content_vector" : vector_bytes
    })
pipe.execute()
print("Saved.")