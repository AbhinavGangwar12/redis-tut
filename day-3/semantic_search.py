import numpy as np
from index import r, index_name
from redis.commands.search.query import Query

query_vector = np.array([0.1, 0.9, 0.1], dtype=np.float32).tobytes()

# FIXED: Removed the curly braces around 'pets' to match your TextField schema
# (Also simplified the r.ft() argument format)
q = Query("(@category:pets)=>[KNN 2 @content_vector $vec AS vector_score]") \
    .sort_by("vector_score") \
    .return_fields("title", "vector_score") \
    .dialect(2)

# FIXED: Removed the unnecessary 'index_name=' keyword argument
results = r.ft(index_name).search(q, query_params={"vec" : query_vector})

for doc in results.docs:
    print(f"Title: {doc.title} | Distance: {doc.vector_score}")