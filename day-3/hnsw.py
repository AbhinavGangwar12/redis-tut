from langchain_redis import RedisVectorStore
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_core.documents import Document 

embeddings = HuggingFaceEmbeddings(model="sentence-transformers/all-MiniLM-L6-v2")
redis_url = "redis://localhost:6379"
index_name = "hnsw_index"

# 1. Define the custom schema explicitly requesting HNSW
custom_schema = {
    "vector": [
        {
            "name": "vector", # LangChain's default vector field name
            "algorithm": "HNSW", 
            "dims": 384, # MUST match your embedding model (all-MiniLM-L6-v2 is 384)
            "distance_metric": "COSINE",
            
            # Optional HNSW Tuning Parameters (these are good defaults):
            "m": 16,               # Max number of outgoing edges per node in the graph
            "ef_construction": 200, # Higher = longer index time, better accuracy
            "ef_runtime": 50        # Higher = slightly slower search, better accuracy
        }
    ]
}

texts = ["Cats are great pets.", "Dogs are loyal companions.", "Invest in S&P 500."]
metadatas = [{"category": "pets"}, {"category": "pets"}, {"category": "finance"}]

docs = []
for i in range(len(texts)):
    docs.append(Document(page_content=texts[i], metadata=metadatas[i]))

# 2. Pass the custom_schema into your vector store
vectorstore = RedisVectorStore.from_documents(
    documents=docs, 
    embedding=embeddings, 
    redis_url=redis_url, 
    index_name=index_name,
    index_schema=custom_schema # <-- Inject it here
)

print("HNSW Index Created!")

# Search works exactly the same way
retriever = vectorstore.as_retriever(search_kwargs={"k": 2})
results = retriever.invoke("Tell me about friendly animals")
print(results)