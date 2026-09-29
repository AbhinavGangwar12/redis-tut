from langchain_redis import RedisVectorStore
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_core.documents import Document 
import redis 

embeddings = HuggingFaceEmbeddings(model="sentence-transformers/all-MiniLM-L6-v2")
redis_url = "redis://localhost:6379"
index_name = "langchain_index"
r = redis.Redis.from_url(redis_url)

texts = ["Cats are great pets.", "Dogs are loyal companions.", "Invest in S&P 500."]
metadatas = [{"category": "pets"}, {"category": "pets"}, {"category": "finance"}]

docs = []
for i in range(len(texts)):
    docs.append(Document(page_content=texts[i], metadata=metadatas[i]))

vectorstore = RedisVectorStore.from_documents(documents=docs, embedding=embeddings, redis_url=redis_url, index_name=index_name)
retreiver = vectorstore.as_retriever(search_kwargs={"k":2, "category" : "pets"})
results = retreiver.invoke("Tell me about friendly animals")
print(results)


# r.bgsave()
# print("Vectorstore successfully saved to Redis disk!")

# we can use vectorstore.similarity_search() as well 
