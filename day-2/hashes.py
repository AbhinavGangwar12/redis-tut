import redis 

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

r.hset("prompt:2", mapping={
    "title" : "Explaination", 
    "body" : "Explain this context",
    "author" : "john", 
    "uses" : 0
})
print(r.hmget("prompt:2", ["body", "author"]))
print(r.hget("prompt:2", "title"))
print(r.hincrby("prompt:2", "uses", 2))
print(r.hdel("prompt:2", "author"))