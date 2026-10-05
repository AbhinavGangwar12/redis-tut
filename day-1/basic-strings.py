import redis 

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

r.set("user:1:name", "JohnDoe")
print(r.get("user:1:name"))

r.mset({"a": 1, "b" : 2})
print(r.mget(["user:1:name", "a", "b"]))

print(r.get("a"))

r.incr("a")
print(r.get("a"))

r.incrby("a", 10)
print(r.get("a"))

r.decr("a")
print(r.get("a"))

r.decrby("a", 5)
print(r.get("a"))