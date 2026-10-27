import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
r.delete("lab:jobs")
try:
    r.xgroup_create("lab:jobs", "workers", id="0", mkstream=True)
except redis.ResponseError:
    pass

for n in range(3):
    r.xadd("lab:jobs", {"n": n})

batch = r.xreadgroup("workers", "w1", {"lab:jobs": ">"}, count=2)
entries = batch[0][1]
first_id, first_fields = entries[0]
r.xack("lab:jobs", "workers", first_id)
print("w1 got", [f["n"] for _, f in entries], "and acked n =", first_fields["n"])
print("pending:", r.xpending("lab:jobs", "workers")["pending"])
print("pending entry:", r.xpending_range("lab:jobs", "workers", min="-", max="+", count=10)[0]["consumer"],
      "delivered", r.xpending_range("lab:jobs", "workers", min="-", max="+", count=10)[0]["times_delivered"], "time(s)")

claimed = r.xautoclaim("lab:jobs", "workers", "w2", min_idle_time=0, start_id="0-0", count=10)
mine = claimed[1]
print("w2 claimed", [f["n"] for _, f in mine])
info = r.xpending_range("lab:jobs", "workers", min="-", max="+", count=10)[0]
print("after claim:", info["consumer"], "delivered", info["times_delivered"], "time(s)")
for entry_id, _ in mine:
    r.xack("lab:jobs", "workers", entry_id)
print("pending after ack:", r.xpending("lab:jobs", "workers")["pending"])
rest = r.xreadgroup("workers", "w2", {"lab:jobs": ">"}, count=10)
print("w2 new:", [f["n"] for _, f in rest[0][1]])

