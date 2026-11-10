import subprocess
import sys
import time
from collections import Counter

import redis
import requests

BASE = "http://127.0.0.1:8000"
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def start_worker(name):
    return subprocess.Popen([sys.executable, "agent_worker.py", name])

def kill(proc):
    subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    proc.wait()

def submit(topic, client):
    return requests.post(BASE + "/tasks", json={"topic": topic},
                         headers={"X-Client-Id": client}, timeout=15)

def task(job_id):
    return requests.get(f"{BASE}/tasks/{job_id}", timeout=15).json()

def wait_for(job_id, field, wanted, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if task(job_id)[field] in wanted:
            break
        time.sleep(0.3)
    return task(job_id)

def reset():
    for pattern in ("agent:*", "stats:agent:*", "cache:tool:*", "done:notify:*", "rl:sw:agent:*"):
        for key in r.scan_iter(pattern):
            r.delete(key)

def step(title):
    print(f"\n--- {title} ---")

reset()

step("1. rate limit: 8 submissions from one client")
print(Counter(submit("redis", "c1").status_code for _ in range(8)))
resp = submit("redis", "c1")
print(resp.status_code, 1 <= int(resp.headers["Retry-After"]) <= 10)
reset()

step("2. happy path: one worker, cold cache")
w1 = start_worker("a1")
t0 = time.time()
job = submit("redis", "c2").json()["job_id"]
done = wait_for(job, "status", {"done"})
first = time.time() - t0
print(done["status"], done["steps"], first >= 6.0)
print(done["result"])

step("3. same topic again: tool cache")
t0 = time.time()
job = submit("redis", "c3").json()["job_id"]
done = wait_for(job, "status", {"done"})
print(done["status"], (time.time() - t0) < first - 1.0, r.get("stats:agent:tool_cache_hits"))

step("4. crash mid-job, resume on another worker")
kill(w1)
for key in list(r.scan_iter("stats:agent:node:*")) + ["stats:agent:notifications"]:
    r.delete(key)
w1 = start_worker("a1")
job = submit("kafka", "c4").json()["job_id"]
wait_for(job, "current", {"draft"})
kill(w1)
after = task(job)
print("after kill:", after["status"], after["current"])
w2 = start_worker("a2")
final = wait_for(job, "status", {"done"}, timeout=60)
print(final["status"], final["attempts"], final["steps"])
print({n: r.get(f"stats:agent:node:{n}") for n in ("research", "draft", "review", "finish")})
print(r.get("stats:agent:notifications"))
kill(w2)

step("5. validation and health")
print(submit("hi", "c5").status_code)
print(requests.post(BASE + "/tasks", json={"topic": "valid topic"}).status_code)
print(sorted(requests.get(BASE + "/health").json()))
