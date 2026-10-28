import subprocess
import sys
import time
from collections import Counter

import redis
import requests

BASE = "http://127.0.0.1:8000"
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def start_worker(name):
    return subprocess.Popen([sys.executable, "worker.py", name])

def submit(kind, payload="x"):
    resp = requests.post(BASE + "/jobs", json={"kind": kind, "payload": payload})
    return resp.json()["job_id"]

def job(job_id):
    return requests.get(f"{BASE}/jobs/{job_id}").json()

def queue():
    return requests.get(BASE + "/queue").json()

def wait_for(job_id, wanted, timeout=40):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if job(job_id)["status"] in wanted:
            break
        time.sleep(0.5)
    return job(job_id)

def step(title):
    print(f"\n--- {title} ---")

def reset():
    for key in ("jobs", "jobs:dead"):
        r.delete(key)
    for key in r.scan_iter("job:*"):
        r.delete(key)
    r.xgroup_create("jobs", "workers", id="0", mkstream=True)

reset()


step("1. two workers, ten jobs")
w1, w2 = start_worker("w1"), start_worker("w2")
ids = [submit("email", f"user{i}") for i in range(10)]
jobs = [wait_for(i, {"done"}, timeout=30) for i in ids]
print(Counter(j["status"] for j in jobs))
print({j["attempts"] for j in jobs})
print(queue())

step("2. a failing job is retried, then dead-lettered")
bad_id = submit("fail", "doomed")
bad = wait_for(bad_id, {"dead"}, timeout=40)
print(bad["status"], bad["attempts"], bad["error"])
print("dead list contains it:", bad_id in [d["job_id"] for d in requests.get(BASE + "/dead-jobs").json()])
print(queue())


step("3. worker crash mid-job")
w1.kill(); w2.kill()
w1.wait(); w2.wait()
w1 = start_worker("w1")
crash_id = submit("report", "quarterly")
wait_for(crash_id, {"running"}, timeout=10)
w1.kill(); w1.wait()
print("status after kill:", job(crash_id)["status"])
print("pending after kill:", queue()["pending"])
w2 = start_worker("w2")
final = wait_for(crash_id, {"done"}, timeout=30)
print(final["status"], final["attempts"])
print(queue())
w2.kill(); w2.wait()