import sys 
import time 
import redis 
from redis.exceptions import ResponseError 

STREAM = "jobs"
GROUP = "workers"
DEAD = "jobs:dead"
CLAIM_IDLE_MS = 5000
MAX_ATTEMPTS = 3
BLOCK_MS = 1000

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def run_handler(kind, payload):
    if kind == "email" : 
        time.sleep(0.5)
        return f"sent:{payload}"
    if kind == 'report':
        time.sleep(2)
        return f"report:{payload}"
    if kind == 'fail':
        raise RuntimeError('boom')
    raise ValueError(f"Unknown kind: {kind}")

def ensure_group():
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise 

def delivery_count(entry_id):
    """delivery_count is the trick from problem 7: ask for the one pending entry and return how many times it was delivered. That number is attempts."""
    rows = r.xpending_range(STREAM, GROUP, min=entry_id, max=entry_id, count=1)
    return rows[0]["times_delivered"] if rows else 1

def entries_of(response):
    """entries_of unwraps xreadgroup nested answer ([[stream, [entries...]]]) into a flat list. It returns [] on a timeout, and also when a stream has no entries."""
    return response[0][1] if response else []

def fail(name, entry_id, fields, attempts, error):
    job_id = fields['job_id']
    key = f"job:{job_id}"
    if attempts < MAX_ATTEMPTS:
        r.hset(key, mapping={"status" : "queued", "error" : error})
        print(f"[{name}] retry {job_id} (attempt {attempts})", flush=True)
        return 

    pipe = r.pipeline()
    pipe.xadd(DEAD, {"job_id": job_id,  "kind" : fields['kind'], "payload" : fields['payload'], "error" : error})
    pipe.hset(key, mapping={"status" : "dead", "error" : error})
    pipe.xack(STREAM, GROUP, entry_id)
    pipe.xdel(STREAM, entry_id)
    pipe.execute()
    print(f"[{name}] dead {job_id}", flush=True)

def process(name, entry_id, fields):
    job_id = fields["job_id"]
    key = f"job:{job_id}"
    attempts = delivery_count(entry_id=entry_id)
    r.hset(key, mapping={"status" : "running", "attempts" : attempts})
    try:
        result = run_handler(fields['kind'], fields['payload'])
    except Exception as e:
        fail(name, entry_id, fields, attempts, str(e))
        return
    pipe = r.pipeline()
    pipe.hset(key, mapping={"status": "done", "result": result})
    pipe.xack(STREAM, GROUP, entry_id)
    pipe.xdel(STREAM, entry_id)
    pipe.execute()
    print(f"[{name}] done {job_id}", flush=True)

def main(name):
    ensure_group()
    print(f"[{name}] started", flush=True)
    for entry_id, fields in entries_of(r.xreadgroup(GROUP, name, {STREAM: "0"},count=10)):
        print(f"[{name}] resuming {fields['job_id']}", flush=True)
        process(name, entry_id, fields)

    while True:
        claimed = r.xautoclaim(STREAM, GROUP, name, min_idle_time=CLAIM_IDLE_MS, start_id="0-0", count=10)[1]
        for entry_id, fields in claimed:
            print(f"[{name}] claimed {fields['job_id']}", flush=True)
            process(name, entry_id, fields)
        batch = r.xreadgroup(GROUP, name, {STREAM: ">"}, count=10, block=BLOCK_MS)
        for entry_id, fields in entries_of(batch):
            process(name, entry_id, fields)


if __name__ == '__main__':
    try:
        main(sys.argv[1])
    except KeyboardInterrupt:
        print(f"[{sys.argv[1]}] stopped", flush=True)
