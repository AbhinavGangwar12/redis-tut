import threading
import redis 
import time
import uuid 

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def run_threads(fn, n_threads=20, n_loops=50):
    def worker():
        for _ in range(n_loops):
            fn()

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

def unsafe_incr():
    value = int(r.get("race:counter") or 0)
    time.sleep(0.001)
    r.set("race:counter", value + 1)
    
def safe_incr():
    r.incr("race:counter")

# Watch [ optimistic locking ]

def withdraw(key, amount):
    with r.pipeline() as pipe:
        while True:
            try:
                pipe.watch(key)
                balance = int(pipe.get(key) or 0)
                if balance < amount:
                    pipe.unwatch()
                    return False
                pipe.multi()
                pipe.set(key, balance - amount)
                pipe.execute()
                return True 
            except redis.WatchError:
                continue

# Locks with unique token-id

def acquire_lock(name, ttl=10):
    token = uuid.uuid4().hex
    if r.set(f"lock:{name}", token, nx=True, ex=ttl):
        return token
    return None

def release_lock(name, token):
    key = f"lock:{name}"
    with r.pipeline() as pipe:
        try:
            pipe.watch(key)
            if pipe.get(key) == token:
                pipe.multi()
                pipe.delete(key)
                pipe.execute()
                return True 
            pipe.unwatch()
            return False 
        except redis.WatchError:
            return False

if __name__ == '__main__':
    # r.delete("race:counter"); run_threads(unsafe_incr); print(r.get("race:counter"))
    # r.delete("race:counter"); run_threads(safe_incr); print(r.get("race:counter"))

    a = acquire_lock("job")
    print(bool(a))
    print(acquire_lock("job"))
    print(release_lock("job", "wrong-token"))
    print(release_lock("job", a))
    print(bool(acquire_lock("job")))