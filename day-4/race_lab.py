import threading
import redis 
import time

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

if __name__ == '__main__':
    r.delete("race:counter"); run_threads(unsafe_incr); print(r.get("race:counter"))
    r.delete("race:counter"); run_threads(safe_incr); print(r.get("race:counter"))