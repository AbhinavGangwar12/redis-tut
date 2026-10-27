import redis 
import time
from concurrent.futures import ThreadPoolExecutor
from redis.exceptions import ResponseError

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def enqueue(stream: str, fields: dict[str, str], maxlen: int = 1000) -> str:
    return r.xadd(stream,fields,id="*",maxlen=maxlen, approximate=True)
    

def read_new(stream: str, group: str, consumer: str, count: int = 10, block_ms: int = 1000) -> list[tuple[str, dict]]:
    try:
        r.xgroup_create(stream, group, "0", mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise 

    response = r.xreadgroup(group, consumer, {stream: ">"}, count=count, block=block_ms)
    if not response:
        return []
    return response[0][1]

def ack(stream: str, group: str , entry_id: str) -> bool:
    return r.xack(stream, group, entry_id) == 1

def pending_count(stream: str, group: str) -> int:
    return r.xpending(stream, group)["pending"]

def recover(stream: str, group: str, consumer: str, min_idle_ms: int, count: int = 10) -> list[tuple[str, dict]]:
    _, claimed, _ = r.xautoclaim(stream, group, consumer, min_idle_ms,count=count)
    return claimed

def handle_entry(stream: str, group: str, entry_id: str, fields: dict, handler, max_attempts: int = 3) -> str:
    try:
        handler(fields)
        r.xack(stream, group, entry_id)
        r.xdel(stream, entry_id)
        return "done"
    except Exception as e:
        pending_info = r.xpending_range(stream, group, min=entry_id, max=entry_id, count=1)
        if pending_info and pending_info[0]['times_delivered'] >= max_attempts:
            dlq_stream = f"{stream}:dead"
            dead_fields = fields.copy()
            dead_fields['error'] = str(e)
            dead_fields['original_id'] = entry_id

            pipe = r.pipeline()
            pipe.xadd(dlq_stream, dead_fields)
            pipe.xack(stream, group,entry_id)
            pipe.xdel(stream, entry_id)
            pipe.execute()

            return "dead"
        return "retry"

def process_one(stream: str, group: str, consumer: str, handler, max_attempts: int = 3) -> str | None:
    try:
        r.xgroup_create(stream, group, "0", mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise 

    response = r.xreadgroup(group, consumer, {stream: ">"}, count=1, block=500)
    if not response:
        return None 
    _, messages = response[0]
    if not messages:
        return None 
    entry_id, fields = messages[0]
    return handle_entry(stream, group, entry_id, fields, handler,max_attempts)


def apply_credit_once(entry_id: str, user: str, amount: int) -> bool:
    script = """
    local balance = KEYS[1]
    local done = KEYS[2]
    local amount = tonumber(ARGV[1])
    if redis.call('SET', done, 'done','NX', 'EX', 86400) then
        redis.call('INCRBY', balance, amount)
        return 1
    end 
    return 0
    """
    lua = r.register_script(script)
    return lua(keys=[f"balance:{user}", f"done:credit:{entry_id}"], args=[amount]) == 1


if __name__ == '__main__':
    # r.delete("q4")
    # ids = [enqueue("q4", {"n": str(i)}) for i in range(3)]
    # a = read_new("q4", "g", "w1", count=2)
    # b = read_new("q4", "g", "w2", count=2)
    # c = read_new("q4", "g", "w2", count=2, block_ms=200)
    # print([f["n"] for _, f in a], [f["n"] for _, f in b], c)
    # print(a[0][0] == ids[0])

    # r.delete("q5")
    # for i in range(3):
    #     enqueue("q5", {"n": str(i)})
    # got = read_new("q5", "g", "w1", count=3)
    # print(pending_count("q5", "g"))
    # print(ack("q5", "g", got[0][0]), ack("q5", "g", got[0][0]))
    # print(pending_count("q5", "g"))

    # r.delete("q6")
    # for i in range(3):
    #     enqueue("q6", {"n": str(i)})
    # read_new("q6", "g", "dead-worker", count=3)
    # time.sleep(1.2)
    # early = recover("q6", "g", "w2", min_idle_ms=5000)
    # late = recover("q6", "g", "w2", min_idle_ms=1000)
    # print(len(early), [f["n"] for _, f in late])
    # print(pending_count("q6", "g"))
    # print({row["consumer"] for row in r.xpending_range("q6", "g", min="-", max="+", count=10)})

    # r.delete("q7", "q7:dead")
    # enqueue("q7", {"n": "1"})

    # def bad(fields):
    #     raise ValueError("boom")

    # print(process_one("q7", "g", "w1", bad))
    # for _ in range(2):
    #     for entry_id, fields in recover("q7", "g", "w1", min_idle_ms=0):
    #         print(handle_entry("q7", "g", entry_id, fields, bad))
    # print(pending_count("q7", "g"))
    # dead = r.xrange("q7:dead")
    # print(len(dead), dead[0][1]["error"], dead[0][1]["n"])
    # enqueue("q7", {"n": "2"})
    # print(process_one("q7", "g", "w1", lambda f: None), pending_count("q7", "g"))
    # print(process_one("q7", "g", "w1", bad))

    r.delete("balance:ann", "done:credit:1-0", "done:credit:2-0")
    print(apply_credit_once("1-0", "ann", 50))
    print(apply_credit_once("1-0", "ann", 50))
    print(apply_credit_once("2-0", "ann", 25))
    print(r.get("balance:ann"))
    print(0 < r.ttl("done:credit:1-0") <= 86400)

    r.delete("balance:bob", "done:credit:9-0")
    with ThreadPoolExecutor(max_workers=20) as pool:
        res = list(pool.map(lambda _: apply_credit_once("9-0", "bob", 50), range(20)))
    print(res.count(True), r.get("balance:bob"))