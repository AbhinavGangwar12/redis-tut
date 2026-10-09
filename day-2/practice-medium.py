import redis 

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def add_prompt(id, title, body, tags):
    tags = [t.lower() for t in tags]
    pipe = r.pipeline()
    key = f"prompt:{id}"
    pipe.hset(key, mapping={
        "title" : title, "body" : body, "tags" : ",".join(tags), "uses" : 0
    })
    for t in tags:
        pipe.sadd(f"tag:{t}", id)
    pipe.execute()

def get_prompt(id):
    data = r.hgetall(f"prompt:{id}")
    if data is None:
        return None 
    
    return {
        "id": id,
        "title": data["title"],
        "body": data["body"],
        "tags": data["tags"].split(",") if data["tags"] else [],
        "uses": int(data["uses"]),
    }

def find_by_tags(tags, mode):
    if mode not in ("all", "any"):
        raise ValueError("mode must be 'all' or 'any'")
    keys = [f"tag:{t.lower()}" for t in tags]
    if not keys:
        return []
    ids = r.sinter(keys) if mode == "all" else r.sunion(keys)
    prompts = [get_prompt(int(i)) for i in sorted(ids, key=int)]
    return [p for p in prompts if p is not None]

def view(user_id, prompt_id):
    key = f"recent:{user_id}"
    pipe = r.pipeline()
    pipe.lrem(key, 0, prompt_id)
    pipe.lpush(key, prompt_id)
    pipe.ltrim(key, 0, 4)
    pipe.execute()

def recent(user_id):
    return [int(x) for x in r.lrange(f"recent:{user_id}", 0, 4)]



if __name__ == '__main__':
    add_prompt(1, "Summary", "Summarize this topic", "Active")
    print(get_prompt(1))
    