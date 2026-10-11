import redis 
import secrets
import time 

SESSION_TTL = 1800
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
def create_session(user_id):
    token = secrets.token_urlsafe(32)
    key = f"session:{token}"
    pipe = r.pipeline()
    pipe.hset(key, mapping={"user_id" : user_id, "created_at" : int(time.time())})
    pipe.expire(key, SESSION_TTL)
    pipe.execute()
    return token

def get_session(token):
    key = f"session:{token}"
    pipe = r.pipeline()
    pipe.hgetall(key)
    pipe.expire(key, SESSION_TTL)
    data, _ = pipe.execute()
    if not data:
        return None 
    return {"user_id": data["user_id"], "created_at": int(data["created_at"])}

if __name__ == '__main__':
    token = create_session(1)
    for _ in range(5):
        _ = get_session(token=token)
        time.sleep(3)

    print("Scirpt Finished")
