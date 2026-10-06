import redis 

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
# r.set("page:home:views", 0)

# for i in range(5):
#     r.incr("page:home:views")
# print(r.get("page:home:views"))
# print(type(r.get("page:home:views")))

# def hit(user_id: str):
#     count = r.incr(f"hits:{user_id}")
#     if count == 1:
#         r.expire(f"hits:{user_id}", 60)
#     return count 


# def store_session(token, user_id, ttl):
#     r.set(f"session:{token}", user_id, ex=ttl)
# def get_session(token):
#     return r.get(f"session:{token}")


# import secrets 
# import string 

# def generate_code():
#     characters = string.ascii_letters + string.digits 
#     code = ''.join(secrets.choice(characters) for _ in range(6))
#     return code 

# def create_code(url):
#     setted = False
#     c = None 
#     for _ in range(5):
#         code = generate_code()
#         s = r.set(f"url:{code}", url, nx=True)
#         if s:
#             setted = True 
#             c = code 
#             break 

#     if setted:
#         return c 
#     raise Exception("No.")


    
import datetime 

def record_signup():
    today = datetime.date.today()
    key = f"stats:{today.isoformat()}:signups"
    pipe = r.pipeline()
    pipe.incr(key)
    pipe.expire(key, 604800)
    pipe.execute()

def last_7_days():
    today = datetime.date.today()
    keys = [
        f"stats:{(today - datetime.timedelta(days=i)).isoformat()}:signups"
        for i in range(6, -1, -1)
    ]
    counts = r.mget(keys)
    return [int(count) if count is not None else 0 for count in counts]

    
for _ in range(3):
    record_signup()
print(last_7_days())
print(r.ttl(f"stats:{datetime.date.today().isoformat()}:signups"))