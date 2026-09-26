import redis 
import time 

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def check_rate_limit(user_id, max_requests=5, window_seconds=60):
    key = f"rate_limit:{user_id}"
    now = time.time()
    window_start = now - window_seconds

    pipe = r.pipeline()

    # 1. Remove requests older than our window
    # zremrangebyscore removes items with scores between -infinity and our cutoff time
    pipe.zremrangebyscore(key, "-inf", window_start)

    # 2. Count requests remaining in the window
    pipe.zcard(key)

    # 3. Add the current request (using timestamp as both value and score)
    # We use pipeline to ensure this happens efficiently
    pipe.zadd(key, {str(now): now})

    # 4. Set a TTL on the whole ZSet so it cleans itself up if the user goes inactive
    pipe.expire(key, window_seconds)

    results = pipe.execute()

    current_requests = results[1]
    if current_requests >= max_requests:
        return False, "Rate limit exceeded. Try again later."
    return True, f"Request allowed. ({current_requests + 1}/{max_requests})"

if __name__ == '__main__':
    user = "John Doe"
    for _ in range(6):
        allowed, msg = check_rate_limit(user)
        print(msg)
