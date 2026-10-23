import redis 

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('GET', KEYS[1])
end
return 0
"""

release_script = r.register_script(RELEASE_LUA)

def release_lock(name, token):
    return release_script(keys=[f"lock:{name}"], args=[token]) == 1
