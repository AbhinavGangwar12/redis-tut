import redis 
import time

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def mock_llm_call(prompt):
    print("...Calling expensive LLM...")
    time.sleep(2) # Simulate latency
    return f"Response to: {prompt}"

def get_ai_response_basic(prompt):
    cache_key = f"cache:llm:{prompt}"
    cached = r.get(cache_key)
    if cached:
        return "[HIT] {cached}"
    response = mock_llm_call(prompt=prompt)
    r.set(cache_key, response)
    return f"[MISS] {response}"

if __name__ == '__main__':
    print(get_ai_response_basic('What is LangGraph?'))