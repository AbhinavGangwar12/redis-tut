import time
import requests

BASE = "http://127.0.0.1:8000"

def ask(question, category=None, session_id=None):
    body = {"question": question}
    if category:
        body["category"] = category
    if session_id:
        body["session_id"] = session_id
    start = time.time()
    resp = requests.post(BASE + "/ask", json=body, timeout=30)
    return resp, round(time.time() - start, 2)

def stats():
    return requests.get(BASE + "/stats").json()

def step(title):
    print(f"\n--- {title} ---")

step("1. ingest and reset")
resp = requests.post(BASE + "/ingest"); print(resp.status_code, resp.json())
print(requests.delete(BASE + "/cache").status_code)

step("2. cold question goes through RAG")
resp, took = ask("How do I reset my password?")
body = resp.json(); print((body["source"], body["sources"][:1], took >= 1.0))

step("3. same question again: exact cache")
resp, took = ask("How do I reset my password?")
print((resp.json()["source"], took < 0.5))

step("4. paraphrase: semantic cache")
resp, took = ask("how can I reset my password")
print((resp.json()["source"], took < 0.5))

step("5. another topic, then stats")
resp, _ = ask("What is your refund policy for annual plans?")
body = resp.json(); print((body["source"], body["sources"][0]))
print(stats())

step("6. category filter")
resp, _ = ask("Can I send an item back?", category="shipping")
print(resp.json()["sources"][:1])
resp, _ = ask("Can I send an item back?", category="billing")
print(all(not s.startswith("ship") for s in resp.json()["sources"]))

step("7. unknown questions are not cached and cost no LLM call")
before = stats()["llm_calls"]
answers = [ask("What is the capital of France?")[0].json() for _ in range(2)]
print([a["answer"] for a in answers], [a["sources"] for a in answers])
print(stats()["llm_calls"] == before)

step("8. chat history")
ask("How do I enable two-factor authentication?", session_id="s1")
ask("How long does express shipping take?", session_id="s1")
history = requests.get(BASE + "/history/s1").json()
print(len(history), [m["role"] for m in history])
print(requests.get(BASE + "/history/nobody").json())

step("9. validation")
print(ask("hi")[0].status_code)
print(ask("How do I reset my password?", category="legal")[0].status_code)