import redis 
import time 
import operator
from langgraph.graph import StateGraph, START, END 
from typing import TypedDict, Annotated
from yard import exact_cache_set, exact_cache_get, run_once


class State(TypedDict):
    job_id: str 
    topic: str 
    facts: str 
    draft: str 
    result: str 
    log: Annotated[list[str], operator.add] 

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def research(state: State):
    job_id = state["job_id"]
    topic = state["topic"]

    pipe = r.pipeline()
    pipe.incr("stats:agent:node:research")
    pipe.hset(f"agent:job:{job_id}", "current", "research")
    pipe.execute()

    facts = exact_cache_get(question=state.get("topic"))
    if facts:
        r.incr("stats:agent:tool_cache_hits")
    else:
        time.sleep(2)
        facts = f"facts about {topic}"
        exact_cache_set(question=topic, ans=facts)
    return {"facts" : facts, "log" : ["research"]}

def draft(state: State):
    job_id = state.get("job_id")
    pipe = r.pipeline()
    pipe.incr("stats:agent:node:darft")
    pipe.hset(f"agent:job:{job_id}", "current" , "draft")
    pipe.execute()

    time.sleep(2)
    return {"draft" : f"Draft: {state.get("facts")}", "log" : ["draft"]}

def review(state: State):
    job_id = state.get("job_id")
    pipe = r.pipeline()
    pipe.incr("stats:agent:node:review")
    pipe.hset(f"agent:job:{job_id}", "current" , "review")
    pipe.execute()

    time.sleep(2)
    return {"log" : ["review"]}

def review(state: State):
    job_id = state.get("job_id")
    pipe = r.pipeline()
    pipe.incr("stats:agent:node:review")
    pipe.hset(f"agent:job:{job_id}", "current" , "review")
    pipe.execute()

    time.sleep(2)
    return {"log" : ["review"]}


def finish(state: State):
    job_id = state["job_id"]
    
    pipe = r.pipeline()
    pipe.incr("stats:agent:node:finish")
    pipe.hset(f"agent:job:{job_id}", "current", "finish")
    pipe.execute()
    run_once(
        f"done:notify:{job_id}", 
        lambda: r.incr("stats:agent:notifications")
    )
    
    return {"result": state["draft"], "log": ["finish"]}


def build(checkpointer):
    builder = StateGraph(State)
    builder.add_node("research", research)
    builder.add_node("draft", draft)
    builder.add_node("review", review)
    builder.add_node("finish", finish)

    builder.add_edge(START, "research")
    builder.add_edge("research", "draft")
    builder.add_edge("draft", "review")
    builder.add_edge("review", "finish")
    builder.add_edge("finish", END)

    return builder.compile(checkpointer=checkpointer)