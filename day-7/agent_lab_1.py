import operator 
from typing import TypedDict, Annotated 

import redis 
from langgraph.graph import StateGraph, START, END 
from langgraph.checkpoint.redis import RedisSaver

re = redis.Redis(host="localhost", port=6379, decode_responses=True)
class State(TypedDict):
    task: str 
    result: str 
    log: Annotated[list[str], operator.add]

def plan(state: State):
    return {"log" : [f"planned:{state['task']}"]}

def work(state: State):
    if re.get("lab:crash") == "1":
        raise RuntimeError("simulated crash in work")
    return {"log" : ["worked"]}

def finish(state: State):
    return {"log": ["finished"], "result": f"done:{state['task']}"}

def build(checkpointer):
    g = StateGraph(State)
    g.add_node("plan", plan)
    g.add_node("work", work)
    g.add_node("finish", finish)
    g.add_edge(START, "plan")
    g.add_edge("plan", "work")
    g.add_edge("work", "finish")
    g.add_edge("finish", END)
    return g.compile(checkpointer=checkpointer)

