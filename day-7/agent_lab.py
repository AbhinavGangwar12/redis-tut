import operator
import uuid 
from typing import TypedDict, Annotated 

import redis 
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.redis import RedisSaver

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

class State(TypedDict):
    task: str 
    log: Annotated[list[str], operator.add]
    result: str 

def plan(state: State):
    return {"log" : [f"planned:{state['task']}"]}

def work(state: State):
    if r.get("lab:crash") == "1":
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

if __name__ == '__main__':
    thread = f"lab-{uuid.uuid4().hex[:6]}"
    with RedisSaver.from_conn_string("redis://localhost:6379") as checkpointer:
        checkpointer.setup()
        graph = build(checkpointer=checkpointer)
        config = {"configurable" : {"thread_id" : thread}}
        r.set("lab:crash", "1")
        try:
            graph.invoke({"task" : "report", "log": [], "result" : ""}, config=config)
        except RuntimeError as e:
            print("crashed:", e)
        snap = graph.get_state(config=config)
        print("saved log:", snap.values["log"], "| next:", snap.next)
        r.set("lab:crash", "0")
        out = graph.invoke(None, config=config)
        print("resumed:", out["log"], out["result"])
