import redis 
import time 
import json 
from langgraph.graph import StateGraph, START, END
from typing import TypedDict 

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

class AgentState(TypedDict):
    topic: str 
    research_notes: str 
    final_report: str 


def research_node(state: AgentState) -> AgentState:
    print(f"[{state['topic']}] Researching...")
    time.sleep(2) # Simulate LLM call
    return {"research_notes": f"Found facts about {state['topic']}"}

def write_node(state: AgentState):
    print(f"[{state['topic']}] Writing report...")
    time.sleep(1)
    return {"final_report": f"REPORT: {state['research_notes']}"}

workflow = StateGraph(AgentState)
workflow.add_node("researcher", research_node)
workflow.add_node("writer", write_node)
workflow.set_entry_point("researcher")
workflow.add_edge("researcher", "writer")
workflow.add_edge("writer", END)
app = workflow.compile()


def run_worker():
    print("Worker listening for tasks...")

    try:
        r.xgroup_create("agent_tasks_stream", "worker_group", id="0", mkstream=True)
    except Exception:
        pass 

    while True:
        # Read from Stream using Consumer Group
        # > means "give me messages that have never been delivered to other consumers"
        messages = r.xreadgroup("worker_group", "worker_1", {"agent_tasks_stream": ">"}, count=1, block=5000)
        if messages:
            stream_name, message_list = messages[0]
            for message_id, data in message_list:
                job_id = data["job_id"]
                topic = data["topic"]

                r.hset(f"job:{job_id}", "status", "running")

                initial_state = {"topic": topic, "research_notes": "", "final_report": ""}
                final_state = app.invoke(initial_state)

                r.hset(f"job:{job_id}", mapping={
                    "status" : "completed",
                    "result" : final_state["final_report"]
                })

                r.xack("agent_tasks_stream", "worker_group", message_id)
                print(f"Job {job_id} completed and ACK'd.")

run_worker()