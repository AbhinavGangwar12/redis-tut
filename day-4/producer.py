import redis 
import uuid 
from fastapi import FastAPI 

app = FastAPI()
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

@app.post("/research")
async def start_research(topic: str):
    job_id = str(uuid.uuid4())
    r.hset(f"job:{job_id}", mapping={"status": "pending", "topic" : topic})
    r.xadd("agent_tasks_stream", {"job_id" : job_id, "topic":topic}, id="*")
    return {"job_id" : job_id, "message" : "Research started asynchronously."}

@app.get("/research/{job_id}")
async def get_status(job_id: str):
    status = r.hgetall(f"job:{job_id}")
    return status