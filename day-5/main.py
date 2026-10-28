import redis 
import uuid
from redis.exceptions import ResponseError
from typing import Literal
from pydantic import BaseModel, Field
from fastapi import FastAPI , status, HTTPException
from fastapi.responses import JSONResponse

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
app = FastAPI(title="job-queue")

class FieldBody(BaseModel):
    kind : Literal["email", "report", "fail"]
    payload: str = Field(min_length=1, max_length=200)

@app.post("/jobs")
def jobs(payload : FieldBody):
    job_id = uuid.uuid4().hex
    hash_key = f"job:{job_id}"
    job_data = {
        "status" : "queued", 
        "attempts" : 0,
        "result" : "",
        "error" : "",
        "kind" : payload.kind,
        "payload" : payload.payload 
    }
    pipe = r.pipeline()
    pipe.hset(hash_key, mapping=job_data)
    pipe.expire(hash_key, time=86400)
    pipe.xadd("jobs", fields={"job_id": job_id, "kind": payload.kind, "payload": payload.payload})
    pipe.execute()
    return JSONResponse(
        content={"job_id" : job_id, "status" : "queued"},
        status_code=status.HTTP_202_ACCEPTED
    )

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    data = r.hgetall(f"job:{job_id}")
    if not data:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": job_id, "kind": data["kind"], "payload": data["payload"],
            "status": data["status"], "attempts": int(data["attempts"]),
            "result": data["result"], "error": data["error"]}

@app.get("/queue")
def queue():
    pipe = r.pipeline()
    pipe.xlen("jobs")
    pipe.xlen("jobs:dead")
    length, dead = pipe.execute()
    pending = 0
    try:
        pending_info = r.xpending("jobs", "workers")
        pending = pending_info["pending"]
    except ResponseError as e:
        if "NOGROUP" not in str(e):
            raise
    return JSONResponse(
        content={
            "length" : length,
            "pending" : pending,
            "dead" : dead
        },
        status_code=status.HTTP_200_OK
    )

@app.get("/dead-jobs")
def dead_jobs():
    messages = r.xrevrange("jobs:dead", max="+", min="-", count=20)
    result = []
    for msg_id, fields in messages:
        result.append({
            "job_id" : fields.get('job_id'),
            "kind" : fields.get('kind'),
            "payload" : fields.get('payload'),
            "error" : fields.get('error'),
        })
    return result 