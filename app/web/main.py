import uuid
from datetime import datetime, timezone

from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import create_model

from app.etl.pipeline import run_full_etl, run_incremental_etl

app = FastAPI(title="Clickstream ETL", version="0.1.0")

_jobs = {}
_history = []

EtlStartResponse = create_model("EtlStartResponse", id=(str, ...), status=(str, ...))


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _run_job(job_id, mode):
    job = _jobs[job_id]
    job["status"] = "running"
    job["started_at"] = _utc_now()
    try:
        if mode == "full":
            metrics = run_full_etl()
        else:
            metrics = run_incremental_etl()
        job["status"] = "success"
        job["metrics"] = metrics
        job["finished_at"] = _utc_now()
        job.pop("error", None)
    except Exception as exc:
        job["status"] = "failed"
        job["error"] = str(exc)
        job["finished_at"] = _utc_now()
    rec = {"id": job_id, **job}
    _history.insert(0, rec)
    del _history[100:]


@app.post("/etl/full", response_model=EtlStartResponse)
def etl_full(background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"status": "pending", "mode": "full"}
    background_tasks.add_task(_run_job, job_id, "full")
    return EtlStartResponse(id=job_id, status="pending")


@app.post("/etl/incremental", response_model=EtlStartResponse)
def etl_incremental(background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"status": "pending", "mode": "incremental"}
    background_tasks.add_task(_run_job, job_id, "incremental")
    return EtlStartResponse(id=job_id, status="pending")


@app.get("/etl/status/{job_id}")
def etl_status(job_id):
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Unknown job id")
    return {"id": job_id, **_jobs[job_id]}


@app.get("/etl/history")
def etl_history():
    return {"items": list(_history)}
