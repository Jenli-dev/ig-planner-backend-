# jobs.py
from typing import Dict, Any, Optional
import uuid
import time

PENDING = "PENDING"
RUNNING = "RUNNING"
DONE = "DONE"
ERROR = "ERROR"

_store: Dict[str, Dict[str, Any]] = {}

def create_job(kind: str, payload: Dict[str, Any]) -> str:
    job_id = str(uuid.uuid4())
    _store[job_id] = {
        "id": job_id,
        "kind": kind,
        "payload": payload,
        "status": PENDING,
        "result": None,
        "error": None,
        "created_at": time.time(),
        "updated_at": time.time(),
    }
    return job_id

def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    return _store.get(job_id)

def update_job_status(
    job_id: str,
    status: str,
    *,
    result: Any = None,
    error: Optional[str] = None,
) -> None:
    job = _store.get(job_id)
    if not job:
        return
    job["status"] = status
    job["result"] = result
    job["error"] = error
    job["updated_at"] = time.time()
