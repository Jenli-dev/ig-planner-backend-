# jobs.py
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict
from uuid import uuid4
from threading import Lock

class JobStatus(str, Enum):
    queued = "queued"
    running = "running"
    done = "done"
    error = "error"

@dataclass
class Job:
    id: str
    status: JobStatus = JobStatus.queued
    message: Optional[str] = None
    output_url: Optional[str] = None
    preset: Optional[str] = None
    intensity: Optional[float] = None

_jobs: Dict[str, Job] = {}
_lock = Lock()

def create_job(preset: str, intensity: float) -> Job:
    job = Job(id=uuid4().hex, preset=preset, intensity=intensity)
    with _lock:
        _jobs[job.id] = job
    return job

def set_job_status(job_id: str, status: JobStatus, *, message: Optional[str]=None,
                   output_url: Optional[str]=None):
    with _lock:
        job = _jobs.get(job_id)
        if not job:
            return
        job.status = status
        if message is not None:
            job.message = message
        if output_url is not None:
            job.output_url = output_url

def get_job(job_id: str) -> Optional[Job]:
    with _lock:
        return _jobs.get(job_id)
