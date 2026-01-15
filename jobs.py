# jobs.py
import json
import time
from typing import Any, Dict, Optional

import redis.asyncio as redis

from config import settings

PENDING = "PENDING"
RUNNING = "RUNNING"
DONE = "DONE"
ERROR = "ERROR"


def _job_key(job_id: str) -> str:
    return f"{settings.REDIS_PREFIX}:job:{job_id}"


def _now() -> float:
    return time.time()


_redis: Optional[redis.Redis] = None


async def get_redis() -> redis.Redis:
    global _redis
    if _redis is None:
        if not getattr(settings, "REDIS_URL", None):
            raise RuntimeError("REDIS_URL is not set")
        _redis = redis.from_url(settings.REDIS_URL, decode_responses=True)
    return _redis


async def close_redis() -> None:
    """
    Корректно закрывает Redis-подключение (на shutdown).
    """
    global _redis
    if _redis is None:
        return
    try:
        await _redis.aclose()
    finally:
        _redis = None


async def create_job(kind: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    r = await get_redis()

    job_id = payload.get("job_id")
    if not job_id:
        import uuid
        job_id = uuid.uuid4().hex

    data = {
        "job_id": job_id,
        "kind": kind,
        "status": PENDING,
        "payload": payload,
        "result": None,
        "error": None,
        "created_at": _now(),
        "updated_at": _now(),
    }
    await r.set(_job_key(job_id), json.dumps(data), ex=settings.JOB_TTL_SECONDS)
    return data


async def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    r = await get_redis()
    raw = await r.get(_job_key(job_id))
    if not raw:
        return None
    return json.loads(raw)


async def update_job_status(
    job_id: str,
    status: str,
    *,
    result: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    r = await get_redis()
    key = _job_key(job_id)

    raw = await r.get(key)
    if not raw:
        return None

    data = json.loads(raw)
    data["status"] = status
    data["updated_at"] = _now()
    if result is not None:
        data["result"] = result
    if error is not None:
        data["error"] = error

    await r.set(key, json.dumps(data), ex=settings.JOB_TTL_SECONDS)
    return data


# --- Queue helpers ---
async def rpush_job(job_id: str) -> None:
    r = await get_redis()
    await r.rpush(settings.REDIS_QUEUE, job_id)


async def lpush_job(job_id: str) -> None:
    r = await get_redis()
    await r.lpush(settings.REDIS_QUEUE, job_id)


async def brpop_job(timeout: int = 5) -> Optional[str]:
    r = await get_redis()
    item = await r.brpop(settings.REDIS_QUEUE, timeout=timeout)
    if not item:
        return None
    _queue, job_id = item
    return job_id
