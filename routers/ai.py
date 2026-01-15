import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, HTTPException, Query, Request

from jobs import create_job, rpush_job, get_job


router = APIRouter(prefix="/ai", tags=["ai"])

ALLOWED_ASPECTS = {"1:1", "3:4", "4:3", "9:16", "16:9", "5:8"}
RATE_LIMIT_MAX = 10
RATE_LIMIT_WINDOW_SEC = 60
_rate_limit: Dict[str, List[float]] = {}


def _check_rate_limit(request: Request) -> None:
    ip = (request.client.host if request.client else "unknown")
    now = time.time()
    bucket = _rate_limit.get(ip, [])
    bucket = [t for t in bucket if now - t <= RATE_LIMIT_WINDOW_SEC]
    if len(bucket) >= RATE_LIMIT_MAX:
        raise HTTPException(429, "Rate limit exceeded. Try again later.")
    bucket.append(now)
    _rate_limit[ip] = bucket


def _validate_steps(steps: Optional[int]) -> int:
    if steps is None:
        return 30
    if steps < 10 or steps > 50:
        raise HTTPException(400, "steps must be between 10 and 50")
    return steps


def _validate_aspect(aspect_ratio: Optional[str]) -> str:
    ar = (aspect_ratio or "1:1").strip()
    if ar not in ALLOWED_ASPECTS:
        raise HTTPException(400, f"aspect_ratio must be one of {sorted(ALLOWED_ASPECTS)}")
    return ar


def _validate_strength(strength: Optional[float]) -> float:
    if strength is None:
        return 0.6
    if strength < 0 or strength > 1:
        raise HTTPException(400, "strength must be between 0 and 1")
    return strength


@router.post("/generate/text")
async def ai_generate_text(
    request: Request,
    prompt: str = Body(...),
    aspect_ratio: Optional[str] = Body(default="1:1"),
    steps: Optional[int] = Body(default=30),
    seed: Optional[int] = Body(default=None),
):
    _check_rate_limit(request)
    if not prompt.strip():
        raise HTTPException(400, "prompt is required")
    payload = {
        "prompt": prompt.strip(),
        "aspect_ratio": _validate_aspect(aspect_ratio),
        "steps": _validate_steps(steps),
        "seed": seed,
    }
    job = await create_job("image_t2i", payload)
    await rpush_job(job["job_id"])
    return {"ok": True, "job_id": job["job_id"], "status_url": f"/ai/status?job_id={job['job_id']}"}


@router.post("/generate/image")
async def ai_generate_image(
    request: Request,
    image_url: str = Body(...),
    prompt: str = Body(...),
    strength: Optional[float] = Body(default=0.6),
    aspect_ratio: Optional[str] = Body(default="3:4"),
    steps: Optional[int] = Body(default=30),
    seed: Optional[int] = Body(default=None),
):
    _check_rate_limit(request)
    if not prompt.strip():
        raise HTTPException(400, "prompt is required")
    if not image_url.strip():
        raise HTTPException(400, "image_url is required")
    payload = {
        "image_url": image_url.strip(),
        "prompt": prompt.strip(),
        "strength": _validate_strength(strength),
        "aspect_ratio": _validate_aspect(aspect_ratio),
        "steps": _validate_steps(steps),
        "seed": seed,
    }
    job = await create_job("image_i2i", payload)
    await rpush_job(job["job_id"])
    return {"ok": True, "job_id": job["job_id"], "status_url": f"/ai/status?job_id={job['job_id']}"}


@router.post("/generate/batch")
async def ai_generate_batch(
    request: Request,
    image_urls: List[str] = Body(...),
    prompt: str = Body(...),
    strength: Optional[float] = Body(default=0.55),
    aspect_ratio: Optional[str] = Body(default="1:1"),
    steps: Optional[int] = Body(default=30),
    variants_per_image: Optional[int] = Body(default=1),
    seed: Optional[int] = Body(default=None),
):
    _check_rate_limit(request)
    if not prompt.strip():
        raise HTTPException(400, "prompt is required")
    cleaned_urls = [u.strip() for u in image_urls if u and u.strip()]
    if len(cleaned_urls) < 15 or len(cleaned_urls) > 50:
        raise HTTPException(400, "image_urls must contain 15-50 items")
    vpi = int(variants_per_image or 1)
    if vpi < 1 or vpi > 4:
        raise HTTPException(400, "variants_per_image must be between 1 and 4")
    payload = {
        "image_urls": cleaned_urls,
        "prompt": prompt.strip(),
        "strength": _validate_strength(strength),
        "aspect_ratio": _validate_aspect(aspect_ratio),
        "steps": _validate_steps(steps),
        "variants_per_image": vpi,
        "seed": seed,
    }
    job = await create_job("avatar_batch", payload)
    await rpush_job(job["job_id"])
    return {"ok": True, "job_id": job["job_id"], "status_url": f"/ai/status?job_id={job['job_id']}"}


@router.get("/status")
async def ai_status(job_id: str = Query(...)):
    job = await get_job(job_id)
    if not job:
        return {"ok": False, "job_id": job_id, "status": "ERROR", "stage": "error", "error": "Job not found"}
    stage = job.get("stage")
    if not stage:
        status = job.get("status")
        if status == "PENDING":
            stage = "queued"
        elif status == "RUNNING":
            stage = "running"
        elif status == "DONE":
            stage = "done"
        else:
            stage = "error"
    return {
        "ok": True,
        "job_id": job.get("job_id"),
        "kind": job.get("kind"),
        "status": job.get("status"),
        "stage": stage,
        "result": job.get("result"),
        "error": job.get("error"),
    }
