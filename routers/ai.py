import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, HTTPException, Query, Request

from jobs import create_job, rpush_job, get_job
from services.ai_subscription import (
    get_subscription_status,
    check_credits,
    use_credits,
    set_subscription,
    cancel_subscription,
    AIAvatarPlanType,
    _default_user_id,
)


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
    user_id: Optional[str] = Query(None, description="User ID (default: default_user)"),
):
    _check_rate_limit(request)
    if not prompt.strip():
        raise HTTPException(400, "prompt is required")
    
    # Проверяем подписку и кредиты
    uid = user_id or _default_user_id()
    credits_check = await check_credits(uid, "text_to_image")
    if not credits_check["can_proceed"]:
        raise HTTPException(
            402,  # Payment Required
            f"Cannot generate: {credits_check['reason']}. Please check your subscription and credits."
        )
    
    payload = {
        "prompt": prompt.strip(),
        "aspect_ratio": _validate_aspect(aspect_ratio),
        "steps": _validate_steps(steps),
        "seed": seed,
        "user_id": uid,  # Сохраняем user_id для последующего списания кредитов
        "operation_type": "text_to_image",
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
    user_id: Optional[str] = Query(None, description="User ID (default: default_user)"),
):
    _check_rate_limit(request)
    if not prompt.strip():
        raise HTTPException(400, "prompt is required")
    if not image_url.strip():
        raise HTTPException(400, "image_url is required")
    
    # Проверяем подписку и кредиты
    uid = user_id or _default_user_id()
    credits_check = await check_credits(uid, "image_to_image")
    if not credits_check["can_proceed"]:
        raise HTTPException(
            402,  # Payment Required
            f"Cannot generate: {credits_check['reason']}. Please check your subscription and credits."
        )
    
    payload = {
        "image_url": image_url.strip(),
        "prompt": prompt.strip(),
        "strength": _validate_strength(strength),
        "aspect_ratio": _validate_aspect(aspect_ratio),
        "steps": _validate_steps(steps),
        "seed": seed,
        "user_id": uid,  # Сохраняем user_id для последующего списания кредитов
        "operation_type": "image_to_image",
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
    user_id: Optional[str] = Query(None, description="User ID (default: default_user)"),
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
    
    # Проверяем подписку и кредиты для avatar batch
    uid = user_id or _default_user_id()
    credits_check = await check_credits(uid, "avatar_batch")
    if not credits_check["can_proceed"]:
        raise HTTPException(
            402,  # Payment Required
            f"Cannot generate avatar batch: {credits_check['reason']}. Please check your subscription and credits."
        )
    
    payload = {
        "image_urls": cleaned_urls,
        "prompt": prompt.strip(),
        "strength": _validate_strength(strength),
        "aspect_ratio": _validate_aspect(aspect_ratio),
        "steps": _validate_steps(steps),
        "variants_per_image": vpi,
        "seed": seed,
        "user_id": uid,  # Сохраняем user_id для последующего списания кредитов
        "operation_type": "avatar_batch",
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


# ===== AI Avatar Subscription Endpoints =====

@router.get("/subscription/status")
async def ai_subscription_status(
    user_id: Optional[str] = Query(None, description="User ID (default: default_user)"),
):
    """
    Получает статус подписки AI Avatar пользователя.
    """
    uid = user_id or _default_user_id()
    status = await get_subscription_status(uid)
    return {"ok": True, **status}


@router.post("/credits/check")
async def ai_credits_check(
    operation_type: str = Body(..., description="Operation type: text_to_image, image_to_image, or avatar_batch"),
    user_id: Optional[str] = Body(None, description="User ID (default: default_user)"),
):
    """
    Проверяет, достаточно ли кредитов для операции.
    """
    uid = user_id or _default_user_id()
    
    if operation_type not in ["text_to_image", "image_to_image", "avatar_batch"]:
        raise HTTPException(400, "Invalid operation_type. Must be: text_to_image, image_to_image, or avatar_batch")
    
    result = await check_credits(uid, operation_type)
    return {"ok": True, **result}


@router.get("/credits/balance")
async def ai_credits_balance(
    user_id: Optional[str] = Query(None, description="User ID (default: default_user)"),
):
    """
    Получает баланс кредитов пользователя.
    """
    uid = user_id or _default_user_id()
    status = await get_subscription_status(uid)
    return {
        "ok": True,
        "credits_remaining": status["credits_remaining"],
        "daily_credits_used": status["daily_credits_used"],
        "daily_limit": status["daily_limit"],
        "is_active": status["is_active"],
    }


@router.post("/subscription/activate")
async def ai_subscription_activate(
    plan_type: str = Body(..., description="Plan type: weekly, monthly, or yearly"),
    user_id: Optional[str] = Body(None, description="User ID (default: default_user)"),
    expires_at: Optional[str] = Body(None, description="Expiration date (ISO format, optional)"),
):
    """
    Активирует подписку AI Avatar для пользователя.
    Обычно вызывается из webhook после успешной покупки в App Store.
    """
    uid = user_id or _default_user_id()
    
    try:
        plan = AIAvatarPlanType(plan_type)
    except ValueError:
        raise HTTPException(400, f"Invalid plan_type. Must be: {', '.join([p.value for p in AIAvatarPlanType])}")
    
    expires_dt = None
    if expires_at:
        from datetime import datetime
        try:
            expires_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(400, "Invalid expires_at format. Use ISO format.")
    
    subscription = await set_subscription(uid, plan, expires_dt)
    return {"ok": True, "subscription": subscription}


@router.post("/subscription/cancel")
async def ai_subscription_cancel(
    user_id: Optional[str] = Body(None, description="User ID (default: default_user)"),
):
    """
    Отменяет подписку AI Avatar пользователя.
    Обычно вызывается из webhook при отмене подписки в App Store.
    """
    uid = user_id or _default_user_id()
    cancelled = await cancel_subscription(uid)
    return {"ok": True, "cancelled": cancelled}
