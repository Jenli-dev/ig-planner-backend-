from fastapi import APIRouter, Body, Query, HTTPException
from typing import Optional, List, Dict, Any
import asyncio
import httpx
from uuid import uuid4
from datetime import datetime, timezone

from services.ig_state import load_state
from http_client import RetryClient
from meta_config import GRAPH_BASE, CLOUDINARY_CLOUD
from cloudinary_utils import cld_inject_transform, CLOUD_REELS_TRANSFORM
from services.ig_publish import publish_reel
from time_utils import iso_to_utc, sleep_seconds_until


router = APIRouter(prefix="/ig", tags=["ig"])


async def _wait_container_ready(
    client: RetryClient,
    *,
    creation_id: str,
    access_token: str,
    max_wait_sec: int = 60,
    sleep_sec: float = 2.0,
):
    """
    Poll container status before publish to avoid "Media ID is not available".
    Works for both image and video containers; will return when FINISHED.
    """
    waited = 0.0
    status_code = "IN_PROGRESS"
    status_text = None
    while waited < max_wait_sec:
        rstat = await client.get(
            f"{GRAPH_BASE}/{creation_id}",
            params={"fields": "status,status_code", "access_token": access_token},
            retries=4,
            timeout=30,
        )
        try:
            rstat.raise_for_status()
        except httpx.HTTPStatusError as e:
            return {
                "ok": False,
                "stage": "check_status",
                "status": (e.response.status_code if e.response else None),
                "creation_id": creation_id,
                "error": (e.response.json() if e.response else str(e)),
            }
        payload_stat = rstat.json() or {}
        status_code = payload_stat.get("status_code") or "IN_PROGRESS"
        status_text = payload_stat.get("status")
        if status_code == "FINISHED":
            return {"ok": True}
        if status_code == "ERROR":
            return {
                "ok": False,
                "stage": "processing",
                "status_code": status_code,
                "status": status_text,
                "creation_id": creation_id,
            }
        await asyncio.sleep(sleep_sec)
        waited += sleep_sec
    return {
        "ok": False,
        "stage": "timeout",
        "status_code": status_code,
        "status": status_text,
        "creation_id": creation_id,
        "waited_sec": waited,
    }


# ── IG: latest media ────────────────────────────────────────────────────
@router.get("/media")
async def ig_media(
    limit: int = 12,
    after: Optional[str] = None,
    account_id: Optional[str] = Query(None, description="Account ID (page_id) to use"),
):
    st = await load_state(account_id=account_id)
    ig_id, page_token = st["ig_id"], st["page_token"]

    params = {
        "access_token": page_token,
        "limit": max(1, min(limit, 50)),
        "fields": ",".join(
            [
                "id",
                "caption",
                "media_type",
                "media_url",
                "permalink",
                "thumbnail_url",
                "timestamp",
                "product_type",
            ]
        ),
    }
    if after:
        params["after"] = after

    async with RetryClient() as client:
        r = await client.get(f"{GRAPH_BASE}/{ig_id}/media", params=params, retries=4)
        r.raise_for_status()
        payload = r.json()

    return {
        "ok": True,
        "count": len(payload.get("data", [])),
        "data": payload.get("data", []),
        "paging": payload.get("paging", {}),
    }


# ── IG: COMMENTS (list + create/reply/moderation) ───────────────────────
@router.get("/comments")
async def ig_comments(media_id: str = Query(...), limit: int = 25):
    st = await load_state()
    async with RetryClient() as client:
        r = await client.get(
            f"{GRAPH_BASE}/{media_id}/comments",
            params={
                "access_token": st["page_token"],
                "limit": max(1, min(limit, 50)),
                "fields": "id,text,username,timestamp",
            },
            retries=4,
        )
        r.raise_for_status()
        payload = r.json()

    return {
        "ok": True,
        "data": payload.get("data", []),
        "paging": payload.get("paging", {}),
    }


# ---- MOCK DM (если нужно — оставляем, потом вынесем/удалим) ------------
MESSAGES = [
    {
        "id": "17894320123981723",
        "from": "user_123",
        "text": "Hello!",
        "timestamp": "2025-10-09T13:25:00Z",
    },
    {
        "id": "17894320123981724",
        "from": "user_456",
        "text": "Hi there!",
        "timestamp": "2025-10-09T13:26:10Z",
    },
]

@router.get("/messages")
async def get_ig_messages(limit: int = Query(5)):
    return {"messages": MESSAGES[: max(1, min(limit, 50))]}

@router.get("/message")
async def get_ig_message(message_id: str = Query(..., description="IG DM message id")):
    for m in MESSAGES:
        if m["id"] == message_id:
            return m
    raise HTTPException(status_code=404, detail="Message not found")

@router.post("/message/reply")
async def reply_ig_message(payload: dict = Body(...)):
    message_id = payload.get("message_id")
    message_text = payload.get("message")
    if not message_id or not message_text:
        raise HTTPException(
            status_code=400,
            detail="Fields 'message_id' and 'message' are required",
        )

    new_msg = {
        "id": str(uuid4().int)[:14],
        "from": "page_owner",
        "text": message_text,
        "in_reply_to": message_id,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    MESSAGES.insert(0, new_msg)
    return {"status": "ok", "replied_to": message_id, "message_id": new_msg["id"]}
# ---- /MOCK -------------------------------------------------------------


@router.post("/comment")
async def ig_comment(
    media_id: Optional[str] = Body(default=None),
    message: str = Body(..., embed=True),
    reply_to_comment_id: Optional[str] = Body(default=None, embed=True),
):
    if not media_id and not reply_to_comment_id:
        raise HTTPException(400, "Provide media_id OR reply_to_comment_id")

    st = await load_state()
    async with RetryClient() as client:
        if reply_to_comment_id:
            r = await client.post(
                f"{GRAPH_BASE}/{reply_to_comment_id}/replies",
                data={"access_token": st["page_token"], "message": message},
                retries=4,
            )
        else:
            r = await client.post(
                f"{GRAPH_BASE}/{media_id}/comments",
                data={"access_token": st["page_token"], "message": message},
                retries=4,
            )
        r.raise_for_status()

    return {"ok": True, "result": r.json()}


@router.post("/comments/hide")
async def ig_comment_hide(
    comment_id: str = Body(..., embed=True),
    hide: bool = Body(default=True, embed=True),
):
    st = await load_state()
    async with RetryClient() as client:
        try:
            r = await client.post(
                f"{GRAPH_BASE}/{comment_id}",
                data={
                    "hide": "true" if hide else "false",
                    "access_token": st["page_token"],
                },
                retries=4,
            )
            r.raise_for_status()
            return {"ok": True}
        except httpx.HTTPStatusError as e:
            return {
                "ok": False,
                "status": (e.response.status_code if e.response else None),
                "error": (e.response.json() if e.response else str(e)),
            }


@router.post("/comments/delete")
async def ig_comment_delete(comment_id: str = Body(..., embed=True)):
    st = await load_state()
    async with RetryClient() as client:
        r = await client.delete(
            f"{GRAPH_BASE}/{comment_id}",
            params={"access_token": st["page_token"]},
            retries=4,
        )
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            return {
                "ok": False,
                "status": e.response.status_code,
                "error": e.response.json(),
            }
    return {"ok": True}


@router.post("/comments/reply-many")
async def ig_comments_reply_many(
    comment_ids: List[str] = Body(..., embed=True),
    message: str = Body(..., embed=True),
    delay_ms: int = Body(default=600, embed=True),
):
    st = await load_state()
    results = []
    async with RetryClient() as client:
        for cid in comment_ids:
            try:
                r = await client.post(
                    f"{GRAPH_BASE}/{cid}/replies",
                    data={"access_token": st["page_token"], "message": message},
                    retries=4,
                )
                r.raise_for_status()
                results.append({"comment_id": cid, "ok": True, "id": r.json().get("id")})
            except httpx.HTTPStatusError as e:
                results.append(
                    {
                        "comment_id": cid,
                        "ok": False,
                        "status": (e.response.status_code if e.response else None),
                        "error": (e.response.json() if e.response else str(e)),
                    }
                )
            await asyncio.sleep(max(0.0, delay_ms / 1000.0))
    return {"ok": True, "results": results}


# ── IG: PUBLISH (image) ─────────────────────────────────────────────────
@router.post("/publish/image")
async def ig_publish_image(
    image_url: str = Body(..., embed=True),
    caption: Optional[str] = Body(default=None, embed=True),
):
    st = await load_state()
    async with RetryClient() as client:
        try:
            payload = {"image_url": image_url, "access_token": st["page_token"]}
            if caption:
                payload["caption"] = caption

            r1 = await client.post(
                f"{GRAPH_BASE}/{st['ig_id']}/media",
                data=payload,
                retries=4,
                timeout=60,
            )
            r1.raise_for_status()
            creation_id = r1.json().get("id")
            if not creation_id:
                return {"ok": False, "stage": "create", "error": "No creation_id in response"}

            wait = await _wait_container_ready(
                client,
                creation_id=creation_id,
                access_token=st["page_token"],
                max_wait_sec=60,
                sleep_sec=2,
            )
            if not wait.get("ok"):
                return wait

            r2 = await client.post(
                f"{GRAPH_BASE}/{st['ig_id']}/media_publish",
                data={"creation_id": creation_id, "access_token": st["page_token"]},
                retries=4,
                timeout=60,
            )
            r2.raise_for_status()
            return {"ok": True, "creation_id": creation_id, "published": r2.json()}

        except httpx.HTTPStatusError as e:
            try:
                err_json = e.response.json()
            except Exception:
                err_json = {"raw": (e.response.text[:500] if e.response else str(e))}
            return {
                "ok": False,
                "stage": "graph",
                "status": (e.response.status_code if e.response else None),
                "error": err_json,
            }
        except Exception as e:
            return {"ok": False, "stage": "client", "error": str(e)}

# ── IG: PUBLISH (REELS video) ──────────────────────────────────────────
@router.post("/publish/video")
async def ig_publish_video(
    video_url: str = Body(..., embed=True),
    caption: Optional[str] = Body(default=None, embed=True),
    cover_url: Optional[str] = Body(default=None, embed=True),
    share_to_feed: bool = Body(default=True, embed=True),
):
    return await publish_reel(
        video_url=video_url,
        caption=caption,
        cover_url=cover_url,
        share_to_feed=share_to_feed,
    )


@router.post("/publish/video_from_cloudinary")
async def ig_publish_video_from_cloudinary(
    public_id: str = Body(..., embed=True),
    caption: Optional[str] = Body(default=None, embed=True),
    share_to_feed: bool = Body(default=True, embed=True),
    cover_url: Optional[str] = Body(default=None, embed=True),
):
    if not CLOUDINARY_CLOUD:
        raise HTTPException(400, "Cloudinary not configured: set CLOUDINARY_CLOUD")

    base_url = f"https://res.cloudinary.com/{CLOUDINARY_CLOUD}/video/upload/{public_id}.mp4"

    return await publish_reel(
        video_url=base_url,   # трансформ внутри сервиса
        caption=caption,
        cover_url=cover_url,
        share_to_feed=share_to_feed,
    )


# ── IG: PUBLISH STORIES (image/video) ───────────────────────────────────
@router.post("/publish/story/image")
async def ig_publish_story_image(
    image_url: str = Body(..., embed=True),
):
    st = await load_state()
    async with RetryClient() as client:
        payload = {
            "access_token": st["page_token"],
            "image_url": image_url,
            "media_type": "STORIES",
        }
        try:
            r1 = await client.post(
                f"{GRAPH_BASE}/{st['ig_id']}/media",
                data=payload,
                retries=4,
                timeout=60,
            )
            r1.raise_for_status()
            creation_id = (r1.json() or {}).get("id")
            if not creation_id:
                return {"ok": False, "stage": "create", "error": "No creation_id in response"}

            wait = await _wait_container_ready(
                client,
                creation_id=creation_id,
                access_token=st["page_token"],
                max_wait_sec=60,
                sleep_sec=2,
            )
            if not wait.get("ok"):
                return wait

            r2 = await client.post(
                f"{GRAPH_BASE}/{st['ig_id']}/media_publish",
                data={"creation_id": creation_id, "access_token": st["page_token"]},
                retries=4,
                timeout=60,
            )
            r2.raise_for_status()
            return {"ok": True, "creation_id": creation_id, "published": r2.json()}
        except httpx.HTTPStatusError as e:
            try:
                err_json = e.response.json()
            except Exception:
                err_json = {"raw": (e.response.text[:500] if e.response else str(e))}
            return {
                "ok": False,
                "stage": "graph",
                "status": (e.response.status_code if e.response else None),
                "error": err_json,
            }
        except Exception as e:
            return {"ok": False, "stage": "client", "error": str(e)}


@router.post("/publish/story/video")
async def ig_publish_story_video(
    video_url: str = Body(..., embed=True),
):
    st = await load_state()
    async with RetryClient() as client:
        payload = {
            "access_token": st["page_token"],
            "video_url": video_url,
            "media_type": "STORIES",
        }
        try:
            r1 = await client.post(
                f"{GRAPH_BASE}/{st['ig_id']}/media",
                data=payload,
                retries=4,
                timeout=180,
            )
            r1.raise_for_status()
            creation_id = (r1.json() or {}).get("id")
            if not creation_id:
                return {"ok": False, "stage": "create", "error": "No creation_id in response"}

            wait = await _wait_container_ready(
                client,
                creation_id=creation_id,
                access_token=st["page_token"],
                max_wait_sec=150,
                sleep_sec=2,
            )
            if not wait.get("ok"):
                return wait

            r2 = await client.post(
                f"{GRAPH_BASE}/{st['ig_id']}/media_publish",
                data={"creation_id": creation_id, "access_token": st["page_token"]},
                retries=4,
                timeout=60,
            )
            r2.raise_for_status()
            return {"ok": True, "creation_id": creation_id, "published": r2.json()}
        except httpx.HTTPStatusError as e:
            try:
                err_json = e.response.json()
            except Exception:
                err_json = {"raw": (e.response.text[:500] if e.response else str(e))}
            return {
                "ok": False,
                "stage": "graph",
                "status": (e.response.status_code if e.response else None),
                "error": err_json,
            }
        except Exception as e:
            return {"ok": False, "stage": "client", "error": str(e)}


# ── IG: PROFILE ──────────────────────────────────────────────────────────
@router.get("/profile")
async def ig_profile(
    account_id: Optional[str] = Query(None, description="Account ID (page_id) to use"),
):
    """Получает базовую информацию о профиле Instagram"""
    st = await load_state(account_id=account_id)
    ig_id, page_token = st["ig_id"], st["page_token"]

    async with RetryClient() as client:
        r = await client.get(
            f"{GRAPH_BASE}/{ig_id}",
            params={
                "access_token": page_token,
                "fields": ",".join([
                    "id",
                    "username",
                    "name",
                    "biography",
                    "profile_picture_url",
                    "media_count",
                    "followers_count",
                    "follows_count",
                ]),
            },
            retries=4,
        )
        r.raise_for_status()
        data = r.json()

    return {
        "ok": True,
        "id": data.get("id"),
        "username": data.get("username"),
        "name": data.get("name"),
        "biography": data.get("biography"),
        "profile_picture_url": data.get("profile_picture_url"),
        "media_count": data.get("media_count"),
        "followers_count": data.get("followers_count"),
        "follows_count": data.get("follows_count"),
    }


# ── IG: INSIGHTS (media, smart metrics) ────────────────────────────────
REELS_METRICS = [
    "views",
    "likes",
    "comments",
    "shares",
    "saved",
    "total_interactions",
    "ig_reels_avg_watch_time",
    "ig_reels_video_view_total_time",
]
PHOTO_METRICS = [
    "impressions",
    "reach",
    "saved",
    "likes",
    "comments",
    "shares",
    "total_interactions",
]
CAROUSEL_METRICS = [
    "impressions",
    "reach",
    "saved",
    "likes",
    "comments",
    "shares",
    "total_interactions",
]
VIDEO_METRICS = ["views", "likes", "comments", "shares", "saved", "total_interactions"]

ACCOUNT_INSIGHT_ALLOWED = {
    "impressions",
    "reach",
    "profile_views",
    "email_contacts",
    "get_directions_clicks",
    "website_clicks",
    "phone_call_clicks",
    "text_message_clicks",
}


def _pick_metrics_for_media(product_type: str) -> List[str]:
    pt = (product_type or "").upper()
    if pt in ("REELS", "CLIPS"):
        return REELS_METRICS
    if pt in ("CAROUSEL_ALBUM", "CAROUSEL"):
        return CAROUSEL_METRICS
    if pt in ("IMAGE", "PHOTO"):
        return PHOTO_METRICS
    return VIDEO_METRICS


@router.get("/insights/media")
async def ig_media_insights(
    media_id: str = Query(..., description="Media ID"),
    metrics: str = Query("", description="Comma-separated metrics; if empty — auto by media type"),
):
    st = await load_state()
    async with RetryClient() as client:
        # media_type
        try:
            r1 = await client.get(
                f"{GRAPH_BASE}/{media_id}",
                params={"fields": "media_type", "access_token": st["page_token"]},
                retries=4,
            )
            r1.raise_for_status()
            media_type = (r1.json() or {}).get("media_type", "")
        except httpx.HTTPStatusError as e:
            return {
                "ok": False,
                "stage": "get_media_type",
                "status": (e.response.status_code if e.response is not None else None),
                "error": (e.response.json() if (e.response is not None) else str(e)),
            }

        # product_type (best-effort)
        product_type = None
        try:
            r2 = await client.get(
                f"{GRAPH_BASE}/{media_id}",
                params={"fields": "product_type", "access_token": st["page_token"]},
                retries=4,
            )
            if r2.status_code == 200:
                product_type = (r2.json() or {}).get("product_type")
        except Exception:
            pass

        mt_upper = (product_type or media_type or "").upper()
        req_metrics = (
            [m.strip() for m in metrics.split(",") if m.strip()]
            if metrics
            else _pick_metrics_for_media(mt_upper)
        )

        # safety: impressions often not allowed for some types
        if mt_upper in ("IMAGE", "PHOTO", "CAROUSEL", "CAROUSEL_ALBUM", "VIDEO"):
            req_metrics = [m for m in req_metrics if m != "impressions"]

        try:
            ins = await client.get(
                f"{GRAPH_BASE}/{media_id}/insights",
                params={"metric": ",".join(req_metrics), "access_token": st["page_token"]},
            )
            ins.raise_for_status()
        except httpx.HTTPStatusError as e:
            try:
                err_json = e.response.json()
            except Exception:
                err_json = {}
            return {
                "ok": False,
                "stage": "insights",
                "status": (e.response.status_code if e.response is not None else None),
                "error": err_json or str(e),
            }

        data = ins.json() or {}
        return {
            "ok": True,
            "media_type": media_type,
            "product_type": product_type,
            "metrics": req_metrics,
            "data": data,
        }


@router.get("/insights/account")
async def ig_account_insights(
    metrics: str = Query("impressions,reach,profile_views"),
    period: str = Query("day"),
    account_id: Optional[str] = Query(None, description="Account ID (page_id) to use"),
):
    st = await load_state(account_id=account_id)
    req_metrics = [m.strip() for m in metrics.split(",") if m.strip()]

    bad = [m for m in req_metrics if m not in ACCOUNT_INSIGHT_ALLOWED]
    if bad:
        return {
            "ok": False,
            "stage": "validate",
            "error": f"Unsupported metrics: {bad}. Allowed: {sorted(ACCOUNT_INSIGHT_ALLOWED)}",
        }

    allowed_periods = {"day", "week", "days_28"}
    if period not in allowed_periods:
        return {
            "ok": False,
            "stage": "validate",
            "error": f"Unsupported period: {period}. Allowed: {sorted(allowed_periods)}",
        }

    async with RetryClient() as client:
        try:
            params = {
                "metric": ",".join(req_metrics),
                "period": period,
                "access_token": st["page_token"],
            }
            # profile_views requires metric_type=total_value
            if "profile_views" in req_metrics:
                params["metric_type"] = "total_value"
            r = await client.get(
                f"{GRAPH_BASE}/{st['ig_id']}/insights",
                params=params,
                retries=4,
            )
            r.raise_for_status()
            data = r.json() or {}
            return {"ok": True, "metrics": req_metrics, "period": period, "data": data}
        except httpx.HTTPStatusError as e:
            try:
                err = e.response.json()
            except Exception:
                err = {
                    "status": e.response.status_code,
                    "text": (e.response.text[:500] if e.response else str(e)),
                }
            return {
                "ok": False,
                "stage": "graph_account_insights",
                "status": (e.response.status_code if e.response else None),
                "metrics": req_metrics,
                "period": period,
                "error": err,
                "hint": "Попробуй другой period (week/days_28) или метрику.",
            }


@router.post("/comment/after_publish")
async def ig_comment_after_publish(
    media_id: str = Body(..., embed=True),
    message: str = Body(..., embed=True),
):
    st = await load_state()
    async with RetryClient() as client:
        r = await client.post(
            f"{GRAPH_BASE}/{media_id}/comments",
            data={"message": message, "access_token": st["page_token"]},
            retries=4,
        )
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            return {"ok": False, "stage": "comment", "error": e.response.json()}
        return {"ok": True, "result": r.json()}


# 8) SCHEDULER (in-memory; dev)
JOBS: Dict[str, Dict[str, Any]] = {}


async def _publish_job(
    job_id: str, ig_id: str, page_token: str, creation_id: str, run_at: datetime
):
    wait = sleep_seconds_until(run_at)
    await asyncio.sleep(wait)
    if JOBS.get(job_id, {}).get("status") == "canceled":
        return
    async with RetryClient() as client:
        try:
            r = await client.post(
                f"{GRAPH_BASE}/{ig_id}/media_publish",
                data={"creation_id": creation_id, "access_token": page_token},
                retries=4,
            )
            r.raise_for_status()
            JOBS[job_id]["status"] = "done"
            JOBS[job_id]["result"] = r.json()
        except Exception as e:
            JOBS[job_id]["status"] = "error"
            JOBS[job_id]["error"] = str(e)


@router.post("/schedule")
async def ig_schedule(
    creation_id: str = Body(..., embed=True),
    publish_at: str = Body(..., embed=True),  # ISO, e.g. 2025-09-08T12:00:00Z
):
    st = await load_state()
    run_at = iso_to_utc(publish_at)
    job_id = uuid4().hex
    JOBS[job_id] = {
        "status": "scheduled",
        "creation_id": creation_id,
        "publish_at": run_at.isoformat(),
    }
    asyncio.create_task(
        _publish_job(job_id, st["ig_id"], st["page_token"], creation_id, run_at)
    )
    return {
        "ok": True,
        "job_id": job_id,
        "status": "scheduled",
        "publish_at_utc": run_at.isoformat(),
    }


@router.get("/schedule")
def ig_schedule_list():
    return {"ok": True, "jobs": JOBS}


@router.delete("/schedule/{job_id}")
def ig_schedule_cancel(job_id: str):
    if job_id not in JOBS:
        raise HTTPException(404, "Job not found")
    JOBS[job_id]["status"] = "canceled"
    return {"ok": True}


# 9) BATCH PUBLISH
@router.post("/publish/batch")
async def ig_publish_batch(
    items: List[Dict[str, Any]] = Body(..., embed=True),
    throttle_ms: int = Body(500, embed=True),
):
    st = await load_state()
    results = []
    async with RetryClient() as client:
        for it in items:
            t = (it.get("type") or "").lower()
            try:
                if t == "image":
                    payload = {
                        "image_url": it["image_url"],
                        "access_token": st["page_token"],
                    }
                    if it.get("caption"):
                        payload["caption"] = it["caption"]

                    r1 = await client.post(
                        f"{GRAPH_BASE}/{st['ig_id']}/media", data=payload
                    )
                    r1.raise_for_status()
                    creation_id = r1.json().get("id")
                    if not creation_id:
                        raise RuntimeError("No creation_id in response")

                    wait = await _wait_container_ready(
                        client,
                        creation_id=creation_id,
                        access_token=st["page_token"],
                        max_wait_sec=60,
                        sleep_sec=2,
                    )
                    if not wait.get("ok"):
                        results.append({"type": "image", "ok": False, "error": wait, "creation_id": creation_id})
                        continue

                    r2 = await client.post(
                        f"{GRAPH_BASE}/{st['ig_id']}/media_publish",
                        data={
                            "creation_id": creation_id,
                            "access_token": st["page_token"],
                        },
                    )
                    r2.raise_for_status()
                    results.append(
                        {
                            "type": "image",
                            "ok": True,
                            "creation_id": creation_id,
                            "published": r2.json(),
                        }
                    )

                elif t in ("video", "reel"):
                    payload = {
                        "video_url": it["video_url"],
                        "media_type": "REELS",
                        "access_token": st["page_token"],
                        "share_to_feed": (
                            "true" if it.get("share_to_feed", True) else "false"
                        ),
                    }
                    if it.get("caption"):
                        payload["caption"] = it["caption"]
                    if it.get("cover_url"):
                        payload["cover_url"] = it["cover_url"]

                    r1 = await client.post(
                        f"{GRAPH_BASE}/{st['ig_id']}/media", data=payload
                    )
                    r1.raise_for_status()
                    creation_id = r1.json().get("id")

                    done = False
                    waited = 0
                    while waited < 120:
                        rs = await client.get(
                            f"{GRAPH_BASE}/{creation_id}",
                            params={
                                "fields": "status_code",
                                "access_token": st["page_token"],
                            },
                        )
                        rs.raise_for_status()
                        sc = (rs.json() or {}).get("status_code")
                        if sc == "FINISHED":
                            done = True
                            break
                        if sc == "ERROR":
                            raise RuntimeError("Processing error")
                        await asyncio.sleep(2)
                        waited += 2

                    if not done:
                        raise RuntimeError("Processing timeout")

                    r2 = await client.post(
                        f"{GRAPH_BASE}/{st['ig_id']}/media_publish",
                        data={
                            "creation_id": creation_id,
                            "access_token": st["page_token"],
                        },
                    )
                    r2.raise_for_status()
                    results.append(
                        {
                            "type": "reel",
                            "ok": True,
                            "creation_id": creation_id,
                            "published": r2.json(),
                        }
                    )

                else:
                    results.append(
                        {"ok": False, "error": f"Unsupported type: {t}", "item": it}
                    )

            except httpx.HTTPStatusError as e:
                results.append(
                    {
                        "ok": False,
                        "status": e.response.status_code,
                        "error": e.response.json(),
                        "item": it,
                    }
                )
            except Exception as e:
                results.append({"ok": False, "error": str(e), "item": it})

            await asyncio.sleep(max(0.0, throttle_ms / 1000.0))

    return {"ok": True, "results": results}
