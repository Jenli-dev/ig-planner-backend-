from fastapi import APIRouter, Body, Query, HTTPException
from typing import Optional, List
import asyncio
import httpx
from uuid import uuid4
from datetime import datetime, timezone

from services.ig_state import load_state
from http_client import RetryClient
from meta_config import GRAPH_BASE, CLOUDINARY_CLOUD
from cloudinary_utils import cld_inject_transform, CLOUD_REELS_TRANSFORM
from services.ig_publish import publish_reel


router = APIRouter(prefix="/ig", tags=["ig"])


# ── IG: latest media ────────────────────────────────────────────────────
@router.get("/media")
async def ig_media(limit: int = 12, after: Optional[str] = None):
    st = await load_state()
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
):
    st = await load_state()
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
            r = await client.get(
                f"{GRAPH_BASE}/{st['ig_id']}/insights",
                params={
                    "metric": ",".join(req_metrics),
                    "period": period,
                    "access_token": st["page_token"],
                },
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
