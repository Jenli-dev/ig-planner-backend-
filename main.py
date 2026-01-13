import os
import json
import asyncio
import uuid
import subprocess
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, HTTPException, Body, Query, Response
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles

from config import settings
from services.ig_state import load_state

from health_router import router as health_router
from media_router import router as media_router
from routers.auth import router as auth_router
from routers.util import router as util_router
from routers.flow import router as flow_router
from ffmpeg_utils import FFMPEG, has_ffmpeg
from file_utils import download_to, ext_from_url, uuid_name, public_url
from jobs import brpop_job, get_job, update_job_status, RUNNING, DONE, ERROR, close_redis
from paths import STATIC_DIR, UPLOAD_DIR, OUT_DIR, ensure_dirs


from http_client import RetryClient
from time_utils import iso_to_utc, sleep_seconds_until

from cloudinary_utils import cld_inject_transform, CLOUD_REELS_TRANSFORM

from meta_config import (
    GRAPH_BASE,
    ME_URL,
    VERIFY_TOKEN,
    IG_LONG_TOKEN,
    CLOUDINARY_CLOUD,
)


ensure_dirs()

app = FastAPI(title=settings.APP_NAME)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

app.include_router(health_router)
app.include_router(media_router)
app.include_router(auth_router)
app.include_router(util_router)
app.include_router(flow_router)

# ── JOB WORKERS (Redis-backed queue is handled inside jobs.py) ──────────
VIDEO_WORKERS = int(os.getenv("VIDEO_WORKERS", "1"))

async def _process_video_task(job_id: str) -> None:
    job = await get_job(job_id)
    if not job:
        await update_job_status(job_id, ERROR, error="Job not found")
        return

    payload = job.get("payload") or {}
    url = (payload.get("url") or "").strip()
    preset = (payload.get("preset") or "cinematic").strip().lower()
    try:
        intensity = float(payload.get("intensity", 0.7))
    except Exception:
        intensity = 0.7
    intensity = max(0.0, min(1.0, intensity))

    if not url:
        await update_job_status(job_id, ERROR, error="payload.url is required")
        return

    if not has_ffmpeg():
        await update_job_status(job_id, ERROR, error="ffmpeg not available on server")
        return

    # 1) Resolve input file (local /static/... or download)
    try:
        if url.startswith("/static/"):
            rel = url[len("/static/"):]
            src = STATIC_DIR / rel
            if not src.exists():
                await update_job_status(job_id, ERROR, error=f"Local file not found: {src}")
                return
        else:
            src = UPLOAD_DIR / uuid_name("src", ext_from_url(url, ".mp4"))
            await download_to(url, src)
    except Exception as e:
        await update_job_status(job_id, ERROR, error=f"download/open failed: {e}")
        return

    # 2) Build very small filter set
    k = intensity
    if preset in ("bw", "b&w", "mono", "blackwhite", "black_white"):
        vf = "hue=s=0"
    else:
        # cinematic-ish: slight contrast/sat + tiny gamma tweak
        # keep it simple and stable
        contrast = 1.0 + 0.20 * k
        saturation = 1.0 + 0.15 * k
        gamma = 1.0 - 0.05 * k
        vf = f"eq=contrast={contrast}:saturation={saturation}:gamma={gamma}"

    out = OUT_DIR / uuid_name("flt_vid_out", ".mp4")

    cmd = [
        FFMPEG, "-y",
        "-i", str(src),
        "-vf", vf,
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "21",
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        "-c:a", "aac",
        "-b:a", "128k",
        str(out),
    ]

    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        err = (p.stderr or "")[-1200:]
        await update_job_status(job_id, ERROR, error=f"ffmpeg failed: {err}")
        return

    await update_job_status(
        job_id,
        DONE,
        result={"output_url": public_url(out, STATIC_DIR)},
    )

async def _worker_loop(worker_idx: int):
    while True:
        job_id = await brpop_job(timeout=5)
        if not job_id:
            continue

        job = await get_job(job_id)
        if not job:
            continue

        try:
            await update_job_status(job_id, RUNNING)

            if (job.get("kind") or "").lower() == "video_filter":
                await _process_video_task(job_id)
            else:
                await update_job_status(
                    job_id, ERROR, error=f"Unknown job kind: {job.get('kind')}"
                )

        except asyncio.CancelledError:
            raise
        except Exception as e:
            await update_job_status(job_id, ERROR, error=str(e))

@app.on_event("startup")
async def _startup():
    app.state._workers = [
        asyncio.create_task(_worker_loop(i))
        for i in range(max(1, VIDEO_WORKERS))
    ]

@app.on_event("shutdown")
async def _shutdown():
    for t in getattr(app.state, "_workers", []):
        t.cancel()
    await asyncio.gather(*getattr(app.state, "_workers", []), return_exceptions=True)
    await close_redis()

@app.get("/webhooks/instagram", response_class=PlainTextResponse)
async def instagram_webhook_verify(
    hub_mode: str = Query(None, alias="hub.mode"),
    hub_challenge: str = Query(None, alias="hub.challenge"),
    hub_verify_token: str = Query(None, alias="hub.verify_token"),
):
    """
    Step 1: Webhook verify (GET) — Meta calls this once.
    We must return hub.challenge exactly.
    """
    if hub_mode == "subscribe" and hub_verify_token == VERIFY_TOKEN:
        return hub_challenge or ""
    raise HTTPException(status_code=403, detail="Verification failed")


@app.post("/webhooks/instagram")
async def instagram_webhook_events(payload: dict):
    """
    Step 2: Meta sends events here (POST)
    For Review, we must accept any payload and return 200 OK.
    """
    print("=== IG WEBHOOK EVENT ===")
    print(json.dumps(payload, indent=2))

    # For safety: ALWAYS return 200
    return {"status": "received", "ok": True}


# ── Pages diagnostics ───────────────────────────────────────────────────
@app.get("/me/pages")
async def me_pages():
    if not IG_LONG_TOKEN:
        return {"ok": False, "error": "IG_ACCESS_TOKEN not set"}

    out = []
    async with RetryClient() as client:
        r = await client.get(
            f"{ME_URL}/accounts",
            params={"access_token": IG_LONG_TOKEN, "fields": "id,name,access_token"},
            retries=4,
        )
        try:
            r.raise_for_status()
            pages_all = list(r.json().get("data") or [])
        except httpx.HTTPStatusError as e:
            return {
                "ok": False,
                "status": e.response.status_code,
                "error": e.response.json(),
            }
        if not pages_all:
            b = await client.get(
                f"{ME_URL}/businesses", params={"access_token": IG_LONG_TOKEN}
            )
            b.raise_for_status()
            for biz in b.json().get("data") or []:
                bid = biz.get("id")
                if not bid:
                    continue
                op = await client.get(
                    f"{GRAPH_BASE}/{bid}/owned_pages",
                    params={
                        "access_token": IG_LONG_TOKEN,
                        "fields": "id,name,access_token",
                    },
                )
                op.raise_for_status()
                pages_all.extend(op.json().get("data") or [])

        # по каждой странице пробуем вытащить привязанный IG business-аккаунт
        for p in pages_all:
            pid = p.get("id")
            name = p.get("name")
            ptok = (
                p.get("access_token") or IG_LONG_TOKEN
            )  # если у страницы нет собственного токена
            has_ig = None
            ig = None
            if pid:
                r2 = await client.get(
                    f"{GRAPH_BASE}/{pid}",
                    params={
                        "fields": "instagram_business_account{id,username}",
                        "access_token": ptok,
                    },
                )
                try:
                    r2.raise_for_status()
                    ig = (r2.json() or {}).get("instagram_business_account")
                    has_ig = bool(ig and ig.get("id"))
                except httpx.HTTPStatusError:
                    has_ig = False
            out.append(
                {"id": pid, "name": name, "has_instagram_business": has_ig, "ig": ig}
            )

    return {"ok": True, "pages": out}

# ── IG: latest media ────────────────────────────────────────────────────
@app.get("/ig/media")
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
@app.get("/ig/comments")
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

from uuid import uuid4

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

@app.get("/ig/messages")
async def get_ig_messages(limit: int = Query(5)):
    return {"messages": MESSAGES[:limit]}

@app.get("/ig/message")
async def get_ig_message(message_id: str = Query(..., description="IG DM message id")):
    for m in MESSAGES:
        if m["id"] == message_id:
            return m
    raise HTTPException(status_code=404, detail="Message not found")

@app.post("/ig/message/reply")
async def reply_ig_message(payload: dict = Body(...)):
    message_id = payload.get("message_id")
    message_text = payload.get("message")
    if not message_id or not message_text:
        raise HTTPException(status_code=400, detail="Fields 'message_id' and 'message' are required")

    new_msg = {
        "id": str(uuid4().int)[:14],
        "from": "page_owner",
        "text": message_text,
        "in_reply_to": message_id,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    MESSAGES.insert(0, new_msg)
    return {"status": "ok", "replied_to": message_id, "message_id": new_msg["id"]}
# ---- /Mock ----

@app.post("/ig/comment")
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


@app.post("/ig/comments/hide")
async def ig_comment_hide(
    comment_id: str = Body(..., embed=True), hide: bool = Body(default=True, embed=True)
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


@app.post("/ig/comments/delete")
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


@app.post("/ig/comments/reply-many")
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
@app.post("/ig/publish/image")
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
@app.post("/ig/publish/video")
async def ig_publish_video(
    video_url: str = Body(..., embed=True),
    caption: Optional[str] = Body(default=None, embed=True),
    cover_url: Optional[str] = Body(default=None, embed=True),
    share_to_feed: bool = Body(default=True, embed=True),
):
    video_url = cld_inject_transform(video_url, CLOUD_REELS_TRANSFORM)
    st = await load_state()

    async with RetryClient() as client:
        payload = {
            "access_token": st["page_token"],
            "video_url": video_url,
            "media_type": "REELS",
            "share_to_feed": "true" if share_to_feed else "false",
        }
        if caption:
            payload["caption"] = caption
        if cover_url:
            payload["cover_url"] = cover_url

        # 1) создать контейнер
        try:
            r1 = await client.post(
                f"{GRAPH_BASE}/{st['ig_id']}/media",
                data=payload,
                retries=4,
                timeout=180,
            )
            r1.raise_for_status()
        except httpx.HTTPStatusError as e:
            return {
                "ok": False,
                "stage": "create_container",
                "status": (e.response.status_code if e.response else None),
                "error": (e.response.json() if e.response else str(e)),
            }

        creation_id = (r1.json() or {}).get("id")
        if not creation_id:
            raise HTTPException(500, "Failed to create video container")

        # 2) ожидание обработки
        max_wait_sec = 150
        sleep_sec = 2
        waited = 0
        status_code = "IN_PROGRESS"
        status_text = None

        while waited < max_wait_sec:
            rstat = await client.get(
                f"{GRAPH_BASE}/{creation_id}",
                params={"fields": "status,status_code", "access_token": st["page_token"]},
                retries=4,
                timeout=60,
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
                break
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

        if status_code != "FINISHED":
            return {
                "ok": False,
                "stage": "timeout",
                "status_code": status_code,
                "status": status_text,
                "creation_id": creation_id,
                "waited_sec": waited,
            }

        # 3) публикация
        try:
            r2 = await client.post(
                f"{GRAPH_BASE}/{st['ig_id']}/media_publish",
                data={"creation_id": creation_id, "access_token": st["page_token"]},
                retries=4,
                timeout=60,
            )
            r2.raise_for_status()
        except httpx.HTTPStatusError as e:
            return {
                "ok": False,
                "stage": "publish",
                "status": (e.response.status_code if e.response else None),
                "creation_id": creation_id,
                "error": (e.response.json() if e.response else str(e)),
            }

        return {"ok": True, "creation_id": creation_id, "published": r2.json()}


@app.post("/ig/publish/video_from_cloudinary")
async def ig_publish_video_from_cloudinary(
    public_id: str = Body(..., embed=True),
    caption: Optional[str] = Body(default=None, embed=True),
    share_to_feed: bool = Body(default=True, embed=True),
    cover_url: Optional[str] = Body(default=None, embed=True),
):
    if not CLOUDINARY_CLOUD:
        raise HTTPException(400, "Cloudinary not configured: set CLOUDINARY_CLOUD")
    base_url = (
        f"https://res.cloudinary.com/{CLOUDINARY_CLOUD}/video/upload/{public_id}.mp4"
    )
    video_url = cld_inject_transform(base_url, CLOUD_REELS_TRANSFORM)
    # делегируем в общий обработчик
    return await ig_publish_video(
        video_url=video_url,
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
# Разрешённые метрики для account insights (Graph API)
ACCOUNT_INSIGHT_ALLOWED = {
    "impressions",
    "reach",
    "profile_views",
    "email_contacts",
    "get_directions_clicks",
    "website_clicks",
    "phone_call_clicks",
    "text_message_clicks",
    # "follower_count",  # добавь при необходимости, если используешь этот показатель
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


@app.get("/ig/insights/media")
async def ig_media_insights(
    media_id: str = Query(..., description="Media ID"),
    metrics: str = Query(
        "", description="Comma-separated metrics; if empty — auto by media type"
    ),
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

        # product_type (мягко)
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

        if mt_upper in ("IMAGE", "PHOTO", "CAROUSEL", "CAROUSEL_ALBUM", "VIDEO"):
            req_metrics = [m for m in req_metrics if m != "impressions"]

        try:
            ins = await client.get(
                f"{GRAPH_BASE}/{media_id}/insights",
                params={
                    "metric": ",".join(req_metrics),
                    "access_token": st["page_token"],
                },
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


@app.get("/ig/insights/account")
async def ig_account_insights(
    metrics: str = Query("impressions,reach,profile_views"),
    period: str = Query("day"),
):
    """
    Возвращает аккаунт-инсайты и НИКОГДА не роняет 500 из-за 400 от Graph.
    Если Graph вернул 400/403 и т.п. — отдаём {ok:false, ...} с подробностями.
    """
    st = await load_state()
    req_metrics = [m.strip() for m in metrics.split(",") if m.strip()]

    bad = [m for m in req_metrics if m not in ACCOUNT_INSIGHT_ALLOWED]
    if bad:
        return {
            "ok": False,
            "stage": "validate",
            "error": f"Unsupported metrics: {bad}. Allowed: {sorted(ACCOUNT_INSIGHT_ALLOWED)}",
        }

    # допустимые периоды у IG: day | week | days_28
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
            # Вернём понятную ошибку от Graph (частый кейс: «metric not available for this period»)
            try:
                err = e.response.json()
            except Exception:
                err = {"status": e.response.status_code, "text": (e.response.text[:500] if e.response else str(e))}
            return {
                "ok": False,
                "stage": "graph_account_insights",
                "status": (e.response.status_code if e.response else None),
                "metrics": req_metrics,
                "period": period,
                "error": err,
                "hint": "Попробуй другой period (week/days_28) или метрику. На свежих аккаунтах day часто пустой.",
            }

@app.post("/ig/comment/after_publish")
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
            return {
                "ok": False,
                "stage": "comment",
                "error": e.response.json(),
            }
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


@app.post("/ig/schedule")
async def ig_schedule(
    creation_id: str = Body(..., embed=True),
    publish_at: str = Body(..., embed=True),  # ISO, e.g. 2025-09-08T12:00:00Z
):
    st = await load_state()
    run_at = iso_to_utc(publish_at)
    job_id = uuid.uuid4().hex
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


@app.get("/ig/schedule")
def ig_schedule_list():
    return {"ok": True, "jobs": JOBS}


@app.delete("/ig/schedule/{job_id}")
def ig_schedule_cancel(job_id: str):
    if job_id not in JOBS:
        raise HTTPException(404, "Job not found")
    JOBS[job_id]["status"] = "canceled"
    return {"ok": True}


# 9) CAPTION SUGGEST
@app.post("/caption/suggest")
def caption_suggest(
    topic: str = Body(..., embed=True),
    tone: str = Body("friendly", embed=True),
    hashtags: Optional[List[str]] = Body(default=None, embed=True),
    cta: str = Body("Подписывайся, чтобы не пропустить новое!", embed=True),
):
    tone_map = {
        "friendly": "Давайте поговорим про",
        "bold": "Разносим мифы о",
        "expert": "Коротко по делу:",
        "fun": "Ну что, погнали:",
    }
    head = tone_map.get(tone, tone_map["friendly"])
    base = f"{head} {topic}."
    hs = ""
    if hashtags:
        hs = " " + " ".join(
            [h if h.startswith("#") else f"#{h}" for h in hashtags[:12]]
        )
    caption = f"{base}\n\n{cta}{hs}"
    return {"ok": True, "caption": caption}


# 10) BATCH PUBLISH
@app.post("/ig/publish/batch")
async def ig_publish_batch(
    items: List[Dict[str, Any]] = Body(..., embed=True),
    throttle_ms: int = Body(500, embed=True),
):
    st = await load_state()
    results = []
    async with httpx.AsyncClient(timeout=120) as client:
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


# корневой пинг
@app.get("/")
def root():
    return {
        "ok": True,
        "service": "meta-ig-tools",
        "endpoints": [
            "/health",
            "/auth/oauth/start",
            "/auth/oauth/callback",
            "/me/instagram",
            "/me/pages",
            "/auth/debug/scopes",
            "/ig/media",
            "/ig/comments",
            "/ig/comment",
            "/ig/comments/hide",
            "/ig/comments/delete",
            "/ig/comments/reply-many",
            "/ig/publish/image",
            "/ig/publish/video",
            "/ig/publish/video_from_cloudinary",
            "/ig/insights/media",
            "/ig/insights/account",
            "/media/validate",
            "/media/filter/video",
            "/media/filter/status",
            "/ig/schedule",
            "/caption/suggest",
            "/ig/publish/batch",
            "/ig/comment/after_publish",
            "/util/cloudinary/upload",
            "/util/cleanup",
            "/util/fonts",
        ],
    }


@app.head("/")
def root_head():
    return Response(status_code=200)


if __name__ == "__main__":
    try:
        import uvicorn

        uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
    except Exception:
        pass
