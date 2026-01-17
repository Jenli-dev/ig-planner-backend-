import asyncio
import subprocess
from typing import Optional, List

from fastapi import FastAPI, Body, Response
from fastapi.staticfiles import StaticFiles

from config import settings

from health_router import router as health_router
from media_router import router as media_router
from routers.auth import router as auth_router
from routers.util import router as util_router
from routers.flow import router as flow_router
from routers.me import router as me_router
from routers.ig import router as ig_router
from routers.webhooks import router as webhooks_router
from routers.ai import router as ai_router
from routers.uploads import router as uploads_router
from routers.analytics import router as analytics_router
from routers.accounts import router as accounts_router
from ai_worker import process_ai_job
from ffmpeg_utils import FFMPEG, has_ffmpeg
from file_utils import download_to, ext_from_url, uuid_name, public_url
from jobs import brpop_job, get_job, update_job_status, RUNNING, DONE, ERROR, close_redis
from paths import STATIC_DIR, UPLOAD_DIR, OUT_DIR, ensure_dirs



ensure_dirs()

app = FastAPI(title=settings.APP_NAME)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

app.include_router(health_router)
app.include_router(media_router)
app.include_router(auth_router)
app.include_router(util_router)
app.include_router(flow_router)
app.include_router(me_router)
app.include_router(ig_router)
app.include_router(webhooks_router)
app.include_router(ai_router)
app.include_router(uploads_router)
app.include_router(analytics_router)
app.include_router(accounts_router)

# ── JOB WORKERS (Redis-backed queue is handled inside jobs.py) ──────────
VIDEO_WORKERS = settings.VIDEO_WORKERS

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

            kind = (job.get("kind") or "").lower()
            if kind == "video_filter":
                await _process_video_task(job_id)
            elif kind in ("image_t2i", "image_i2i", "avatar_batch"):
                await process_ai_job(job_id, job)
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


# корневой пинг
@app.get("/")
def root():
    return {
        "ok": True,
        "service": "meta-ig-tools",
        "endpoints": [
            "/health",
            "/uploads/image",
            "/ai/generate/text",
            "/ai/generate/image",
            "/ai/generate/batch",
            "/ai/status",
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
            "/ig/publish/story/image",
            "/ig/publish/story/video",
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

        uvicorn.run(app, host=settings.HOST, port=settings.PORT)
    except Exception:
        pass
