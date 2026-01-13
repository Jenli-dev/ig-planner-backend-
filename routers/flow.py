import asyncio
import time
import subprocess
from pathlib import Path
from typing import Dict, Optional, Any
import httpx
from fastapi import APIRouter, Body
from errors import fail

from paths import STATIC_DIR, OUT_DIR
from file_utils import uuid_name
from ffmpeg_utils import FFMPEG, has_ffmpeg
from fonts_utils import PIL_OK, pick_font
from meta_config import CLOUDINARY_CLOUD, CLOUDINARY_UNSIGNED_PRESET

from jobs import create_job, get_job, lpush_job
from services.ig_publish import publish_reel

PIL_AVAILABLE = False
Image = None
ImageDraw = None
textwrap = None
_save_image_rgb = None

if PIL_OK:
    try:
        from PIL import Image, ImageDraw
        import textwrap
        from media_utils import _save_image_rgb
        PIL_AVAILABLE = True
    except Exception:
        PIL_AVAILABLE = False
    

router = APIRouter(prefix="/flow", tags=["flow"])

async def _cloudinary_unsigned_upload_file(
    path: Path,
    *,
    resource_type: str = "video",
    folder: Optional[str] = None,
    public_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Загрузка локального файла в Cloudinary (unsigned).
    Требуются ENV: CLOUDINARY_CLOUD и CLOUDINARY_UNSIGNED_PRESET.
    """
    if not CLOUDINARY_CLOUD or not CLOUDINARY_UNSIGNED_PRESET:
        raise RuntimeError("Cloudinary not configured: set CLOUDINARY_CLOUD and CLOUDINARY_UNSIGNED_PRESET")
    endpoint = (
        f"https://api.cloudinary.com/v1_1/{CLOUDINARY_CLOUD}/{resource_type}/upload"
    )
    data = {"upload_preset": CLOUDINARY_UNSIGNED_PRESET}
    if folder:
        data["folder"] = folder
    if public_id:
        data["public_id"] = public_id

    # определим mime
    mime = "video/mp4" if resource_type == "video" else "image/jpeg"
    files = {"file": (path.name, path.read_bytes(), mime)}

    async with httpx.AsyncClient(timeout=300) as client:
        r = await client.post(endpoint, data=data, files=files)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            try:
                err = e.response.json()
            except Exception:
                err = {
                    "status": e.response.status_code,
                    "text": e.response.text[:500],
                }
            raise RuntimeError(f"Cloudinary upload failed: {err}")
        return r.json()


@router.post("/filter-and-publish")
async def flow_filter_and_publish(
    url: str = Body(..., embed=True),
    preset: str = Body("cinematic", embed=True),
    intensity: float = Body(0.7, embed=True),
    caption: Optional[str] = Body(None, embed=True),
    share_to_feed: bool = Body(True, embed=True),
    cover_url: Optional[str] = Body(None, embed=True),
    timeout_sec: int = Body(600, embed=True),
    poll_interval_sec: float = Body(1.5, embed=True),
    cloudinary_folder: Optional[str] = Body(None, embed=True),
):
    """
    Сценарий: фильтруем видео → заливаем в Cloudinary (unsigned) → публикуем в IG.
    Требуются ENV: IG_ACCESS_TOKEN (+ страница с IG бизнес-аккаунтом) и CLOUDINARY_*.
    """
    # 1) enqueue
    payload = {"url": url, "preset": preset, "intensity": float(intensity)}

    job = await create_job(kind="video_filter", payload=payload)
    job_id = job["job_id"]  # create_job всегда возвращает dict

    await lpush_job(job_id)
    
    # 2) wait for DONE (or ERROR/timeout)
    deadline = time.time() + max(10, timeout_sec)
    last_status = None
    while time.time() < deadline:
        j = await get_job(job_id)
        if not j:
            await asyncio.sleep(poll_interval_sec)
            continue
        st = (j.get("status") or "").upper()
        last_status = {"status": st, "result": j.get("result"), "error": j.get("error")}
        if st == "DONE":
            break
        if st == "ERROR":
            return {
                "ok": False,
                "stage": "filter",
                "job_id": job_id,
                "error": j.get("error") or "unknown error",
                "last_status": last_status,
            }
        await asyncio.sleep(poll_interval_sec)

    if not last_status or last_status.get("status") != "DONE":
        return {
            "ok": False,
            "stage": "filter",
            "job_id": job_id,
            "error": "timeout waiting filter result",
        }

    result = (await get_job(job_id) or {}).get("result") or {}
    out_url_local = result.get("output_url")
    if not out_url_local:
        return {
            "ok": False,
            "stage": "filter",
            "job_id": job_id,
            "error": "no output_url from filter",
        }

    # 3) upload to Cloudinary (unsigned)
    try:
        # превращаем output_url в абсолютный локальный путь
        # пример output_url: "/static/out/flt_vid_out_xxx.mp4"
        if out_url_local.startswith("/static/"):
            rel = out_url_local[len("/static/") :]  # "out/xxx.mp4"
            local_path = STATIC_DIR / rel
        else:
            # на всякий случай: возьмём basename и посмотрим в OUT_DIR
            local_path = OUT_DIR / Path(out_url_local).name

        if not local_path.exists():
            return {
                "ok": False,
                "stage": "cloudinary",
                "error": f"local file not found: {local_path} (from output_url={out_url_local})",
            }

        cld_resp = await _cloudinary_unsigned_upload_file(
            local_path,
            resource_type="video",
            folder=cloudinary_folder,
        )
        secure_url = cld_resp.get("secure_url")
        if not secure_url:
            return {
                "ok": False,
                "stage": "cloudinary",
                "error": "no secure_url in Cloudinary response",
            }

    except Exception as e:
        return fail(e, stage="cloudinary", job_id=job_id)

    # 4) publish to IG (используем наш уже существующий обработчик)
    try:
        publish_resp = await publish_reel(
            video_url=secure_url,
            caption=caption,
            cover_url=cover_url,
            share_to_feed=share_to_feed,
        )
    except Exception as e:
        return fail(e, stage="publish", job_id=job_id)

    return {
        "ok": True,
        "job_id": job_id,
        "filtered_local": out_url_local,
        "cloudinary": {
            "secure_url": secure_url,
            "public_id": cld_resp.get("public_id"),
        },
        "publish": publish_resp,
    }


# === FLOW: filter → cover → Cloudinary → publish =======================

@router.post("/filter-publish-with-cover")
async def flow_filter_publish_with_cover(
    url: str = Body(..., embed=True),
    preset: str = Body("cinematic", embed=True),
    intensity: float = Body(0.7, embed=True),
    caption: Optional[str] = Body(None, embed=True),
    share_to_feed: bool = Body(True, embed=True),
    # cover options
    at: float = Body(1.0, embed=True, description="секунда кадра для обложки"),
    title: Optional[str] = Body(None, embed=True),
    title_pos: str = Body("bottom", embed=True),  # bottom | top
    title_font: Optional[str] = Body(
        None, embed=True
    ),  # напр. "Inter" (если установлен)
    title_padding: int = Body(32, embed=True),
    cloudinary_folder: Optional[str] = Body(None, embed=True),
    timeout_sec: int = Body(600, embed=True),
    poll_interval_sec: float = Body(1.5, embed=True),
):
    """
    Фильтруем видео → извлекаем кадр и рисуем титул → грузим в Cloudinary → публикуем в IG с cover.
    Требуются ENV: IG_ACCESS_TOKEN (+страница с IG бизнес-аккаунтом) и CLOUDINARY_*.
    """
    # 1) фильтруем видео (enqueue + ожидание)
    payload = {"url": url, "preset": preset, "intensity": float(intensity)}

    job = await create_job(kind="video_filter", payload=payload)
    job_id = job["job_id"]

    await lpush_job(job_id)  # <-- ОБЯЗАТЕЛЬНО, иначе воркер не увидит job

    deadline = time.time() + max(10, timeout_sec)
    result = None
    while time.time() < deadline:
        j = await get_job(job_id)
        if j:
            st = (j.get("status") or "").upper()
            if st == "DONE":
                result = j.get("result") or {}
                break
            if st == "ERROR":
                return {
                    "ok": False,
                    "stage": "filter",
                    "job_id": job_id,
                    "error": j.get("error"),
                }
        await asyncio.sleep(poll_interval_sec)

    if not result or not result.get("output_url"):
        return {
            "ok": False,
            "stage": "filter",
            "job_id": job_id,
            "error": "timeout or no output_url",
        }

    # локальный путь до отфильтрованного видео
    if str(result["output_url"]).startswith("/static/"):
        rel = str(result["output_url"])[len("/static/") :]  # "out/xxx.mp4"
        local_video_path = STATIC_DIR / rel
    else:
        local_video_path = OUT_DIR / Path(str(result["output_url"])).name

    if not local_video_path.exists():
        return {
            "ok": False,
            "stage": "local_video",
            "error": f"not found: {local_video_path}",
        }

    # 2) вытаскиваем кадр для обложки (ffmpeg)
    if not has_ffmpeg():
        return {"ok": False, "stage": "ffmpeg", "error": "ffmpeg not available"}

    frame = OUT_DIR / uuid_name("cover_frame", ".jpg")
    p = await asyncio.to_thread(
        subprocess.run,
        [
            FFMPEG,
            "-y",
            "-ss", str(max(0.0, at)),
            "-i", str(local_video_path),
            "-frames:v", "1",
            "-q:v", "2",
            str(frame),
        ],
        capture_output=True,
        text=True,
    )
    if p.returncode != 0:
        return {"ok": False, "stage": "cover_frame", "stderr": p.stderr[-800:]}

    # 3) рисуем заголовок (если задан) — PIL
    cover_path = frame
    if title and PIL_AVAILABLE:
        try:
            img = Image.open(frame).convert("RGBA")
            draw = ImageDraw.Draw(img)
            font = pick_font(size=64, name=title_font)

            wrapped = textwrap.fill(title, width=20)
            # оценка размеров текста
            if hasattr(draw, "multiline_textbbox"):
                bbox = draw.multiline_textbbox(
                    (0, 0), wrapped, font=font, spacing=4, align="left"
                )
                tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
            else:
                try:
                    bbox = draw.textbbox((0, 0), wrapped, font=font)
                    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
                except Exception:
                    tw, th = draw.textsize(wrapped, font=font)

            pad = max(8, int(title_padding))
            if title_pos == "top":
                xy = (pad, pad)
            else:
                xy = (pad, img.height - th - pad)

            # полупрозрачный бэкграунд под текст
            bg = Image.new("RGBA", (tw + pad * 2, th + pad * 2), (0, 0, 0, 160))
            img.paste(bg, (xy[0] - pad, xy[1] - pad), bg)
            draw.multiline_text(
                xy, wrapped, font=font, fill=(255, 255, 255, 255), spacing=4
            )

            cover_rgba = OUT_DIR / uuid_name("cover", ".png")
            img.save(cover_rgba)

            # JPEG для Cloudinary (экономичнее)
            cover_jpg = OUT_DIR / uuid_name("cover", ".jpg")
            _save_image_rgb(Image.open(cover_rgba), cover_jpg, quality=92)
            cover_path = cover_jpg
        except Exception:
            # fail-safe — шлём исходный кадр
            cover_path = frame

    # 4) Cloudinary: грузим видео + обложку
    try:
        cld_video = await _cloudinary_unsigned_upload_file(
            local_video_path, resource_type="video", folder=cloudinary_folder
        )
        cld_cover = await _cloudinary_unsigned_upload_file(
            cover_path, resource_type="image", folder=cloudinary_folder
        )
    except Exception as e:
        return fail(e, stage="cloudinary", job_id=job_id)

    video_secure_url = cld_video.get("secure_url")
    cover_secure_url = cld_cover.get("secure_url")
    if not video_secure_url or not cover_secure_url:
        return fail(
            "upload failed",
            stage="cloudinary",
            job_id=job_id,
            video_ok=bool(video_secure_url),
            cover_ok=bool(cover_secure_url),
        )

    # 5) Публикуем в IG с cover_url
    try:
        publish_resp = await publish_reel(
            video_url=video_secure_url,
            caption=caption,
            cover_url=cover_secure_url,
            share_to_feed=share_to_feed,
        )
    except Exception as e:
        return fail(e, stage="publish", job_id=job_id)

    return {
        "ok": True,
        "job_id": job_id,
        "filtered_local": str(local_video_path),
        "cover_local": str(cover_path),
        "cloudinary": {
            "video_public_id": cld_video.get("public_id"),
            "video_url": video_secure_url,
            "cover_public_id": cld_cover.get("public_id"),
            "cover_url": cover_secure_url,
        },
        "publish": publish_resp,
    }


