# media_router.py
import textwrap
import subprocess
from pathlib import Path
from typing import Optional, Dict, Any, List

from fastapi import APIRouter, Body, Query, HTTPException

from jobs import create_job, get_job, rpush_job
from ffmpeg_utils import FFMPEG, FFPROBE, has_ffmpeg, ffprobe_json
from file_utils import uuid_name, ext_from_url, public_url, download_to
from fonts_utils import PIL_OK

from paths import STATIC_DIR, UPLOAD_DIR, OUT_DIR

# OPTIONAL: Pillow modules (используем только если PIL_OK)
try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageEnhance
except Exception:
    Image = None  # type: ignore
    ImageDraw = None  # type: ignore
    ImageFont = None  # type: ignore
    ImageFilter = None  # type: ignore
    ImageEnhance = None  # type: ignore

# Pillow 10+ resampling constant (если Image доступен)
try:
    RESAMPLE_LANCZOS = Image.Resampling.LANCZOS  # type: ignore[attr-defined]
except Exception:
    RESAMPLE_LANCZOS = getattr(Image, "LANCZOS", getattr(Image, "BICUBIC", 3)) if Image else None


router = APIRouter(prefix="/media", tags=["media"])


def parse_aspect(aspect: Optional[str]) -> Optional[float]:
    if not aspect:
        return None
    s = str(aspect).strip()
    if ":" in s:
        a, b = s.split(":", 1)
        try:
            return float(a) / float(b)
        except Exception:
            return None
    try:
        return float(s)
    except Exception:
        return None


def image_open_rgba(path: Path):
    if not PIL_OK or Image is None:
        raise RuntimeError("Pillow (PIL) is not installed. Install pillow.")
    return Image.open(path).convert("RGBA")


def save_image_rgb(img, dst: Path, quality: int = 90):
    if not PIL_OK or Image is None:
        raise RuntimeError("Pillow (PIL) is not installed.")
    img_rgb = img.convert("RGB")
    img_rgb.save(dst, format="JPEG", quality=quality, optimize=True, progressive=True)


# 1) VALIDATE
@router.post("/validate")
async def media_validate(
    url: str = Body(..., embed=True),
    type: str = Body(..., embed=True, description="video|image"),
    target: str = Body("REELS", embed=True),
):
    try:
        ext = ext_from_url(url, default=".bin")
        tmp = UPLOAD_DIR / uuid_name("dl", ext)
        await download_to(url, tmp)
    except Exception as e:
        return {"ok": False, "stage": "download", "error": str(e)}

    info: Dict[str, Any] = {"path": str(tmp), "size": tmp.stat().st_size}
    compatible, reasons = True, []

    if type.lower() == "video":
        if not has_ffmpeg():
            return {"ok": False, "error": "ffmpeg/ffprobe is not available on server."}
        try:
            meta = ffprobe_json(tmp)
            info["ffprobe"] = meta

            vstreams = [s for s in meta.get("streams", []) if s.get("codec_type") == "video"]
            astreams = [s for s in meta.get("streams", []) if s.get("codec_type") == "audio"]
            fmt = meta.get("format", {})
            duration = float(fmt.get("duration", 0) or 0)

            if target.upper() == "REELS":
                if duration <= 0 or duration > 90:
                    compatible = False
                    reasons.append("Duration must be 0–90s for safe Reels.")

            if vstreams:
                v = vstreams[0]
                codec = v.get("codec_name")
                width = int(v.get("width") or 0)
                height = int(v.get("height") or 0)
                pix_fmt = v.get("pix_fmt")

                fps = 0.0
                try:
                    a, b = (v.get("r_frame_rate", "0/1") or "0/1").split("/")
                    fps = float(a) / float(b)
                except Exception:
                    pass

                if codec and codec != "h264":
                    compatible = False
                    reasons.append(f"Video codec {codec} != h264")
                if pix_fmt and pix_fmt != "yuv420p":
                    reasons.append(f"pix_fmt {pix_fmt} != yuv420p")
                if width > 1080 or height > 1920:
                    reasons.append("Resolution will be downscaled (OK).")
                if fps > 60:
                    reasons.append("FPS >60 — лучше снизить до 30.")

            if target.upper() == "REELS":
                if not astreams:
                    reasons.append("No audio stream — допустимо, но добавьте звук.")
                else:
                    ac = astreams[0].get("codec_name")
                    if ac and ac != "aac":
                        reasons.append(f"Audio codec {ac} != aac (will be transcoded).")

        except Exception as e:
            return {"ok": False, "stage": "ffprobe", "error": str(e)}

    else:
        if not PIL_OK:
            return {"ok": False, "error": "Pillow is not installed on server."}
        try:
            im_raw = Image.open(tmp)  # type: ignore
            w, h = im_raw.size
            info["image"] = {"width": w, "height": h, "mode": im_raw.mode}
            if target.upper() == "IMAGE" and max(w, h) > 2160:
                reasons.append("Очень крупное изображение — будет ужато до 1080 по длинной стороне.")
        except Exception as e:
            return {"ok": False, "stage": "image_open", "error": str(e)}

    return {
        "ok": True,
        "compatible": compatible,
        "reasons": reasons,
        "media_info": info,
        "local_url": public_url(tmp, STATIC_DIR),
    }


# 2) TRANSCODE VIDEO
@router.post("/transcode/video")
async def media_transcode_video(
    url: str = Body(..., embed=True),
    target_aspect: Optional[str] = Body(default="9:16", embed=True),
    max_duration_sec: int = Body(default=90, embed=True),
    max_width: int = Body(default=1080, embed=True),
    fps: int = Body(default=30, embed=True),
    normalize_audio: bool = Body(default=True, embed=True),
):
    if not has_ffmpeg():
        return {"ok": False, "error": "ffmpeg not available."}

    try:
        src = UPLOAD_DIR / uuid_name("src", ext_from_url(url, ".mp4"))
        await download_to(url, src)
    except Exception as e:
        return {"ok": False, "stage": "download", "error": str(e)}

    aspect = parse_aspect(target_aspect) or (9 / 16)
    out = OUT_DIR / uuid_name("ready", ".mp4")

    vf = [
        f"scale='min({max_width},iw)':-2",
        "setsar=1",
        f"crop='min(iw,ih*{aspect}):ih'",
        f"fps={fps}" if fps > 0 else None,
        "format=yuv420p",
    ]
    vf = [x for x in vf if x]

    af = ["loudnorm=I=-16:TP=-1.5:LRA=11"] if normalize_audio else []

    cmd = [
        FFMPEG, "-y",
        "-i", str(src),
        "-t", str(max_duration_sec),
        "-vf", ",".join(vf),
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "21",
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
    ]
    if af:
        cmd += ["-af", ",".join(af)]
    cmd += ["-c:a", "aac", "-b:a", "128k", str(out)]

    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        return {"ok": False, "stage": "ffmpeg", "stderr": (p.stderr or "")[-1000:]}

    return {"ok": True, "output_url": public_url(out, STATIC_DIR)}


# 3) RESIZE IMAGE
@router.post("/resize/image")
async def media_resize_image(
    url: str = Body(..., embed=True),
    target_aspect: str = Body("1:1", embed=True),
    max_width: int = Body(1080, embed=True),
    fit: str = Body("cover", embed=True, description="cover|contain"),
    background: str = Body("black", embed=True),
):
    if not PIL_OK:
        return {"ok": False, "error": "Pillow not installed."}
    try:
        src = UPLOAD_DIR / uuid_name("img", ext_from_url(url, ".jpg"))
        await download_to(url, src)
        img = image_open_rgba(src)
    except Exception as e:
        return {"ok": False, "stage": "download/open", "error": str(e)}

    asp = parse_aspect(target_aspect) or 1.0
    tw = max_width
    th = int(round(tw / asp))

    if fit == "contain":
        if isinstance(background, str) and background.lower() == "blur":
            bg = img.copy().resize((tw, th), RESAMPLE_LANCZOS).filter(ImageFilter.GaussianBlur(radius=24))  # type: ignore
            canvas = bg.convert("RGBA")
        else:
            try:
                canvas = Image.new("RGBA", (tw, th), background)  # type: ignore
            except Exception:
                canvas = Image.new("RGBA", (tw, th), "black")  # type: ignore

        img_ratio = img.width / img.height
        if img_ratio > asp:
            nw = tw
            nh = int(round(nw / img_ratio))
        else:
            nh = th
            nw = int(round(nh * img_ratio))

        img_res = img.resize((nw, nh), RESAMPLE_LANCZOS)
        x = (tw - nw) // 2
        y = (th - nh) // 2
        canvas.paste(img_res, (x, y), img_res)

        out = OUT_DIR / uuid_name("img_resized", ".jpg")
        save_image_rgb(canvas, out, quality=90)
        return {"ok": True, "output_url": public_url(out, STATIC_DIR)}

    # cover
    img_ratio = img.width / img.height
    if img_ratio > asp:
        new_w = int(round(img.height * asp))
        left = (img.width - new_w) // 2
        box = (left, 0, left + new_w, img.height)
    else:
        new_h = int(round(img.width / asp))
        top = (img.height - new_h) // 2
        box = (0, top, img.width, top + new_h)

    img_c = img.crop(box).resize((tw, th), RESAMPLE_LANCZOS)
    out = OUT_DIR / uuid_name("img_cover", ".jpg")
    save_image_rgb(img_c, out, quality=92)
    return {"ok": True, "output_url": public_url(out, STATIC_DIR)}


# 4) REEL COVER (grab frame + optional text)
@router.post("/reel-cover")
async def media_reel_cover(
    video_url: str = Body(..., embed=True),
    at: float = Body(1.0, embed=True),
    overlay: Optional[Dict[str, Any]] = Body(default=None, embed=True),
):
    if not has_ffmpeg():
        return {"ok": False, "error": "ffmpeg not available."}
    try:
        src = UPLOAD_DIR / uuid_name("vid", ext_from_url(video_url, ".mp4"))
        await download_to(video_url, src)
    except Exception as e:
        return {"ok": False, "stage": "download", "error": str(e)}

    frame = OUT_DIR / uuid_name("cover_frame", ".jpg")
    p = subprocess.run(
        [FFMPEG, "-y", "-ss", str(max(0.0, at)), "-i", str(src), "-frames:v", "1", "-q:v", "2", str(frame)],
        capture_output=True,
        text=True,
    )
    if p.returncode != 0:
        return {"ok": False, "stage": "ffmpeg", "stderr": (p.stderr or "")[-1000:]}

    if overlay and PIL_OK and Image is not None:
        try:
            img = Image.open(frame).convert("RGBA")  # type: ignore
            draw = ImageDraw.Draw(img)  # type: ignore
            text = (overlay or {}).get("text") or ""
            pos = (overlay or {}).get("pos") or "bottom"
            padding = int((overlay or {}).get("padding") or 32)

            if text:
                wrapped = textwrap.fill(text, width=20)
                font = ImageFont.load_default()  # type: ignore
                bbox = draw.multiline_textbbox((0, 0), wrapped, font=font, spacing=4, align="left")  # type: ignore
                tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]

                xy = (padding, img.height - th - padding) if pos == "bottom" else (padding, padding)
                bg = Image.new("RGBA", (tw + padding * 2, th + padding * 2), (0, 0, 0, 160))  # type: ignore
                img.paste(bg, (xy[0] - padding, xy[1] - padding), bg)
                draw.multiline_text(xy, wrapped, font=font, fill=(255, 255, 255, 255), spacing=4)  # type: ignore

            out = OUT_DIR / uuid_name("cover", ".jpg")
            save_image_rgb(img, out, quality=92)
            return {"ok": True, "cover_url": public_url(out, STATIC_DIR)}
        except Exception as e:
            return {"ok": True, "cover_url": public_url(frame, STATIC_DIR), "note": f"PIL overlay skipped: {e}"}

    return {"ok": True, "cover_url": public_url(frame, STATIC_DIR)}


# 5) WATERMARK (image or video)
@router.post("/watermark")
async def media_watermark(
    url: str = Body(..., embed=True),
    logo_url: str = Body(..., embed=True),
    position: str = Body("br", embed=True),
    opacity: float = Body(0.85, embed=True),
    margin: int = Body(24, embed=True),
    type: Optional[str] = Body(None, embed=True),
):
    ext = ext_from_url(url, "")
    is_video = type == "video" or ext.lower() in (".mp4", ".mov", ".m4v", ".webm")

    try:
        src = UPLOAD_DIR / uuid_name("wm_src", ext or ".bin")
        await download_to(url, src)
        logo = UPLOAD_DIR / uuid_name("wm_logo", ext_from_url(logo_url, ".png"))
        await download_to(logo_url, logo)
    except Exception as e:
        return {"ok": False, "stage": "download", "error": str(e)}

    if not is_video:
        if not PIL_OK or Image is None:
            return {"ok": False, "error": "Pillow not installed."}
        try:
            base = image_open_rgba(src)
            mark = Image.open(logo).convert("RGBA")  # type: ignore

            target_w = max(64, base.width // 6)
            ratio = target_w / mark.width
            mark = mark.resize((target_w, int(mark.height * ratio)), RESAMPLE_LANCZOS)

            if opacity < 1.0:
                alpha = mark.split()[-1].point(lambda p: int(p * opacity))
                mark.putalpha(alpha)

            if position in ("tr", "rt"):
                x = base.width - mark.width - margin
                y = margin
            elif position in ("tl", "lt"):
                x = margin
                y = margin
            elif position in ("bl", "lb"):
                x = margin
                y = base.height - mark.height - margin
            else:
                x = base.width - mark.width - margin
                y = base.height - mark.height - margin

            base.paste(mark, (x, y), mark)
            out = OUT_DIR / uuid_name("wm_img", ".jpg")
            save_image_rgb(base, out, quality=92)
            return {"ok": True, "output_url": public_url(out, STATIC_DIR)}
        except Exception as e:
            return {"ok": False, "stage": "image_wm", "error": str(e)}

    # video watermark
    if not has_ffmpeg():
        return {"ok": False, "error": "ffmpeg not available."}

    pos_map = {
        "tr": f"main_w-overlay_w-{margin}:{margin}",
        "tl": f"{margin}:{margin}",
        "bl": f"{margin}:main_h-overlay_h-{margin}",
        "br": f"main_w-overlay_w-{margin}:main_h-overlay_h-{margin}",
    }
    expr = pos_map.get(position, pos_map["br"])

    out = OUT_DIR / uuid_name("wm_vid", ".mp4")
    cmd = [
        FFMPEG, "-y",
        "-i", str(src),
        "-i", str(logo),
        "-filter_complex", f"[1]format=rgba,colorchannelmixer=aa={opacity}[lg];[0][lg]overlay={expr}",
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "21",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        str(out),
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        return {"ok": False, "stage": "ffmpeg", "stderr": (p.stderr or "")[-1000:]}

    return {"ok": True, "output_url": public_url(out, STATIC_DIR)}


# 6) FILTERS (image)
@router.post("/filter/image")
async def media_filter_image(
    url: str = Body(..., embed=True),
    preset: str = Body("cinematic", embed=True),
    intensity: float = Body(0.7, embed=True),
):
    if not PIL_OK or Image is None:
        return {"ok": False, "error": "Pillow not installed."}

    try:
        src = UPLOAD_DIR / uuid_name("flt_img", ext_from_url(url, ".jpg"))
        await download_to(url, src)
        img = Image.open(src).convert("RGB")  # type: ignore
    except Exception as e:
        return {"ok": False, "stage": "download/open", "error": str(e)}

    k = max(0.0, min(1.0, float(intensity)))
    pkey = (preset or "").lower().strip()

    try:
        out_img = img
        if pkey in ("b&w", "bw", "mono", "blackwhite"):
            out_img = img.convert("L").convert("RGB")
        else:
            out_img = ImageEnhance.Contrast(img).enhance(1 + 0.12 * k)  # type: ignore
            out_img = ImageEnhance.Color(out_img).enhance(1 + 0.10 * k)  # type: ignore
            out_img = out_img.filter(ImageFilter.GaussianBlur(radius=0.3 * k))  # type: ignore

        out = OUT_DIR / uuid_name("flt_img_out", ".jpg")
        out_img.save(out, quality=92, optimize=True, progressive=True)
        return {"ok": True, "preset": pkey, "intensity": k, "output_url": public_url(out, STATIC_DIR)}
    except Exception as e:
        return {"ok": False, "stage": "filter", "error": str(e)}


# 7) FILTER VIDEO (enqueue)
@router.post("/filter/video")
async def enqueue_filter_video(body: dict = Body(...)):
    url = body.get("url")
    preset = body.get("preset")
    intensity = body.get("intensity", 0.7)

    if not url:
        raise HTTPException(400, "Field 'url' is required")
    if preset is None:
        raise HTTPException(400, "Field 'preset' is required")

    payload = {"url": url, "preset": preset, "intensity": float(intensity)}

    job = await create_job(kind="video_filter", payload=payload)
    job_id = job["job_id"]

    await rpush_job(job_id)

    return {
        "ok": True,
        "job_id": job_id,
        "status_url": f"/media/filter/status?job_id={job_id}",
    }


@router.get("/filter/status")
async def media_filter_status(job_id: str):
    job = await get_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return {
        "ok": True,
        "job_id": job_id,
        "kind": job.get("kind"),
        "status": job.get("status"),
        "created_at": job.get("created_at"),
        "updated_at": job.get("updated_at"),
        "result": job.get("result"),
        "error": job.get("error"),
    }
