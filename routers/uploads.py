from pathlib import Path
import uuid

from fastapi import APIRouter, File, UploadFile, HTTPException

from cloudinary_utils import cloudinary_unsigned_upload_file
from config import settings


router = APIRouter(prefix="/uploads", tags=["uploads"])

ALLOWED_CONTENT_TYPES = {"image/jpeg", "image/png", "image/webp"}
ALLOWED_VIDEO_TYPES = {"video/mp4", "video/quicktime", "video/x-msvideo", "video/mpeg"}
MAX_UPLOAD_BYTES = 15 * 1024 * 1024
MAX_VIDEO_UPLOAD_BYTES = 100 * 1024 * 1024  # 100MB for videos


@router.post("/image")
async def upload_image(file: UploadFile = File(...)):
    if file.content_type not in ALLOWED_CONTENT_TYPES:
        raise HTTPException(400, "Unsupported file type. Use jpg/png/webp.")

    tmp_dir = Path(settings.MEDIA_TMP_DIR)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    suffix = Path(file.filename or "").suffix or ".jpg"
    tmp_path = tmp_dir / f"upload_{uuid.uuid4().hex}{suffix}"

    size = 0
    try:
        with tmp_path.open("wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_UPLOAD_BYTES:
                    raise HTTPException(413, "File too large (max 15MB).")
                f.write(chunk)

        cld = await cloudinary_unsigned_upload_file(tmp_path, resource_type="image", timeout_sec=120)
        image_url = cld.get("secure_url")
        if not image_url:
            raise HTTPException(502, "Cloudinary upload failed: no secure_url")
        return {"ok": True, "image_url": image_url}
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass


@router.post("/video")
async def upload_video(file: UploadFile = File(...)):
    if file.content_type not in ALLOWED_VIDEO_TYPES:
        raise HTTPException(400, "Unsupported file type. Use mp4/mov/avi/mpeg.")

    tmp_dir = Path(settings.MEDIA_TMP_DIR)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    suffix = Path(file.filename or "").suffix or ".mp4"
    tmp_path = tmp_dir / f"upload_{uuid.uuid4().hex}{suffix}"

    size = 0
    try:
        with tmp_path.open("wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_VIDEO_UPLOAD_BYTES:
                    raise HTTPException(413, f"File too large (max {MAX_VIDEO_UPLOAD_BYTES // (1024*1024)}MB).")
                f.write(chunk)

        cld = await cloudinary_unsigned_upload_file(tmp_path, resource_type="video", timeout_sec=300)
        video_url = cld.get("secure_url")
        if not video_url:
            raise HTTPException(502, "Cloudinary upload failed: no secure_url")
        return {"ok": True, "video_url": video_url}
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
