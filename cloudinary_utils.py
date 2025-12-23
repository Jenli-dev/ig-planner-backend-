# cloudinary_utils.py
import os
from pathlib import Path
from typing import Any, Dict, Optional

import httpx
from fastapi import HTTPException

# Cloudinary env
CLOUDINARY_CLOUD = os.getenv("CLOUDINARY_CLOUD", "").strip()
CLOUDINARY_UNSIGNED_PRESET = os.getenv("CLOUDINARY_UNSIGNED_PRESET", "").strip()


# --- Cloudinary auto-transform for Reels (если прислали прямой Cloudinary URL)
def cld_inject_transform(url: str, transform: str) -> str:
    marker = "/upload/"
    if "res.cloudinary.com" in url and marker in url and "/video/" in url:
        host, rest = url.split(marker, 1)
        first_seg = rest.split("/", 1)[0]
        # если уже есть трансформация (обычно сегмент с запятыми)
        if "," in first_seg:
            return url
        return f"{host}{marker}{transform}/{rest}"
    return url


# Рекомендуемая трансформация для Reels
CLOUD_REELS_TRANSFORM = (
    "c_fill,w_1080,h_1920,fps_30,vc_h264:baseline,br_3500k,ac_aac/so_0:20/f_mp4"
)


async def cloudinary_unsigned_upload_file(
    path: Path,
    *,
    resource_type: str = "video",
    folder: Optional[str] = None,
    public_id: Optional[str] = None,
    timeout_sec: int = 300,
) -> Dict[str, Any]:
    """
    Загрузка локального файла в Cloudinary (unsigned).
    Требуются ENV: CLOUDINARY_CLOUD и CLOUDINARY_UNSIGNED_PRESET.
    """
    cloud = (os.getenv("CLOUDINARY_CLOUD", CLOUDINARY_CLOUD) or "").strip()
    preset = (os.getenv("CLOUDINARY_UNSIGNED_PRESET", CLOUDINARY_UNSIGNED_PRESET) or "").strip()

    if not cloud or not preset:
        raise HTTPException(
            400,
            "Cloudinary not configured: set CLOUDINARY_CLOUD and CLOUDINARY_UNSIGNED_PRESET",
        )

    endpoint = f"https://api.cloudinary.com/v1_1/{cloud}/{resource_type}/upload"
    data: Dict[str, Any] = {"upload_preset": preset}
    if folder:
        data["folder"] = folder
    if public_id:
        data["public_id"] = public_id

    # mime/content-type
    content_type = (
        "video/mp4" if resource_type in ("video", "auto") else "image/jpeg"
    )
    files = {"file": (path.name, path.read_bytes(), content_type)}

    async with httpx.AsyncClient(timeout=timeout_sec) as client:
        r = await client.post(endpoint, data=data, files=files)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            try:
                err = e.response.json()
            except Exception:
                err = {"status": e.response.status_code, "text": e.response.text[:500]}
            raise HTTPException(502, f"Cloudinary upload failed: {err}") from None
        return r.json()


async def cloudinary_unsigned_upload_bytes(
    data_bytes: bytes,
    *,
    filename: str,
    resource_type: str = "image",
    folder: Optional[str] = None,
    public_id: Optional[str] = None,
    timeout_sec: int = 300,
) -> Dict[str, Any]:
    cloud = (os.getenv("CLOUDINARY_CLOUD", CLOUDINARY_CLOUD) or "").strip()
    preset = (os.getenv("CLOUDINARY_UNSIGNED_PRESET", CLOUDINARY_UNSIGNED_PRESET) or "").strip()

    if not cloud or not preset:
        raise HTTPException(
            400,
            "Cloudinary not configured: set CLOUDINARY_CLOUD and CLOUDINARY_UNSIGNED_PRESET",
        )

    endpoint = f"https://api.cloudinary.com/v1_1/{cloud}/{resource_type}/upload"
    form: Dict[str, Any] = {"upload_preset": preset}
    if folder:
        form["folder"] = folder
    if public_id:
        form["public_id"] = public_id

    content_type = (
        "image/jpeg" if resource_type in ("image", "auto") else "application/octet-stream"
    )
    files = {"file": (filename, data_bytes, content_type)}

    async with httpx.AsyncClient(timeout=timeout_sec) as client:
        r = await client.post(endpoint, data=form, files=files)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            try:
                err = e.response.json()
            except Exception:
                err = {"status": e.response.status_code, "text": e.response.text[:500]}
            raise HTTPException(502, f"Cloudinary upload failed: {err}") from None
        return r.json()
