from typing import Optional, List
import time

import httpx
from fastapi import APIRouter, Body, HTTPException

from meta_config import CLOUDINARY_CLOUD, CLOUDINARY_UNSIGNED_PRESET
from paths import UPLOAD_DIR, OUT_DIR
from fonts_utils import font_index

router = APIRouter(prefix="/util", tags=["util"])

@router.post("/cloudinary/upload")
async def cloudinary_upload(
    file_url: str = Body(..., embed=True),
    resource_type: str = Body("auto", embed=True),
    folder: Optional[str] = Body(None, embed=True),
    public_id: Optional[str] = Body(None, embed=True),
):
    if not CLOUDINARY_CLOUD or not CLOUDINARY_UNSIGNED_PRESET:
        raise HTTPException(400, "Cloudinary env missing: set CLOUDINARY_CLOUD and CLOUDINARY_UNSIGNED_PRESET")

    endpoint = f"https://api.cloudinary.com/v1_1/{CLOUDINARY_CLOUD}/{resource_type}/upload"
    form = {"file": file_url, "upload_preset": CLOUDINARY_UNSIGNED_PRESET}
    if folder:
        form["folder"] = folder
    if public_id:
        form["public_id"] = public_id

    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.post(endpoint, data=form)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            try:
                err = e.response.json()
            except Exception:
                err = {"status_code": e.response.status_code, "text": e.response.text[:500]}
            return {"ok": False, "stage": "cloudinary", "error": err}

    payload = r.json()
    return {
        "ok": True,
        "resource_type": resource_type,
        "secure_url": payload.get("secure_url"),
        "public_id": payload.get("public_id"),
        "format": payload.get("format"),
        "width": payload.get("width"),
        "height": payload.get("height"),
        "duration": payload.get("duration"),
    }

@router.delete("/cleanup")
def cleanup_tmp(hours: int = 12):
    cutoff = time.time() - hours * 3600
    removed = []
    for d in [UPLOAD_DIR, OUT_DIR]:
        for p in d.glob("*"):
            try:
                if p.is_file() and p.stat().st_mtime < cutoff:
                    p.unlink()
                    removed.append(p.name)
            except Exception:
                pass
    return {"ok": True, "removed": removed, "count": len(removed)}

@router.get("/fonts")
def list_fonts(q: Optional[str] = None, limit: int = 100):
    idx = font_index()
    items = sorted(idx.items())
    if q:
        ql = q.lower()
        items = [(k, v) for k, v in items if ql in k]
    items = items[: max(1, min(limit, 500))]
    return {"ok": True, "count": len(items), "fonts": [{"name": k, "path": v} for k, v in items]}
