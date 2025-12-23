# media_utils.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from fonts_utils import PIL_OK  # Pillow availability flag
from paths import STATIC_DIR, UPLOAD_DIR, OUT_DIR
from ffmpeg_utils import FFMPEG, FFPROBE, has_ffmpeg, ffprobe_json
from file_utils import uuid_name, ext_from_url, public_url, download_to

# OPTIONAL: Pillow objects (только если PIL_OK)
try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageEnhance  # type: ignore

    # Pillow 10+ resampling constant
    try:
        RESAMPLE_LANCZOS = Image.Resampling.LANCZOS  # type: ignore[attr-defined]
    except Exception:
        RESAMPLE_LANCZOS = getattr(Image, "LANCZOS", getattr(Image, "BICUBIC", 3))
except Exception:
    Image = None  # type: ignore
    ImageDraw = None  # type: ignore
    ImageFont = None  # type: ignore
    ImageFilter = None  # type: ignore
    ImageEnhance = None  # type: ignore
    RESAMPLE_LANCZOS = None


# ---- Backward-compatible helpers (legacy names) ------------------------

def _has_ffmpeg() -> bool:
    return has_ffmpeg()


def _public_url(local_path: Path) -> str:
    return public_url(local_path, STATIC_DIR)


def _ext_from_url(url: str, default: str = ".bin") -> str:
    return ext_from_url(url, default=default)


def _uuid_name(prefix: str, ext: str) -> str:
    return uuid_name(prefix, ext)


def _parse_aspect(aspect: Optional[str]) -> Optional[float]:
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


async def _download_to(url: str, dst_path: Path) -> Path:
    return await download_to(url, dst_path)


def _ffprobe_json(path: Path) -> Dict[str, Any]:
    return ffprobe_json(path)


def _image_open(path: Path):
    if not PIL_OK or Image is None:
        raise RuntimeError("Pillow (PIL) is not installed. Run: pip install pillow")
    return Image.open(path).convert("RGBA")


def _save_image_rgb(img, dst: Path, quality: int = 90) -> None:
    if not PIL_OK or Image is None:
        raise RuntimeError("Pillow (PIL) is not installed.")
    img_rgb = img.convert("RGB")
    img_rgb.save(dst, format="JPEG", quality=quality, optimize=True, progressive=True)
