# fonts_utils.py
import os
from pathlib import Path
from typing import Dict, List, Optional

# OPTIONAL: Pillow (без жёсткой зависимости)
try:
    from PIL import Image, ImageFont
    PIL_OK = True
except Exception:
    PIL_OK = False
    Image = None  # type: ignore
    ImageFont = None  # type: ignore


# ---- Fonts: robust discovery & presets ---------------------------------
CUSTOM_FONT_DIR = os.getenv("FONTS_DIR", "").strip()

_SYS_FONT_DIRS = [
    "/usr/share/fonts",
    "/usr/local/share/fonts",
    str(Path.home() / ".local/share/fonts"),
    "/Library/Fonts",
    "/System/Library/Fonts",
    "C:\\Windows\\Fonts",
]
if CUSTOM_FONT_DIR:
    _SYS_FONT_DIRS.insert(0, CUSTOM_FONT_DIR)

_FONT_FALLBACKS = [
    "Inter",
    "DejaVuSans",
    "NotoSans",
    "Roboto",
    "OpenSans",
    "Arial",
    "Helvetica",
    "SegoeUI",
    "SFNS",
    "SanFrancisco",
    "LiberationSans",
]

_FONT_CACHE: Dict[str, str] = {}
_FONT_INDEX: Optional[Dict[str, str]] = None


def _scan_fonts_once() -> Dict[str, str]:
    exts = {".ttf", ".otf", ".ttc"}
    found: Dict[str, str] = {}
    for root in _SYS_FONT_DIRS:
        p = Path(root)
        if not p.exists():
            continue
        for fp in p.rglob("*"):
            try:
                if fp.suffix.lower() in exts and fp.is_file():
                    found[fp.stem.lower()] = str(fp)
            except Exception:
                pass
    return found


def font_index() -> Dict[str, str]:
    global _FONT_INDEX
    if _FONT_INDEX is None:
        _FONT_INDEX = _scan_fonts_once()
    return _FONT_INDEX


def resolve_font_path(preferred_names: List[str]) -> Optional[str]:
    idx = font_index()
    names = [n.strip().lower() for n in preferred_names if n and n.strip()]
    for name in names:
        if name in idx:
            return idx[name]
        for stem, path in idx.items():
            if name in stem:
                return path
    return None


def pick_font(size: int = 48, name: Optional[str] = None):
    if not PIL_OK:
        raise RuntimeError("Pillow not available")

    candidates: List[str] = []
    if name:
        candidates.append(name)
    candidates.extend(_FONT_FALLBACKS)

    for cand in candidates:
        cache_key = f"{cand}:{size}".lower()
        if cache_key in _FONT_CACHE:
            try:
                return ImageFont.truetype(_FONT_CACHE[cache_key], size=size)  # type: ignore
            except Exception:
                pass

        path = resolve_font_path([cand])
        if path:
            try:
                font = ImageFont.truetype(path, size=size)  # type: ignore
                _FONT_CACHE[cache_key] = path
                return font
            except Exception:
                continue

    return ImageFont.load_default()  # type: ignore
