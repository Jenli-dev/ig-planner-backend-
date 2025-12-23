# ffmpeg_utils.py
import os
import json
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, Optional, Set


# ---- resolve ffmpeg/ffprobe binaries (Homebrew, /usr/local, PATH)
def _pick_bin(*candidates: str) -> str:
    for c in candidates:
        if isinstance(c, str) and c:
            p = Path(c)
            if p.is_absolute() and p.exists():
                return c
            w = shutil.which(c)
            if w:
                return w
    return candidates[-1] if candidates else "ffmpeg"


FFMPEG = _pick_bin(
    os.getenv("FFMPEG_BIN"),
    "ffmpeg",
    "/opt/homebrew/bin/ffmpeg",
    "/usr/local/bin/ffmpeg",
)

FFPROBE = _pick_bin(
    os.getenv("FFPROBE_BIN"),
    "ffprobe",
    "/opt/homebrew/bin/ffprobe",
    "/usr/local/bin/ffprobe",
)


# ── availability ───────────────────────────────────────────────────────
def has_ffmpeg() -> bool:
    try:
        p1 = subprocess.run(
            [FFMPEG, "-version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        p2 = subprocess.run(
            [FFPROBE, "-version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return p1.returncode == 0 and p2.returncode == 0
    except Exception:
        return False


# ── versions ───────────────────────────────────────────────────────────
def ffmpeg_version() -> Optional[str]:
    try:
        p = subprocess.run([FFMPEG, "-version"], capture_output=True, text=True, check=False)
        if p.returncode != 0:
            return None
        first = (p.stdout or "").splitlines()[:1]
        return first[0].strip() if first else None
    except Exception:
        return None


def ffprobe_version() -> Optional[str]:
    try:
        p = subprocess.run([FFPROBE, "-version"], capture_output=True, text=True, check=False)
        if p.returncode != 0:
            return None
        first = (p.stdout or "").splitlines()[:1]
        return first[0].strip() if first else None
    except Exception:
        return None


# ── ffmpeg: filters (cached) ───────────────────────────────────────────
_FFMPEG_FILTERS_CACHE: Optional[Set[str]] = None


def ffmpeg_available_filters() -> Set[str]:
    """
    Возвращает множество имён доступных видеофильтров ffmpeg.
    Кешируется на время жизни процесса.
    """
    global _FFMPEG_FILTERS_CACHE
    if _FFMPEG_FILTERS_CACHE is not None:
        return _FFMPEG_FILTERS_CACHE

    try:
        p = subprocess.run(
            [FFMPEG, "-hide_banner", "-filters"],
            capture_output=True,
            text=True,
            check=False,
        )
        names: Set[str] = set()
        for line in (p.stdout or "").splitlines():
            line = line.strip()
            if not line or line.startswith(("#", "-", "Filters:")):
                continue
            parts = line.split()
            if len(parts) >= 2:
                cand = parts[1].strip()
                if cand and all(ch.isalnum() or ch in "._-" for ch in cand):
                    names.add(cand)

        _FFMPEG_FILTERS_CACHE = names
    except Exception:
        _FFMPEG_FILTERS_CACHE = set()

    return _FFMPEG_FILTERS_CACHE


def ffmpeg_has_filter(name: str) -> bool:
    return name in ffmpeg_available_filters()


# ── ffprobe ────────────────────────────────────────────────────────────
def ffprobe_json(path: Path) -> Dict[str, Any]:
    cmd = [
        FFPROBE,
        "-v",
        "error",
        "-show_format",
        "-show_streams",
        "-of",
        "json",
        str(path),
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip() or "ffprobe failed")
    try:
        return json.loads(p.stdout)
    except Exception as e:
        raise RuntimeError(f"ffprobe parse error: {e}")


# ── diagnostics for /health ────────────────────────────────────────────
def ffmpeg_diag() -> Dict[str, Any]:
    """
    Удобный диагностический пакет:
    - доступность ffmpeg / ffprobe
    - пути до бинарей
    - версии
    - наличие ключевых фильтров
    """
    ok = has_ffmpeg()
    key_filters = ["scale", "fps", "boxblur", "gblur", "vignette"]

    return {
        "ok": ok,
        "ffmpeg_bin": FFMPEG,
        "ffprobe_bin": FFPROBE,
        "ffmpeg_version": ffmpeg_version(),
        "ffprobe_version": ffprobe_version(),
        "filters": {f: ffmpeg_has_filter(f) for f in key_filters} if ok else {},
    }
