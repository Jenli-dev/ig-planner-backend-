import os
import secrets
import json
import time
import asyncio
import uuid
import re
import subprocess
import textwrap
import shutil
from jobs import create_job, get_job, update_job_status, RUNNING, DONE, ERROR
from typing import Optional, List, Dict, Any
from urllib.parse import urlencode
from pathlib import Path
import random

import httpx
from fastapi import FastAPI, HTTPException, Body, Query
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
from datetime import datetime, timezone

# OPTIONAL: Pillow (без жёсткой зависимости)
try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageEnhance

    PIL_OK = True
except Exception:
    PIL_OK = False
    Image = None  # чтобы не было NameError, если ниже случайно обратимся

# Pillow 10+ compatibility for resampling
if PIL_OK:
    try:
        RESAMPLE_LANCZOS = Image.Resampling.LANCZOS  # Pillow >=10
    except Exception:
        # Pillow <10 (или нет атрибута Resampling)
        RESAMPLE_LANCZOS = getattr(Image, "LANCZOS", getattr(Image, "BICUBIC", 3))
else:
    RESAMPLE_LANCZOS = None
# ── ENV ────────────────────────────────────────────────────────────────
load_dotenv()  # локально читает .env; на Render переменные берутся из Settings

# Meta / OAuth
APP_ID = os.getenv("META_APP_ID", "").strip()
APP_SECRET = os.getenv("META_APP_SECRET", "").strip()
REDIRECT_URI = os.getenv("META_REDIRECT_URI", "").strip()
META_AUTH = "https://www.facebook.com/v21.0/dialog/oauth"
META_GRAPH = "https://graph.facebook.com/v21.0"

# Алиасы для совместимости со старым кодом
GRAPH_BASE = META_GRAPH
ME_URL = f"{META_GRAPH}/me"
TOKEN_URL = f"{META_GRAPH}/oauth/access_token"

# Instagram / Cloudinary / прочее
IG_LONG_TOKEN = os.getenv("IG_ACCESS_TOKEN", "").strip()  # длинный user/page токен
PAGE_ID_ENV = os.getenv("PAGE_ID", "").strip()  # можно не задавать

CLOUDINARY_CLOUD = os.getenv("CLOUDINARY_CLOUD", "").strip()
CLOUDINARY_UNSIGNED_PRESET = os.getenv("CLOUDINARY_UNSIGNED_PRESET", "").strip()
JWT_SECRET = os.getenv("JWT_SECRET", "super_secret_key").strip()

# Для /oauth/start
STATE_STORE = set()


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


def _has_ffmpeg() -> bool:
    try:
        subprocess.run(
            [FFMPEG, "-version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        subprocess.run(
            [FFPROBE, "-version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return True
    except Exception:
        return False


# ── ffmpeg: фильтры (кеш доступности) ──────────────────────────────────
_FFMPEG_FILTERS_CACHE: Optional[set] = None


def _ffmpeg_available_filters() -> set:
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
        names = set()
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


def _ffmpeg_has_filter(name: str) -> bool:
    """Проверяет наличие конкретного фильтра ffmpeg (с кешем)."""
    return name in _ffmpeg_available_filters()


# --- Cloudinary auto-transform for Reels (если прислали прямой Cloudinary URL)
def _cld_inject_transform(url: str, transform: str) -> str:
    marker = "/upload/"
    if "res.cloudinary.com" in url and marker in url and "/video/" in url:
        host, rest = url.split(marker, 1)
        first_seg = rest.split("/", 1)[0]
        if "," in first_seg:
            return url
        return f"{host}{marker}{transform}/{rest}"
    return url


# Рекомендуемая трансформация для Reels
_CLOUD_REELS_TRANSFORM = (
    "c_fill,w_1080,h_1920,fps_30,vc_h264:baseline,br_3500k,ac_aac/so_0:20/f_mp4"
)

# Важно: публикация контента, комментарии, инсайты, страницы
SCOPES = ",".join(
    [
        "pages_show_list",
        "instagram_basic",
        "pages_read_engagement",
        "instagram_manage_insights",
        "pages_manage_metadata",
        "business_management",
        "instagram_manage_comments",
        "instagram_content_publish",
    ]
)

# ── static dirs ────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
UPLOAD_DIR = STATIC_DIR / "uploads"
OUT_DIR = STATIC_DIR / "out"
for d in [STATIC_DIR, UPLOAD_DIR, OUT_DIR]:
    d.mkdir(parents=True, exist_ok=True)

app = FastAPI()
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# --- worker loop + video processor (совместимо с новым jobs.py) ---


# === background jobs ===
VIDEO_WORKERS = int(os.getenv("VIDEO_WORKERS", "1"))
job_queue: asyncio.Queue[str] = asyncio.Queue()
app.state.worker_tasks: list[asyncio.Task] = []


async def _worker_loop():
    while True:
        job_id = await job_queue.get()
        try:
            job = get_job(job_id)
            if not job:
                continue

            update_job_status(job_id, RUNNING)
            # передаём job_id и payload
            result = await _process_video_task(job_id, job["payload"])

            # если обработчик сам не проставил DONE — делаем здесь
            j = get_job(job_id)
            if j and j["status"] not in (DONE, ERROR):
                update_job_status(job_id, DONE, result=result)
        except Exception as e:
            update_job_status(job_id, ERROR, error=str(e))
        finally:
            job_queue.task_done()


async def _process_video_task(job_id: str, payload: Dict) -> Dict:
    """
    Скачивает видео, собирает цепочку фильтров и прогоняет ffmpeg,
    обновляя прогресс в jobs: downloading → preparing → encoding (с % до 100).
    Требуются хелперы/константы: _has_ffmpeg, _download_to, _uuid_name,
    _ext_from_url, _ffmpeg_has_filter, _public_url, _ffprobe_json,
    а также FFMPEG, UPLOAD_DIR, OUT_DIR, update_job_status.
    """
    # --- импорт локальный на всякий

    # 0) старт: объявим прогресс
    update_job_status(job_id, RUNNING, result={"stage": "downloading", "progress": 10})

    url = payload["url"]
    preset = payload.get("preset", "cinematic")
    intensity = float(payload.get("intensity", 0.7))

    if not _has_ffmpeg():
        update_job_status(job_id, ERROR, error="ffmpeg not available")
        raise RuntimeError("ffmpeg not available")

    # 1) скачать исходник
    src = UPLOAD_DIR / _uuid_name("flt_vid", _ext_from_url(url, ".mp4"))
    await _download_to(url, src)
    update_job_status(
        job_id, RUNNING, result={"stage": "preparing_filters", "progress": 30}
    )

    # 2) нормализуем интенсивность
    k = max(0.0, min(1.0, intensity))

    def _chain(*filters: str) -> str:
        return ",".join([f for f in filters if f and str(f).strip()])

    pkey = (preset or "cinematic").lower().strip()

    # 3) словарь фильтров
    vf_map: Dict[str, str] = {
        "b&w": "hue=s=0",
        "warm": _chain(
            f"curves=red='0/0 {0.50}/{0.60+0.20*k:.3f} 1/1'",
            f"curves=blue='0/0 {0.40}/{0.30-0.20*k:.3f} 1/1'",
            f"eq=saturation={1+0.05*k:.3f}",
        ),
        "cool": _chain(
            f"curves=blue='0/0 {0.50}/{0.60+0.20*k:.3f} 1/1'",
            f"curves=red='0/0 {0.40}/{0.30-0.20*k:.3f} 1/1'",
            f"eq=saturation={1+0.03*k:.3f}",
        ),
        "boost": _chain(
            f"eq=contrast={1+0.25*k:.3f}:saturation={1+0.25*k:.3f}:brightness={0.02*k:.3f}",
            "unsharp",
        ),
        "cinematic": _chain(
            f"eq=contrast={1+0.12*k:.3f}:saturation={1+0.12*k:.3f}",
            f"gblur=sigma={0.30+0.70*k:.3f}",
            "unsharp",
            "vignette",
        ),
        "teal_orange": _chain(
            f"colorbalance=bs={-0.20*k:.3f}:gs=0:rs={0.10*k:.3f}",
            f"eq=saturation={1+0.10*k:.3f}",
            "vignette",
        ),
        "vignette": _chain("vignette"),
        "matte": _chain(
            f"eq=contrast={1-0.18*k:.3f}:saturation={1-0.05*k:.3f}",
        ),
        "pastel": _chain(
            f"eq=contrast={1-0.15*k:.3f}:saturation={1+0.05*k:.3f}",
            f"gblur=sigma={1.00+2.00*k:.3f}",
        ),
        "hdr": _chain(
            f"unsharp=luma_msize_x=7:luma_msize_y=7:luma_amount={1.0+1.2*k:.3f}",
            f"eq=contrast={1+0.20*k:.3f}",
        ),
        "sepia": _chain(
            "colorchannelmixer=.393:.769:.189:0:.349:.686:.168:0:.272:.534:.131:0:0:0:0:1",
            f"eq=contrast={1-0.18*k:.3f}:saturation={1-0.05*k:.3f}",
        ),
        "bleach_bypass": _chain(
            f"eq=saturation={1-0.35*k:.3f}:contrast={1+0.30*k:.3f}",
            f"noise=alls={5+20*k:.0f}:allf=t+u",
        ),
        "grain": _chain(
            f"noise=alls={10+50*k:.0f}:allf=t+u",
        ),
        "clarity": _chain(
            f"unsharp=luma_msize_x=7:luma_msize_y=7:luma_amount={1.0+1.8*k:.3f}",
        ),
        "fade_soft": _chain(
            f"eq=contrast={1-0.12*k:.3f}:saturation={1-0.08*k:.3f}:brightness={0.01*k:.3f}",
            f"gblur=sigma={0.50+1.00*k:.3f}",
        ),
        "deband": _chain(
            f"gradfun=strength={0.50+0.80*k:.3f}",
        ),
    }
    vf = vf_map.get(pkey, vf_map["cinematic"])

    # 4) проверка поддержки фильтров
    supp = {name: _ffmpeg_has_filter(name) for name in ["gblur", "boxblur", "vignette"]}

    gblur_fallback_used = False
    vignette_paramless_used = False
    vignette_removed = False

    # gblur → boxblur (или удалить)
    if "gblur" in vf and not supp.get("gblur"):
        if supp.get("boxblur"):
            vf = re.sub(
                r"gblur\s*=\s*sigma\s*=\s*([\d.]+)",
                lambda m: f"boxblur={round(2*float(m.group(1))+0.5, 2)}:1",
                vf,
            )
            gblur_fallback_used = True
        else:
            vf = re.sub(
                r"(,)?gblur\s*=\s*[^,]+(,)?",
                lambda m: "," if m.group(1) and m.group(2) else "",
                vf,
            ).strip(",")
            gblur_fallback_used = True

    # vignette → без параметров, либо удалить
    if "vignette" in vf:
        if supp.get("vignette"):
            # Проверим: поддерживает ли ffmpeg параметр vignette без аргументов
            vignette_paramless_used = True
            # Если фильтр не проходит, можно попробовать убрать его
            # (но оставим в цепочке как есть, если поддержка есть)
        else:
            # fallback — убираем vignette
            vf = vf.replace(",vignette", "")
            vignette_removed = True

    # --- stage: encoding ---
    update_job_status(
        job_id, RUNNING, result={"stage": "encoding", "progress": 70, "vf": vf}
    )

    # 5) запуск ffmpeg с прогрессом
    # Получим длительность исходника, чтобы нормировать прогресс
    total_dur = None
    try:
        meta = _ffprobe_json(src)  # type: ignore[name-defined]
        fmt = meta.get("format", {})
        total_dur = float(fmt.get("duration") or 0) or None
    except Exception:
        total_dur = None  # не критично

    out_path = OUT_DIR / _uuid_name("flt_vid_out", ".mp4")  # type: ignore[name-defined]
    cmd = [
        FFMPEG,
        "-y",
        "-i",
        str(src),  # type: ignore[name-defined]
        "-vf",
        vf,
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "21",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-movflags",
        "+faststart",
        str(out_path),
    ]

    # Асинхронный запуск ffmpeg и чтение stderr для прогресса
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )

    last_pct = 70  # мы уже показали "encoding" 70% перед запуском
    last_t = 0.0
    time_re = re.compile(r"time=(\d+):(\d+):(\d+(?:\.\d+)?)")

    while True:
        if proc.stderr is None:
            break
        line = await proc.stderr.readline()
        if not line:
            break
        s = line.decode("utf-8", errors="ignore").strip()

        m = time_re.search(s)
        if m:
            h, mi, sec = int(m.group(1)), int(m.group(2)), float(m.group(3))
            cur_sec = h * 3600 + mi * 60 + sec
            if total_dur and total_dur > 0:
                frac = min(1.0, max(0.0, cur_sec / total_dur))
                pct = 70 + int(frac * 29)  # 70..99
            else:
                pct = min(99, last_pct + 1)

            now = time.time()
            if pct != last_pct and (now - last_t) >= 0.3:
                update_job_status(
                    job_id,
                    RUNNING,
                    result={"stage": "encoding", "progress": pct, "vf": vf},
                )
                last_pct = pct
                last_t = now

    rc = await proc.wait()
    if rc != 0:
        err_tail = ""
        try:
            if proc.stderr is not None:
                rem = await proc.stderr.read()
                err_tail = (rem or b"").decode("utf-8", errors="ignore")[-500:]
        except Exception:
            pass
        update_job_status(job_id, ERROR, error=(err_tail or "ffmpeg failed")[-500:])
        raise RuntimeError(f"ffmpeg failed: {err_tail}")

    result = {
        "ok": True,
        "preset": pkey,
        "intensity": k,
        "vf": vf,
        "gblur_fallback": gblur_fallback_used,
        "vignette_paramless": vignette_paramless_used,
        "vignette_removed": vignette_removed,
        "output_url": _public_url(out_path),  # type: ignore[name-defined]
    }

    # --- stage: done ---
    update_job_status(job_id, DONE, result=result)
    return result


# сверху:
# from jobs import get_job, update_job_status, PENDING, RUNNING, DONE, ERROR


async def _worker_loop():
    while True:
        job_id = await job_queue.get()
        try:
            job = get_job(job_id)
            if not job:
                continue

            update_job_status(job_id, RUNNING)
            # передаём и job_id, и payload — удобно для логики сохранения результата
            result = await _process_video_task(job_id, job["payload"])

            # если процессор сам не проставил DONE — делаем это здесь
            j = get_job(job_id)
            if j and j["status"] not in (DONE, ERROR):
                update_job_status(job_id, DONE, result=result)
        except Exception as e:
            update_job_status(job_id, ERROR, error=str(e))
        finally:
            job_queue.task_done()


@app.on_event("startup")
async def _start_workers():
    for _ in range(VIDEO_WORKERS):
        task = asyncio.create_task(_worker_loop())
        app.state.worker_tasks.append(task)


@app.on_event("shutdown")
async def _stop_workers():
    for t in app.state.worker_tasks:
        t.cancel()


# ── CORS (если надо дёргать из фронта) ─────────────────────────────────
try:
    from fastapi.middleware.cors import CORSMiddleware

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
except Exception:
    pass


# ── helpers ────────────────────────────────────────────────────────────
def _public_url(local_path: Path) -> str:
    rel = local_path.relative_to(STATIC_DIR).as_posix()
    return f"/static/{rel}"


def _ext_from_url(url: str, default=".bin") -> str:
    guess = os.path.splitext(url.split("?")[0])[1]
    return guess if guess else default


async def _download_to(url: str, dst_path: Path) -> Path:
    """
    Скачивает файл по URL в dst_path.
    - follow_redirects=True — чтобы принимать 302 (picsum/fastly, и т.п.)
    - Вежливый User-Agent — для Wikimedia/CDN
    """
    headers = {
        "User-Agent": "ig-planner/1.0 (+https://ig-planner-backend.onrender.com)",
        "Accept": "*/*",
    }
    async with httpx.AsyncClient(
        timeout=120, headers=headers, follow_redirects=True
    ) as client:
        r = await client.get(url)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            status = e.response.status_code if e.response is not None else "?"
            location = (
                e.response.headers.get("Location") if e.response is not None else None
            )
            hint = f"\nRedirect location: {location}" if location else ""
            raise RuntimeError(f"Download failed ({status}) {url}{hint}") from None
        dst_path.write_bytes(r.content)
    return dst_path


# --- HTTP client with retries -------------------------------------------------
DEFAULT_TIMEOUT = httpx.Timeout(connect=10, read=30, write=30, pool=30)


class RetryClient(httpx.AsyncClient):
    async def request(
        self, method, url, *args, retries: int = 3, backoff: float = 0.5, **kwargs
    ):
        attempt = 0
        while True:
            try:
                return await super().request(
                    method,
                    url,
                    *args,
                    timeout=kwargs.pop("timeout", DEFAULT_TIMEOUT),
                    **kwargs,
                )
            except (
                httpx.ConnectError,
                httpx.ReadTimeout,
                httpx.WriteError,
                httpx.RemoteProtocolError,
            ):
                attempt += 1
                if attempt > retries:
                    raise
                await asyncio.sleep(
                    backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.2)
                )


def _uuid_name(prefix: str, ext: str) -> str:
    ext = ext if ext.startswith(".") else f".{ext}"
    return f"{prefix}_{uuid.uuid4().hex}{ext}"


def _parse_aspect(aspect: Optional[str]) -> Optional[float]:
    if not aspect:
        return None
    if ":" in aspect:
        a, b = aspect.split(":")
        try:
            return float(a) / float(b)
        except Exception:
            return None
    try:
        return float(aspect)
    except Exception:
        return None


def _ffprobe_json(path: Path) -> Dict[str, Any]:
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


def _image_open(path: Path) -> Image.Image:
    if not PIL_OK:
        raise RuntimeError("Pillow (PIL) is not installed. Run: pip install pillow")
    return Image.open(path).convert("RGBA")


def _save_image_rgb(img: Image.Image, dst: Path, quality=90):
    if not PIL_OK:
        raise RuntimeError("Pillow (PIL) is not installed.")
    img_rgb = img.convert("RGB")
    img_rgb.save(dst, format="JPEG", quality=quality, optimize=True, progressive=True)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso_to_utc(ts: str) -> datetime:
    if ts.endswith("Z"):
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)


def _sleep_seconds_until(dt: datetime) -> float:
    delta = (dt - _now_utc()).total_seconds()
    return max(0.0, delta)


# ---- Fonts: robust discovery & presets ---------------------------------
# Можно указать папку со своими шрифтами через ENV (положить туда .ttf/.otf)
CUSTOM_FONT_DIR = os.getenv("FONTS_DIR", "").strip()

# Часто встречающиеся системные папки со шрифтами (linux/macos/windows)
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

# Базовые кандидаты на «дефолтные» семейства (по убыванию предпочтения)
# Можно дополнять — главное, чтобы имя было частью файла (без расширения).
_FONT_FALLBACKS = [
    # популярные свободные
    "Inter",
    "DejaVuSans",
    "NotoSans",
    "Roboto",
    "OpenSans",
    # системные
    "Arial",
    "Helvetica",
    "SegoeUI",
    "SFNS",
    "SanFrancisco",
    "LiberationSans",
]

# Кеш найденных путей: {"inter:400": "/path/Inter-Regular.ttf", ...}
_FONT_CACHE: Dict[str, str] = {}


def _scan_fonts_once() -> Dict[str, str]:
    """
    Индексируем шрифты в указанных директориях (один раз за процесс).
    Ключ — имя файла без расширения в нижнем регистре.
    """
    exts = {".ttf", ".otf", ".ttc"}
    found: Dict[str, str] = {}
    for root in _SYS_FONT_DIRS:
        p = Path(root)
        if not p.exists():
            continue
        for fp in p.rglob("*"):
            try:
                if fp.suffix.lower() in exts and fp.is_file():
                    key = fp.stem.lower()  # например "inter-regular"
                    found[key] = str(fp)
            except Exception:
                pass
    return found


# Индекс шрифтов (лениво заполняется при первом вызове)
_FONT_INDEX: Optional[Dict[str, str]] = None


def _font_index() -> Dict[str, str]:
    global _FONT_INDEX
    if _FONT_INDEX is None:
        _FONT_INDEX = _scan_fonts_once()
    return _FONT_INDEX


def _resolve_font_path(preferred_names: List[str]) -> Optional[str]:
    """
    Пытается найти путь к шрифту по списку «человеческих» названий/семейств:
    ["Inter", "Inter-Regular", "Arial"] и т.п.
    Сопоставляет по вхождению имени в stem (без расширения), нечувствительно к регистру.
    """
    idx = _font_index()
    names = [n.strip().lower() for n in preferred_names if n and n.strip()]
    for name in names:
        # сначала точное совпадение
        if name in idx:
            return idx[name]
        # потом «по вхождению»
        for stem, path in idx.items():
            if name in stem:
                return path
    return None


def _pick_font(size: int = 48, name: Optional[str] = None) -> "ImageFont.FreeTypeFont":
    """
    Универсальный загрузчик шрифта.
    - name: желаемое семейство/файл (например, 'Inter', 'NotoSans', 'Arial', 'OpenSans-SemiBold').
    - если не найден — пробуем _FONT_FALLBACKS, затем PIL default.
    """
    if not PIL_OK:
        raise RuntimeError("Pillow not available")

    # соберём кандидатов: указанный name -> fallbacks
    candidates = []
    if name:
        candidates.append(name)
    candidates.extend(_FONT_FALLBACKS)

    # пробуем кеш и файловую систему
    for cand in candidates:
        cache_key = f"{cand}:{size}".lower()
        if cache_key in _FONT_CACHE:
            try:
                return ImageFont.truetype(_FONT_CACHE[cache_key], size=size)
            except Exception:
                pass
        path = _resolve_font_path([cand])
        if path:
            try:
                font = ImageFont.truetype(path, size=size)
                _FONT_CACHE[cache_key] = path
                return font
            except Exception:
                continue

    # крайний случай — встроенный шрифт PIL
    return ImageFont.load_default()


# ── LIVE state: берём всё из ENV и Graph API ────────────────────────────
async def _resolve_page_and_ig_id(client: RetryClient) -> Dict[str, Any]:
    """
    Возвращает page_id, ig_id (instagram_business_account.id) и ig_username.
    1) Если PAGE_ID задан в ENV — используем его.
    2) Иначе берём первую доступную страницу из /me/accounts.
    """
    if not IG_LONG_TOKEN:
        raise HTTPException(500, "IG_ACCESS_TOKEN is not set in env.")

    # 1) page_id
    page_id = PAGE_ID_ENV
    if not page_id:
        r = await client.get(
            f"{ME_URL}/accounts",
            params={"access_token": IG_LONG_TOKEN, "fields": "id,name,access_token"},
            retries=4,
        )
        r.raise_for_status()
        pages = r.json().get("data") or []
        if not pages:
            raise HTTPException(400, "No Pages available for this token.")
        page_id = pages[0]["id"]

    # 2) ig_id + username
    r2 = await client.get(
        f"{GRAPH_BASE}/{page_id}",
        params={
            "fields": "instagram_business_account{id,username}",
            "access_token": IG_LONG_TOKEN,
        },
        retries=4,
    )
    r2.raise_for_status()
    ig = r2.json().get("instagram_business_account") or {}
    ig_id = ig.get("id")
    ig_username = ig.get("username")
    if not ig_id:
        raise HTTPException(400, "This Page has no instagram_business_account linked.")

    return {"page_id": page_id, "ig_id": ig_id, "ig_username": ig_username}


async def _resolve_page_and_ig_id(client: RetryClient) -> Dict[str, Any]:
    # запрашиваем страницы пользователя
    r = await client.get(
        f"{ME_URL}/accounts",
        params={"access_token": IG_LONG_TOKEN, "fields": "id,name,access_token"},
        retries=4,
    )
    r.raise_for_status()
    pages = r.json().get("data", [])
    if not pages:
        raise HTTPException(400, "No pages found")

    page = pages[0]
    page_id = page["id"]

    # получаем IG аккаунт этой страницы
    r2 = await client.get(
        f"{GRAPH_BASE}/{page_id}",
        params={"fields": "instagram_business_account", "access_token": IG_LONG_TOKEN},
        retries=4,
    )
    r2.raise_for_status()
    ig_id = (r2.json().get("instagram_business_account") or {}).get("id")
    if not ig_id:
        raise HTTPException(400, "No Instagram business account linked")

    # получаем имя IG
    r3 = await client.get(
        f"{GRAPH_BASE}/{ig_id}",
        params={"fields": "username", "access_token": IG_LONG_TOKEN},
        retries=4,
    )
    r3.raise_for_status()
    ig_username = (r3.json() or {}).get("username")

    return {
        "page_id": page_id,
        "ig_id": ig_id,
        "ig_username": ig_username,
    }


async def _load_state() -> Dict[str, Any]:
    """
    Итоговое состояние для публикации:
    - page_access_token = IG_ACCESS_TOKEN (длинный)
    - ig_id из Graph API
    """
    async with RetryClient() as client:
        resolved = await _resolve_page_and_ig_id(client)
        return {
            "ig_id": resolved["ig_id"],
            "page_token": IG_LONG_TOKEN,  # используем как page_access_token
            "user_token": IG_LONG_TOKEN,  # и как user_long_lived
            "page_id": resolved["page_id"],
            "ig_username": resolved["ig_username"],
        }


# ── Healthcheck ──────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"ok": True, "ffmpeg": _has_ffmpeg(), "pillow": PIL_OK}


# ── OAuth start ─────────────────────────────────────────────────────────
@app.get("/oauth/start")
def oauth_start():
    if not APP_ID or not APP_SECRET or not REDIRECT_URI:
        raise HTTPException(
            500, "META_APP_ID / META_APP_SECRET / META_REDIRECT_URI are not set in env."
        )
    state = secrets.token_urlsafe(16)
    STATE_STORE.add(state)
    params = {
        "client_id": APP_ID,
        "redirect_uri": REDIRECT_URI,
        "state": state,
        "scope": SCOPES,
    }
    return RedirectResponse(f"{META_AUTH}?{urlencode(params)}")


# ── OAuth callback (без записи в файл; просто отдаём токены) ────────────
@app.get("/oauth/callback")
async def oauth_callback(
    code: Optional[str] = None, state: Optional[str] = None, error: Optional[str] = None
):
    if error:
        raise HTTPException(400, f"OAuth error: {error}")
    if not code or not state or state not in STATE_STORE:
        raise HTTPException(400, "Invalid state or code")
    STATE_STORE.discard(state)

    async with RetryClient() as client:  # 1) code -> short-lived user token
        r = await client.get(
            TOKEN_URL,
            params={
                "client_id": APP_ID,
                "client_secret": APP_SECRET,
                "redirect_uri": REDIRECT_URI,
                "code": code,
            },
            retries=4,
        )
        r.raise_for_status()
        short = r.json()

        # 2) short-lived -> long-lived user token
        r2 = await client.get(
            TOKEN_URL,
            params={
                "grant_type": "fb_exchange_token",
                "client_id": APP_ID,
                "client_secret": APP_SECRET,
                "fb_exchange_token": short["access_token"],
            },
        )
        r2.raise_for_status()
        long_user = r2.json()
        user_token = long_user["access_token"]

        # 3) вернём пользователю (без сохранения на диск)
        return {
            "ok": True,
            "short_lived": short,
            "long_lived": {
                "access_token": user_token,
                "token_type": long_user.get("token_type"),
                "expires_in": long_user.get("expires_in"),
            },
            "note": "Сохраните IG_ACCESS_TOKEN в переменных окружения сервера.",
        }


# рядом с /oauth/callback
@app.get("/auth/callback")
async def auth_callback(
    code: Optional[str] = None, state: Optional[str] = None, error: Optional[str] = None
):
    return await oauth_callback(code=code, state=state, error=error)


# ── Token tools: refresh & info ────────────────────────────────────────
@app.get("/auth/token-info")
async def auth_token_info():
    """
    Диагностика токена: /debug_token. Помогает понять срок действия и скоупы.
    """
    if not IG_LONG_TOKEN:
        raise HTTPException(400, "IG_ACCESS_TOKEN is not set in env.")
    if not APP_ID or not APP_SECRET:
        raise HTTPException(500, "META_APP_ID / META_APP_SECRET are not set.")
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(
            f"{GRAPH_BASE}/debug_token",
            params={
                "input_token": IG_LONG_TOKEN,
                "access_token": f"{APP_ID}|{APP_SECRET}",
            },
        )
        r.raise_for_status()
        return {"ok": True, "data": r.json().get("data", {})}


@app.post("/auth/refresh-token")
async def auth_refresh_token(current_token: Optional[str] = Body(None, embed=True)):
    """
    Обновляет long-lived user token ещё на ~60 дней.
    Если current_token не передан — берём IG_ACCESS_TOKEN из ENV.
    Возвращает НОВЫЙ токен — его нужно руками сохранить в Render env.
    """
    token_to_refresh = (current_token or IG_LONG_TOKEN or "").strip()
    if not token_to_refresh:
        raise HTTPException(
            400,
            "No token to refresh. Provide current_token or set IG_ACCESS_TOKEN in env.",
        )
    if not APP_ID or not APP_SECRET:
        raise HTTPException(500, "META_APP_ID / META_APP_SECRET are not set.")

    async with RetryClient() as client:
        r = await client.get(
            TOKEN_URL,
            params={
                "grant_type": "fb_exchange_token",
                "client_id": APP_ID,
                "client_secret": APP_SECRET,
                "fb_exchange_token": token_to_refresh,
            },
            retries=4,
        )
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            # отдадим подробности от Graph
            try:
                err = e.response.json()
            except Exception:
                err = {"error": {"message": e.response.text if e.response else str(e)}}
            raise HTTPException(e.response.status_code if e.response else 500, err)

        data = r.json()
        new_token = data.get("access_token")
        return {
            "ok": True,
            "new_access_token": new_token,
            "token_type": data.get("token_type"),
            "expires_in": data.get("expires_in"),
            "note": "Сохрани new_access_token в Render → Environment как IG_ACCESS_TOKEN и перезапусти сервис.",
        }


# ── Who am I (IG) ───────────────────────────────────────────────────────
@app.get("/me/instagram")
async def me_instagram():
    st = await _load_state()
    return {
        "ok": True,
        "page_id": st["page_id"],
        "instagram_business_account": {
            "id": st["ig_id"],
            "username": st["ig_username"],
        },
    }


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


# ── Debug scopes ────────────────────────────────────────────────────────
@app.get("/debug/scopes")
async def debug_scopes():
    if not IG_LONG_TOKEN:
        raise HTTPException(400, "IG_ACCESS_TOKEN not set")
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(
            f"{GRAPH_BASE}/debug_token",
            params={
                "input_token": IG_LONG_TOKEN,
                "access_token": f"{APP_ID}|{APP_SECRET}",
            },
        )
        r.raise_for_status()
        info = r.json().get("data", {})
    return {
        "ok": True,
        "is_valid": info.get("is_valid"),
        "scopes": info.get("scopes", []),
        "type": info.get("type"),
    }


# ── IG: latest media ────────────────────────────────────────────────────
@app.get("/ig/media")
async def ig_media(limit: int = 12, after: Optional[str] = None):
    st = await _load_state()
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
    st = await _load_state()
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


@app.post("/ig/comment")
async def ig_comment(
    media_id: Optional[str] = Body(default=None),
    message: str = Body(..., embed=True),
    reply_to_comment_id: Optional[str] = Body(default=None, embed=True),
):
    if not media_id and not reply_to_comment_id:
        raise HTTPException(400, "Provide media_id OR reply_to_comment_id")
    st = await _load_state()
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
    st = await _load_state()
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            f"{GRAPH_BASE}/{comment_id}",
            data={
                "hide": "true" if hide else "false",
                "access_token": st["page_token"],
            },
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


@app.post("/ig/comments/delete")
async def ig_comment_delete(comment_id: str = Body(..., embed=True)):
    st = await _load_state()
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.delete(
            f"{GRAPH_BASE}/{comment_id}", params={"access_token": st["page_token"]}
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
    st = await _load_state()
    results = []
    async with httpx.AsyncClient(timeout=20) as client:
        for cid in comment_ids:
            try:
                r = await client.post(
                    f"{GRAPH_BASE}/{cid}/replies",
                    data={"access_token": st["page_token"], "message": message},
                )
                r.raise_for_status()
                results.append(
                    {"comment_id": cid, "ok": True, "id": r.json().get("id")}
                )
            except httpx.HTTPStatusError as e:
                results.append(
                    {
                        "comment_id": cid,
                        "ok": False,
                        "status": e.response.status_code,
                        "error": e.response.json(),
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
    st = await _load_state()
    async with httpx.AsyncClient(timeout=60) as client:
        try:
            payload = {"image_url": image_url, "access_token": st["page_token"]}
            if caption:
                payload["caption"] = caption
            r1 = await client.post(f"{GRAPH_BASE}/{st['ig_id']}/media", data=payload)
            r1.raise_for_status()
            creation_id = r1.json().get("id")
            if not creation_id:
                return {
                    "ok": False,
                    "stage": "create",
                    "error": "No creation_id in response",
                }

            r2 = await client.post(
                f"{GRAPH_BASE}/{st['ig_id']}/media_publish",
                data={"creation_id": creation_id, "access_token": st["page_token"]},
            )
            r2.raise_for_status()
            return {"ok": True, "creation_id": creation_id, "published": r2.json()}
        except httpx.HTTPStatusError as e:
            try:
                err_json = e.response.json()
            except Exception:
                err_json = {"raw": e.response.text[:500]}
            return {
                "ok": False,
                "stage": "graph",
                "status": e.response.status_code,
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
    video_url = _cld_inject_transform(video_url, _CLOUD_REELS_TRANSFORM)
    st = await _load_state()

    async with httpx.AsyncClient(timeout=180) as client:
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

        r1 = await client.post(f"{GRAPH_BASE}/{st['ig_id']}/media", data=payload)
        try:
            r1.raise_for_status()
        except httpx.HTTPStatusError as e:
            return {
                "ok": False,
                "stage": "create_container",
                "status": e.response.status_code,
                "error": e.response.json(),
            }
        creation_id = (r1.json() or {}).get("id")
        if not creation_id:
            raise HTTPException(500, "Failed to create video container")

        # Ждём обработку
        max_wait_sec = 150
        sleep_sec = 2
        waited = 0
        status_code = "IN_PROGRESS"
        status_text = None

        while waited < max_wait_sec:
            rstat = await client.get(
                f"{GRAPH_BASE}/{creation_id}",
                params={
                    "fields": "status,status_code",
                    "access_token": st["page_token"],
                },
            )
            try:
                rstat.raise_for_status()
            except httpx.HTTPStatusError as e:
                return {
                    "ok": False,
                    "stage": "check_status",
                    "status": e.response.status_code,
                    "creation_id": creation_id,
                    "error": e.response.json(),
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

        r2 = await client.post(
            f"{GRAPH_BASE}/{st['ig_id']}/media_publish",
            data={"creation_id": creation_id, "access_token": st["page_token"]},
        )
        try:
            r2.raise_for_status()
        except httpx.HTTPStatusError as e:
            return {
                "ok": False,
                "stage": "publish",
                "status": e.response.status_code,
                "creation_id": creation_id,
                "error": e.response.json(),
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
    video_url = _cld_inject_transform(base_url, _CLOUD_REELS_TRANSFORM)
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
    st = await _load_state()
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
    st = await _load_state()
    req_metrics = [m.strip() for m in metrics.split(",") if m.strip()]
    bad = [m for m in req_metrics if m not in ACCOUNT_INSIGHT_ALLOWED]
    if bad:
        raise HTTPException(
            400,
            f"Unsupported metrics: {bad}. Allowed: {sorted(ACCOUNT_INSIGHT_ALLOWED)}",
        )

    async with RetryClient() as client:
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
        return r.json()


@app.post("/util/cloudinary/upload")
async def cloudinary_upload(
    file_url: str = Body(..., embed=True),
    resource_type: str = Body("auto", embed=True),
    folder: Optional[str] = Body(None, embed=True),
    public_id: Optional[str] = Body(None, embed=True),
):
    if not CLOUDINARY_CLOUD or not CLOUDINARY_UNSIGNED_PRESET:
        raise HTTPException(
            400,
            "Cloudinary env missing: set CLOUDINARY_CLOUD and CLOUDINARY_UNSIGNED_PRESET",
        )
    form = {"file": file_url, "upload_preset": CLOUDINARY_UNSIGNED_PRESET}
    if folder:
        form["folder"] = folder
    if public_id:
        form["public_id"] = public_id
    endpoint = (
        f"https://api.cloudinary.com/v1_1/{CLOUDINARY_CLOUD}/{resource_type}/upload"
    )
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            r = await client.post(endpoint, data=form)
            r.raise_for_status()
            payload = r.json()
    except httpx.HTTPStatusError as e:
        try:
            err = e.response.json()
        except Exception:
            err = {"status_code": e.response.status_code, "text": e.response.text[:500]}
        return {"ok": False, "stage": "cloudinary", "error": err}
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


@app.post("/ig/comment/after_publish")
async def ig_comment_after_publish(
    media_id: str = Body(..., embed=True),
    message: str = Body(..., embed=True),
):
    st = await _load_state()
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


# === FLOW: filter → (upload to Cloudinary) → publish to IG =============


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
        raise HTTPException(
            400,
            "Cloudinary not configured: set CLOUDINARY_CLOUD and CLOUDINARY_UNSIGNED_PRESET",
        )

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
            raise HTTPException(502, f"Cloudinary upload failed: {err}")
        return r.json()


@app.post("/flow/filter-and-publish")
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
    try:
        job_obj_or_id = create_job(kind="video_filter", payload=payload)
    except TypeError:
        # совместимость со старой сигнатурой
        job_obj_or_id = create_job(payload=payload)

    job_id = getattr(job_obj_or_id, "id", job_obj_or_id)
    if not isinstance(job_id, str):
        job_id = str(job_id)
    await job_queue.put(job_id)

    # 2) wait for DONE (or ERROR/timeout)
    deadline = time.time() + max(10, timeout_sec)
    last_status = None
    while time.time() < deadline:
        j = get_job(job_id)
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

    result = (get_job(job_id) or {}).get("result") or {}
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

    except HTTPException as he:
        # пробрасываем как есть — FastAPI сам превратит в ответ
        raise he
    except Exception as e:
        return {"ok": False, "stage": "cloudinary", "error": str(e)}

    # 4) publish to IG (используем наш уже существующий обработчик)
    try:
        publish_resp = await ig_publish_video(
            video_url=secure_url,
            caption=caption,
            cover_url=cover_url,
            share_to_feed=share_to_feed,
        )
    except HTTPException as he:
        raise he
    except Exception as e:
        return {"ok": False, "stage": "publish", "error": str(e)}

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


async def _cloudinary_unsigned_upload_bytes(
    data: bytes,
    *,
    filename: str,
    resource_type: str = "image",
    folder: Optional[str] = None,
    public_id: Optional[str] = None,
) -> Dict[str, Any]:
    if not CLOUDINARY_CLOUD or not CLOUDINARY_UNSIGNED_PRESET:
        raise HTTPException(
            400,
            "Cloudinary not configured: set CLOUDINARY_CLOUD and CLOUDINARY_UNSIGNED_PRESET",
        )

    endpoint = (
        f"https://api.cloudinary.com/v1_1/{CLOUDINARY_CLOUD}/{resource_type}/upload"
    )
    form = {"upload_preset": CLOUDINARY_UNSIGNED_PRESET}
    if folder:
        form["folder"] = folder
    if public_id:
        form["public_id"] = public_id

    files = {
        "file": (
            filename,
            data,
            "image/jpeg" if resource_type == "image" else "application/octet-stream",
        )
    }

    async with httpx.AsyncClient(timeout=300) as client:
        r = await client.post(endpoint, data=form, files=files)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            try:
                err = e.response.json()
            except Exception:
                err = {"status": e.response.status_code, "text": e.response.text[:500]}
            raise HTTPException(502, f"Cloudinary upload failed: {err}")
        return r.json()


@app.post("/flow/filter-publish-with-cover")
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
    try:
        job_obj_or_id = create_job(kind="video_filter", payload=payload)
    except TypeError:
        job_obj_or_id = create_job(payload=payload)

    job_id = getattr(job_obj_or_id, "id", job_obj_or_id)
    if not isinstance(job_id, str):
        job_id = str(job_id)
    await job_queue.put(job_id)

    deadline = time.time() + max(10, timeout_sec)
    result = None
    while time.time() < deadline:
        j = get_job(job_id)
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
    if not _has_ffmpeg():
        return {"ok": False, "stage": "ffmpeg", "error": "ffmpeg not available"}

    frame = OUT_DIR / _uuid_name("cover_frame", ".jpg")
    p = subprocess.run(
        [
            FFMPEG,
            "-y",
            "-ss",
            str(max(0.0, at)),
            "-i",
            str(local_video_path),
            "-frames:v",
            "1",
            "-q:v",
            "2",
            str(frame),
        ],
        capture_output=True,
        text=True,
    )
    if p.returncode != 0:
        return {"ok": False, "stage": "cover_frame", "stderr": p.stderr[-800:]}

    # 3) рисуем заголовок (если задан) — PIL
    cover_path = frame
    if title and PIL_OK:
        try:
            img = Image.open(frame).convert("RGBA")
            draw = ImageDraw.Draw(img)
            font = _pick_font(size=64, name=title_font)

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

            cover_rgba = OUT_DIR / _uuid_name("cover", ".png")
            img.save(cover_rgba)

            # JPEG для Cloudinary (экономичнее)
            cover_jpg = OUT_DIR / _uuid_name("cover", ".jpg")
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
    except HTTPException as he:
        raise he
    except Exception as e:
        return {"ok": False, "stage": "cloudinary", "error": str(e)}

    video_secure_url = cld_video.get("secure_url")
    cover_secure_url = cld_cover.get("secure_url")
    if not video_secure_url or not cover_secure_url:
        return {
            "ok": False,
            "stage": "cloudinary",
            "error": "upload failed",
            "video_ok": bool(video_secure_url),
            "cover_ok": bool(cover_secure_url),
        }

    # 5) Публикуем в IG с cover_url
    try:
        publish_resp = await ig_publish_video(
            video_url=video_secure_url,
            caption=caption,
            cover_url=cover_secure_url,
            share_to_feed=share_to_feed,
        )
    except HTTPException as he:
        raise he
    except Exception as e:
        return {"ok": False, "stage": "publish", "error": str(e)}

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


# ======================================================================
#                           MEDIA TOOLBOX
# ======================================================================


# 1) VALIDATE
@app.post("/media/validate")
async def media_validate(
    url: str = Body(..., embed=True),
    type: str = Body(..., embed=True, description="video|image"),
    target: str = Body("REELS", embed=True),
):
    try:
        ext = _ext_from_url(url, default=".bin")
        tmp = UPLOAD_DIR / _uuid_name("dl", ext)
        await _download_to(url, tmp)
    except Exception as e:
        return {"ok": False, "stage": "download", "error": str(e)}

    info: Dict[str, Any] = {"path": str(tmp), "size": tmp.stat().st_size}
    compatible, reasons = True, []

    if type.lower() == "video":
        if not _has_ffmpeg():
            return {"ok": False, "error": "ffmpeg/ffprobe is not available on server."}
        try:
            meta = _ffprobe_json(tmp)
            info["ffprobe"] = meta

            vstreams = [
                s for s in meta.get("streams", []) if s.get("codec_type") == "video"
            ]
            astreams = [
                s for s in meta.get("streams", []) if s.get("codec_type") == "audio"
            ]
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

                if codec != "h264":
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
                    if ac != "aac":
                        reasons.append(f"Audio codec {ac} != aac (will be transcoded).")
        except Exception as e:
            return {"ok": False, "stage": "ffprobe", "error": str(e)}

    else:
        if not PIL_OK:
            return {"ok": False, "error": "Pillow is not installed on server."}
        try:
            im_raw = Image.open(tmp)
            w, h = im_raw.size
            info["image"] = {"width": w, "height": h, "mode": im_raw.mode}
            if target.upper() == "IMAGE" and max(w, h) > 2160:
                reasons.append(
                    "Очень крупное изображение — будет ужато до 1080 по длинной стороне."
                )
        except Exception as e:
            return {"ok": False, "stage": "image_open", "error": str(e)}

    return {
        "ok": True,
        "compatible": compatible,
        "reasons": reasons,
        "media_info": info,
        "local_url": _public_url(tmp),
    }


# 2) TRANSCODE VIDEO
@app.post("/media/transcode/video")
async def media_transcode_video(
    url: str = Body(..., embed=True),
    target_aspect: Optional[str] = Body(default="9:16", embed=True),
    max_duration_sec: int = Body(default=90, embed=True),
    max_width: int = Body(default=1080, embed=True),
    fps: int = Body(default=30, embed=True),
    normalize_audio: bool = Body(default=True, embed=True),
):
    if not _has_ffmpeg():
        return {"ok": False, "error": "ffmpeg not available."}
    try:
        src = UPLOAD_DIR / _uuid_name("src", _ext_from_url(url, ".mp4"))
        await _download_to(url, src)
    except Exception as e:
        return {"ok": False, "stage": "download", "error": str(e)}

    aspect = _parse_aspect(target_aspect) or (9 / 16)
    out = OUT_DIR / _uuid_name("ready", ".mp4")

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
        FFMPEG,
        "-y",
        "-i",
        str(src),
        "-t",
        str(max_duration_sec),
        "-vf",
        ",".join(vf),
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "21",
        "-pix_fmt",
        "yuv420p",
        "-movflags",
        "+faststart",
    ]
    if af:
        cmd += ["-af", ",".join(af)]
    cmd += ["-c:a", "aac", "-b:a", "128k", str(out)]

    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        return {"ok": False, "stage": "ffmpeg", "stderr": p.stderr[-1000:]}
    return {"ok": True, "output_url": _public_url(out)}


# 3) RESIZE IMAGE
@app.post("/media/resize/image")
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
        src = UPLOAD_DIR / _uuid_name("img", _ext_from_url(url, ".jpg"))
        await _download_to(url, src)
        img = _image_open(src)
    except Exception as e:
        return {"ok": False, "stage": "download/open", "error": str(e)}

    asp = _parse_aspect(target_aspect) or 1.0
    tw = max_width
    th = int(round(tw / asp))

    if fit == "contain":
        if isinstance(background, str) and background.lower() == "blur":
            bg = (
                img.copy()
                .resize((tw, th), RESAMPLE_LANCZOS)
                .filter(ImageFilter.GaussianBlur(radius=24))
            )
            canvas = bg.convert("RGBA")
        else:
            try:
                canvas = Image.new("RGBA", (tw, th), background)
            except Exception:
                canvas = Image.new("RGBA", (tw, th), "black")

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
        out = OUT_DIR / _uuid_name("img_resized", ".jpg")
        _save_image_rgb(canvas, out, quality=90)
        return {"ok": True, "output_url": _public_url(out)}

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
    out = OUT_DIR / _uuid_name("img_cover", ".jpg")
    _save_image_rgb(img_c, out, quality=92)
    return {"ok": True, "output_url": _public_url(out)}


# 4) REEL COVER (grab frame + optional text)
@app.post("/media/reel-cover")
async def media_reel_cover(
    video_url: str = Body(..., embed=True),
    at: float = Body(1.0, embed=True),
    overlay: Optional[Dict[str, Any]] = Body(default=None, embed=True),
):
    if not _has_ffmpeg():
        return {"ok": False, "error": "ffmpeg not available."}
    try:
        src = UPLOAD_DIR / _uuid_name("vid", _ext_from_url(video_url, ".mp4"))
        await _download_to(video_url, src)
    except Exception as e:
        return {"ok": False, "stage": "download", "error": str(e)}

    frame = OUT_DIR / _uuid_name("cover_frame", ".jpg")
    p = subprocess.run(
        [
            FFMPEG,
            "-y",
            "-ss",
            str(max(0.0, at)),
            "-i",
            str(src),
            "-frames:v",
            "1",
            "-q:v",
            "2",
            str(frame),
        ],
        capture_output=True,
        text=True,
    )
    if p.returncode != 0:
        return {"ok": False, "stage": "ffmpeg", "stderr": p.stderr[-1000:]}

    if overlay and PIL_OK:
        try:
            img = Image.open(frame).convert("RGBA")
            draw = ImageDraw.Draw(img)

            text = (overlay or {}).get("text") or ""
            pos = (overlay or {}).get("pos") or "bottom"
            padding = int((overlay or {}).get("padding") or 32)
            font_name = (overlay or {}).get(
                "font"
            )  # например "Inter", "NotoSans", "OpenSans-SemiBold"
            font = _pick_font(size=48, name=font_name)

            if text:
                wrapped = textwrap.fill(text, width=20)
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

                if pos == "bottom":
                    xy = (padding, img.height - th - padding)
                elif pos == "top":
                    xy = (padding, padding)
                else:
                    xy = (padding, padding)

                bg = Image.new(
                    "RGBA", (tw + padding * 2, th + padding * 2), (0, 0, 0, 160)
                )
                img.paste(bg, (xy[0] - padding, xy[1] - padding), bg)
                draw.multiline_text(
                    xy, wrapped, font=font, fill=(255, 255, 255, 255), spacing=4
                )

            out = OUT_DIR / _uuid_name("cover", ".jpg")
            _save_image_rgb(img, out, quality=92)
            return {"ok": True, "cover_url": _public_url(out)}
        except Exception as e:
            return {
                "ok": True,
                "cover_url": _public_url(frame),
                "note": f"PIL overlay skipped: {e}",
            }

    return {"ok": True, "cover_url": _public_url(frame)}


# 5) WATERMARK (image or video)
@app.post("/media/watermark")
async def media_watermark(
    url: str = Body(..., embed=True),
    logo_url: str = Body(..., embed=True),
    position: str = Body("br", embed=True),
    opacity: float = Body(0.85, embed=True),
    margin: int = Body(24, embed=True),
    type: Optional[str] = Body(None, embed=True),
):
    ext = _ext_from_url(url, "")
    is_video = type == "video" or ext.lower() in (".mp4", ".mov", ".m4v", ".webm")
    try:
        src = UPLOAD_DIR / _uuid_name("wm_src", ext or ".bin")
        await _download_to(url, src)
        logo = UPLOAD_DIR / _uuid_name("wm_logo", _ext_from_url(logo_url, ".png"))
        await _download_to(logo_url, logo)
    except Exception as e:
        return {"ok": False, "stage": "download", "error": str(e)}

    if not is_video:
        if not PIL_OK:
            return {"ok": False, "error": "Pillow not installed."}
        try:
            base = _image_open(src)
            mark = Image.open(logo).convert("RGBA")

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
            out = OUT_DIR / _uuid_name("wm_img", ".jpg")
            _save_image_rgb(base, out, quality=92)
            return {"ok": True, "output_url": _public_url(out)}
        except Exception as e:
            return {"ok": False, "stage": "image_wm", "error": str(e)}
    else:
        if not _has_ffmpeg():
            return {"ok": False, "error": "ffmpeg not available."}

        pos_map = {
            "tr": f"main_w-overlay_w-{margin}:{margin}",
            "tl": f"{margin}:{margin}",
            "bl": f"{margin}:main_h-overlay_h-{margin}",
            "br": f"main_w-overlay_w-{margin}:main_h-overlay_h-{margin}",
        }
        expr = pos_map.get(position, pos_map["br"])

        out = OUT_DIR / _uuid_name("wm_vid", ".mp4")
        cmd = [
            FFMPEG,
            "-y",
            "-i",
            str(src),
            "-i",
            str(logo),
            "-filter_complex",
            f"[1]format=rgba,colorchannelmixer=aa={opacity}[lg];[0][lg]overlay={expr}",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "21",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-movflags",
            "+faststart",
            str(out),
        ]
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode != 0:
            return {"ok": False, "stage": "ffmpeg", "stderr": p.stderr[-1000:]}

        return {"ok": True, "output_url": _public_url(out)}


# 6) FILTERS (image)
@app.post("/media/filter/image")
async def media_filter_image(
    url: str = Body(..., embed=True),
    preset: str = Body("cinematic", embed=True),
    intensity: float = Body(0.7, embed=True),
):
    if not PIL_OK:
        return {"ok": False, "error": "Pillow not installed."}
    try:
        src = UPLOAD_DIR / _uuid_name("flt_img", _ext_from_url(url, ".jpg"))
        await _download_to(url, src)
        img = Image.open(src).convert("RGB")
    except Exception as e:
        return {"ok": False, "stage": "download/open", "error": str(e)}

    # clamp 0..1
    k = max(0.0, min(1.0, float(intensity)))

    try:
        out_img = img

        def _blend_color(base: Image.Image, rgb: tuple, alpha: float) -> Image.Image:
            overlay = Image.new("RGB", base.size, rgb)
            return Image.blend(base, overlay, max(0.0, min(1.0, alpha)))

        def _vignette(base: Image.Image, strength: float) -> Image.Image:
            w, h = base.size
            pad = int(min(w, h) * (0.15 + 0.25 * strength))
            m = Image.new("L", (w, h), 0)
            draw = ImageDraw.Draw(m)
            draw.ellipse((pad, pad, w - pad, h - pad), fill=255)
            m = m.filter(
                ImageFilter.GaussianBlur(
                    radius=int(min(w, h) * (0.06 + 0.12 * strength))
                )
            )
            dark = ImageEnhance.Brightness(base).enhance(1 - 0.25 * strength)
            return Image.composite(dark, base, m)

        p = (preset or "").lower().strip()

        if p in ("b&w", "bw", "mono", "blackwhite"):
            out_img = img.convert("L").convert("RGB")

        elif p in ("warm", "warmth"):
            r, g, b = img.split()
            r = ImageEnhance.Brightness(r).enhance(1 + 0.15 * k)
            b = ImageEnhance.Brightness(b).enhance(1 - 0.10 * k)
            out_img = Image.merge("RGB", (r, g, b))
            out_img = ImageEnhance.Color(out_img).enhance(1 + 0.10 * k)

        elif p in ("cool", "cold"):
            r, g, b = img.split()
            b = ImageEnhance.Brightness(b).enhance(1 + 0.15 * k)
            r = ImageEnhance.Brightness(r).enhance(1 - 0.10 * k)
            out_img = Image.merge("RGB", (r, g, b))
            out_img = ImageEnhance.Color(out_img).enhance(1 + 0.05 * k)

        elif p in ("boost", "pop"):
            out_img = ImageEnhance.Contrast(img).enhance(1 + 0.35 * k)
            out_img = ImageEnhance.Color(out_img).enhance(1 + 0.35 * k)
            out_img = ImageEnhance.Sharpness(out_img).enhance(1 + 0.25 * k)

        elif p in ("cinematic", "cinema", "film"):
            out_img = ImageEnhance.Contrast(img).enhance(1 + 0.15 * k)
            out_img = ImageEnhance.Color(out_img).enhance(1 + 0.12 * k)
            out_img = out_img.filter(ImageFilter.GaussianBlur(radius=0.5 * k))
            out_img = ImageEnhance.Sharpness(out_img).enhance(1 + 0.2 * k)
            out_img = _vignette(out_img, 0.5 * k)

        elif p in ("teal_orange", "teal-orange", "tealorange"):
            out_img = ImageEnhance.Contrast(img).enhance(1 + 0.10 * k)
            out_img = ImageEnhance.Color(out_img).enhance(1 + 0.10 * k)
            out_img = _blend_color(out_img, (0, 128, 128), 0.08 * k)
            out_img = _blend_color(out_img, (255, 140, 0), 0.06 * k)
            out_img = _vignette(out_img, 0.35 * k)

        elif p in ("pastel", "soft"):
            out_img = ImageEnhance.Contrast(img).enhance(1 - 0.15 * k)
            out_img = ImageEnhance.Color(out_img).enhance(1 + 0.05 * k)
            glow = out_img.filter(ImageFilter.GaussianBlur(radius=2 + 4 * k))
            out_img = Image.blend(out_img, glow, 0.25 * k)

        elif p in ("matte", "fade"):
            out_img = ImageEnhance.Contrast(img).enhance(1 - 0.20 * k)
            out_img = _blend_color(out_img, (20, 20, 20), 0.10 * k)
            out_img = ImageEnhance.Color(out_img).enhance(1 - 0.05 * k)

        elif p in ("hdr", "hdrish", "detail"):
            out_img = ImageEnhance.Sharpness(img).enhance(1 + 0.6 * k)
            out_img = ImageEnhance.Contrast(out_img).enhance(1 + 0.20 * k)
            local = out_img.filter(ImageFilter.DETAIL)
            out_img = Image.blend(out_img, local, 0.35 * k)

        elif p in ("sepia",):
            gray = img.convert("L")
            out_img = Image.merge("RGB", (gray, gray, gray))
            out_img = _blend_color(out_img, (112, 66, 20), 0.35 * k)
            out_img = ImageEnhance.Contrast(out_img).enhance(1 + 0.05 * k)

        elif p in ("vintage",):
            out_img = ImageEnhance.Color(img).enhance(1 - 0.15 * k)
            out_img = _blend_color(out_img, (230, 210, 180), 0.12 * k)
            out_img = _vignette(out_img, 0.45 * k)

        elif p in ("clarity", "structure"):
            hi = ImageEnhance.Sharpness(img).enhance(1 + 0.8 * k)
            lo = img.filter(ImageFilter.GaussianBlur(radius=1 + 2 * k))
            out_img = Image.blend(hi, lo, 0.15 * k)
            out_img = ImageEnhance.Contrast(out_img).enhance(1 + 0.10 * k)

        else:
            out_img = ImageEnhance.Contrast(img).enhance(1 + 0.12 * k)
            out_img = ImageEnhance.Color(out_img).enhance(1 + 0.10 * k)
            out_img = out_img.filter(ImageFilter.GaussianBlur(radius=0.3 * k))

        out = OUT_DIR / _uuid_name("flt_img_out", ".jpg")
        out_img.save(out, quality=92, optimize=True, progressive=True)
        return {"ok": True, "preset": p, "intensity": k, "output_url": _public_url(out)}
    except Exception as e:
        return {"ok": False, "stage": "filter", "error": str(e)}


# NEW: очередь — ставим задачу на фоновую обработку видео
@app.post("/media/filter/video")
async def enqueue_filter_video(body: dict = Body(...)):
    """
    Ставит задачу на фоновую обработку видео.
    Вход: { "url": "...", "preset": "cinematic", "intensity": 0.7 }
    Выход: { ok, job_id, status_url }
    """
    url = body.get("url")
    preset = body.get("preset")
    intensity = body.get("intensity", 0.7)

    if not url:
        raise HTTPException(status_code=400, detail="Field 'url' is required")
    if preset is None:
        raise HTTPException(status_code=400, detail="Field 'preset' is required")

    payload = {"url": url, "preset": preset, "intensity": float(intensity)}

    # Совместимость с разными версиями jobs.py
    try:
        # Вариант 1: create_job(kind=..., payload=...) -> str id
        job_obj_or_id = create_job(kind="video_filter", payload=payload)
    except TypeError:
        try:
            # Вариант 2: create_job(payload=..., preset=...) -> Job or id
            job_obj_or_id = create_job(payload=payload, preset="video_filter")
        except TypeError:
            # Вариант 3: create_job(payload=...) -> Job or id
            job_obj_or_id = create_job(payload=payload)

    # извлекаем строковый id
    job_id = getattr(job_obj_or_id, "id", job_obj_or_id)
    if not isinstance(job_id, str):
        job_id = str(job_id)

    await job_queue.put(job_id)

    return {
        "ok": True,
        "job_id": job_id,
        "status_url": f"/media/filter/status?job_id={job_id}",
    }


@app.get("/media/filter/status")
async def media_filter_status(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

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


# 7) COMPOSITE COVER
@app.post("/media/composite/cover")
async def media_composite_cover(
    frame_url: str = Body(..., embed=True),
    title: str = Body("", embed=True),
    bg: str = Body("blur", embed=True),
    size: str = Body("1080x1920", embed=True),
):
    if not PIL_OK:
        return {"ok": False, "error": "Pillow not installed."}

    def _measure_multiline(
        draw: ImageDraw.ImageDraw,
        text: str,
        font: ImageFont.ImageFont,
        spacing: int = 4,
        wrap_width: int = 20,
    ):
        lines = []
        for para in text.split("\n"):
            para = para.strip()
            if para:
                wrapped = textwrap.wrap(para, width=wrap_width) or [""]
                lines.extend(wrapped)
            else:
                lines.append("")
        if hasattr(draw, "multiline_textbbox"):
            bbox = draw.multiline_textbbox(
                (0, 0), "\n".join(lines), font=font, spacing=spacing, align="center"
            )
            return bbox[2] - bbox[0], bbox[3] - bbox[1], "\n".join(lines)
        # fallback для старых PIL
        max_w = 0
        total_h = 0
        for i, line in enumerate(lines):
            try:
                bbox = draw.textbbox((0, 0), line, font=font)
                lw, lh = bbox[2] - bbox[0], bbox[3] - bbox[1]
            except Exception:
                lw, lh = draw.textsize(line, font=font)
            max_w = max(max_w, lw)
            total_h += lh + (spacing if i < len(lines) - 1 else 0)
        return max_w, total_h, "\n".join(lines)

    try:
        w, h = [int(x) for x in size.lower().split("x")]
    except Exception:
        w, h = 1080, 1920

    try:
        src = UPLOAD_DIR / _uuid_name("frame", _ext_from_url(frame_url, ".jpg"))
        await _download_to(frame_url, src)
        img = Image.open(src).convert("RGB")
    except Exception as e:
        return {"ok": False, "stage": "download/open", "error": str(e)}

    # фон
    try:
        if bg == "solid":
            canvas = Image.new("RGB", (w, h), "#0b0b0b")
        elif bg == "gradient":
            grad = Image.new("RGB", (1, h))
            top = (10, 10, 10)
            bottom = (40, 6, 60)
            for y in range(h):
                t = y / max(1, h - 1)
                c = tuple(int(top[i] * (1 - t) + bottom[i] * t) for i in range(3))
                grad.putpixel((0, y), c)
            canvas = grad.resize((w, h), RESAMPLE_LANCZOS)
        else:
            canvas = (
                img.copy()
                .resize((w, h), RESAMPLE_LANCZOS)
                .filter(ImageFilter.GaussianBlur(radius=24))
            )
    except Exception as e:
        return {"ok": False, "stage": "background", "error": str(e)}

    # вставляем фрейм
    try:
        max_frame_h = int(h * 0.8)
        ratio = img.width / img.height
        frame_h = max_frame_h
        frame_w = int(round(frame_h * ratio))
        if frame_w > int(w * 0.9):
            frame_w = int(w * 0.9)
            frame_h = int(round(frame_w / ratio))
        frame_res = img.resize((frame_w, frame_h), RESAMPLE_LANCZOS)
        x = (w - frame_w) // 2
        y = int(h * 0.1)
        canvas.paste(frame_res, (x, y))
    except Exception as e:
        return {"ok": False, "stage": "paste_frame", "error": str(e)}

    # заголовок
    if title:
        try:
            rgba = canvas.convert("RGBA")
            draw = ImageDraw.Draw(rgba)
            font = _pick_font(size=64)
            tw, th, wrapped = _measure_multiline(
                draw, title, font, spacing=4, wrap_width=20
            )
            bx = (w - tw) // 2
            by = y + frame_h + 24
            pad = 24
            rect = Image.new(
                "RGBA", (max(1, tw) + pad * 2, max(1, th) + pad * 2), (0, 0, 0, 160)
            )
            rgba.paste(rect, (bx - pad, by - pad), rect)
            draw.multiline_text(
                (bx, by),
                wrapped,
                font=font,
                fill=(255, 255, 255, 255),
                spacing=4,
                align="center",
            )
            canvas = rgba.convert("RGB")
        except Exception:
            pass

    try:
        out = OUT_DIR / _uuid_name("cover_comp", ".jpg")
        canvas.save(out, quality=92, optimize=True, progressive=True)
        return {"ok": True, "output_url": _public_url(out)}
    except Exception as e:
        return {"ok": False, "stage": "save", "error": str(e)}


# 8) SCHEDULER (in-memory; dev)
JOBS: Dict[str, Dict[str, Any]] = {}


async def _publish_job(
    job_id: str, ig_id: str, page_token: str, creation_id: str, run_at: datetime
):
    wait = _sleep_seconds_until(run_at)
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
    st = await _load_state()
    run_at = _iso_to_utc(publish_at)
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
    st = await _load_state()
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


# ── Housekeeping: cleanup old temp files ────────────────────────────────
@app.delete("/util/cleanup")
def cleanup_tmp(hours: int = 12):
    """
    Удаляет файлы из uploads/ и out/, которым больше N часов.
    Default: 12h
    """
    cutoff = time.time() - hours * 3600
    removed = []
    for d in [UPLOAD_DIR, OUT_DIR]:
        for p in d.glob("*"):
            try:
                if p.is_file() and p.stat().st_mtime < cutoff:
                    p.unlink()
                    removed.append(p.name)
            except Exception:
                # Игнорируем частные ошибки удаления (busy, race, perms)
                pass
    return {"ok": True, "removed": removed, "count": len(removed)}


@app.get("/util/fonts")
def list_fonts(q: Optional[str] = None, limit: int = 100):
    idx = _font_index()
    items = sorted(idx.items())
    if q:
        ql = q.lower()
        items = [(k, v) for k, v in items if ql in k]
    items = items[: max(1, min(limit, 500))]
    return {
        "ok": True,
        "count": len(items),
        "fonts": [{"name": k, "path": v} for k, v in items],
    }


# корневой пинг
@app.get("/")
def root():
    return {
        "ok": True,
        "service": "meta-ig-tools",
        "endpoints": [
            "/health",
            "/oauth/start",
            "/oauth/callback",
            "/me/instagram",
            "/me/pages",
            "/debug/scopes",
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
            "/media/transcode/video",
            "/media/resize/image",
            "/media/reel-cover",
            "/media/watermark",
            "/media/filter/image",
            "/media/filter/video",
            "/media/filter/status",
            "/media/composite/cover",
            "/ig/schedule",
            "/caption/suggest",
            "/ig/publish/batch",
            "/ig/comment/after_publish",
            "/util/cloudinary/upload",
            "/util/cleanup",
            "/util/fonts",
        ],
    }


if __name__ == "__main__":
    try:
        import uvicorn

        uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
    except Exception:
        pass
