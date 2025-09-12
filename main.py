import os, secrets, json, time, asyncio, uuid, subprocess, textwrap, shutil
from typing import Optional, List, Dict, Any
from urllib.parse import urlencode
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Body, Query
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
from datetime import datetime, timezone

# OPTIONAL: Pillow (без жёсткой зависимости)
try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageEnhance
    PIL_OK = True
except Exception:
    PIL_OK = False

# ── ENV ────────────────────────────────────────────────────────────────
load_dotenv()
APP_ID = os.getenv("META_APP_ID")
APP_SECRET = os.getenv("META_APP_SECRET")
REDIRECT_URI = os.getenv("META_REDIRECT_URI")

GRAPH_BASE = "https://graph.facebook.com/v21.0"
META_AUTH = "https://www.facebook.com/v21.0/dialog/oauth"
TOKEN_URL = f"{GRAPH_BASE}/oauth/access_token"
ME_URL = f"{GRAPH_BASE}/me"

STATE_STORE = set()

# ── Cloudinary (unsigned) ───────────────────────────────────────────────
CLOUDINARY_CLOUD = os.getenv("CLOUDINARY_CLOUD") or os.getenv("CLOUDINARY_CLOUD_NAME")
CLOUDINARY_UNSIGNED_PRESET = os.getenv("CLOUDINARY_UNSIGNED_PRESET")  # e.g. "unsigned_public"

# ---- resolve ffmpeg/ffprobe binaries (Homebrew, /usr/local, PATH)
def _pick_bin(*candidates: str) -> str:
    for c in candidates:
        if isinstance(c, str) and c:
            # если это абсолютный путь — проверим, что файл существует
            p = Path(c)
            if p.is_absolute() and p.exists():
                return c
            # если это просто имя — ищем в PATH
            w = shutil.which(c)
            if w:
                return w
    # ничего не нашли — вернём последний кандидат; _has_ffmpeg() отловит отсутствие
    return candidates[-1] if candidates else "ffmpeg"
    
FFMPEG = _pick_bin(
    os.getenv("FFMPEG_BIN"),
    "ffmpeg",
    "/opt/homebrew/bin/ffmpeg",
    "/usr/local/bin/ffmpeg"
)
FFPROBE = _pick_bin(
    os.getenv("FFPROBE_BIN"),
    "ffprobe",
    "/opt/homebrew/bin/ffprobe",
    "/usr/local/bin/ffprobe"
)

def _has_ffmpeg() -> bool:
    try:
        subprocess.run([FFMPEG, "-version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        subprocess.run([FFPROBE, "-version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        return True
    except Exception:
        return False


# --- Cloudinary auto-transform for Reels (если прислали прямой Cloudinary URL)
def _cld_inject_transform(url: str, transform: str) -> str:
    marker = "/upload/"
    if "res.cloudinary.com" in url and marker in url and "/video/" in url:
        host, rest = url.split(marker, 1)
        first_seg = rest.split("/", 1)[0]
        if "," in first_seg:  # похоже, что трансформация уже есть
            return url
        return f"{host}{marker}{transform}/{rest}"
    return url

# Рекомендуемая трансформация для Reels
_CLOUD_REELS_TRANSFORM = "c_fill,w_1080,h_1920,fps_30,vc_h264:baseline,br_3500k,ac_aac/so_0:20/f_mp4"

# Важно: публикация контента, комментарии, инсайты, страницы
SCOPES = ",".join([
    "pages_show_list",
    "instagram_basic",
    "pages_read_engagement",
    "instagram_manage_insights",
    "pages_manage_metadata",
    "business_management",
    "instagram_manage_comments",
    "instagram_content_publish",
])

# ── static dirs ────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
UPLOAD_DIR = STATIC_DIR / "uploads"
OUT_DIR = STATIC_DIR / "out"
for d in [STATIC_DIR, UPLOAD_DIR, OUT_DIR]:
    d.mkdir(parents=True, exist_ok=True)

app = FastAPI()
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ── CORS (если надо дёргать из фронта) ─────────────────────────────────
try:
    from fastapi.middleware.cors import CORSMiddleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],    # при необходимости сузить до своих доменов
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
    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.get(url)
        r.raise_for_status()
        dst_path.write_bytes(r.content)
    return dst_path

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
        FFPROBE, "-v", "error",
        "-show_format", "-show_streams",
        "-of", "json",
        str(path)
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

def _pick_font(size: int = 48) -> ImageFont.FreeTypeFont:
    if PIL_OK:
        try:
            return ImageFont.truetype("Arial.ttf", size=size)
        except Exception:
            return ImageFont.load_default()
    raise RuntimeError("Pillow not available")

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _iso_to_utc(ts: str) -> datetime:
    if ts.endswith("Z"):
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)

def _sleep_seconds_until(dt: datetime) -> float:
    delta = (dt - _now_utc()).total_seconds()
    return max(0.0, delta)

def _load_state() -> Dict[str, Any]:
    """Читаем tokens.json и возвращаем ig_id/page_token/user_token."""
    try:
        with open("tokens.json", "r") as f:
            data = json.load(f)
        ig = (data.get("ig_link") or {}).get("instagram_business_account") or {}
        ig_id = ig.get("id")
        page_token = (data.get("tokens") or {}).get("page_access_token")
        user_token = (data.get("tokens") or {}).get("user_long_lived")
        if not ig_id or not page_token:
            raise RuntimeError("Missing ig_id or page_access_token. Re-run OAuth.")
        return {"ig_id": ig_id, "page_token": page_token, "user_token": user_token, "raw": data}
    except Exception as e:
        raise HTTPException(400, f"State not ready: {e}")

# ── Healthcheck ────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"ok": True, "ffmpeg": _has_ffmpeg(), "pillow": PIL_OK}

# ── OAuth start ─────────────────────────────────────────────────────────
@app.get("/oauth/start")
def oauth_start():
    # guard на ENV: чтобы ловить проблему сразу
    if not APP_ID or not APP_SECRET or not REDIRECT_URI:
        raise HTTPException(500, "META_APP_ID / META_APP_SECRET / META_REDIRECT_URI are not set in env (.env).")
    state = secrets.token_urlsafe(16)
    STATE_STORE.add(state)
    params = {
        "client_id": APP_ID,
        "redirect_uri": REDIRECT_URI,
        "state": state,
        "scope": SCOPES,
    }
    return RedirectResponse(f"{META_AUTH}?{urlencode(params)}")

# ── OAuth callback ──────────────────────────────────────────────────────
@app.get("/oauth/callback")
async def oauth_callback(
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None
):
    if error:
        raise HTTPException(400, f"OAuth error: {error}")
    if not code or not state or state not in STATE_STORE:
        raise HTTPException(400, "Invalid state or code")
    STATE_STORE.discard(state)

    async with httpx.AsyncClient(timeout=30) as client:
        # 1) code -> short-lived user token
        r = await client.get(TOKEN_URL, params={
            "client_id": APP_ID,
            "client_secret": APP_SECRET,
            "redirect_uri": REDIRECT_URI,
            "code": code,
        })
        r.raise_for_status()
        short = r.json()

        # 2) short-lived -> long-lived user token
        r2 = await client.get(TOKEN_URL, params={
            "grant_type": "fb_exchange_token",
            "client_id": APP_ID,
            "client_secret": APP_SECRET,
            "fb_exchange_token": short["access_token"],
        })
        r2.raise_for_status()
        long_user = r2.json()
        user_token = long_user["access_token"]

        # 3) сначала /me/accounts
        pages = await client.get(f"{ME_URL}/accounts", params={"access_token": user_token})
        pages.raise_for_status()
        found_pages = pages.json().get("data") or []

        # 3b) если пусто — через бизнесы (owned_pages)
        if not found_pages:
            b = await client.get(f"{ME_URL}/businesses", params={"access_token": user_token})
            b.raise_for_status()
            biz_list = (b.json().get("data") or [])
            agg: List[Dict[str, Any]] = []
            for biz in biz_list:
                bid = biz.get("id")
                if not bid:
                    continue
                op = await client.get(
                    f"{GRAPH_BASE}/{bid}/owned_pages",
                    params={"access_token": user_token, "fields": "id,name,access_token"}
                )
                op.raise_for_status()
                agg.extend(op.json().get("data") or [])
            found_pages = agg

        # 4) ищем первую страницу с IG Business
        ig_info = None
        page_token = None
        page_id = None
        for p in found_pages:
            pid = p.get("id")
            ptok = p.get("access_token")
            if not pid or not ptok:
                continue
            r3 = await client.get(
                f"{GRAPH_BASE}/{pid}",
                params={"fields": "instagram_business_account{id,username}", "access_token": ptok}
            )
            r3.raise_for_status()
            info = r3.json()
            igba = (info.get("instagram_business_account") or {})
            if igba.get("id"):
                page_id = pid
                page_token = ptok
                ig_info = info
                break

        # если не нашли страницу с привязанным Instagram Business — объясняем сразу
        if not page_token or not ig_info:
            raise HTTPException(
                400,
                "Не найдена страница с привязанным Instagram Business. "
                "Проверьте связку в Meta Business Suite и повторите OAuth."
            )

        payload = {
            "saved_at": int(time.time()),
            "page_id": page_id,
            "ig_link": ig_info,
            "tokens": {
                "user_long_lived": user_token,
                "page_access_token": page_token,
            }
        }
        try:
            with open("tokens.json", "w") as f:
                json.dump(payload, f)
        except Exception:
            pass

        safe_ig = (ig_info or {}).get("instagram_business_account") or {}
        return JSONResponse({
            "ok": True,
            "page_id": page_id,
            "instagram_business_account": {
                "id": safe_ig.get("id"),
                "username": safe_ig.get("username"),
            },
            "note": "Tokens saved server-side (tokens.json)."
        })
# ── Who am I (IG) ───────────────────────────────────────────────────────
@app.get("/me/instagram")
def me_instagram():
    try:
        with open("tokens.json", "r") as f:
            data = json.load(f)
        ig = (data.get("ig_link") or {}).get("instagram_business_account") or {}
        return {
            "ok": True,
            "page_id": data.get("page_id"),
            "instagram_business_account": {"id": ig.get("id"), "username": ig.get("username")},
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ── Pages diagnostics ───────────────────────────────────────────────────
@app.get("/me/pages")
async def me_pages():
    try:
        with open("tokens.json", "r") as f:
            data = json.load(f)
        user_long = data["tokens"]["user_long_lived"]
    except Exception as e:
        return {"ok": False, "error": f"read tokens: {e}"}

    out = []
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{ME_URL}/accounts", params={"access_token": user_long})
        r.raise_for_status()
        pages_a = r.json().get("data") or []
        pages_all = list(pages_a)

        if not pages_a:
            b = await client.get(f"{ME_URL}/businesses", params={"access_token": user_long})
            b.raise_for_status()
            for biz in (b.json().get("data") or []):
                bid = biz.get("id")
                if not bid:
                    continue
                op = await client.get(
                    f"{GRAPH_BASE}/{bid}/owned_pages",
                    params={"access_token": user_long, "fields": "id,name,access_token"}
                )
                op.raise_for_status()
                pages_all.extend(op.json().get("data") or [])

        async with httpx.AsyncClient(timeout=30) as client2:
            for p in pages_all:
                pid = p.get("id")
                ptok = p.get("access_token")
                name = p.get("name")
                has_ig = None
                ig = None
                if pid and ptok:
                    r2 = await client2.get(
                        f"{GRAPH_BASE}/{pid}",
                        params={"fields": "instagram_business_account{id,username}", "access_token": ptok}
                    )
                    r2.raise_for_status()
                    ig = r2.json().get("instagram_business_account")
                    has_ig = bool(ig and ig.get("id"))
                out.append({"id": pid, "name": name, "has_instagram_business": has_ig, "ig": ig})

    return {"ok": True, "pages": out}

# ── Debug scopes ────────────────────────────────────────────────────────
@app.get("/debug/scopes")
async def debug_scopes():
    try:
        with open("tokens.json", "r") as f:
            data = json.load(f)
        user_token = data["tokens"]["user_long_lived"]
    except Exception as e:
        raise HTTPException(400, f"Tokens not found: {e}")

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(
            f"{GRAPH_BASE}/debug_token",
            params={"input_token": user_token, "access_token": f"{APP_ID}|{APP_SECRET}"}
        )
        r.raise_for_status()
        info = r.json().get("data", {})

    return {"ok": True, "is_valid": info.get("is_valid"), "scopes": info.get("scopes", []), "type": info.get("type")}

# ── IG: latest media ────────────────────────────────────────────────────
@app.get("/ig/media")
async def ig_media(limit: int = 12, after: Optional[str] = None):
    state = _load_state()
    ig_id, page_token = state["ig_id"], state["page_token"]

    params = {
        "access_token": page_token,
        "limit": max(1, min(limit, 50)),
        "fields": ",".join([
            "id", "caption", "media_type", "media_url", "permalink",
            "thumbnail_url", "timestamp", "product_type"
        ])
    }
    if after:
        params["after"] = after

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{GRAPH_BASE}/{ig_id}/media", params=params)
        r.raise_for_status()
        payload = r.json()

    return {"ok": True, "count": len(payload.get("data", [])), "data": payload.get("data", []), "paging": payload.get("paging", {})}

# ── IG: COMMENTS (list + create/reply) ──────────────────────────────────
@app.get("/ig/comments")
async def ig_comments(media_id: str = Query(..., description="IG media id"), limit: int = 25):
    state = _load_state()
    page_token = state["page_token"]

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(
            f"{GRAPH_BASE}/{media_id}/comments",
            params={
                "access_token": page_token,
                "limit": max(1, min(limit, 50)),
                "fields": "id,text,username,timestamp"
            }
        )
        r.raise_for_status()
        payload = r.json()

    return {"ok": True, "data": payload.get("data", []), "paging": payload.get("paging", {})}

@app.post("/ig/comment")
async def ig_comment(
    media_id: Optional[str] = Body(default=None),
    message: str = Body(..., embed=True),
    reply_to_comment_id: Optional[str] = Body(default=None, embed=True),
):
    if not media_id and not reply_to_comment_id:
        raise HTTPException(400, "Provide media_id OR reply_to_comment_id")

    state = _load_state()
    page_token = state["page_token"]

    async with httpx.AsyncClient(timeout=30) as client:
        if reply_to_comment_id:
            r = await client.post(
                f"{GRAPH_BASE}/{reply_to_comment_id}/replies",
                data={"access_token": page_token, "message": message}
            )
        else:
            r = await client.post(
                f"{GRAPH_BASE}/{media_id}/comments",
                data={"access_token": page_token, "message": message}
            )
        r.raise_for_status()
        payload = r.json()

    return {"ok": True, "result": payload}

# Модерация комментов
@app.post("/ig/comments/hide")
async def ig_comment_hide(comment_id: str = Body(..., embed=True), hide: bool = Body(default=True, embed=True)):
    state = _load_state()
    page_token = state["page_token"]
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(f"{GRAPH_BASE}/{comment_id}",
                              data={"hide": "true" if hide else "false", "access_token": page_token})
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            return {"ok": False, "status": e.response.status_code, "error": e.response.json()}
    return {"ok": True}

@app.post("/ig/comments/delete")
async def ig_comment_delete(comment_id: str = Body(..., embed=True)):
    state = _load_state()
    page_token = state["page_token"]
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.delete(f"{GRAPH_BASE}/{comment_id}", params={"access_token": page_token})
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            return {"ok": False, "status": e.response.status_code, "error": e.response.json()}
    return {"ok": True}

@app.post("/ig/comments/reply-many")
async def ig_comments_reply_many(
    comment_ids: List[str] = Body(..., embed=True),
    message: str = Body(..., embed=True),
    delay_ms: int = Body(default=600, embed=True),
):
    state = _load_state()
    page_token = state["page_token"]
    results = []
    async with httpx.AsyncClient(timeout=20) as client:
        for cid in comment_ids:
            try:
                r = await client.post(f"{GRAPH_BASE}/{cid}/replies",
                                      data={"access_token": page_token, "message": message})
                r.raise_for_status()
                results.append({"comment_id": cid, "ok": True, "id": r.json().get("id")})
            except httpx.HTTPStatusError as e:
                results.append({"comment_id": cid, "ok": False, "status": e.response.status_code, "error": e.response.json()})
            await asyncio.sleep(max(0.0, delay_ms / 1000.0))
    return {"ok": True, "results": results}

# ── IG: PUBLISH (image) ─────────────────────────────────────────────────
@app.post("/ig/publish/image")
async def ig_publish_image(
    image_url: str = Body(..., embed=True, description="Public https image"),
    caption: Optional[str] = Body(default=None, embed=True),
):
    state = _load_state()
    ig_id, page_token = state["ig_id"], state["page_token"]

    async with httpx.AsyncClient(timeout=60) as client:
        try:
            payload = {"image_url": image_url, "access_token": page_token}
            if caption:
                payload["caption"] = caption
            r1 = await client.post(f"{GRAPH_BASE}/{ig_id}/media", data=payload)
            r1.raise_for_status()
            creation_id = r1.json().get("id")
            if not creation_id:
                return {"ok": False, "stage": "create", "error": "No creation_id in response"}

            r2 = await client.post(
                f"{GRAPH_BASE}/{ig_id}/media_publish",
                data={"creation_id": creation_id, "access_token": page_token}
            )
            r2.raise_for_status()
            published = r2.json()
            return {"ok": True, "creation_id": creation_id, "published": published}

        except httpx.HTTPStatusError as e:
            try:
                err_json = e.response.json()
            except Exception:
                err_json = {"raw": e.response.text[:500]}
            return {
                "ok": False,
                "stage": "graph",
                "status": e.response.status_code,
                "error": err_json
            }
        except Exception as e:
            return {"ok": False, "stage": "client", "error": str(e)}

# ── IG: PUBLISH (REELS video) ──────────────────────────────────────────
@app.post("/ig/publish/video")
async def ig_publish_video(
    video_url: str = Body(..., embed=True, description="Public https video"),
    caption: Optional[str] = Body(default=None, embed=True),
    cover_url: Optional[str] = Body(default=None, embed=True, description="Optional cover image for reels"),
    share_to_feed: bool = Body(default=True, embed=True, description="Показывать ролик в сетке/ленте профиля"),
):
    # если прислали прямой Cloudinary URL без трансформации — автоматически подставим рекомендованный пресет
    video_url = _cld_inject_transform(video_url, _CLOUD_REELS_TRANSFORM)

    state = _load_state()
    ig_id, page_token = state["ig_id"], state["page_token"]

    async with httpx.AsyncClient(timeout=180) as client:
        payload = {
            "access_token": page_token,
            "video_url": video_url,
            "media_type": "REELS",
            "share_to_feed": "true" if share_to_feed else "false",
        }
        if caption:
            payload["caption"] = caption
        if cover_url:
            payload["cover_url"] = cover_url

        r1 = await client.post(f"{GRAPH_BASE}/{ig_id}/media", data=payload)
        try:
            r1.raise_for_status()
        except httpx.HTTPStatusError as e:
            return {"ok": False, "stage": "create_container", "status": e.response.status_code, "error": e.response.json()}
        creation_id = (r1.json() or {}).get("id")
        if not creation_id:
            raise HTTPException(500, "Failed to create video container")

        # Ожидание обработки: IN_PROGRESS | FINISHED | ERROR
        max_wait_sec = 150
        sleep_sec = 2
        waited = 0
        status_code = "IN_PROGRESS"
        status_text = None

        while waited < max_wait_sec:
            rstat = await client.get(
                f"{GRAPH_BASE}/{creation_id}",
                params={"fields": "status,status_code", "access_token": page_token}
            )
            try:
                rstat.raise_for_status()
            except httpx.HTTPStatusError as e:
                return {"ok": False, "stage": "check_status", "status": e.response.status_code, "creation_id": creation_id, "error": e.response.json()}
            payload_stat = rstat.json() or {}
            status_code = payload_stat.get("status_code") or "IN_PROGRESS"
            status_text = payload_stat.get("status")
            if status_code == "FINISHED":
                break
            if status_code == "ERROR":
                return {"ok": False, "stage": "processing", "status_code": status_code, "status": status_text, "creation_id": creation_id}
            await asyncio.sleep(sleep_sec)
            waited += sleep_sec

        if status_code != "FINISHED":
            return {
                "ok": False, "stage": "timeout",
                "status_code": status_code, "status": status_text,
                "creation_id": creation_id, "waited_sec": waited
            }

        r2 = await client.post(
            f"{GRAPH_BASE}/{ig_id}/media_publish",
            data={"creation_id": creation_id, "access_token": page_token}
        )
        try:
            r2.raise_for_status()
        except httpx.HTTPStatusError as e:
            return {"ok": False, "stage": "publish", "status": e.response.status_code, "creation_id": creation_id, "error": e.response.json()}
        published = r2.json()

    return {"ok": True, "creation_id": creation_id, "published": published}


@app.post("/ig/publish/video_from_cloudinary")
async def ig_publish_video_from_cloudinary(
    public_id: str = Body(..., embed=True, description="Cloudinary public_id, например: Public/reel_test_1757597008"),
    caption: Optional[str] = Body(default=None, embed=True),
    share_to_feed: bool = Body(default=True, embed=True),
    cover_url: Optional[str] = Body(default=None, embed=True, description="Необязательная обложка (https)"),
):
    """
    Публикация Reels по public_id из Cloudinary.
    Автоматически применяет рекомендуемую трансформацию для IG Reels.
    """
    if not CLOUDINARY_CLOUD:
        raise HTTPException(400, "Cloudinary not configured: set CLOUDINARY_CLOUD")

    # Базовый URL Cloudinary (без трансформации)
    base_url = f"https://res.cloudinary.com/{CLOUDINARY_CLOUD}/video/upload/{public_id}.mp4"

    # Вставим рекомендуемую трансформацию (если её ещё нет)
    video_url = _cld_inject_transform(base_url, _CLOUD_REELS_TRANSFORM)

    # Дальше — обычная публикация, как в /ig/publish/video
    state = _load_state()
    ig_id, page_token = state["ig_id"], state["page_token"]

    async with httpx.AsyncClient(timeout=180) as client:
        payload = {
            "access_token": page_token,
            "video_url": video_url,
            "media_type": "REELS",
            "share_to_feed": "true" if share_to_feed else "false",
        }
        if caption:
            payload["caption"] = caption
        if cover_url:
            payload["cover_url"] = cover_url

        # 1) создаём контейнер
        r1 = await client.post(f"{GRAPH_BASE}/{ig_id}/media", data=payload)
        try:
            r1.raise_for_status()
        except httpx.HTTPStatusError as e:
            return {"ok": False, "stage": "create_container", "status": e.response.status_code, "error": e.response.json()}
        creation_id = (r1.json() or {}).get("id")
        if not creation_id:
            raise HTTPException(500, "Failed to create video container")

        # 2) ждём обработку
        max_wait_sec = 150
        sleep_sec = 2
        waited = 0
        status_code = "IN_PROGRESS"
        status_text = None

        while waited < max_wait_sec:
            rstat = await client.get(
                f"{GRAPH_BASE}/{creation_id}",
                params={"fields": "status,status_code", "access_token": page_token}
            )
            try:
                rstat.raise_for_status()
            except httpx.HTTPStatusError as e:
                return {"ok": False, "stage": "check_status", "status": e.response.status_code, "creation_id": creation_id, "error": e.response.json()}
            payload_stat = rstat.json() or {}
            status_code = payload_stat.get("status_code") or "IN_PROGRESS"
            status_text = payload_stat.get("status")
            if status_code == "FINISHED":
                break
            if status_code == "ERROR":
                return {"ok": False, "stage": "processing", "status_code": status_code, "status": status_text, "creation_id": creation_id}
            await asyncio.sleep(sleep_sec)
            waited += sleep_sec

        if status_code != "FINISHED":
            return {
                "ok": False, "stage": "timeout",
                "status_code": status_code, "status": status_text,
                "creation_id": creation_id, "waited_sec": waited
            }

        # 3) публикуем
        r2 = await client.post(
            f"{GRAPH_BASE}/{ig_id}/media_publish",
            data={"creation_id": creation_id, "access_token": page_token}
        )
        try:
            r2.raise_for_status()
        except httpx.HTTPStatusError as e:
            return {"ok": False, "stage": "publish", "status": e.response.status_code, "creation_id": creation_id, "error": e.response.json()}
        published = r2.json()

    return {"ok": True, "creation_id": creation_id, "published": published}
    
# ── IG: INSIGHTS (media, smart metrics) ────────────────────────────────
REELS_METRICS = [
    "views", "likes", "comments", "shares", "saved",
    "total_interactions",
    "ig_reels_avg_watch_time", "ig_reels_video_view_total_time",
]
PHOTO_METRICS = [
    "impressions", "reach", "saved",
    "likes", "comments", "shares", "total_interactions",
]
CAROUSEL_METRICS = [
    "impressions", "reach", "saved",
    "likes", "comments", "shares", "total_interactions",
]
VIDEO_METRICS = [
    "views", "likes", "comments", "shares", "saved", "total_interactions",
]

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
    media_id: str = Query(..., description="IG media id"),
    metrics: Optional[str] = Query(None, description="Опционально: через запятую. Если не задано — подберём автоматически."),
):
    state = _load_state()
    page_token = state["page_token"]

    async with httpx.AsyncClient(timeout=30) as client:
        # 1) пробуем получить media_type (+ product_type, если доступно)
        media_type: Optional[str] = None
        product_type: Optional[str] = None

        # Первый запрос: только media_type (безопасно для ShadowIGMedia)
        r1 = await client.get(
            f"{GRAPH_BASE}/{media_id}",
            params={"fields": "media_type", "access_token": page_token}
        )
        try:
            r1.raise_for_status()
            media_type = (r1.json() or {}).get("media_type", "")
        except httpx.HTTPStatusError as e:
            return {
                "ok": False,
                "stage": "get_media_type",
                "status": e.response.status_code,
                "error": e.response.json(),
            }

        # Второй запрос (мягкая попытка): product_type (может упасть — игнорируем)
        try:
            r2 = await client.get(
                f"{GRAPH_BASE}/{media_id}",
                params={"fields": "product_type", "access_token": page_token}
            )
            if r2.status_code == 200:
                product_type = (r2.json() or {}).get("product_type")
        except Exception:
            product_type = None

        # 2) подбираем метрики
        mt_upper = (product_type or media_type or "").upper()
        req_metrics = (
            [m.strip() for m in metrics.split(",") if m.strip()]
            if metrics else _pick_metrics_for_media(mt_upper)
        )

        # Подчищаем устаревшие/недоступные метрики для media в v22+
        if mt_upper in ("IMAGE", "PHOTO", "CAROUSEL", "CAROUSEL_ALBUM", "VIDEO"):
            req_metrics = [m for m in req_metrics if m != "impressions"]

        # 3) инсайты с защитой: если API ругнётся на неподдерживаемую метрику — повторим без неё
        try:
            ins = await client.get(
                f"{GRAPH_BASE}/{media_id}/insights",
                params={"metric": ",".join(req_metrics), "access_token": page_token}
            )
            ins.raise_for_status()
        except httpx.HTTPStatusError as e:
            # Поймаем текст ошибки, чтобы понять, нужно ли ретраить с урезанным списком
            try:
                err_json = e.response.json()
            except Exception:
                err_json = {}
            msg = ((err_json or {}).get("error") or {}).get("message", "")
            lower = msg.lower()
            if "no longer supported" in lower or "is not supported" in lower:
                fallback = [m for m in req_metrics if m != "impressions"]
                if fallback and fallback != req_metrics:
                    ins = await client.get(
                        f"{GRAPH_BASE}/{media_id}/insights",
                        params={"metric": ",".join(fallback), "access_token": page_token}
                    )
                    ins.raise_for_status()
                    req_metrics = fallback
                else:
                    return {
                        "ok": False, "stage": "insights",
                        "status": e.response.status_code, "media_type": mt_upper,
                        "metrics": req_metrics, "error": err_json
                    }
            else:
                return {
                    "ok": False, "stage": "insights",
                    "status": e.response.status_code, "media_type": mt_upper,
                    "metrics": req_metrics, "error": err_json
                }

        data = ins.json().get("data", [])
        return {"ok": True, "media_type": mt_upper, "metrics": req_metrics, "data": data}

# ── IG: INSIGHTS (account) ─────────────────────────────────────────────
ACCOUNT_INSIGHT_ALLOWED = {"impressions", "reach", "profile_views"}

@app.get("/ig/insights/account")
async def ig_account_insights(
    metrics: str = Query("impressions,reach,profile_views"),
    period: str = Query("day", description="day|week|days_28"),
):
    state = _load_state()
    ig_id, page_token = state["ig_id"], state["page_token"]

    req_metrics = [m.strip() for m in metrics.split(",") if m.strip()]
    bad = [m for m in req_metrics if m not in ACCOUNT_INSIGHT_ALLOWED]
    if bad:
        raise HTTPException(400, f"Unsupported metrics: {bad}. Allowed: {sorted(ACCOUNT_INSIGHT_ALLOWED)}")

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(
            f"{GRAPH_BASE}/{ig_id}/insights",
            params={"metric": ",".join(req_metrics), "period": period, "access_token": page_token}
        )
        r.raise_for_status()
        data = r.json().get("data", [])

    return {"ok": True, "data": data}
    
@app.post("/util/cloudinary/upload")
async def cloudinary_upload(
    file_url: str = Body(..., embed=True, description="Публичный https URL картинки или видео"),
    resource_type: str = Body("auto", embed=True, description='image|video|raw|auto'),
    folder: Optional[str] = Body(None, embed=True, description="Необязательная папка (Cloudinary folder)"),
    public_id: Optional[str] = Body(None, embed=True, description="Необязательный желаемый public_id без расширения"),
):
    """
    Заливает внешний URL в Cloudinary через unsigned upload preset.
    Требуются ENV:
      CLOUDINARY_CLOUD=<cloud_name>
      CLOUDINARY_UNSIGNED_PRESET=<preset>
    """
    if not CLOUDINARY_CLOUD or not CLOUDINARY_UNSIGNED_PRESET:
        raise HTTPException(400, "Cloudinary env missing: set CLOUDINARY_CLOUD and CLOUDINARY_UNSIGNED_PRESET")

    # собираем форму
    form = {
        "file": file_url,
        "upload_preset": CLOUDINARY_UNSIGNED_PRESET,
    }
    if folder:
        form["folder"] = folder
    if public_id:
        form["public_id"] = public_id

    endpoint = f"https://api.cloudinary.com/v1_1/{CLOUDINARY_CLOUD}/{resource_type}/upload"
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            r = await client.post(endpoint, data=form)
            r.raise_for_status()
            payload = r.json()
    except httpx.HTTPStatusError as e:
        # Пробрасываем полезное сообщение Cloudinary
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
    state = _load_state()
    page_token = state["page_token"]
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(f"{GRAPH_BASE}/{media_id}/comments",
                              data={"message": message, "access_token": page_token})
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            return {"ok": False, "stage": "comment", "error": e.response.json()}
        return {"ok": True, "result": r.json()}

# ======================================================================
#                           MEDIA TOOLBOX (10)
# ======================================================================

# 1) VALIDATE
@app.post("/media/validate")
async def media_validate(
    url: str = Body(..., embed=True),
    type: str = Body(..., embed=True, description="video|image"),
    target: str = Body("REELS", embed=True, description="REELS|IMAGE - для правил совместимости"),
):
    try:
        ext = _ext_from_url(url, default=".bin")
        tmp = UPLOAD_DIR / _uuid_name("dl", ext)
        await _download_to(url, tmp)
    except Exception as e:
        return {"ok": False, "stage": "download", "error": str(e)}

    info: Dict[str, Any] = {"path": str(tmp), "size": tmp.stat().st_size}
    compatible = True
    reasons: List[str] = []

    if type.lower() == "video":
        if not _has_ffmpeg():
            return {"ok": False, "error": "ffmpeg/ffprobe is not available on server."}
        try:
            meta = _ffprobe_json(tmp)
            info["ffprobe"] = meta
            vstreams = [s for s in meta.get("streams", []) if s.get("codec_type") == "video"]
            astreams = [s for s in meta.get("streams", []) if s.get("codec_type") == "audio"]
            fmt = meta.get("format", {})
            duration = float(fmt.get("duration", 0)) if fmt.get("duration") else 0.0

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
                    r = v.get("r_frame_rate", "0/1")
                    a, b = r.split("/")
                    fps = float(a) / float(b)
                except Exception:
                    pass

                if codec != "h264":
                    compatible = False
                    reasons.append(f"Video codec {codec} != h264")
                if pix_fmt and pix_fmt != "yuv420p":
                    compatible = False
                    reasons.append(f"pix_fmt {pix_fmt} != yuv420p")
                if width > 1080 or height > 1920:
                    reasons.append("Resolution will be downscaled by transcode (OK).")
                if fps > 60:
                    reasons.append("FPS >60 — лучше снизить до 30.")

            if target.upper() == "REELS":
                if not astreams:
                    reasons.append("No audio stream — допустимо, но добавьте звук при монтаже.")
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
            # читаем «сырым» ради оригинального mode, не конвертируя
            im_raw = Image.open(tmp)
            w, h = im_raw.size
            info["image"] = {"width": w, "height": h, "mode": im_raw.mode}
            if target.upper() == "IMAGE":
                if max(w, h) > 1080 * 2:
                    reasons.append("Очень крупное изображение — будет ужато до 1080 по длинной стороне.")
        except Exception as e:
            return {"ok": False, "stage": "image_open", "error": str(e)}

    return {
        "ok": True,
        "compatible": compatible,
        "reasons": reasons,
        "media_info": info,
        "local_url": _public_url(tmp)
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

    aspect = _parse_aspect(target_aspect) or (9/16)
    out = OUT_DIR / _uuid_name("ready", ".mp4")

    vf = []
    vf.append(f"scale='min({max_width},iw)':-2")
    vf.append("setsar=1")
    vf.append(f"crop='min(iw,ih*{aspect}):ih'")
    if fps > 0:
        vf.append(f"fps={fps}")
    vf.append("format=yuv420p")

    af = []
    if normalize_audio:
        af.append("loudnorm=I=-16:TP=-1.5:LRA=11")

    cmd = [
        FFMPEG, "-y", "-i", str(src),
        "-t", str(max_duration_sec),
        "-vf", ",".join(vf),
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "21",
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
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
    background: str = Body(
        "black",
        embed=True,
        description='Фон при fit="contain": "black" | "white" | "#RRGGBB" | "#RRGGBBAA" | "blur"'
    ),
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
        # --- фон под "contain"
        if isinstance(background, str) and background.lower() == "blur":
            # размытый фон из исходной картинки
            bg = img.copy().resize((tw, th), Image.LANCZOS)
            bg = bg.filter(ImageFilter.GaussianBlur(radius=24))
            canvas = bg.convert("RGBA")
        else:
            # поддержка color name / #RRGGBB / #RRGGBBAA; при ошибке — чёрный
            try:
                canvas = Image.new("RGBA", (tw, th), background)
            except Exception:
                canvas = Image.new("RGBA", (tw, th), "black")

        # --- вписываем изображение с сохранением пропорций
        img_ratio = img.width / img.height
        if img_ratio > asp:
            nw = tw
            nh = int(round(nw / img_ratio))
        else:
            nh = th
            nw = int(round(nh * img_ratio))

        img_res = img.resize((nw, nh), Image.LANCZOS)
        x = (tw - nw) // 2
        y = (th - nh) // 2
        canvas.paste(img_res, (x, y), img_res)

        out = OUT_DIR / _uuid_name("img_resized", ".jpg")
        _save_image_rgb(canvas, out, quality=90)
        return {"ok": True, "output_url": _public_url(out)}

    else:
        img_ratio = img.width / img.height
        if img_ratio > asp:
            new_w = int(round(img.height * asp))
            left = (img.width - new_w) // 2
            box = (left, 0, left + new_w, img.height)
        else:
            new_h = int(round(img.width / asp))
            top = (img.height - new_h) // 2
            box = (0, top, img.width, top + new_h)

        img_c = img.crop(box).resize((tw, th), Image.LANCZOS)
        out = OUT_DIR / _uuid_name("img_cover", ".jpg")
        _save_image_rgb(img_c, out, quality=92)
        return {"ok": True, "output_url": _public_url(out)}
        
# 4) REEL COVER (grab frame + optional text)
@app.post("/media/reel-cover")
async def media_reel_cover(
    video_url: str = Body(..., embed=True),
    at: float = Body(1.0, embed=True, description="timestamp seconds"),
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
    cmd = [FFMPEG, "-y", "-ss", str(max(0.0, at)), "-i", str(src), "-frames:v", "1", "-q:v", "2", str(frame)]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        return {"ok": False, "stage": "ffmpeg", "stderr": p.stderr[-1000:]}

    if overlay and PIL_OK:
        try:
            img = Image.open(frame).convert("RGBA")
            draw = ImageDraw.Draw(img)

            text = (overlay or {}).get("text") or ""
            pos = (overlay or {}).get("pos") or "bottom"
            padding = int((overlay or {}).get("padding") or 32)
            font = _pick_font(size=48)

            if text:
                wrapped = textwrap.fill(text, width=20)

                # размеры текста (современный метод + fallbacks)
                if hasattr(draw, "multiline_textbbox"):
                    bbox = draw.multiline_textbbox((0, 0), wrapped, font=font, spacing=4, align="left")
                    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
                else:
                    try:
                        bbox = draw.textbbox((0, 0), wrapped, font=font)
                        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
                    except Exception:
                        tw, th = draw.textsize(wrapped, font=font)

                # позиция
                if pos == "bottom":
                    xy = (padding, img.height - th - padding)
                elif pos == "top":
                    xy = (padding, padding)
                else:
                    xy = (padding, padding)

                # подложка под текст
                bg = Image.new("RGBA", (tw + padding * 2, th + padding * 2), (0, 0, 0, 160))
                img.paste(bg, (xy[0] - padding, xy[1] - padding), bg)

                # сам текст
                draw.multiline_text(xy, wrapped, font=font, fill=(255, 255, 255, 255), spacing=4)

            out = OUT_DIR / _uuid_name("cover", ".jpg")
            _save_image_rgb(img, out, quality=92)
            return {"ok": True, "cover_url": _public_url(out)}
        except Exception as e:
            return {"ok": True, "cover_url": _public_url(frame), "note": f"PIL overlay skipped: {e}"}

    return {"ok": True, "cover_url": _public_url(frame)}

# 5) WATERMARK (image or video)
@app.post("/media/watermark")
async def media_watermark(
    url: str = Body(..., embed=True),
    logo_url: str = Body(..., embed=True),
    position: str = Body("br", embed=True, description="tr|tl|br|bl"),
    opacity: float = Body(0.85, embed=True),
    margin: int = Body(24, embed=True),
    type: Optional[str] = Body(None, embed=True, description="image|video"),
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
            mark = mark.resize((target_w, int(mark.height * ratio)), Image.LANCZOS)
            if opacity < 1.0:
                alpha = mark.split()[-1].point(lambda p: int(p * opacity))
                mark.putalpha(alpha)

            if position in ("tr", "rt"):
                x = base.width - mark.width - margin; y = margin
            elif position in ("tl", "lt"):
                x = margin; y = margin
            elif position in ("bl", "lb"):
                x = margin; y = base.height - mark.height - margin
            else:
                x = base.width - mark.width - margin; y = base.height - mark.height - margin
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
            FFMPEG, "-y",
            "-i", str(src), "-i", str(logo),
            "-filter_complex", f"[1]format=rgba,colorchannelmixer=aa={opacity}[lg];[0][lg]overlay={expr}",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "21",
            "-c:a", "aac", "-b:a", "128k",
            "-movflags", "+faststart",
            str(out)
        ]
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode != 0:
            return {"ok": False, "stage": "ffmpeg", "stderr": p.stderr[-1000:]}
        return {"ok": True, "output_url": _public_url(out)}

# 6) FILTERS (image/video)
@app.post("/media/filter/image")
async def media_filter_image(
    url: str = Body(..., embed=True),
    preset: str = Body("cinematic", embed=True, description="b&w|cinematic|warm|cool|boost"),
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

    try:
        if preset == "b&w":
            out_img = img.convert("L").convert("RGB")
        elif preset == "warm":
            r, g, b = img.split()
            r = ImageEnhance.Brightness(r).enhance(1 + 0.1*intensity)
            b = ImageEnhance.Brightness(b).enhance(1 - 0.1*intensity)
            out_img = Image.merge("RGB", (r, g, b))
        elif preset == "cool":
            r, g, b = img.split()
            b = ImageEnhance.Brightness(b).enhance(1 + 0.1*intensity)
            r = ImageEnhance.Brightness(r).enhance(1 - 0.1*intensity)
            out_img = Image.merge("RGB", (r, g, b))
        elif preset == "boost":
            out_img = ImageEnhance.Contrast(img).enhance(1 + 0.3*intensity)
            out_img = ImageEnhance.Color(out_img).enhance(1 + 0.3*intensity)
            out_img = ImageEnhance.Sharpness(out_img).enhance(1 + 0.2*intensity)
        else:
            out_img = ImageEnhance.Contrast(img).enhance(1.15)
            out_img = ImageEnhance.Color(out_img).enhance(1.1)
            out_img = out_img.filter(ImageFilter.GaussianBlur(radius=0.3*intensity))
        out = OUT_DIR / _uuid_name("flt_img_out", ".jpg")
        out_img.save(out, quality=92, optimize=True, progressive=True)
        return {"ok": True, "output_url": _public_url(out)}
    except Exception as e:
        return {"ok": False, "stage": "filter", "error": str(e)}

@app.post("/media/filter/video")
async def media_filter_video(
    url: str = Body(..., embed=True),
    preset: str = Body("cinematic", embed=True, description="b&w|cinematic|warm|cool|boost"),
    intensity: float = Body(0.7, embed=True),
):
    if not _has_ffmpeg():
        return {"ok": False, "error": "ffmpeg not available."}
    try:
        src = UPLOAD_DIR / _uuid_name("flt_vid", _ext_from_url(url, ".mp4"))
        await _download_to(url, src)
    except Exception as e:
        return {"ok": False, "stage": "download", "error": str(e)}

    vf_map = {
        "b&w": "hue=s=0",
        "warm": "curves=red='0/0 0.5/0.6 1/1':blue='0/0 0.4/0.3 1/1'",
        "cool": "curves=blue='0/0 0.5/0.6 1/1':red='0/0 0.4/0.3 1/1'",
        "boost": "eq=contrast=1.15:saturation=1.15:brightness=0.02,unsharp",
        "cinematic": "eq=contrast=1.1:saturation=1.1,unsharp",
    }
    vf = vf_map.get(preset, vf_map["cinematic"])
    out = OUT_DIR / _uuid_name("flt_vid_out", ".mp4")
    cmd = [
        FFMPEG, "-y", "-i", str(src),
        "-vf", vf,
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "21",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        str(out)
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        return {"ok": False, "stage": "ffmpeg", "stderr": p.stderr[-1000:]}
    return {"ok": True, "output_url": _public_url(out)}

# 7) COMPOSITE COVER — устойчивый вариант измерения текста
@app.post("/media/composite/cover")
async def media_composite_cover(
    frame_url: str = Body(..., embed=True),
    title: str = Body("", embed=True),
    bg: str = Body("blur", embed=True, description="blur|solid|gradient"),
    size: str = Body("1080x1920", embed=True),
):
    if not PIL_OK:
        return {"ok": False, "error": "Pillow not installed."}

    # Универсальный измеритель многострочного текста
    def _measure_multiline(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, spacing: int = 4, wrap_width: int = 20):
        # Приводим к строкам (wrap для длинных)
        lines = []
        for para in text.split("\n"):
            para = para.strip()
            if not para:
                lines.append("")
            else:
                lines.extend(textwrap.wrap(para, width=wrap_width) or [""])
        if not lines:
            lines = [""]

        # Если доступна современная API — используем её целиком
        if hasattr(draw, "multiline_textbbox"):
            bbox = draw.multiline_textbbox((0, 0), "\n".join(lines), font=font, spacing=spacing, align="center")
            w = bbox[2] - bbox[0]
            h = bbox[3] - bbox[1]
            return w, h, "\n".join(lines)

        # Фоллбэк: считаем по строкам через textbbox
        max_w = 0
        total_h = 0
        for idx, line in enumerate(lines):
            bbox = draw.textbbox((0, 0), line, font=font)
            lw = bbox[2] - bbox[0]
            lh = bbox[3] - bbox[1]
            max_w = max(max_w, lw)
            total_h += lh
            if idx < len(lines) - 1:
                total_h += spacing
        return max_w, total_h, "\n".join(lines)

    # Парсим размер
    try:
        w, h = [int(x) for x in size.lower().split("x")]
    except Exception:
        w, h = 1080, 1920

    # Грузим исходный кадр/картинку
    try:
        src = UPLOAD_DIR / _uuid_name("frame", _ext_from_url(frame_url, ".jpg"))
        await _download_to(frame_url, src)
        img = Image.open(src).convert("RGB")
    except Exception as e:
        return {"ok": False, "stage": "download/open", "error": str(e)}

    # Фон
    try:
        if bg == "solid":
            canvas = Image.new("RGB", (w, h), "#0b0b0b")
        elif bg == "gradient":
            grad = Image.new("RGB", (1, h))
            top = (10, 10, 10); bottom = (40, 6, 60)
            for y in range(h):
                t = y / max(1, h - 1)
                c = tuple(int(top[i] * (1 - t) + bottom[i] * t) for i in range(3))
                grad.putpixel((0, y), c)
            canvas = grad.resize((w, h))
        else:  # blur
            bg_img = img.copy().resize((w, h), Image.LANCZOS)
            bg_img = bg_img.filter(ImageFilter.GaussianBlur(radius=24))
            canvas = bg_img
    except Exception as e:
        return {"ok": False, "stage": "background", "error": str(e)}

    # Вписываем кадр
    try:
        max_frame_h = int(h * 0.8)
        ratio = img.width / img.height
        frame_h = max_frame_h
        frame_w = int(round(frame_h * ratio))
        if frame_w > int(w * 0.9):
            frame_w = int(w * 0.9)
            frame_h = int(round(frame_w / ratio))

        frame_res = img.resize((frame_w, frame_h), Image.LANCZOS)
        x = (w - frame_w) // 2
        y = int(h * 0.1)
        canvas.paste(frame_res, (x, y))
    except Exception as e:
        return {"ok": False, "stage": "paste_frame", "error": str(e)}

    # Заголовок (опционально)
    if title:
        try:
            rgba = canvas.convert("RGBA")
            draw = ImageDraw.Draw(rgba)
            font = _pick_font(size=64)
            tw, th, wrapped = _measure_multiline(draw, title, font, spacing=4, wrap_width=20)

            bx = (w - tw) // 2
            by = y + frame_h + 24
            pad = 24
            rect = Image.new("RGBA", (max(1, tw) + pad * 2, max(1, th) + pad * 2), (0, 0, 0, 160))
            rgba.paste(rect, (bx - pad, by - pad), rect)
            draw.multiline_text((bx, by), wrapped, font=font, fill=(255, 255, 255, 255), spacing=4, align="center")
            canvas = rgba.convert("RGB")
        except Exception as e:
            # Если не получилось наложить текст — всё равно отдадим картинку без заголовка
            pass

    try:
        out = OUT_DIR / _uuid_name("cover_comp", ".jpg")
        canvas.save(out, quality=92, optimize=True, progressive=True)
        return {"ok": True, "output_url": _public_url(out)}
    except Exception as e:
        return {"ok": False, "stage": "save", "error": str(e)}    
# 8) SCHEDULER (in-memory; dev)
JOBS: Dict[str, Dict[str, Any]] = {}

async def _publish_job(job_id: str, ig_id: str, page_token: str, creation_id: str, run_at: datetime):
    wait = _sleep_seconds_until(run_at)
    await asyncio.sleep(wait)

    # если отменили до момента публикации — выходим
    if JOBS.get(job_id, {}).get("status") == "canceled":
        return

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            r = await client.post(
                f"{GRAPH_BASE}/{ig_id}/media_publish",
                data={"creation_id": creation_id, "access_token": page_token}
            )
            r.raise_for_status()
            res = r.json()
            JOBS[job_id]["status"] = "done"
            JOBS[job_id]["result"] = res
        except Exception as e:
            JOBS[job_id]["status"] = "error"
            JOBS[job_id]["error"] = str(e)
            
@app.post("/ig/schedule")
async def ig_schedule(
    creation_id: str = Body(..., embed=True),
    publish_at: str = Body(..., embed=True, description="ISO, e.g. 2025-09-08T12:00:00Z"),
):
    state = _load_state()
    ig_id, page_token = state["ig_id"], state["page_token"]
    run_at = _iso_to_utc(publish_at)
    job_id = uuid.uuid4().hex
    JOBS[job_id] = {"status": "scheduled", "creation_id": creation_id, "publish_at": run_at.isoformat()}
    asyncio.create_task(_publish_job(job_id, ig_id, page_token, creation_id, run_at))
    return {"ok": True, "job_id": job_id, "status": "scheduled", "publish_at_utc": run_at.isoformat()}

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
        # ограничим 12 тегами и гарантируем решётку
        hs = " " + " ".join([h if h.startswith("#") else f"#{h}" for h in hashtags[:12]])
    caption = f"{base}\n\n{cta}{hs}"
    return {"ok": True, "caption": caption}

# 10) BATCH PUBLISH
@app.post("/ig/publish/batch")
async def ig_publish_batch(
    items: List[Dict[str, Any]] = Body(..., embed=True),
    throttle_ms: int = Body(500, embed=True),
):
    """
    items: [
      { "type":"image", "image_url":"...", "caption":"..." },
      { "type":"reel",  "video_url":"...", "caption":"...", "cover_url":"...", "share_to_feed": true }
    ]
    """
    state = _load_state()
    ig_id, page_token = state["ig_id"], state["page_token"]

    results = []
    async with httpx.AsyncClient(timeout=120) as client:
        for it in items:
            t = (it.get("type") or "").lower()
            try:
                if t == "image":
                    payload = {"image_url": it["image_url"], "access_token": page_token}
                    if it.get("caption"):
                        payload["caption"] = it["caption"]
                    r1 = await client.post(f"{GRAPH_BASE}/{ig_id}/media", data=payload)
                    r1.raise_for_status()
                    creation_id = r1.json().get("id")

                    r2 = await client.post(
                        f"{GRAPH_BASE}/{ig_id}/media_publish",
                        data={"creation_id": creation_id, "access_token": page_token}
                    )
                    r2.raise_for_status()
                    results.append({
                        "type": "image",
                        "ok": True,
                        "creation_id": creation_id,
                        "published": r2.json()
                    })

                elif t in ("video", "reel"):
                    payload = {
                        "video_url": it["video_url"],
                        "media_type": "REELS",
                        "access_token": page_token,
                        "share_to_feed": "true" if it.get("share_to_feed", True) else "false",
                    }
                    if it.get("caption"):
                        payload["caption"] = it["caption"]
                    if it.get("cover_url"):
                        payload["cover_url"] = it["cover_url"]

                    r1 = await client.post(f"{GRAPH_BASE}/{ig_id}/media", data=payload)
                    r1.raise_for_status()
                    creation_id = r1.json().get("id")

                    # простое ожидание обработки контейнера
                    done = False
                    waited = 0
                    while waited < 120:
                        rs = await client.get(
                            f"{GRAPH_BASE}/{creation_id}",
                            params={"fields": "status_code", "access_token": page_token}
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
                        f"{GRAPH_BASE}/{ig_id}/media_publish",
                        data={"creation_id": creation_id, "access_token": page_token}
                    )
                    r2.raise_for_status()
                    results.append({
                        "type": "reel",
                        "ok": True,
                        "creation_id": creation_id,
                        "published": r2.json()
                    })

                else:
                    results.append({"ok": False, "error": f"Unsupported type: {t}", "item": it})

            except httpx.HTTPStatusError as e:
                # ошибки Graph API
                results.append({
                    "ok": False,
                    "status": e.response.status_code,
                    "error": e.response.json(),
                    "item": it
                })
            except Exception as e:
                results.append({"ok": False, "error": str(e), "item": it})

            # троттлинг между публикациями
            await asyncio.sleep(max(0.0, throttle_ms / 1000.0))

    return {"ok": True, "results": results}


# (опционально) удобный корневой пинг
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
            "/ig/insights/media",
            "/ig/insights/account",
            "/media/validate",
            "/media/transcode/video",
            "/media/resize/image",
            "/media/reel-cover",
            "/media/watermark",
            "/media/filter/image",
            "/media/filter/video",
            "/media/composite/cover",
            "/ig/schedule",
            "/caption/suggest",
            "/ig/publish/batch",
        ]
    }

# (не обязательно) локальный раннер
if __name__ == "__main__":
    try:
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
    except Exception:
        # uvicorn может быть не установлен — это ок
        pass   