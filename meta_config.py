# meta_config.py
import os
from dotenv import load_dotenv

# Загружаем .env только локально (на Render переменные приходят из окружения)
if os.getenv("ENV", "").lower() == "dev":
    load_dotenv()

# ─────────────────────────────────────────────────────────────
# Meta / Facebook / Instagram API
# ─────────────────────────────────────────────────────────────

META_API_VERSION = os.getenv("META_API_VERSION", "v21.0").strip()

META_AUTH_URL = f"https://www.facebook.com/{META_API_VERSION}/dialog/oauth"
META_GRAPH_URL = f"https://graph.facebook.com/{META_API_VERSION}"

GRAPH_BASE = META_GRAPH_URL
ME_URL = f"{META_GRAPH_URL}/me"
TOKEN_URL = f"{META_GRAPH_URL}/oauth/access_token"

META_APP_ID = os.getenv("META_APP_ID", "").strip()
META_APP_SECRET = os.getenv("META_APP_SECRET", "").strip()
META_REDIRECT_URI = os.getenv("META_REDIRECT_URI", "").strip()

META_VERIFY_TOKEN = os.getenv("META_VERIFY_TOKEN", "my_verify_token").strip()

# ─────────────────────────────────────────────────────────────
# Instagram / Pages
# ─────────────────────────────────────────────────────────────

IG_ACCESS_TOKEN = os.getenv("IG_ACCESS_TOKEN", "").strip()
PAGE_ID = os.getenv("PAGE_ID", "").strip()

# OAuth scopes (строкой — как ждёт Meta)
META_SCOPES = ",".join(
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

# ─────────────────────────────────────────────────────────────
# Cloudinary
# ─────────────────────────────────────────────────────────────

CLOUDINARY_CLOUD = os.getenv("CLOUDINARY_CLOUD", "").strip()
CLOUDINARY_UNSIGNED_PRESET = os.getenv("CLOUDINARY_UNSIGNED_PRESET", "").strip()

CLOUD_REELS_TRANSFORM = (
    "c_fill,"
    "w_1080,h_1920,"
    "fps_30,"
    "vc_h264:baseline,"
    "br_3500k,"
    "ac_aac,"
    "so_0:20,"
    "f_mp4"
)

# ─────────────────────────────────────────────────────────────
# Security / misc
# ─────────────────────────────────────────────────────────────

JWT_SECRET = os.getenv("JWT_SECRET", "super_secret_key").strip()

# ─────────────────────────────────────────────────────────────
# Backward-compatible aliases (for legacy main.py code)
# ─────────────────────────────────────────────────────────────

APP_ID = META_APP_ID
APP_SECRET = META_APP_SECRET
REDIRECT_URI = META_REDIRECT_URI
META_AUTH = META_AUTH_URL
META_GRAPH = META_GRAPH_URL

IG_LONG_TOKEN = IG_ACCESS_TOKEN
PAGE_ID_ENV = PAGE_ID

SCOPES = META_SCOPES
VERIFY_TOKEN = META_VERIFY_TOKEN
