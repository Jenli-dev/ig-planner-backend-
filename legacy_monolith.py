"""
LEGACY MONOLITH

Здесь временно живёт весь старый код из main.py:
- helpers
- ffmpeg / PIL / Cloudinary
- OAuth
- IG endpoints
- flows
- utils
- scheduler

Файл подключается через import в main.py,
чтобы ничего не потерять и не сломать.
"""

# ❗️ВАЖНО:
# Мы НЕ создаём новый FastAPI app
# Мы берём app из main.py
from main import app  # noqa: F401


# ===== ДАЛЬШЕ КОПИРУЕШЬ КОД 1 В 1 ИЗ main.py =====
# ВСЁ, что было ниже сборки app в main.py:
#
#   - helpers
#   - download / ffmpeg / PIL
#   - OAuth endpoints
#   - webhooks
#   - ig/*
#   - util/*
#   - flows
#   - scheduler
#
# Просто вставляешь сюда БЕЗ ИЗМЕНЕНИЙ
# ===============================================


# ── helpers ────────────────────────────────────────────────────────────
# (ниже — ВСТАВЬ ВЕСЬ КУСОК, который ты уже присылал)
#
# def _public_url(...)
# def _download_to(...)
# def _ffprobe_json(...)
# ...
#
# @app.get("/oauth/start")
# @app.post("/flow/filter-and-publish")
# @app.post("/ig/publish/video")
# и т.д.
#
# ⛔️ НИЧЕГО НЕ ПЕРЕИМЕНОВЫВАТЬ
# ⛔️ НИЧЕГО НЕ ОПТИМИЗИРОВАТЬ
# ⛔️ НИЧЕГО НЕ УДАЛЯТЬ
