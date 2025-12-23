from pathlib import Path

# базовая директория backend/
BASE_DIR = Path(__file__).resolve().parent

# static
STATIC_DIR = BASE_DIR / "static"
UPLOAD_DIR = STATIC_DIR / "uploads"
OUT_DIR = STATIC_DIR / "out"


def ensure_dirs():
    for d in (STATIC_DIR, UPLOAD_DIR, OUT_DIR):
        d.mkdir(parents=True, exist_ok=True)
