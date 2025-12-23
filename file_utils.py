# file_utils.py
import os
import uuid
from pathlib import Path
from typing import Optional

import httpx


def uuid_name(prefix: str, ext: str) -> str:
    ext = ext if ext.startswith(".") else f".{ext}"
    return f"{prefix}_{uuid.uuid4().hex}{ext}"


def ext_from_url(url: str, default: str = ".bin") -> str:
    guess = os.path.splitext(url.split("?")[0])[1]
    return guess if guess else default


def public_url(local_path: Path, static_dir: Path) -> str:
    rel = local_path.relative_to(static_dir).as_posix()
    return f"/static/{rel}"


async def download_to(
    url: str,
    dst_path: Path,
    *,
    timeout_sec: float = 120,
    max_bytes: int = 200 * 1024 * 1024,  # 200 MB safety limit
    headers: Optional[dict] = None,
) -> Path:
    """
    Скачивает файл по URL в dst_path.
    - follow_redirects=True: CDN/302
    - atomic write: сначала во временный .part, затем replace()
    - max_bytes: защита от огромных файлов
    """
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    base_headers = {
        "User-Agent": "ig-planner/1.0 (+https://ig-planner-backend.onrender.com)",
        "Accept": "*/*",
    }
    if headers:
        base_headers.update(headers)

    tmp_path = dst_path.with_suffix(dst_path.suffix + ".part")

    timeout = httpx.Timeout(connect=10, read=timeout_sec, write=timeout_sec, pool=timeout_sec)

    async with httpx.AsyncClient(timeout=timeout, headers=base_headers, follow_redirects=True) as client:
        async with client.stream("GET", url) as r:
            try:
                r.raise_for_status()
            except httpx.HTTPStatusError as e:
                status = e.response.status_code if e.response is not None else "?"
                location = e.response.headers.get("Location") if e.response is not None else None
                hint = f"\nRedirect location: {location}" if location else ""
                raise RuntimeError(f"Download failed ({status}) {url}{hint}") from None

            # если сервер отдал Content-Length — проверим заранее
            cl = r.headers.get("Content-Length")
            if cl:
                try:
                    if int(cl) > max_bytes:
                        raise RuntimeError(f"Download too large: {cl} bytes > {max_bytes}")
                except ValueError:
                    pass

            written = 0
            with tmp_path.open("wb") as f:
                async for chunk in r.aiter_bytes():
                    if not chunk:
                        continue
                    written += len(chunk)
                    if written > max_bytes:
                        try:
                            f.close()
                        finally:
                            if tmp_path.exists():
                                tmp_path.unlink(missing_ok=True)
                        raise RuntimeError(f"Download too large: exceeded {max_bytes} bytes")
                    f.write(chunk)

    tmp_path.replace(dst_path)
    return dst_path
