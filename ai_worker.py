import asyncio
from pathlib import Path
from typing import Any, Dict, List, Tuple

import httpx

from ai_providers import (
    AIProviderError,
    fal_t2i,
    fal_i2i,
    replicate_t2i,
    replicate_i2i,
)
from cloudinary_utils import cloudinary_unsigned_upload_file
from config import settings
from file_utils import download_to, uuid_name
from jobs import update_job_status, DONE, ERROR, RUNNING


async def _download_and_upload(urls: List[str]) -> List[str]:
    tmp_dir = Path(settings.MEDIA_TMP_DIR)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    out_urls: List[str] = []
    for url in urls:
        ext = ".jpg"
        if url.lower().endswith(".png"):
            ext = ".png"
        dst = tmp_dir / uuid_name("ai_out", ext)
        try:
            await download_to(url, dst, timeout_sec=120)
            cld = await cloudinary_unsigned_upload_file(dst, resource_type="image", timeout_sec=120)
            secure_url = cld.get("secure_url")
            if secure_url:
                out_urls.append(secure_url)
        finally:
            try:
                dst.unlink(missing_ok=True)
            except Exception:
                pass
    return out_urls


def _is_retryable_error(exc: Exception) -> bool:
    if isinstance(exc, AIProviderError):
        return exc.retryable
    if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)):
        return True
    return False


async def _with_retries(func, retries: int = 2):
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return await func()
        except Exception as exc:
            last_exc = exc
            if attempt >= retries or not _is_retryable_error(exc):
                break
            await asyncio.sleep(1 + attempt)
    raise last_exc


async def _run_t2i(payload: Dict[str, Any]) -> Tuple[str, List[str], Dict[str, Any]]:
    primary = (settings.AI_PROVIDER or "fal").lower()
    if primary == "replicate":
        primary_name, primary_fn = "replicate", lambda: replicate_t2i(payload, timeout_sec=300)
        fallback_name, fallback_fn = "fal", lambda: fal_t2i(payload, timeout_sec=300)
    else:
        primary_name, primary_fn = "fal", lambda: fal_t2i(payload, timeout_sec=300)
        fallback_name, fallback_fn = "replicate", lambda: replicate_t2i(payload, timeout_sec=300)

    try:
        urls, meta = await _with_retries(primary_fn, retries=2)
        return primary_name, urls, meta
    except Exception as exc:
        if _is_retryable_error(exc):
            try:
                urls, meta = await _with_retries(fallback_fn, retries=2)
                return fallback_name, urls, meta
            except Exception:
                raise exc
        raise exc


async def _run_i2i(payload: Dict[str, Any]) -> Tuple[str, List[str], Dict[str, Any]]:
    primary = (settings.AI_PROVIDER or "fal").lower()
    if primary == "replicate":
        primary_name, primary_fn = "replicate", lambda: replicate_i2i(payload, timeout_sec=300)
        fallback_name, fallback_fn = "fal", lambda: fal_i2i(payload, timeout_sec=300)
    else:
        primary_name, primary_fn = "fal", lambda: fal_i2i(payload, timeout_sec=300)
        fallback_name, fallback_fn = "replicate", lambda: replicate_i2i(payload, timeout_sec=300)

    try:
        urls, meta = await _with_retries(primary_fn, retries=2)
        return primary_name, urls, meta
    except Exception as exc:
        if _is_retryable_error(exc):
            try:
                urls, meta = await _with_retries(fallback_fn, retries=2)
                return fallback_name, urls, meta
            except Exception:
                raise exc
        raise exc


async def process_ai_job(job_id: str, job: Dict[str, Any]) -> None:
    kind = (job.get("kind") or "").lower()
    payload = job.get("payload") or {}
    try:
        print(f"[ai] job_id={job_id} kind={kind} stage=running")
        await update_job_status(job_id, RUNNING, stage="running")
        if kind == "image_t2i":
            provider, urls, meta = await _run_t2i(payload)
            print(f"[ai] job_id={job_id} kind={kind} provider={provider} stage=uploading")
            await update_job_status(job_id, RUNNING, stage="uploading")
            uploaded = await _download_and_upload(urls)
            if not uploaded:
                raise RuntimeError("No images uploaded to Cloudinary")
            result = {"provider": provider, "images": uploaded, "meta": meta}
            await update_job_status(job_id, DONE, result=result, stage="done")
            print(f"[ai] job_id={job_id} kind={kind} provider={provider} stage=done")
            return

        if kind == "image_i2i":
            provider, urls, meta = await _run_i2i(payload)
            print(f"[ai] job_id={job_id} kind={kind} provider={provider} stage=uploading")
            await update_job_status(job_id, RUNNING, stage="uploading")
            uploaded = await _download_and_upload(urls)
            if not uploaded:
                raise RuntimeError("No images uploaded to Cloudinary")
            result = {"provider": provider, "images": uploaded, "meta": meta}
            await update_job_status(job_id, DONE, result=result, stage="done")
            print(f"[ai] job_id={job_id} kind={kind} provider={provider} stage=done")
            return

        if kind == "avatar_batch":
            image_urls = payload.get("image_urls") or []
            variants = int(payload.get("variants_per_image") or 1)
            items: List[Dict[str, Any]] = []
            success_count = 0
            for src_url in image_urls:
                item: Dict[str, Any] = {"source_image_url": src_url, "generated_images": [], "meta": {}}
                try:
                    generated: List[str] = []
                    provider = None
                    meta: Dict[str, Any] = {}
                    for _ in range(variants):
                        provider, urls, meta = await _run_i2i(
                            {
                                **payload,
                                "image_url": src_url,
                            }
                        )
                        uploaded = await _download_and_upload(urls)
                        generated.extend(uploaded)
                    item["generated_images"] = generated
                    item["meta"] = {**meta, "provider": provider}
                    if generated:
                        success_count += 1
                except Exception as exc:
                    item["error"] = str(exc)
                items.append(item)
                await asyncio.sleep(0)  # yield

            if success_count == 0:
                await update_job_status(job_id, ERROR, error="All batch items failed", stage="error")
                print(f"[ai] job_id={job_id} kind={kind} stage=error error=All batch items failed")
                return

            result = {
                "provider": "mixed",
                "items": items,
                "summary": {
                    "count_sources": len(image_urls),
                    "variants_per_image": variants,
                    "total_generated": sum(len(it.get("generated_images") or []) for it in items),
                },
            }
            await update_job_status(job_id, DONE, result=result, stage="done")
            print(f"[ai] job_id={job_id} kind={kind} stage=done")
            return

        await update_job_status(job_id, ERROR, error=f"Unknown AI job kind: {kind}", stage="error")
    except Exception as exc:
        await update_job_status(job_id, ERROR, error=str(exc), stage="error")
        print(f"[ai] job_id={job_id} kind={kind} stage=error error={exc}")
