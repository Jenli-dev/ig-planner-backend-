import asyncio
from typing import Any, Dict, List, Optional, Tuple

import httpx

from config import settings


class AIProviderError(Exception):
    def __init__(self, message: str, *, retryable: bool = False, status_code: Optional[int] = None):
        super().__init__(message)
        self.retryable = retryable
        self.status_code = status_code


def _extract_image_urls(payload: Dict[str, Any]) -> List[str]:
    images = []
    if isinstance(payload.get("images"), list):
        for it in payload["images"]:
            if isinstance(it, dict) and it.get("url"):
                images.append(it["url"])
            elif isinstance(it, str):
                images.append(it)
    if isinstance(payload.get("image"), str):
        images.append(payload["image"])
    if isinstance(payload.get("output"), list):
        for it in payload["output"]:
            if isinstance(it, str):
                images.append(it)
    if isinstance(payload.get("output"), str):
        images.append(payload["output"])
    return [u for u in images if isinstance(u, str) and u.strip()]


async def _http_post_json(
    url: str,
    *,
    headers: Dict[str, str],
    body: Dict[str, Any],
    timeout_sec: float,
) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=timeout_sec) as client:
        r = await client.post(url, json=body, headers=headers)
        if r.status_code >= 500:
            raise AIProviderError(f"Provider 5xx: {r.status_code}", retryable=True, status_code=r.status_code)
        if r.status_code >= 400:
            try:
                err = r.json()
            except Exception:
                err = r.text
            raise AIProviderError(f"Provider 4xx: {r.status_code}: {err}", retryable=False, status_code=r.status_code)
        return r.json()


async def fal_t2i(payload: Dict[str, Any], *, timeout_sec: float = 300) -> Tuple[List[str], Dict[str, Any]]:
    if not settings.FAL_KEY:
        raise AIProviderError("FAL_KEY is not set", retryable=False)
    headers = {"Authorization": f"Key {settings.FAL_KEY}"}
    body = {
        "prompt": payload.get("prompt"),
        "aspect_ratio": payload.get("aspect_ratio"),
        "steps": payload.get("steps"),
        "seed": payload.get("seed"),
    }
    data = await _http_post_json(settings.FAL_T2I_ENDPOINT, headers=headers, body=body, timeout_sec=timeout_sec)
    urls = _extract_image_urls(data)
    if not urls:
        raise AIProviderError(f"No images in FAL response: {data}", retryable=False)
    return urls, {"model": settings.FAL_T2I_ENDPOINT, "aspect_ratio": payload.get("aspect_ratio"), "seed": payload.get("seed")}


async def fal_i2i(payload: Dict[str, Any], *, timeout_sec: float = 300) -> Tuple[List[str], Dict[str, Any]]:
    if not settings.FAL_KEY:
        raise AIProviderError("FAL_KEY is not set", retryable=False)
    headers = {"Authorization": f"Key {settings.FAL_KEY}"}
    body = {
        "prompt": payload.get("prompt"),
        "image_urls": [payload.get("image_url")],
        "strength": payload.get("strength"),
        "aspect_ratio": payload.get("aspect_ratio"),
        "num_inference_steps": payload.get("steps"),
        "seed": payload.get("seed"),
    }
    data = await _http_post_json(settings.FAL_I2I_ENDPOINT, headers=headers, body=body, timeout_sec=timeout_sec)
    urls = _extract_image_urls(data)
    if not urls:
        raise AIProviderError(f"No images in FAL response: {data}", retryable=False)
    return urls, {"model": settings.FAL_I2I_ENDPOINT, "aspect_ratio": payload.get("aspect_ratio"), "seed": payload.get("seed")}


async def _replicate_poll(
    prediction_id: str,
    *,
    token: str,
    timeout_sec: float = 300,
) -> Dict[str, Any]:
    headers = {"Authorization": f"Token {token}"}
    async with httpx.AsyncClient(timeout=timeout_sec) as client:
        for _ in range(120):
            r = await client.get(f"https://api.replicate.com/v1/predictions/{prediction_id}", headers=headers)
            if r.status_code >= 500:
                raise AIProviderError(f"Replicate 5xx: {r.status_code}", retryable=True, status_code=r.status_code)
            if r.status_code >= 400:
                raise AIProviderError(f"Replicate 4xx: {r.status_code}: {r.text}", retryable=False, status_code=r.status_code)
            data = r.json()
            status = data.get("status")
            if status in ("succeeded", "failed", "canceled"):
                return data
            await asyncio.sleep(2)
    raise AIProviderError("Replicate polling timeout", retryable=True)


async def replicate_t2i(payload: Dict[str, Any], *, timeout_sec: float = 300) -> Tuple[List[str], Dict[str, Any]]:
    if not settings.REPLICATE_API_TOKEN or not settings.REPLICATE_T2I_MODEL:
        raise AIProviderError("Replicate token/model not set", retryable=False)
    headers = {"Authorization": f"Token {settings.REPLICATE_API_TOKEN}"}
    body = {
        "version": settings.REPLICATE_T2I_MODEL,
        "input": {
            "prompt": payload.get("prompt"),
            "aspect_ratio": payload.get("aspect_ratio"),
            "num_inference_steps": payload.get("steps"),
            "seed": payload.get("seed"),
        },
    }
    data = await _http_post_json("https://api.replicate.com/v1/predictions", headers=headers, body=body, timeout_sec=timeout_sec)
    prediction_id = data.get("id")
    if not prediction_id:
        raise AIProviderError(f"Replicate create failed: {data}", retryable=False)
    final = await _replicate_poll(prediction_id, token=settings.REPLICATE_API_TOKEN, timeout_sec=timeout_sec)
    if final.get("status") != "succeeded":
        raise AIProviderError(f"Replicate failed: {final}", retryable=False)
    output = final.get("output")
    urls = []
    if isinstance(output, list):
        urls = [u for u in output if isinstance(u, str)]
    elif isinstance(output, str):
        urls = [output]
    if not urls:
        raise AIProviderError(f"No images in Replicate response: {final}", retryable=False)
    return urls, {"model": settings.REPLICATE_T2I_MODEL, "aspect_ratio": payload.get("aspect_ratio"), "seed": payload.get("seed")}


async def replicate_i2i(payload: Dict[str, Any], *, timeout_sec: float = 300) -> Tuple[List[str], Dict[str, Any]]:
    if not settings.REPLICATE_API_TOKEN or not settings.REPLICATE_I2I_MODEL:
        raise AIProviderError("Replicate token/model not set", retryable=False)
    headers = {"Authorization": f"Token {settings.REPLICATE_API_TOKEN}"}
    body = {
        "version": settings.REPLICATE_I2I_MODEL,
        "input": {
            "image": payload.get("image_url"),
            "prompt": payload.get("prompt"),
            "strength": payload.get("strength"),
            "aspect_ratio": payload.get("aspect_ratio"),
            "num_inference_steps": payload.get("steps"),
            "seed": payload.get("seed"),
        },
    }
    data = await _http_post_json("https://api.replicate.com/v1/predictions", headers=headers, body=body, timeout_sec=timeout_sec)
    prediction_id = data.get("id")
    if not prediction_id:
        raise AIProviderError(f"Replicate create failed: {data}", retryable=False)
    final = await _replicate_poll(prediction_id, token=settings.REPLICATE_API_TOKEN, timeout_sec=timeout_sec)
    if final.get("status") != "succeeded":
        raise AIProviderError(f"Replicate failed: {final}", retryable=False)
    output = final.get("output")
    urls = []
    if isinstance(output, list):
        urls = [u for u in output if isinstance(u, str)]
    elif isinstance(output, str):
        urls = [output]
    if not urls:
        raise AIProviderError(f"No images in Replicate response: {final}", retryable=False)
    return urls, {"model": settings.REPLICATE_I2I_MODEL, "aspect_ratio": payload.get("aspect_ratio"), "seed": payload.get("seed")}
