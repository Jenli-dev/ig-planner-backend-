#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx


def request_json(
    client: httpx.Client,
    method: str,
    path: str,
    *,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: float = 180.0,
) -> Tuple[int, Any]:
    url = f"{client.base_url}{path}"
    try:
        resp = client.request(method, url, json=json_body, timeout=timeout)
        try:
            payload = resp.json()
        except Exception:
            payload = resp.text
        return resp.status_code, payload
    except Exception as exc:
        return 0, {"error": str(exc)}


def request_multipart(
    client: httpx.Client,
    path: str,
    *,
    file_name: str,
    file_bytes: bytes,
    content_type: str,
    timeout: float = 180.0,
) -> Tuple[int, Any]:
    url = f"{client.base_url}{path}"
    try:
        files = {"file": (file_name, file_bytes, content_type)}
        resp = client.post(url, files=files, timeout=timeout)
        try:
            payload = resp.json()
        except Exception:
            payload = resp.text
        return resp.status_code, payload
    except Exception as exc:
        return 0, {"error": str(exc)}


def record(results: List[Dict[str, Any]], name: str, status: int, payload: Any) -> None:
    ok = bool(status and 200 <= status < 300)
    results.append({"name": name, "ok": ok, "status": status, "payload": payload})


def pick_media(items: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    img = next((m for m in items if (m.get("media_type") or "").upper() == "IMAGE"), None)
    vid = next((m for m in items if (m.get("media_type") or "").upper() == "VIDEO"), None)
    any_item = items[0] if items else None
    return img, vid, any_item


def poll_job(
    client: httpx.Client,
    status_path: str,
    *,
    timeout_sec: int = 300,
    sleep_sec: int = 2,
) -> Tuple[int, Any]:
    end_at = time.time() + timeout_sec
    last_status, last_payload = 0, {}
    while time.time() < end_at:
        last_status, last_payload = request_json(client, "GET", status_path)
        if isinstance(last_payload, dict) and last_payload.get("status") in ("DONE", "ERROR"):
            break
        time.sleep(sleep_sec)
    return last_status, last_payload


def fetch_bytes(url: str) -> Tuple[Optional[bytes], Optional[str]]:
    try:
        with httpx.Client(timeout=30) as client:
            r = client.get(url)
            r.raise_for_status()
            ctype = r.headers.get("Content-Type", "image/jpeg")
            return r.content, ctype
    except Exception:
        return None, None


def main() -> int:
    parser = argparse.ArgumentParser(description="E2E checks for IG Planner backend")
    parser.add_argument("--base-url", default=os.getenv("BASE_URL", "http://127.0.0.1:8000"))
    parser.add_argument("--allow-mutating", action="store_true", help="Allow comment create/delete and cleanup.")
    parser.add_argument("--allow-publish", action="store_true", help="Allow publish endpoints.")
    parser.add_argument("--allow-filter", action="store_true", help="Allow /media/filter/video checks.")
    parser.add_argument("--allow-cloudinary", action="store_true", help="Allow Cloudinary upload checks.")
    parser.add_argument("--allow-ai", action="store_true", help="Allow AI generation checks.")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    results: List[Dict[str, Any]] = []

    with httpx.Client(base_url=args.base_url) as client:
        status, payload = request_json(client, "GET", "/health")
        record(results, "GET /health", status, payload)

        status, payload = request_json(client, "GET", "/")
        record(results, "GET /", status, payload)

        status, payload = request_json(client, "GET", "/me/instagram")
        record(results, "GET /me/instagram", status, payload)

        status, payload = request_json(client, "GET", "/ig/media")
        record(results, "GET /ig/media", status, payload)
        media_items = payload.get("data", []) if isinstance(payload, dict) else []
        img, vid, any_item = pick_media(media_items)

        if any_item and any_item.get("id"):
            status, payload = request_json(
                client,
                "GET",
                f"/ig/insights/media?media_id={any_item['id']}",
            )
            record(results, "GET /ig/insights/media", status, payload)

        status, payload = request_json(
            client,
            "GET",
            "/ig/insights/account?metrics=reach,profile_views&period=day",
        )
        record(results, "GET /ig/insights/account", status, payload)

        if any_item and any_item.get("id"):
            status, payload = request_json(
                client,
                "GET",
                f"/ig/comments?media_id={any_item['id']}&limit=5",
            )
            record(results, "GET /ig/comments", status, payload)

        comment_id = None
        if args.allow_mutating and any_item and any_item.get("id"):
            status, payload = request_json(
                client,
                "POST",
                "/ig/comment",
                json_body={"media_id": any_item["id"], "message": "e2e comment"},
            )
            record(results, "POST /ig/comment", status, payload)
            if isinstance(payload, dict):
                comment_id = (payload.get("result") or {}).get("id")

        if args.allow_mutating and comment_id:
            status, payload = request_json(
                client,
                "POST",
                "/ig/comments/reply-many",
                json_body={"comment_ids": [comment_id], "message": "e2e reply", "delay_ms": 100},
            )
            record(results, "POST /ig/comments/reply-many", status, payload)

            status, payload = request_json(
                client,
                "POST",
                "/ig/comments/hide",
                json_body={"comment_id": comment_id, "hide": True},
            )
            record(results, "POST /ig/comments/hide", status, payload)

            status, payload = request_json(
                client,
                "POST",
                "/ig/comments/delete",
                json_body={"comment_id": comment_id},
            )
            record(results, "POST /ig/comments/delete", status, payload)

        if img and img.get("media_url"):
            status, payload = request_json(
                client,
                "POST",
                "/media/validate",
                json_body={"url": img["media_url"], "type": "image", "target": "IMAGE"},
            )
            record(results, "POST /media/validate (image)", status, payload)

        if vid and vid.get("media_url"):
            status, payload = request_json(
                client,
                "POST",
                "/media/validate",
                json_body={"url": vid["media_url"], "type": "video", "target": "REELS"},
            )
            record(results, "POST /media/validate (video)", status, payload)

        if args.allow_filter and vid and vid.get("media_url"):
            status, payload = request_json(
                client,
                "POST",
                "/media/filter/video",
                json_body={"url": vid["media_url"], "preset": "cinematic", "intensity": 0.7},
            )
            record(results, "POST /media/filter/video", status, payload)

            if isinstance(payload, dict) and payload.get("job_id"):
                job_id = payload["job_id"]
                s2, p2 = poll_job(client, f"/media/filter/status?job_id={job_id}", timeout_sec=180)
                record(results, "GET /media/filter/status", s2, p2)

        cloud_img_url = None
        cloud_vid_url = None
        cloud_vid_public_id = None

        if args.allow_cloudinary:
            if img and img.get("media_url"):
                status, payload = request_json(
                    client,
                    "POST",
                    "/util/cloudinary/upload",
                    json_body={"file_url": img["media_url"], "resource_type": "image", "folder": "ig_planner_tests"},
                )
                record(results, "POST /util/cloudinary/upload (image)", status, payload)
                if isinstance(payload, dict):
                    cloud_img_url = payload.get("secure_url")

            if vid and vid.get("media_url"):
                status, payload = request_json(
                    client,
                    "POST",
                    "/util/cloudinary/upload",
                    json_body={"file_url": vid["media_url"], "resource_type": "video", "folder": "ig_planner_tests"},
                    timeout=300,
                )
                record(results, "POST /util/cloudinary/upload (video)", status, payload)
                if isinstance(payload, dict):
                    cloud_vid_url = payload.get("secure_url")
                    cloud_vid_public_id = payload.get("public_id")

        if args.allow_publish and cloud_img_url:
            status, payload = request_json(
                client,
                "POST",
                "/ig/publish/image",
                json_body={"image_url": cloud_img_url, "caption": "e2e image via cloudinary"},
                timeout=180,
            )
            record(results, "POST /ig/publish/image", status, payload)

        if args.allow_publish and cloud_vid_url:
            status, payload = request_json(
                client,
                "POST",
                "/ig/publish/video",
                json_body={"video_url": cloud_vid_url, "caption": "e2e video via cloudinary", "share_to_feed": True},
                timeout=300,
            )
            record(results, "POST /ig/publish/video", status, payload)

        if args.allow_publish and cloud_vid_public_id:
            status, payload = request_json(
                client,
                "POST",
                "/ig/publish/video_from_cloudinary",
                json_body={"public_id": cloud_vid_public_id, "caption": "e2e video via public_id", "share_to_feed": True},
                timeout=300,
            )
            record(results, "POST /ig/publish/video_from_cloudinary", status, payload)

        # Stories publishing
        if args.allow_publish and cloud_img_url:
            status, payload = request_json(
                client,
                "POST",
                "/ig/publish/story/image",
                json_body={"image_url": cloud_img_url},
                timeout=180,
            )
            record(results, "POST /ig/publish/story/image", status, payload)

        if args.allow_publish and cloud_vid_url:
            status, payload = request_json(
                client,
                "POST",
                "/ig/publish/story/video",
                json_body={"video_url": cloud_vid_url},
                timeout=300,
            )
            record(results, "POST /ig/publish/story/video", status, payload)

        status, payload = request_json(client, "GET", "/util/fonts?limit=5")
        record(results, "GET /util/fonts", status, payload)

        if args.allow_mutating:
            status, payload = request_json(client, "DELETE", "/util/cleanup?hours=0")
            record(results, "DELETE /util/cleanup", status, payload)

        # AI endpoints
        if args.allow_ai:
            ai_image_url = None
            if args.allow_cloudinary:
                source_url = None
                if img and img.get("media_url"):
                    source_url = img["media_url"]
                if not source_url:
                    source_url = "https://www.gstatic.com/webp/gallery/1.jpg"
                data_bytes, ctype = fetch_bytes(source_url)
                if data_bytes:
                    status, payload = request_multipart(
                        client,
                        "/uploads/image",
                        file_name="e2e.jpg",
                        file_bytes=data_bytes,
                        content_type=ctype or "image/jpeg",
                        timeout=120,
                    )
                    record(results, "POST /uploads/image", status, payload)
                    if isinstance(payload, dict):
                        ai_image_url = payload.get("image_url")

            status, payload = request_json(
                client,
                "POST",
                "/ai/generate/text",
                json_body={"prompt": "studio portrait, soft light", "aspect_ratio": "1:1", "steps": 30},
                timeout=60,
            )
            record(results, "POST /ai/generate/text", status, payload)
            if isinstance(payload, dict) and payload.get("job_id"):
                s2, p2 = poll_job(client, f"/ai/status?job_id={payload['job_id']}", timeout_sec=300)
                record(results, "GET /ai/status (t2i)", s2, p2)

            if ai_image_url:
                status, payload = request_json(
                    client,
                    "POST",
                    "/ai/generate/image",
                    json_body={
                        "image_url": ai_image_url,
                        "prompt": "cinematic portrait, warm tones",
                        "strength": 0.6,
                        "aspect_ratio": "3:4",
                        "steps": 30,
                    },
                    timeout=60,
                )
                record(results, "POST /ai/generate/image", status, payload)
                if isinstance(payload, dict) and payload.get("job_id"):
                    s2, p2 = poll_job(client, f"/ai/status?job_id={payload['job_id']}", timeout_sec=300)
                    record(results, "GET /ai/status (i2i)", s2, p2)

                batch_urls = [ai_image_url] * 15
                status, payload = request_json(
                    client,
                    "POST",
                    "/ai/generate/batch",
                    json_body={
                        "image_urls": batch_urls,
                        "prompt": "avatar style, clean skin, studio light",
                        "strength": 0.55,
                        "aspect_ratio": "1:1",
                        "steps": 30,
                        "variants_per_image": 1,
                    },
                    timeout=60,
                )
                record(results, "POST /ai/generate/batch", status, payload)
                if isinstance(payload, dict) and payload.get("job_id"):
                    s2, p2 = poll_job(client, f"/ai/status?job_id={payload['job_id']}", timeout_sec=600)
                    record(results, "GET /ai/status (batch)", s2, p2)

    # summary
    total = len(results)
    failed = [r for r in results if not r["ok"]]
    print(json.dumps({"total": total, "failed": len(failed)}, indent=2))
    if args.verbose or failed:
        for r in results:
            if args.verbose or not r["ok"]:
                print(f"{r['name']} -> {r['status']}")
                print(r["payload"])
                print("---")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
