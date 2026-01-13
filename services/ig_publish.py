# services/ig_publish.py
import asyncio
import httpx
from typing import Optional
from http_client import RetryClient
from meta_config import GRAPH_BASE
from services.ig_state import load_state
from cloudinary_utils import cld_inject_transform, CLOUD_REELS_TRANSFORM

async def publish_reel(
    *,
    video_url: str,
    caption: Optional[str] = None,
    cover_url: Optional[str] = None,
    share_to_feed: bool = True,
):
    video_url = cld_inject_transform(video_url, CLOUD_REELS_TRANSFORM)
    st = await load_state()

    async with RetryClient() as client:
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

        r1 = await client.post(f"{GRAPH_BASE}/{st['ig_id']}/media", data=payload, retries=4, timeout=180)
        r1.raise_for_status()
        creation_id = (r1.json() or {}).get("id")
        if not creation_id:
            return {"ok": False, "stage": "create_container", "error": "no creation_id"}

        # wait status
        max_wait_sec, sleep_sec, waited = 150, 2, 0
        while waited < max_wait_sec:
            rstat = await client.get(
                f"{GRAPH_BASE}/{creation_id}",
                params={"fields": "status,status_code", "access_token": st["page_token"]},
                retries=4,
                timeout=60,
            )
            rstat.raise_for_status()
            js = rstat.json() or {}
            sc = js.get("status_code") or "IN_PROGRESS"
            if sc == "FINISHED":
                break
            if sc == "ERROR":
                return {"ok": False, "stage": "processing", "creation_id": creation_id, "status": js.get("status"), "status_code": sc}
            await asyncio.sleep(sleep_sec)
            waited += sleep_sec

        if sc != "FINISHED":
            return {"ok": False, "stage": "timeout", "creation_id": creation_id, "waited_sec": waited}

        r2 = await client.post(
            f"{GRAPH_BASE}/{st['ig_id']}/media_publish",
            data={"creation_id": creation_id, "access_token": st["page_token"]},
            retries=4,
            timeout=60,
        )
        r2.raise_for_status()
        return {"ok": True, "creation_id": creation_id, "published": r2.json()}
