from typing import Dict, Any

from fastapi import HTTPException

from http_client import RetryClient
from meta_config import IG_LONG_TOKEN, PAGE_ID_ENV, ME_URL, GRAPH_BASE


async def _resolve_page_and_ig_id(client: RetryClient) -> Dict[str, Any]:
    if not IG_LONG_TOKEN:
        raise HTTPException(500, "IG_ACCESS_TOKEN is not set in env.")

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


async def load_state() -> Dict[str, Any]:
    async with RetryClient() as client:
        resolved = await _resolve_page_and_ig_id(client)
        return {
            "ig_id": resolved["ig_id"],
            "page_token": IG_LONG_TOKEN,
            "user_token": IG_LONG_TOKEN,
            "page_id": resolved["page_id"],
            "ig_username": resolved["ig_username"],
        }
