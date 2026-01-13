# routers/me.py
from fastapi import APIRouter
import httpx

from services.ig_state import load_state
from http_client import RetryClient
from meta_config import IG_LONG_TOKEN, ME_URL, GRAPH_BASE

router = APIRouter(prefix="/me", tags=["me"])

@router.get("/instagram")
async def me_instagram():
    st = await load_state()
    return {
        "ok": True,
        "page_id": st["page_id"],
        "instagram_business_account": {"id": st["ig_id"], "username": st["ig_username"]},
    }

@router.get("/pages")
async def me_pages():
    if not IG_LONG_TOKEN:
        return {"ok": False, "error": "IG_ACCESS_TOKEN not set"}

    out = []
    async with RetryClient() as client:
        r = await client.get(
            f"{ME_URL}/accounts",
            params={"access_token": IG_LONG_TOKEN, "fields": "id,name,access_token"},
            retries=4,
        )
        try:
            r.raise_for_status()
            pages_all = list(r.json().get("data") or [])
        except httpx.HTTPStatusError as e:
            return {"ok": False, "status": e.response.status_code, "error": e.response.json()}

        if not pages_all:
            b = await client.get(f"{ME_URL}/businesses", params={"access_token": IG_LONG_TOKEN})
            b.raise_for_status()
            for biz in b.json().get("data") or []:
                bid = biz.get("id")
                if not bid:
                    continue
                op = await client.get(
                    f"{GRAPH_BASE}/{bid}/owned_pages",
                    params={"access_token": IG_LONG_TOKEN, "fields": "id,name,access_token"},
                )
                op.raise_for_status()
                pages_all.extend(op.json().get("data") or [])

        for p in pages_all:
            pid = p.get("id")
            name = p.get("name")
            ptok = p.get("access_token") or IG_LONG_TOKEN
            has_ig = None
            ig = None
            if pid:
                r2 = await client.get(
                    f"{GRAPH_BASE}/{pid}",
                    params={"fields": "instagram_business_account{id,username}", "access_token": ptok},
                )
                try:
                    r2.raise_for_status()
                    ig = (r2.json() or {}).get("instagram_business_account")
                    has_ig = bool(ig and ig.get("id"))
                except httpx.HTTPStatusError:
                    has_ig = False
            out.append({"id": pid, "name": name, "has_instagram_business": has_ig, "ig": ig})

    return {"ok": True, "pages": out}
