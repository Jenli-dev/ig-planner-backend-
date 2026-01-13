import json
from fastapi import APIRouter, HTTPException, Query, Body
from fastapi.responses import PlainTextResponse

from meta_config import VERIFY_TOKEN

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


@router.get("/instagram", response_class=PlainTextResponse)
async def instagram_webhook_verify(
    hub_mode: str = Query(None, alias="hub.mode"),
    hub_challenge: str = Query(None, alias="hub.challenge"),
    hub_verify_token: str = Query(None, alias="hub.verify_token"),
):
    if hub_mode == "subscribe" and hub_verify_token == VERIFY_TOKEN:
        return hub_challenge or ""
    raise HTTPException(status_code=403, detail="Verification failed")


@router.post("/instagram")
async def instagram_webhook_events(payload: dict = Body(...)):
    print("=== IG WEBHOOK EVENT ===")
    print(json.dumps(payload, indent=2))
    return {"status": "received", "ok": True}
    
