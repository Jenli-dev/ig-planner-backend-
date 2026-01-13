import secrets
from typing import Optional
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, HTTPException, Body
from fastapi.responses import RedirectResponse

from http_client import RetryClient
from meta_config import (
    APP_ID,
    APP_SECRET,
    REDIRECT_URI,
    SCOPES,
    META_AUTH,
    TOKEN_URL,
    GRAPH_BASE,
    IG_LONG_TOKEN,
)

router = APIRouter(prefix="/auth", tags=["auth"])
STATE_STORE = set()


@router.get("/oauth/start")
def oauth_start():
    if not APP_ID or not APP_SECRET or not REDIRECT_URI:
        raise HTTPException(
            500, "META_APP_ID / META_APP_SECRET / META_REDIRECT_URI are not set in env."
        )
    state = secrets.token_urlsafe(16)
    STATE_STORE.add(state)
    params = {
        "client_id": APP_ID,
        "redirect_uri": REDIRECT_URI,
        "state": state,
        "scope": ",".join(SCOPES) if isinstance(SCOPES, (list, tuple)) else SCOPES,
    }
    return RedirectResponse(f"{META_AUTH}?{urlencode(params)}")


@router.get("/oauth/callback")
async def oauth_callback(
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
):
    if error:
        raise HTTPException(400, f"OAuth error: {error}")
    if not code or not state or state not in STATE_STORE:
        raise HTTPException(400, "Invalid state or code")
    STATE_STORE.discard(state)

    async with RetryClient() as client:
        # 1) code -> short-lived
        r = await client.get(
            TOKEN_URL,
            params={
                "client_id": APP_ID,
                "client_secret": APP_SECRET,
                "redirect_uri": REDIRECT_URI,
                "code": code,
            },
            retries=4,
        )
        r.raise_for_status()
        short = r.json()

        # 2) short-lived -> long-lived
        r2 = await client.get(
            TOKEN_URL,
            params={
                "grant_type": "fb_exchange_token",
                "client_id": APP_ID,
                "client_secret": APP_SECRET,
                "fb_exchange_token": short["access_token"],
            },
            retries=4,
        )
        r2.raise_for_status()
        long_user = r2.json()
        user_token = long_user["access_token"]

        return {
            "ok": True,
            "short_lived": short,
            "long_lived": {
                "access_token": user_token,
                "token_type": long_user.get("token_type"),
                "expires_in": long_user.get("expires_in"),
            },
            "note": "Сохраните IG_ACCESS_TOKEN в переменных окружения сервера.",
        }


@router.get("/debug/scopes")
async def debug_scopes():
    """
    Token debug endpoint (moved from main.py).
    Returns scopes-format for convenience.
    """
    if not IG_LONG_TOKEN:
        raise HTTPException(400, "IG_ACCESS_TOKEN is not set in env.")
    if not APP_ID or not APP_SECRET:
        raise HTTPException(500, "META_APP_ID / META_APP_SECRET are not set.")

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(
            f"{GRAPH_BASE}/debug_token",
            params={
                "input_token": IG_LONG_TOKEN,
                "access_token": f"{APP_ID}|{APP_SECRET}",
            },
        )
        r.raise_for_status()

    info = r.json().get("data", {})
    return {
        "ok": True,
        "is_valid": info.get("is_valid"),
        "scopes": info.get("scopes", []),
        "type": info.get("type"),
        "data": info,
    }


@router.post("/refresh-token")
async def refresh_token(current_token: Optional[str] = Body(None, embed=True)):
    token_to_refresh = (current_token or IG_LONG_TOKEN or "").strip()
    if not token_to_refresh:
        raise HTTPException(
            400,
            "No token to refresh. Provide current_token or set IG_ACCESS_TOKEN in env.",
        )
    if not APP_ID or not APP_SECRET:
        raise HTTPException(500, "META_APP_ID / META_APP_SECRET are not set.")

    async with RetryClient() as client:
        r = await client.get(
            TOKEN_URL,
            params={
                "grant_type": "fb_exchange_token",
                "client_id": APP_ID,
                "client_secret": APP_SECRET,
                "fb_exchange_token": token_to_refresh,
            },
            retries=4,
        )
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            try:
                err = e.response.json()
            except Exception:
                err = {"error": {"message": e.response.text if e.response else str(e)}}
            raise HTTPException(e.response.status_code if e.response else 500, err)

        data = r.json()
        new_token = data.get("access_token")
        return {
            "ok": True,
            "new_access_token": new_token,
            "token_type": data.get("token_type"),
            "expires_in": data.get("expires_in"),
            "note": "Сохрани new_access_token в Render → Environment как IG_ACCESS_TOKEN и перезапусти сервис.",
        }
