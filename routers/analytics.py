from typing import Optional, Dict, Any
from fastapi import APIRouter, Body, HTTPException
import httpx
import base64
import jwt
import time
from datetime import datetime, timedelta

from config import settings

router = APIRouter(prefix="/analytics", tags=["analytics"])


# MARK: - Apple Search Ads Attribution

@router.post("/search-ads/attribution")
async def search_ads_attribution(
    attribution_token: str = Body(...),
    idfa: Optional[str] = Body(None),
    timestamp: Optional[float] = Body(None),
):
    """
    Receive attribution token from iOS app and forward to Apple Search Ads API
    
    Requirements:
    1. Apple Search Ads API Key (.p8 file)
    2. Key ID from App Store Connect
    3. Issuer ID from App Store Connect
    """
    if not all([settings.APPLE_SEARCH_ADS_KEY_ID, settings.APPLE_SEARCH_ADS_ISSUER_ID, settings.APPLE_SEARCH_ADS_PRIVATE_KEY]):
        raise HTTPException(
            status_code=500,
            detail="Apple Search Ads API credentials not configured"
        )
    
    try:
        # Generate JWT token for Apple Search Ads API
        private_key = base64.b64decode(settings.APPLE_SEARCH_ADS_PRIVATE_KEY).decode('utf-8')
        
        headers = {
            "alg": "ES256",
            "kid": settings.APPLE_SEARCH_ADS_KEY_ID,
            "typ": "JWT"
        }
        
        payload = {
            "iss": settings.APPLE_SEARCH_ADS_ISSUER_ID,
            "iat": int(time.time()),
            "exp": int(time.time()) + 3600,  # 1 hour
            "aud": "appstoreconnect-v1"
        }
        
        token = jwt.encode(payload, private_key, algorithm="ES256", headers=headers)
        
        # Call Apple Search Ads Attribution API
        # Note: This is a simplified example. Actual API endpoint may differ.
        async with httpx.AsyncClient() as client:
            # This is a placeholder - actual endpoint structure may vary
            # See: https://developer.apple.com/documentation/appstoreconnectapi
            response = await client.post(
                "https://api.appstoreconnect.apple.com/v1/searchAds/attribution",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                },
                json={
                    "attribution_token": attribution_token,
                    "idfa": idfa
                }
            )
            
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Apple Search Ads API error: {response.text}"
                )
            
            data = response.json()
            return {
                "ok": True,
                "campaign_id": data.get("campaignId"),
                "ad_group_id": data.get("adGroupId"),
                "keyword": data.get("keyword"),
                "click_date": data.get("clickDate")
            }
            
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process Search Ads attribution: {str(e)}"
        )


# MARK: - Apphud Events Forwarding

@router.post("/apphud/event")
async def apphud_event(
    event_name: str = Body(...),
    user_id: Optional[str] = Body(None),
    properties: Optional[Dict[str, Any]] = Body(None),
):
    """
    Forward events to Apphud API
    
    Requires: APPHUD_API_KEY in environment
    """
    if not settings.APPHUD_API_KEY:
        raise HTTPException(
            status_code=500,
            detail="Apphud API key not configured"
        )
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://api.apphud.com/v1/customers/events",
                headers={
                    "Authorization": f"Bearer {settings.APPHUD_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "name": event_name,
                    "user_id": user_id,
                    "properties": properties or {}
                },
                timeout=10.0
            )
            
            response.raise_for_status()
            return {"ok": True, "data": response.json()}
            
    except httpx.HTTPStatusError as e:
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"Apphud API error: {e.response.text}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to send event to Apphud: {str(e)}"
        )


# MARK: - Adapty Events Forwarding

@router.post("/adapty/event")
async def adapty_event(
    event_name: str = Body(...),
    user_id: Optional[str] = Body(None),
    params: Optional[Dict[str, Any]] = Body(None),
):
    """
    Forward events to Adapty API
    
    Requires: ADAPTY_API_KEY in environment
    """
    if not settings.ADAPTY_API_KEY:
        raise HTTPException(
            status_code=500,
            detail="Adapty API key not configured"
        )
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://api.adapty.io/api/v2/sdk/events",
                headers={
                    "Authorization": f"Bearer {settings.ADAPTY_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "name": event_name,
                    "customer_user_id": user_id,
                    "params": params or {}
                },
                timeout=10.0
            )
            
            response.raise_for_status()
            return {"ok": True, "data": response.json()}
            
    except httpx.HTTPStatusError as e:
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"Adapty API error: {e.response.text}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to send event to Adapty: {str(e)}"
        )
