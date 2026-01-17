from typing import Dict, Any, Optional

from fastapi import HTTPException

from http_client import RetryClient
from meta_config import IG_LONG_TOKEN, PAGE_ID_ENV, ME_URL, GRAPH_BASE
from services.account_manager import (
    get_active_account,
    get_account,
    _default_user_id,
)


async def _resolve_page_and_ig_id(
    client: RetryClient, access_token: str, page_id: Optional[str] = None
) -> Dict[str, Any]:
    """Разрешает page_id и ig_id для заданного access_token"""
    if not access_token:
        raise HTTPException(500, "Access token is required.")

    if not page_id:
        # Пытаемся получить первый Page
        r = await client.get(
            f"{ME_URL}/accounts",
            params={"access_token": access_token, "fields": "id,name,access_token"},
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
            "access_token": access_token,
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


async def load_state(
    user_id: Optional[str] = None, account_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Загружает состояние для работы с Instagram API.
    
    Args:
        user_id: ID пользователя (если None, используется default_user)
        account_id: ID аккаунта (если None, используется активный аккаунт)
    
    Returns:
        Словарь с данными аккаунта: ig_id, page_token, user_token, page_id, ig_username
    """
    user_id = user_id or _default_user_id()
    
    # Пытаемся получить аккаунт из Redis
    account = None
    if account_id:
        account = await get_account(user_id, account_id)
    else:
        account = await get_active_account(user_id)
    
    # Если аккаунт найден в Redis, используем его
    if account:
        access_token = account.get("access_token")
        page_id = account.get("page_id")
        ig_id = account.get("ig_id")
        ig_username = account.get("ig_username")
        
        # Если ig_id отсутствует, получаем его через API
        if not ig_id:
            async with RetryClient() as client:
                resolved = await _resolve_page_and_ig_id(client, access_token, page_id)
                ig_id = resolved["ig_id"]
                ig_username = resolved["ig_username"]
                # Обновляем кеш
                account["ig_id"] = ig_id
                account["ig_username"] = ig_username
        
        return {
            "ig_id": ig_id,
            "page_token": access_token,
            "user_token": access_token,
            "page_id": page_id,
            "ig_username": ig_username,
        }
    
    # Fallback: используем старую логику (из env переменных)
    if not IG_LONG_TOKEN:
        raise HTTPException(500, "IG_ACCESS_TOKEN is not set in env and no account found.")
    
    async with RetryClient() as client:
        resolved = await _resolve_page_and_ig_id(client, IG_LONG_TOKEN, PAGE_ID_ENV)
        return {
            "ig_id": resolved["ig_id"],
            "page_token": IG_LONG_TOKEN,
            "user_token": IG_LONG_TOKEN,
            "page_id": resolved["page_id"],
            "ig_username": resolved["ig_username"],
        }
