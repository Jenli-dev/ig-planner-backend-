# routers/accounts.py
"""
Роутер для управления множественными Instagram аккаунтами.
"""
import json
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, Body
import httpx

from http_client import RetryClient
from meta_config import ME_URL, GRAPH_BASE, APP_ID, APP_SECRET
from jobs import get_redis
from services.account_manager import (
    add_account,
    get_account,
    list_accounts,
    set_active_account,
    get_active_account,
    remove_account,
    _default_user_id,
    initialize_from_env,
    _account_key,
)

router = APIRouter(prefix="/accounts", tags=["accounts"])


@router.get("/list")
async def accounts_list(user_id: Optional[str] = Query(None, description="User ID (default: default_user)")):
    """
    Получает список всех аккаунтов пользователя.
    """
    user_id = user_id or _default_user_id()
    accounts = await list_accounts(user_id)
    return {"ok": True, "accounts": accounts, "count": len(accounts)}


@router.get("/active")
async def accounts_active(user_id: Optional[str] = Query(None, description="User ID (default: default_user)")):
    """
    Получает активный аккаунт пользователя.
    """
    user_id = user_id or _default_user_id()
    account = await get_active_account(user_id)
    
    if not account:
        # Если нет активного аккаунта, пытаемся инициализировать из env
        account_data = await initialize_from_env()
        if account_data:
            account = await get_active_account(user_id)
    
    if not account:
        return {"ok": False, "error": "No active account found"}
    
    # Не возвращаем токен в ответе (безопасность)
    account_safe = {k: v for k, v in account.items() if k != "access_token"}
    return {"ok": True, "account": account_safe}


@router.post("/switch")
async def accounts_switch(
    account_id: str = Body(..., embed=True, description="Account ID (page_id) to switch to"),
    user_id: Optional[str] = Body(None, embed=True, description="User ID (default: default_user)"),
):
    """
    Переключается на другой аккаунт.
    """
    user_id = user_id or _default_user_id()
    
    success = await set_active_account(user_id, account_id)
    if not success:
        raise HTTPException(404, f"Account {account_id} not found")
    
    account = await get_account(user_id, account_id)
    if not account:
        raise HTTPException(404, f"Account {account_id} not found")
    
    account_safe = {k: v for k, v in account.items() if k != "access_token"}
    return {"ok": True, "message": "Account switched", "account": account_safe}


@router.post("/add")
async def accounts_add(
    access_token: str = Body(..., embed=True, description="Instagram access token"),
    page_id: Optional[str] = Body(None, embed=True, description="Facebook Page ID (optional)"),
    user_id: Optional[str] = Body(None, embed=True, description="User ID (default: default_user)"),
):
    """
    Добавляет новый аккаунт из access_token.
    """
    user_id = user_id or _default_user_id()
    
    async with RetryClient() as client:
        # Получаем список Pages
        r = await client.get(
            f"{ME_URL}/accounts",
            params={"access_token": access_token, "fields": "id,name,access_token"},
            retries=4,
        )
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=e.response.status_code if e.response else 400,
                detail=f"Failed to get pages: {e.response.json() if e.response else str(e)}"
            )
        
        pages = r.json().get("data") or []
        if not pages:
            raise HTTPException(400, "No Pages available for this token.")
        
        # Используем указанный page_id или первый из списка
        selected_page = None
        if page_id:
            selected_page = next((p for p in pages if p["id"] == page_id), None)
            if not selected_page:
                raise HTTPException(404, f"Page {page_id} not found in your Pages")
        else:
            selected_page = pages[0]
        
        page_id = selected_page["id"]
        page_name = selected_page.get("name", "Instagram Account")
        page_token = selected_page.get("access_token") or access_token
        
        # Получаем Instagram данные
        r2 = await client.get(
            f"{GRAPH_BASE}/{page_id}",
            params={
                "fields": "instagram_business_account{id,username}",
                "access_token": page_token,
            },
            retries=4,
        )
        r2.raise_for_status()
        ig = r2.json().get("instagram_business_account") or {}
        ig_id = ig.get("id")
        ig_username = ig.get("username")
        
        if not ig_id:
            raise HTTPException(400, "This Page has no instagram_business_account linked.")
        
        # Сохраняем аккаунт
        account = await add_account(
            user_id=user_id,
            page_id=page_id,
            page_name=page_name,
            access_token=page_token,
            ig_id=ig_id,
            ig_username=ig_username,
        )
        
        # Если это первый аккаунт, делаем его активным
        existing_accounts = await list_accounts(user_id)
        if len(existing_accounts) == 0:
            await set_active_account(user_id, page_id)
        
        account_safe = {k: v for k, v in account.items() if k != "access_token"}
        return {"ok": True, "message": "Account added", "account": account_safe}


@router.delete("/remove/{account_id}")
async def accounts_remove(
    account_id: str,
    user_id: Optional[str] = Query(None, description="User ID (default: default_user)"),
):
    """
    Удаляет аккаунт.
    """
    user_id = user_id or _default_user_id()
    
    success = await remove_account(user_id, account_id)
    if not success:
        raise HTTPException(404, f"Account {account_id} not found")
    
    return {"ok": True, "message": "Account removed"}


@router.get("/refresh/{account_id}")
async def accounts_refresh(
    account_id: str,
    user_id: Optional[str] = Query(None, description="User ID (default: default_user)"),
):
    """
    Обновляет данные аккаунта (получает свежие данные из Instagram API).
    """
    user_id = user_id or _default_user_id()
    
    account = await get_account(user_id, account_id)
    if not account:
        raise HTTPException(404, f"Account {account_id} not found")
    
    access_token = account.get("access_token")
    page_id = account.get("page_id")
    
    async with RetryClient() as client:
        # Обновляем Instagram данные
        r = await client.get(
            f"{GRAPH_BASE}/{page_id}",
            params={
                "fields": "instagram_business_account{id,username}",
                "access_token": access_token,
            },
            retries=4,
        )
        r.raise_for_status()
        ig = r.json().get("instagram_business_account") or {}
        ig_id = ig.get("id")
        ig_username = ig.get("username")
        
        if not ig_id:
            raise HTTPException(400, "This Page has no instagram_business_account linked.")
        
        # Обновляем аккаунт
        account["ig_id"] = ig_id
        account["ig_username"] = ig_username
        
        # Сохраняем обновленные данные
        r_redis = await get_redis()
        account_key = _account_key(user_id, account_id)
        await r_redis.set(account_key, json.dumps(account))
        
        account_safe = {k: v for k, v in account.items() if k != "access_token"}
        return {"ok": True, "message": "Account refreshed", "account": account_safe}
