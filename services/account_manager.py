# services/account_manager.py
"""
Менеджер для управления множественными Instagram аккаунтами.
Хранит информацию об аккаунтах в Redis.
"""
import json
from typing import Dict, Any, List, Optional
import redis.asyncio as redis

from config import settings
from jobs import get_redis


def _account_key(user_id: str, account_id: str) -> str:
    """Redis ключ для конкретного аккаунта"""
    return f"{settings.REDIS_PREFIX}:account:{user_id}:{account_id}"


def _user_accounts_key(user_id: str) -> str:
    """Redis ключ для списка аккаунтов пользователя"""
    return f"{settings.REDIS_PREFIX}:accounts:{user_id}"


def _default_user_id() -> str:
    """Возвращает дефолтный user_id (пока используем один токен из env)"""
    # TODO: В будущем здесь будет реальный user_id из JWT токена или сессии
    return "default_user"


async def add_account(
    user_id: str,
    page_id: str,
    page_name: str,
    access_token: str,
    ig_id: Optional[str] = None,
    ig_username: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Добавляет новый аккаунт в хранилище.
    
    Args:
        user_id: ID пользователя
        page_id: ID Facebook Page
        page_name: Название страницы
        access_token: Access token для этого аккаунта
        ig_id: Instagram Business Account ID (опционально)
        ig_username: Instagram username (опционально)
    
    Returns:
        Данные добавленного аккаунта
    """
    r = await get_redis()
    
    account_id = page_id  # Используем page_id как account_id
    
    account_data = {
        "account_id": account_id,
        "page_id": page_id,
        "page_name": page_name,
        "access_token": access_token,
        "ig_id": ig_id,
        "ig_username": ig_username,
        "is_active": True,
        "created_at": json.dumps({"timestamp": __import__("time").time()}),
    }
    
    # Сохраняем данные аккаунта
    account_key = _account_key(user_id, account_id)
    await r.set(account_key, json.dumps(account_data))
    
    # Добавляем account_id в список аккаунтов пользователя
    user_accounts_key = _user_accounts_key(user_id)
    await r.sadd(user_accounts_key, account_id)
    
    return account_data


async def get_account(user_id: str, account_id: str) -> Optional[Dict[str, Any]]:
    """Получает данные конкретного аккаунта"""
    r = await get_redis()
    account_key = _account_key(user_id, account_id)
    raw = await r.get(account_key)
    if not raw:
        return None
    return json.loads(raw)


async def list_accounts(user_id: str) -> List[Dict[str, Any]]:
    """Получает список всех аккаунтов пользователя"""
    r = await get_redis()
    user_accounts_key = _user_accounts_key(user_id)
    account_ids = await r.smembers(user_accounts_key)
    
    accounts = []
    for account_id in account_ids:
        account = await get_account(user_id, account_id)
        if account:
            # Не возвращаем токен в списке (безопасность)
            account_safe = {k: v for k, v in account.items() if k != "access_token"}
            accounts.append(account_safe)
    
    return accounts


async def set_active_account(user_id: str, account_id: str) -> bool:
    """Устанавливает активный аккаунт для пользователя"""
    r = await get_redis()
    
    # Проверяем, существует ли аккаунт
    account = await get_account(user_id, account_id)
    if not account:
        return False
    
    # Сохраняем ID активного аккаунта
    active_key = f"{settings.REDIS_PREFIX}:active_account:{user_id}"
    await r.set(active_key, account_id)
    
    return True


async def get_active_account(user_id: str) -> Optional[Dict[str, Any]]:
    """Получает активный аккаунт пользователя"""
    r = await get_redis()
    active_key = f"{settings.REDIS_PREFIX}:active_account:{user_id}"
    account_id = await r.get(active_key)
    
    if not account_id:
        return None
    
    return await get_account(user_id, account_id)


async def remove_account(user_id: str, account_id: str) -> bool:
    """Удаляет аккаунт из хранилища"""
    r = await get_redis()
    
    account_key = _account_key(user_id, account_id)
    user_accounts_key = _user_accounts_key(user_id)
    active_key = f"{settings.REDIS_PREFIX}:active_account:{user_id}"
    
    # Удаляем из списка аккаунтов
    await r.srem(user_accounts_key, account_id)
    
    # Удаляем данные аккаунта
    await r.delete(account_key)
    
    # Если это был активный аккаунт, очищаем
    active_account_id = await r.get(active_key)
    if active_account_id == account_id:
        await r.delete(active_key)
    
    return True


async def initialize_from_env() -> Optional[Dict[str, Any]]:
    """
    Инициализирует аккаунт из переменных окружения (для обратной совместимости).
    Использует IG_ACCESS_TOKEN и PAGE_ID из env.
    """
    if not settings.IG_ACCESS_TOKEN:
        return None
    
    user_id = _default_user_id()
    
    # Если в env указан PAGE_ID, используем его
    from meta_config import PAGE_ID_ENV
    page_id = PAGE_ID_ENV
    
    # Пытаемся получить Pages через API
    from http_client import RetryClient
    from meta_config import ME_URL, GRAPH_BASE
    
    async with RetryClient() as client:
        r = await client.get(
            f"{ME_URL}/accounts",
            params={"access_token": settings.IG_ACCESS_TOKEN, "fields": "id,name,access_token"},
            retries=4,
        )
        r.raise_for_status()
        pages = r.json().get("data") or []
        if not pages:
            return None
        
        # Если page_id указан, используем его, иначе берем первый
        selected_page = None
        if page_id:
            selected_page = next((p for p in pages if p["id"] == page_id), None)
            if not selected_page:
                return None
        else:
            selected_page = pages[0]
        
        page_id = selected_page["id"]
        page_name = selected_page.get("name", "Instagram Account")
        page_token = selected_page.get("access_token") or settings.IG_ACCESS_TOKEN
        
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
            return None
        
        # Сохраняем в Redis
        account = await add_account(
            user_id=user_id,
            page_id=page_id,
            page_name=page_name,
            access_token=page_token,
            ig_id=ig_id,
            ig_username=ig_username,
        )
        
        # Устанавливаем как активный (если еще нет активного)
        active = await get_active_account(user_id)
        if not active:
            await set_active_account(user_id, page_id)
        
        return account
