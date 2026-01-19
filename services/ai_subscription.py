# services/ai_subscription.py
"""
Менеджер для управления подписками AI Avatar и кредитной системой.
Хранит информацию о подписках и кредитах в Redis.
"""
import json
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Literal
from enum import Enum

from jobs import get_redis
from config import settings


class AIAvatarPlanType(str, Enum):
    """Типы подписок AI Avatar"""
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    YEARLY = "yearly"


# Конфигурация планов
PLAN_CONFIG = {
    AIAvatarPlanType.WEEKLY: {
        "credits_period": 20,  # кредитов в неделю
        "daily_limit": 5,  # кредитов в день
        "avatar_batch_included": False,
        "period_days": 7,
    },
    AIAvatarPlanType.MONTHLY: {
        "credits_period": 80,  # кредитов в месяц
        "daily_limit": 7,  # 6-8, берем среднее 7
        "avatar_batch_included": True,
        "avatar_batch_credits": 25,  # 1 batch = 25 кредитов
        "period_days": 30,
    },
    AIAvatarPlanType.YEARLY: {
        "credits_period": 1200,  # кредитов в год
        "daily_limit": 10,  # кредитов в день
        "avatar_batch_included": True,
        "avatar_batch_credits": 25,
        "avatar_batch_per_day": 1,  # до 1 batch в день
        "period_days": 365,
    },
}

# Стоимость операций в кредитах
OPERATION_COSTS = {
    "text_to_image": 1,
    "image_to_image": 2,
    "avatar_batch": 25,
}


def _subscription_key(user_id: str) -> str:
    """Redis ключ для подписки пользователя"""
    return f"{settings.REDIS_PREFIX}:ai_subscription:{user_id}"


def _credits_key(user_id: str) -> str:
    """Redis ключ для кредитов пользователя"""
    return f"{settings.REDIS_PREFIX}:ai_credits:{user_id}"


def _daily_usage_key(user_id: str, date: str) -> str:
    """Redis ключ для дневного использования (date в формате YYYY-MM-DD)"""
    return f"{settings.REDIS_PREFIX}:ai_daily_usage:{user_id}:{date}"


def _avatar_batch_usage_key(user_id: str, date: str) -> str:
    """Redis ключ для использования avatar batch за день (только для yearly)"""
    return f"{settings.REDIS_PREFIX}:ai_avatar_batch_usage:{user_id}:{date}"


def _default_user_id() -> str:
    """Возвращает дефолтный user_id"""
    return "default_user"


def _get_today() -> str:
    """Возвращает сегодняшнюю дату в формате YYYY-MM-DD"""
    return datetime.now().strftime("%Y-%m-%d")


async def set_subscription(
    user_id: str,
    plan_type: AIAvatarPlanType,
    expires_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Устанавливает подписку для пользователя.
    
    Args:
        user_id: ID пользователя
        plan_type: Тип подписки
        expires_at: Дата истечения (если None, вычисляется автоматически)
    
    Returns:
        Информация о подписке
    """
    r = await get_redis()
    
    config = PLAN_CONFIG[plan_type]
    now = datetime.now()
    
    if expires_at is None:
        expires_at = now + timedelta(days=config["period_days"])
    
    subscription_data = {
        "user_id": user_id,
        "plan_type": plan_type.value,
        "activated_at": now.isoformat(),
        "expires_at": expires_at.isoformat(),
        "credits_period": config["credits_period"],
        "daily_limit": config["daily_limit"],
        "avatar_batch_included": config["avatar_batch_included"],
    }
    
    if config.get("avatar_batch_credits"):
        subscription_data["avatar_batch_credits"] = config["avatar_batch_credits"]
    
    if config.get("avatar_batch_per_day"):
        subscription_data["avatar_batch_per_day"] = config["avatar_batch_per_day"]
    
    # Сохраняем подписку
    await r.set(
        _subscription_key(user_id),
        json.dumps(subscription_data),
        ex=int((expires_at - now).total_seconds()) + 86400,  # +1 день для запаса
    )
    
    # Инициализируем кредиты
    await reset_credits(user_id, plan_type)
    
    return subscription_data


async def get_subscription(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Получает информацию о подписке пользователя.
    
    Args:
        user_id: ID пользователя
    
    Returns:
        Информация о подписке или None
    """
    r = await get_redis()
    key = _subscription_key(user_id)
    raw = await r.get(key)
    
    if not raw:
        return None
    
    data = json.loads(raw)
    
    # Проверяем, не истекла ли подписка
    expires_at = datetime.fromisoformat(data["expires_at"])
    if datetime.now() > expires_at:
        # Подписка истекла, удаляем
        await r.delete(key)
        await r.delete(_credits_key(user_id))
        return None
    
    return data


async def cancel_subscription(user_id: str) -> bool:
    """
    Отменяет подписку пользователя.
    
    Args:
        user_id: ID пользователя
    
    Returns:
        True если подписка была отменена, False если её не было
    """
    r = await get_redis()
    sub_key = _subscription_key(user_id)
    credits_key = _credits_key(user_id)
    
    deleted = await r.delete(sub_key)
    await r.delete(credits_key)
    
    # Удаляем все дневные счетчики (опционально, можно оставить для статистики)
    # Для простоты не удаляем, они истекут сами по TTL
    
    return deleted > 0


async def reset_credits(user_id: str, plan_type: AIAvatarPlanType) -> Dict[str, Any]:
    """
    Сбрасывает кредиты пользователя согласно плану.
    Вызывается при активации подписки или при сбросе периода.
    
    Args:
        user_id: ID пользователя
        plan_type: Тип подписки
    
    Returns:
        Информация о кредитах
    """
    r = await get_redis()
    config = PLAN_CONFIG[plan_type]
    
    credits_data = {
        "user_id": user_id,
        "credits_remaining": config["credits_period"],
        "reset_at": datetime.now().isoformat(),
    }
    
    # Сохраняем кредиты (TTL = период подписки + 1 день)
    period_seconds = config["period_days"] * 86400
    await r.set(
        _credits_key(user_id),
        json.dumps(credits_data),
        ex=period_seconds + 86400,
    )
    
    return credits_data


async def get_credits(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Получает информацию о кредитах пользователя.
    
    Args:
        user_id: ID пользователя
    
    Returns:
        Информация о кредитах или None
    """
    r = await get_redis()
    key = _credits_key(user_id)
    raw = await r.get(key)
    
    if not raw:
        return None
    
    return json.loads(raw)


async def get_daily_usage(user_id: str, date: Optional[str] = None) -> int:
    """
    Получает количество использованных кредитов за день.
    
    Args:
        user_id: ID пользователя
        date: Дата в формате YYYY-MM-DD (если None, используется сегодня)
    
    Returns:
        Количество использованных кредитов
    """
    if date is None:
        date = _get_today()
    
    r = await get_redis()
    key = _daily_usage_key(user_id, date)
    raw = await r.get(key)
    
    if not raw:
        return 0
    
    usage_data = json.loads(raw)
    return usage_data.get("credits_used", 0)


async def get_avatar_batch_usage(user_id: str, date: Optional[str] = None) -> int:
    """
    Получает количество использованных avatar batch за день (для yearly плана).
    
    Args:
        user_id: ID пользователя
        date: Дата в формате YYYY-MM-DD (если None, используется сегодня)
    
    Returns:
        Количество использованных batch
    """
    if date is None:
        date = _get_today()
    
    r = await get_redis()
    key = _avatar_batch_usage_key(user_id, date)
    raw = await r.get(key)
    
    if not raw:
        return 0
    
    usage_data = json.loads(raw)
    return usage_data.get("batches_used", 0)


async def check_credits(
    user_id: str,
    operation_type: Literal["text_to_image", "image_to_image", "avatar_batch"],
) -> Dict[str, Any]:
    """
    Проверяет, достаточно ли кредитов для операции.
    
    Args:
        user_id: ID пользователя
        operation_type: Тип операции
    
    Returns:
        {
            "can_proceed": bool,
            "credits_needed": int,
            "credits_remaining": int,
            "daily_limit_reached": bool,
            "reason": Optional[str]
        }
    """
    subscription = await get_subscription(user_id)
    if not subscription:
        return {
            "can_proceed": False,
            "credits_needed": OPERATION_COSTS[operation_type],
            "credits_remaining": 0,
            "daily_limit_reached": False,
            "reason": "No active subscription",
        }
    
    plan_type = AIAvatarPlanType(subscription["plan_type"])
    config = PLAN_CONFIG[plan_type]
    credits_needed = OPERATION_COSTS[operation_type]
    
    # Проверяем кредиты
    credits_data = await get_credits(user_id)
    if not credits_data:
        return {
            "can_proceed": False,
            "credits_needed": credits_needed,
            "credits_remaining": 0,
            "daily_limit_reached": False,
            "reason": "Credits data not found",
        }
    
    credits_remaining = credits_data["credits_remaining"]
    
    # Проверяем дневной лимит
    daily_usage = await get_daily_usage(user_id)
    daily_limit = config["daily_limit"]
    
    if daily_usage + credits_needed > daily_limit:
        return {
            "can_proceed": False,
            "credits_needed": credits_needed,
            "credits_remaining": credits_remaining,
            "daily_limit_reached": True,
            "reason": f"Daily limit reached ({daily_limit} credits/day)",
        }
    
    # Для avatar_batch проверяем специальные лимиты
    if operation_type == "avatar_batch":
        if not config.get("avatar_batch_included", False):
            return {
                "can_proceed": False,
                "credits_needed": credits_needed,
                "credits_remaining": credits_remaining,
                "daily_limit_reached": False,
                "reason": "Avatar batch not included in this plan",
            }
        
        # Для yearly плана проверяем лимит batch в день
        if plan_type == AIAvatarPlanType.YEARLY:
            batch_usage = await get_avatar_batch_usage(user_id)
            if batch_usage >= config.get("avatar_batch_per_day", 0):
                return {
                    "can_proceed": False,
                    "credits_needed": credits_needed,
                    "credits_remaining": credits_remaining,
                    "daily_limit_reached": True,
                    "reason": f"Avatar batch daily limit reached ({config.get('avatar_batch_per_day', 0)} batch/day)",
                }
    
    # Проверяем общий баланс кредитов
    if credits_remaining < credits_needed:
        return {
            "can_proceed": False,
            "credits_needed": credits_needed,
            "credits_remaining": credits_remaining,
            "daily_limit_reached": False,
            "reason": f"Insufficient credits (need {credits_needed}, have {credits_remaining})",
        }
    
    return {
        "can_proceed": True,
        "credits_needed": credits_needed,
        "credits_remaining": credits_remaining,
        "daily_limit_reached": False,
        "reason": None,
    }


async def use_credits(
    user_id: str,
    operation_type: Literal["text_to_image", "image_to_image", "avatar_batch"],
) -> Dict[str, Any]:
    """
    Использует кредиты для операции.
    Должен вызываться ПОСЛЕ успешной генерации.
    
    Args:
        user_id: ID пользователя
        operation_type: Тип операции
    
    Returns:
        {
            "success": bool,
            "credits_used": int,
            "credits_remaining": int,
            "error": Optional[str]
        }
    """
    # Сначала проверяем
    check_result = await check_credits(user_id, operation_type)
    if not check_result["can_proceed"]:
        return {
            "success": False,
            "credits_used": 0,
            "credits_remaining": check_result["credits_remaining"],
            "error": check_result["reason"],
        }
    
    r = await get_redis()
    credits_needed = OPERATION_COSTS[operation_type]
    
    # Обновляем кредиты
    credits_key = _credits_key(user_id)
    credits_raw = await r.get(credits_key)
    if not credits_raw:
        return {
            "success": False,
            "credits_used": 0,
            "credits_remaining": 0,
            "error": "Credits data not found",
        }
    
    credits_data = json.loads(credits_raw)
    credits_data["credits_remaining"] -= credits_needed
    credits_data["last_used_at"] = datetime.now().isoformat()
    
    # Сохраняем обновленные кредиты
    ttl = await r.ttl(credits_key)
    await r.set(credits_key, json.dumps(credits_data), ex=ttl if ttl > 0 else 86400)
    
    # Обновляем дневное использование
    today = _get_today()
    daily_key = _daily_usage_key(user_id, today)
    daily_raw = await r.get(daily_key)
    
    if daily_raw:
        daily_data = json.loads(daily_raw)
        daily_data["credits_used"] += credits_needed
    else:
        daily_data = {"credits_used": credits_needed, "date": today}
    
    # TTL до конца дня + 1 день
    now = datetime.now()
    end_of_day = (now.replace(hour=23, minute=59, second=59) + timedelta(days=1))
    ttl_seconds = int((end_of_day - now).total_seconds())
    await r.set(daily_key, json.dumps(daily_data), ex=ttl_seconds)
    
    # Для avatar_batch обновляем счетчик batch
    if operation_type == "avatar_batch":
        subscription = await get_subscription(user_id)
        if subscription:
            plan_type = AIAvatarPlanType(subscription["plan_type"])
            if plan_type == AIAvatarPlanType.YEARLY:
                batch_key = _avatar_batch_usage_key(user_id, today)
                batch_raw = await r.get(batch_key)
                
                if batch_raw:
                    batch_data = json.loads(batch_raw)
                    batch_data["batches_used"] = batch_data.get("batches_used", 0) + 1
                else:
                    batch_data = {"batches_used": 1, "date": today}
                
                await r.set(batch_key, json.dumps(batch_data), ex=ttl_seconds)
    
    return {
        "success": True,
        "credits_used": credits_needed,
        "credits_remaining": credits_data["credits_remaining"],
        "error": None,
    }


async def get_subscription_status(user_id: str) -> Dict[str, Any]:
    """
    Получает полный статус подписки пользователя.
    
    Args:
        user_id: ID пользователя
    
    Returns:
        {
            "is_active": bool,
            "plan_type": Optional[str],
            "credits_remaining": int,
            "daily_credits_used": int,
            "daily_limit": int,
            "can_generate_avatar_batch": bool,
            "expires_at": Optional[str],
            "reset_at": Optional[str],
        }
    """
    subscription = await get_subscription(user_id)
    
    if not subscription:
        return {
            "is_active": False,
            "plan_type": None,
            "credits_remaining": 0,
            "daily_credits_used": 0,
            "daily_limit": 0,
            "can_generate_avatar_batch": False,
            "expires_at": None,
            "reset_at": None,
        }
    
    plan_type = AIAvatarPlanType(subscription["plan_type"])
    config = PLAN_CONFIG[plan_type]
    
    credits_data = await get_credits(user_id)
    credits_remaining = credits_data["credits_remaining"] if credits_data else 0
    
    daily_usage = await get_daily_usage(user_id)
    
    can_generate_avatar_batch = (
        config.get("avatar_batch_included", False) and
        credits_remaining >= OPERATION_COSTS["avatar_batch"] and
        (plan_type != AIAvatarPlanType.YEARLY or 
         await get_avatar_batch_usage(user_id) < config.get("avatar_batch_per_day", 0))
    )
    
    return {
        "is_active": True,
        "plan_type": plan_type.value,
        "credits_remaining": credits_remaining,
        "daily_credits_used": daily_usage,
        "daily_limit": config["daily_limit"],
        "can_generate_avatar_batch": can_generate_avatar_batch,
        "expires_at": subscription["expires_at"],
        "reset_at": credits_data.get("reset_at") if credits_data else None,
    }
