from typing import Optional, Any, Dict, TypeAlias
from pydantic import BaseModel


class BaseAPIResponse(BaseModel):
    """
    Базовый ответ API.
    Можно расширять (request_id, meta и т.д.)
    """
    ok: bool


class OkResponse(BaseAPIResponse):
    ok: bool = True


class ErrorResponse(BaseAPIResponse):
    ok: bool = False
    error: str
    stage: Optional[str] = None


# Для эндпоинтов с произвольным ответом
AnyDictResponse: TypeAlias = Dict[str, Any]
