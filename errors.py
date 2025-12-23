from __future__ import annotations

from typing import Any, Dict, Optional


def ok(**kwargs: Any) -> Dict[str, Any]:
    """
    Единый формат успешного ответа.
    """
    return {"ok": True, **kwargs}


def fail(error: Any, stage: Optional[str] = None, **kwargs: Any) -> Dict[str, Any]:
    """
    Единый формат ошибки (совместим с текущими ожиданиями фронта: ok + error).
    """
    payload: Dict[str, Any] = {"ok": False, "error": str(error)}
    if stage:
        payload["stage"] = stage
    payload.update(kwargs)
    return payload


class APIError(Exception):
    """
    Внутреннее исключение для сервисов/роутеров.
    """
    def __init__(
        self,
        error: Any,
        stage: Optional[str] = None,
        status_code: int = 400,
        extra: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(str(error))
        self.error = str(error)
        self.stage = stage
        self.status_code = status_code
        self.extra = extra or {}
