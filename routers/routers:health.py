from fastapi import APIRouter
from errors import ok

router = APIRouter()

@router.get("/health")
async def health():
    """
    Health-check endpoint.
    Поведение и контракт сохранены.
    """
    return ok()
