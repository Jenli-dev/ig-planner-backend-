import os
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # App
    APP_NAME: str = "IG Planner Backend"
    ENV: str = Field(default="prod", description="dev | prod")
    DEBUG: bool = False

    # Server
    HOST: str = "0.0.0.0"
    PORT: int = 8000

    # Meta / Instagram
    IG_APP_ID: Optional[str] = None
    IG_APP_SECRET: Optional[str] = None
    IG_ACCESS_TOKEN: Optional[str] = None
    IG_VERIFY_TOKEN: Optional[str] = None

    # Cloudinary
    CLOUDINARY_CLOUD: Optional[str] = None
    CLOUDINARY_API_KEY: Optional[str] = None
    CLOUDINARY_API_SECRET: Optional[str] = None
    CLOUDINARY_UNSIGNED_PRESET: Optional[str] = None

    # Media / FFmpeg
    FFMPEG_BIN: str = "ffmpeg"
    FFPROBE_BIN: str = "ffprobe"
    MEDIA_TMP_DIR: str = "/tmp/ig_planner"

    # AI / Generation
    AI_PROVIDER: str = "fal"
    FAL_KEY: Optional[str] = None
    REPLICATE_API_TOKEN: Optional[str] = None
    FAL_I2I_ENDPOINT: str = "https://fal.run/fal-ai/flux/image-to-image"
    REPLICATE_T2I_MODEL: Optional[str] = None
    REPLICATE_I2I_MODEL: Optional[str] = None

    # Jobs
    VIDEO_WORKERS: int = 2
    JOB_TTL_SECONDS: int = 60 * 60  # 1 час

    # Redis
    REDIS_URL: Optional[str] = None
    REDIS_PREFIX: str = "jobs"
    REDIS_QUEUE: str = "jobs:queue"

    # Analytics & Attribution
    APPHUD_API_KEY: Optional[str] = None
    ADAPTY_API_KEY: Optional[str] = None
    
    # Apple Search Ads API
    APPLE_SEARCH_ADS_KEY_ID: Optional[str] = None
    APPLE_SEARCH_ADS_ISSUER_ID: Optional[str] = None
    APPLE_SEARCH_ADS_PRIVATE_KEY: Optional[str] = None  # Base64 encoded .p8 key content

    model_config = SettingsConfigDict(
        env_file=None if os.getenv("DISABLE_DOTENV") == "1" else ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
