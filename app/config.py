from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DRIFT_", env_file=".env", extra="ignore")

    service_name: str = "drift-detection-service"
    log_level: str = "INFO"

    # Redis is optional: if unset, state lives in-process (fine for a single
    # replica / local dev). Set REDIS_URL to persist state across restarts
    # and share state across replicas.
    redis_url: str | None = None
    redis_key_prefix: str = "drift:"

    # API auth (matches your other services' API-key pattern)
    api_key: str | None = None

    # Default detector params (overridable per-stream at registration time)
    default_adwin_delta: float = 0.002
    default_page_hinkley_threshold: float = 50.0


settings = Settings()
