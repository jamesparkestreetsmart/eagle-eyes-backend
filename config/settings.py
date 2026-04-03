# config/settings.py

import json
import os
import time
import logging
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

logger = logging.getLogger(__name__)

CREDENTIAL_CACHE_TTL_SECONDS = 300


@dataclass
class SiteCredentials:
    site_id:  str
    ha_url:   str
    ha_token: str


@dataclass
class Settings:
    secret_name:                  str
    aws_region:                   str
    environment:                  str
    log_level:                    str
    database_url:                 str
    retry_sweep_interval_seconds: int = 60
    max_concurrent_retries:       int = 5
    advisory_lock_key:            int = 12345
    ha_push_timeout_seconds:      int = 25
    supabase_url:                 str = ""
    supabase_anon_key:            str = ""


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    For local development: reads from environment variables directly.
    For production: swap _load_local() for _load_from_secrets_manager().
    """
    return Settings(
        secret_name=os.environ.get("SECRET_NAME", "local"),
        aws_region=os.environ.get("AWS_REGION", "us-east-1"),
        environment=os.environ.get("ENVIRONMENT", "development"),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        database_url=os.environ["DATABASE_URL"],
        ha_push_timeout_seconds=int(os.environ.get("HA_PUSH_TIMEOUT", "25")),
        supabase_url=os.environ.get("SUPABASE_URL", ""),
        supabase_anon_key=os.environ.get("SUPABASE_ANON_KEY", ""),
    )


# ---------------------------------------------------------------------------
# On-demand site credential resolution with TTL cache
# ---------------------------------------------------------------------------

_credential_cache: dict[str, tuple[SiteCredentials, float]] = {}


def get_site_credentials(site_id: str) -> SiteCredentials:
    """
    Local dev: reads HA_URL_{SITE_ID} and HA_TOKEN_{SITE_ID} from env.
    Production: swap for Secrets Manager lookup.
    """
    now = time.monotonic()
    cached = _credential_cache.get(site_id)

    if cached:
        creds, expires_at = cached
        if now < expires_at:
            return creds

    key = site_id.upper().replace("-", "_")
    ha_url   = os.environ.get(f"HA_URL_{key}")
    ha_token = os.environ.get(f"HA_TOKEN_{key}")

    if not ha_url or not ha_token:
        raise ValueError(
            f"No credentials found for site_id '{site_id}'. "
            f"Expected HA_URL_{key} and HA_TOKEN_{key} in environment."
        )

    creds = SiteCredentials(site_id=site_id, ha_url=ha_url, ha_token=ha_token)
    _credential_cache[site_id] = (creds, now + CREDENTIAL_CACHE_TTL_SECONDS)
    return creds