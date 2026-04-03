# services/ha_ack.py

import logging
logger = logging.getLogger(__name__)

import asyncio
import httpx
from enum import Enum
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional


class AckResult(Enum):
    confirmed     = "confirmed"
    timed_out     = "timed_out"
    mismatch      = "mismatch"
    reload_failed = "reload_failed"


class FailureDomain(Enum):
    availability = "availability"
    config       = "config"
    auth         = "auth"
    unknown      = "unknown"


@dataclass
class AutomationState:
    alias: str
    enabled: bool
    entity_id: str
    auto_id: str = ""
    last_updated: Optional[str] = None


@dataclass
class AckOutcome:
    result: AckResult
    checked_at: datetime
    detail: Optional[str] = None
    failure_domain: Optional[FailureDomain] = None
    matched: Optional[AutomationState] = None


def _normalize(s: str) -> str:
    return s.strip().lower()


class HAClient:
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )

    async def trigger_reload(self) -> None:
        resp = await self._client.post(
            f"{self.base_url}/api/services/automation/reload"
        )
        resp.raise_for_status()

    async def fetch_automations(self) -> list[AutomationState]:
        resp = await self._client.get(f"{self.base_url}/api/states")
        resp.raise_for_status()

        automations = []
        for s in resp.json():
            if not s.get("entity_id", "").startswith("automation."):
                continue

            attrs = s.get("attributes") or {}
            alias = (attrs.get("friendly_name") or attrs.get("alias") or "").strip()

            if not alias:
                continue

            automations.append(AutomationState(
                alias=alias,
                enabled=s.get("state") not in ("off", "unavailable"),
                entity_id=s["entity_id"],
                auto_id=attrs.get("id", ""),
                last_updated=s.get("last_updated"),
            ))
        return automations

    async def aclose(self) -> None:
        await self._client.aclose()


async def verify_push(
    client: HAClient,
    expected_alias: str,
    expected_id: Optional[str] = None,
    timeout: int = 120,
    poll_interval: int = 10,
) -> AckOutcome:
    now = lambda: datetime.now(timezone.utc)
    deadline = now() + timedelta(seconds=timeout)
    normalized_expected = _normalize(expected_alias)

    # Step 1: explicit reload
    try:
        await client.trigger_reload()
        logger.info("HA reload triggered for alias=%s", expected_alias)
    except httpx.HTTPStatusError as e:
        domain = (
            FailureDomain.auth
            if e.response.status_code in (401, 403)
            else FailureDomain.availability
        )
        logger.warning("HA reload HTTP error: %s alias=%s", e.response.status_code, expected_alias)
        return AckOutcome(
            result=AckResult.reload_failed,
            checked_at=now(),
            detail=f"Reload HTTP error: {e.response.status_code}",
            failure_domain=domain,
        )
    except httpx.HTTPError as e:
        logger.warning("HA reload failed: %s alias=%s", e, expected_alias)
        return AckOutcome(
            result=AckResult.reload_failed,
            checked_at=now(),
            detail=f"Reload failed: {e}",
            failure_domain=FailureDomain.availability,
        )

    # Step 2: bounded poll loop
    seen_disabled = False

    while now() < deadline:
        try:
            automations = await client.fetch_automations()
        except httpx.HTTPError as e:
            logger.warning("fetch_automations failed: %s", e)
            await asyncio.sleep(poll_interval)
            continue

        logger.info("ack polling: expected_alias=%s expected_id=%s found=%d automations",
                    expected_alias, expected_id, len(automations))

        alias_matches = [
            a for a in automations
            if _normalize(a.alias) == normalized_expected
        ]

        logger.info("ack alias_matches=%s", [(a.alias, a.auto_id) for a in alias_matches])

        if expected_id:
            matches = [a for a in alias_matches if a.auto_id == expected_id]
            if not matches and alias_matches:
                logger.info("alias found but id=%s not yet visible, polling...", expected_id)
                await asyncio.sleep(poll_interval)
                continue
        else:
            matches = alias_matches

        logger.info("ack id_matches=%s", [(a.alias, a.auto_id) for a in matches])

        if len(matches) == 0:
            await asyncio.sleep(poll_interval)
            continue

        if len(matches) > 1:
            return AckOutcome(
                result=AckResult.mismatch,
                checked_at=now(),
                detail=(
                    f"Duplicate alias '{expected_alias}' ({len(matches)} matches). "
                    f"Correctness validation required."
                ),
                failure_domain=FailureDomain.config,
            )

        match = matches[0]

        if not match.enabled:
            seen_disabled = True
            logger.info("alias=%s found but disabled, polling...", expected_alias)
            await asyncio.sleep(poll_interval)
            continue

        return AckOutcome(
            result=AckResult.confirmed,
            checked_at=now(),
            matched=match,
        )

    # Step 3: deadline exceeded
    detail = (
        f"Alias '{expected_alias}' found but never became enabled within {timeout}s."
        if seen_disabled else
        f"Alias '{expected_alias}' never appeared in HA state within {timeout}s."
    )
    logger.warning("ack timed out: %s", detail)
    return AckOutcome(
        result=AckResult.timed_out,
        checked_at=now(),
        detail=detail,
        failure_domain=FailureDomain.availability,
    )