# services/execute_deployments.py
#
# Wires push + ack engine directly to c_ha_automation_deployments.
# No new tables. No parallel systems. One wire.

import asyncio
import logging
import yaml
from datetime import datetime, timezone, timedelta
from typing import Optional
import uuid

import asyncpg
import httpx

from services.ha_ack import HAClient, verify_push, AckResult
from services.alerting import AlertService, OpsAlert
from services.correlation import set_correlation_id, get_correlation_id

logger = logging.getLogger(__name__)

RETRY_BACKOFF = [
    timedelta(minutes=5),
    timedelta(minutes=15),
    timedelta(minutes=45),
]


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def fetch_pending_deployments(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    return await pool.fetch(
        """
        SELECT
            d.deployment_id,
            d.site_id,
            d.automation_key,
            d.desired_yaml,
            d.desired_checksum,
            d.retry_count,
            d.max_retries,
            d.next_retry_at,
            d.last_status,
            s.ha_url,
            s.ha_token
        FROM c_ha_automation_deployments d
        JOIN a_sites s ON s.site_id = d.site_id
        WHERE d.push_confirmed = false
          AND d.render_blocked = false
          AND d.render_validated = true
          AND s.ha_url IS NOT NULL
          AND s.ha_token IS NOT NULL
          AND (
              d.next_retry_at IS NULL
              OR d.next_retry_at <= now()
          )
        ORDER BY d.next_retry_at ASC NULLS FIRST
        """,
    )


async def mark_acknowledged(
    pool: asyncpg.Pool,
    deployment_id: uuid.UUID,
    entity_id: Optional[str],
) -> None:
    await pool.execute(
        """
        UPDATE c_ha_automation_deployments SET
            push_confirmed      = true,
            push_confirmed_at   = now(),
            last_status         = 'acknowledged',
            drift_status        = 'in_sync',
            last_success_at     = now(),
            last_pushed_at      = now(),
            ha_automation_ref   = COALESCE($2, ha_automation_ref),
            last_error          = null,
            updated_at          = now()
        WHERE deployment_id = $1
        """,
        deployment_id, entity_id,
    )


async def mark_failed(
    pool: asyncpg.Pool,
    deployment_id: uuid.UUID,
    error: str,
    retry_count: int,
    max_retries: int,
) -> None:
    new_retry_count = retry_count + 1
    exhausted       = new_retry_count >= (max_retries or 3)
    next_retry_at   = None

    if not exhausted and new_retry_count <= len(RETRY_BACKOFF):
        next_retry_at = _now() + RETRY_BACKOFF[retry_count]

    await pool.execute(
        """
        UPDATE c_ha_automation_deployments SET
            last_status     = $1,
            drift_status    = 'failed',
            last_error      = $2,
            retry_count     = $3,
            next_retry_at   = $4,
            last_pushed_at  = now(),
            updated_at      = now()
        WHERE deployment_id = $5
        """,
        'failed',
        error,
        new_retry_count,
        next_retry_at,
        deployment_id,
    )


async def mark_mismatch(
    pool: asyncpg.Pool,
    deployment_id: uuid.UUID,
    error: str,
) -> None:
    await pool.execute(
        """
        UPDATE c_ha_automation_deployments SET
            last_status   = 'failed',
            drift_status  = 'out_of_sync',
            last_error    = $2,
            updated_at    = now()
        WHERE deployment_id = $1
        """,
        deployment_id, error,
    )


async def execute_one(
    record: asyncpg.Record,
    pool: asyncpg.Pool,
    alert_service: AlertService,
) -> None:
    cid            = set_correlation_id()
    deployment_id  = record["deployment_id"]
    site_id        = record["site_id"]
    automation_key = record["automation_key"]
    ha_url         = record["ha_url"]
    ha_token       = record["ha_token"]
    retry_count    = record["retry_count"] or 0
    max_retries    = record["max_retries"] or 3

    logger.info("Executing deployment", extra={
        "correlation_id": cid,
        "deployment_id":  str(deployment_id),
        "automation_key": automation_key,
        "site_id":        str(site_id),
        "retry_count":    retry_count,
    })

    try:
        payload = yaml.safe_load(record["desired_yaml"])
        alias   = payload.get("alias", "")
        if not alias:
            raise ValueError("desired_yaml has no alias field")

        if "id" not in payload or payload.get("id") == alias:
            payload["id"] = automation_key

    except Exception as e:
        await mark_failed(pool, deployment_id, f"YAML parse error: {e}",
                          retry_count, max_retries)
        return

    client = HAClient(base_url=ha_url, token=ha_token)

    try:
        async with httpx.AsyncClient() as http:
            # delete first — makes writes idempotent
            logger.info("Attempting HA push", extra={
                "ha_url": ha_url,
                "automation_key": automation_key,
                "retry_count": retry_count,
            })
            await http.delete(
                f"{ha_url.rstrip('/')}/api/config/automation/config/{automation_key}",
                headers={
                    "Authorization": f"Bearer {ha_token}",
                    "Content-Type":  "application/json",
                },
                timeout=10,
            )

            # push
            resp = await http.post(
                f"{ha_url.rstrip('/')}/api/config/automation/config/{automation_key}",
                headers={
                    "Authorization": f"Bearer {ha_token}",
                    "Content-Type":  "application/json",
                },
                json=payload,
                timeout=25,
            )
            resp.raise_for_status()

        logger.info("Push succeeded", extra={
            "correlation_id": get_correlation_id(),
            "deployment_id":  str(deployment_id),
            "alias":          alias,
            "status_code":    resp.status_code,
        })

        outcome = await verify_push(client, expected_alias=alias, expected_id=automation_key,) #use stable key for exact match

        match outcome.result:
            case AckResult.confirmed:
                await mark_acknowledged(
                    pool, deployment_id,
                    outcome.matched.entity_id if outcome.matched else None,
                )
                logger.info("Deployment acknowledged", extra={
                    "correlation_id": get_correlation_id(),
                    "deployment_id":  str(deployment_id),
                    "alias":          alias,
                })

            case AckResult.timed_out | AckResult.reload_failed:
                await mark_failed(
                    pool, deployment_id,
                    outcome.detail or "Ack timed out",
                    retry_count, max_retries,
                )
                logger.warning("Deployment ack failed", extra={
                    "correlation_id": get_correlation_id(),
                    "deployment_id":  str(deployment_id),
                    "detail":         outcome.detail,
                })

            case AckResult.mismatch:
                await mark_mismatch(pool, deployment_id, outcome.detail or "Mismatch")
                await alert_service.send(OpsAlert(
                    site_id=str(site_id),
                    alias=alias,
                    deployment_key=automation_key,
                    record_id=str(deployment_id),
                    reason="Mismatch — terminal, requires human intervention",
                    failure_domain="config",
                    last_error=outcome.detail,
                ))

    except Exception as e:
        logger.exception("Unexpected error during deployment", extra={
            "correlation_id": get_correlation_id(),
            "deployment_id":  str(deployment_id),
        })
        await mark_failed(pool, deployment_id, str(e), retry_count, max_retries)
    finally:
        await client.aclose()


async def execute_deployments(
    pool: asyncpg.Pool,
    alert_service: AlertService,
    max_concurrent: int = 5,
) -> None:
    async with pool.acquire() as conn:
        locked = await conn.fetchval(
            "SELECT pg_try_advisory_lock(99999)"
        )
        if not locked:
            logger.debug("Deployment sweep skipped — another worker holds the lock")
            return

        try:
            pending = await fetch_pending_deployments(pool)

            if not pending:
                logger.info("Deployment sweep: 0 pending — nothing to do")
                return

            logger.info(f"Deployment sweep: {len(pending)} pending")

            semaphore = asyncio.Semaphore(max_concurrent)

            async def bounded(record):
                async with semaphore:
                    await execute_one(record, pool, alert_service)

            await asyncio.gather(
                *[bounded(r) for r in pending],
                return_exceptions=True,
            )

        finally:
            await conn.execute("SELECT pg_advisory_unlock(99999)")