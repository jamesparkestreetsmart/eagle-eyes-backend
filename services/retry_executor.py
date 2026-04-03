# services/retry_executor.py

import uuid
import logging

import asyncpg

from repositories.deployment_repository import DeploymentRepository
from services.deployment_state import DeploymentState, FailureDomain
from services.ha_ack import HAClient, AckResult, verify_push
from services.alerting import AlertService, OpsAlert
from services.correlation import set_correlation_id, get_correlation_id

logger = logging.getLogger(__name__)


async def execute_retry(
    record: asyncpg.Record,
    pool: asyncpg.Pool,
    alert_service: AlertService,
    get_ha_client: callable,
    push_automation: callable,
    get_payload: callable,
) -> None:
    """
    Execute one retry attempt for a failed deployment.

    Creates a new deployment record for this attempt — old record preserved.
    Drives the full push → reload → poll → ack flow.
    """
    cid            = set_correlation_id()
    repo           = DeploymentRepository(pool)
    site_id        = record["site_id"]
    alias          = record["alias"]
    deployment_key = record["deployment_key"]

    logger.info("Retrying deployment", extra={
        "correlation_id": cid,
        "site_id":        site_id,
        "alias":          alias,
        "deployment_key": str(deployment_key),
        "retry_count":    record["retry_count"],
    })

    # atomic attempt number inside transaction
    async with pool.acquire() as conn:
        async with conn.transaction():
            attempt_number = await repo.next_attempt_number(
                conn, site_id, deployment_key
            )
            record_id = await repo.create(
                site_id=site_id,
                alias=alias,
                deployment_key=deployment_key,
                attempt_number=attempt_number,
            )

    client: HAClient = get_ha_client(site_id)

    try:
        await repo.transition(record_id, DeploymentState.rendered)
        await repo.transition(record_id, DeploymentState.push_attempted)

        payload = await get_payload(deployment_key)
        await push_automation(site_id, alias, payload)
        await repo.transition(record_id, DeploymentState.pushed)

        outcome = await verify_push(client, expected_alias=alias)

        match outcome.result:
            case AckResult.confirmed:
                await repo.transition(
                    record_id,
                    DeploymentState.acknowledged,
                    ack_outcome=outcome,
                )
                await repo.transition(record_id, DeploymentState.in_sync)
                logger.info("Retry succeeded", extra={
                    "correlation_id": get_correlation_id(),
                    "record_id":      str(record_id),
                    "alias":          alias,
                })

            case AckResult.timed_out | AckResult.reload_failed:
                final_state = await repo.transition(
                    record_id,
                    DeploymentState.failed,
                    last_error=outcome.detail,
                    failure_domain=FailureDomain.availability,
                    ack_outcome=outcome,
                )
                logger.warning("Retry failed", extra={
                    "correlation_id": get_correlation_id(),
                    "record_id":      str(record_id),
                    "detail":         outcome.detail,
                })
                if final_state == DeploymentState.permanent_failure:
                    await alert_service.send(OpsAlert(
                        site_id=site_id,
                        alias=alias,
                        deployment_key=str(deployment_key),
                        record_id=str(record_id),
                        reason="Retries exhausted — permanent failure",
                        failure_domain="availability",
                        last_error=outcome.detail,
                    ))

            case AckResult.mismatch:
                await repo.transition(
                    record_id,
                    DeploymentState.mismatch,
                    last_error=outcome.detail,
                    failure_domain=FailureDomain.config,
                    ack_outcome=outcome,
                )
                await alert_service.send(OpsAlert(
                    site_id=site_id,
                    alias=alias,
                    deployment_key=str(deployment_key),
                    record_id=str(record_id),
                    reason="Mismatch — terminal, requires human intervention",
                    failure_domain="config",
                    last_error=outcome.detail,
                ))

    except Exception as e:
        logger.exception("Unexpected error during retry", extra={
            "correlation_id": get_correlation_id(),
            "record_id":      str(record_id),
            "alias":          alias,
        })
        await repo.transition(
            record_id,
            DeploymentState.failed,
            last_error=str(e),
            failure_domain=FailureDomain.unknown,
        )
    finally:
        await client.aclose()