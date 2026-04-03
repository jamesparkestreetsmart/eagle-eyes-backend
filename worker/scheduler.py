# worker/scheduler.py

import asyncio
import logging

import asyncpg

from services.execute_deployments import execute_deployments
from services.alerting import AlertService

logger = logging.getLogger(__name__)

ADVISORY_LOCK_KEY      = 99999
MAX_CONCURRENT_RETRIES = 1   # keep at 1 for now — proven safe


async def run_deployment_sweep(
    pool: asyncpg.Pool,
    alert_service: AlertService,
) -> None:
    """
    Cluster-safe deployment sweep.

    Advisory lock ensures only one worker runs at a time across
    multiple processes or containers.

    Fetches all rows in c_ha_automation_deployments that need work:
    - push_confirmed = false
    - render_blocked = false
    - render_validated = true
    - next_retry_at is null or overdue

    Executes delete → push → reload → ack for each.
    """
    async with pool.acquire() as conn:
        locked = await conn.fetchval(
            "SELECT pg_try_advisory_lock($1)", ADVISORY_LOCK_KEY
        )
        if not locked:
            logger.debug("Deployment sweep skipped — another worker holds the lock")
            return

        try:
            await execute_deployments(
                pool=pool,
                alert_service=alert_service,
                max_concurrent=MAX_CONCURRENT_RETRIES,
            )
        finally:
            await conn.execute(
                "SELECT pg_advisory_unlock($1)", ADVISORY_LOCK_KEY
            )