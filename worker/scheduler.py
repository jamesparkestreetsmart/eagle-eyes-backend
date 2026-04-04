# worker/scheduler.py

import logging

import asyncpg

from services.execute_deployments import execute_deployments
from services.alerting import AlertService

logger = logging.getLogger(__name__)

MAX_CONCURRENT_RETRIES = 1   # keep at 1 for now — proven safe


async def run_deployment_sweep(
    pool: asyncpg.Pool,
    alert_service: AlertService,
) -> None:
    """
    Deployment sweep — called by scheduler and realtime trigger.

    Concurrency is handled at the row level via SELECT FOR UPDATE SKIP LOCKED
    inside fetch_pending_deployments. Advisory locks are not used — they are
    incompatible with the Supabase transaction pooler (port 6543).
    """
    await execute_deployments(
        pool=pool,
        alert_service=alert_service,
        max_concurrent=MAX_CONCURRENT_RETRIES,
    )