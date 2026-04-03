# repositories/deployment_repository.py

import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

import asyncpg

from services.deployment_state import (
    DeploymentState,
    FailureDomain,
    assert_transition,
)
from services.ha_ack import AckOutcome, AckResult


RETRY_BACKOFF = [
    timedelta(minutes=5),
    timedelta(minutes=15),
    timedelta(minutes=45),
]


def _now() -> datetime:
    return datetime.now(timezone.utc)


class DeploymentRepository:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    # ------------------------------------------------------------------
    # Creation
    # ------------------------------------------------------------------

    async def create(
        self,
        site_id: str,
        alias: str,
        deployment_key: uuid.UUID,
        rendered_checksum: Optional[str] = None,
        attempt_number: int = 1,
    ) -> uuid.UUID:
        record_id = uuid.uuid4()
        await self._pool.execute(
            """
            INSERT INTO deployment_records
                (id, site_id, alias, deployment_key, rendered_checksum,
                 attempt_number, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
            """,
            record_id, site_id, alias, deployment_key,
            rendered_checksum, attempt_number, _now(),
        )
        return record_id

    async def next_attempt_number(
        self,
        conn: asyncpg.Connection,
        site_id: str,
        deployment_key: uuid.UUID,
    ) -> int:
        """
        Compute next attempt number atomically inside a transaction.
        Prevents concurrent retries colliding on the same attempt_number.
        """
        return await conn.fetchval(
            """
            SELECT COALESCE(MAX(attempt_number), 0) + 1
            FROM deployment_records
            WHERE site_id = $1 AND deployment_key = $2
            FOR UPDATE
            """,
            site_id, deployment_key,
        )

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------

    async def transition(
        self,
        record_id: uuid.UUID,
        next_state: DeploymentState,
        *,
        last_error: Optional[str] = None,
        failure_domain: Optional[FailureDomain] = None,
        ack_outcome: Optional[AckOutcome] = None,
    ) -> DeploymentState:
        """
        Validate and apply a state transition.
        Returns the actual final state applied.
        May escalate failed → permanent_failure if retries exhausted.
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT state, retry_count
                    FROM deployment_records
                    WHERE id = $1
                    FOR UPDATE
                    """,
                    record_id,
                )
                if not row:
                    raise ValueError(
                        f"Deployment record {record_id} not found"
                    )

                current = DeploymentState(row["state"])
                assert_transition(current, next_state)

                now           = _now()
                retry_count   = row["retry_count"]
                next_retry_at = None
                applied_state = next_state

                if next_state == DeploymentState.failed:
                    new_retry_count = retry_count + 1
                    if new_retry_count <= len(RETRY_BACKOFF):
                        next_retry_at = now + RETRY_BACKOFF[retry_count]
                    else:
                        applied_state = DeploymentState.permanent_failure

                await conn.execute(
                    """
                    UPDATE deployment_records SET
                        state             = $1,
                        last_error        = COALESCE($2, last_error),
                        failure_domain    = COALESCE($3, failure_domain),
                        next_retry_at     = $4,
                        retry_count       = CASE
                            WHEN $1 IN ('failed', 'permanent_failure')
                            THEN retry_count + 1
                            ELSE retry_count END,
                        pushed_at         = CASE
                            WHEN $1 = 'pushed' THEN now()
                            ELSE pushed_at END,
                        acknowledged_at   = CASE
                            WHEN $1 = 'acknowledged' THEN now()
                            ELSE acknowledged_at END,
                        ack_result        = COALESCE($5, ack_result),
                        ack_detail        = COALESCE($6, ack_detail),
                        matched_entity_id = COALESCE($7, matched_entity_id)
                    WHERE id = $8
                    """,
                    applied_state.value,
                    last_error,
                    failure_domain.value if failure_domain else None,
                    next_retry_at,
                    ack_outcome.result.value if ack_outcome else None,
                    ack_outcome.detail if ack_outcome else None,
                    (
                        ack_outcome.matched.entity_id
                        if ack_outcome and ack_outcome.matched
                        else None
                    ),
                    record_id,
                )

                return applied_state

    # ------------------------------------------------------------------
    # Ops queries
    # ------------------------------------------------------------------

    async def get_pending_retries(self) -> list[asyncpg.Record]:
        return await self._pool.fetch(
            """
            SELECT
                id, site_id, alias, deployment_key,
                retry_count, attempt_number,
                last_error, failure_domain
            FROM deployment_records
            WHERE state = 'failed'
              AND next_retry_at IS NOT NULL
              AND next_retry_at <= $1
            ORDER BY next_retry_at ASC
            """,
            _now(),
        )

    async def get_stuck(
        self,
        stuck_in_state: DeploymentState,
        older_than_minutes: int = 30,
    ) -> list[asyncpg.Record]:
        cutoff = _now() - timedelta(minutes=older_than_minutes)
        return await self._pool.fetch(
            """
            SELECT
                id, site_id, alias, deployment_key,
                state, updated_at, retry_count,
                last_error, failure_domain,
                EXTRACT(EPOCH FROM (now() - updated_at)) / 60 AS stuck_minutes,
                $2 AS threshold_minutes
            FROM deployment_records
            WHERE state = $1 AND updated_at < $3
            ORDER BY updated_at ASC
            """,
            stuck_in_state.value, older_than_minutes, cutoff,
        )

    async def get_health_summary(self) -> list[asyncpg.Record]:
        return await self._pool.fetch(
            """
            SELECT state, COUNT(*) as count
            FROM deployment_records
            GROUP BY state
            ORDER BY state
            """
        )

    async def get_never_acknowledged(
        self,
        site_id: str,
    ) -> list[asyncpg.Record]:
        return await self._pool.fetch(
            """
            SELECT
                id, alias, deployment_key, state,
                created_at, retry_count, last_error
            FROM deployment_records
            WHERE site_id = $1
              AND state NOT IN ('acknowledged', 'in_sync')
            ORDER BY created_at DESC
            """,
            site_id,
        )