# repositories/ops_repository.py

from datetime import datetime, timezone, timedelta
from typing import Optional

import asyncpg


def _now() -> datetime:
    return datetime.now(timezone.utc)


class OpsRepository:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def health_summary_global(self) -> dict:
        rows = await self._pool.fetch(
            """
            SELECT
                drift_status    AS state,
                COUNT(*)        AS count,
                0               AS auth_failures,
                0               AS config_failures
            FROM c_ha_automation_deployments
            GROUP BY drift_status
            ORDER BY drift_status
            """
        )
        total = sum(r["count"] for r in rows)
        return {"total": total, "states": [dict(r) for r in rows]}

    async def health_summary_per_site(self) -> dict:
        rows = await self._pool.fetch(
            """
            SELECT
                site_id::text   AS site_id,
                drift_status    AS state,
                COUNT(*)        AS count
            FROM c_ha_automation_deployments
            GROUP BY site_id, drift_status
            ORDER BY site_id, drift_status
            """
        )
        totals: dict[str, int] = {}
        for r in rows:
            totals[r["site_id"]] = totals.get(r["site_id"], 0) + r["count"]
        return {
            "totals_by_site": totals,
            "states": [dict(r) for r in rows],
        }

    async def get_stuck(
        self,
        stuck_in_state: str,
        older_than_minutes: int = 30,
    ) -> list[asyncpg.Record]:
        cutoff = _now() - timedelta(minutes=older_than_minutes)
        return await self._pool.fetch(
            """
            SELECT
                deployment_id                                           AS id,
                site_id::text                                           AS site_id,
                automation_key                                          AS alias,
                automation_key                                          AS deployment_key,
                drift_status                                            AS state,
                updated_at,
                retry_count,
                last_error,
                NULL::text                                              AS failure_domain,
                EXTRACT(EPOCH FROM (now() - updated_at)) / 60          AS stuck_minutes,
                $2                                                      AS threshold_minutes
            FROM c_ha_automation_deployments
            WHERE drift_status = $1 AND updated_at < $3
            ORDER BY updated_at ASC
            """,
            stuck_in_state, older_than_minutes, cutoff,
        )

    async def get_all_stuck(
        self,
        older_than_minutes: int = 30,
    ) -> list[asyncpg.Record]:
        cutoff = _now() - timedelta(minutes=older_than_minutes)
        return await self._pool.fetch(
            """
            SELECT
                deployment_id                                           AS id,
                site_id::text                                           AS site_id,
                automation_key                                          AS alias,
                automation_key                                          AS deployment_key,
                drift_status                                            AS state,
                updated_at,
                retry_count,
                last_error,
                NULL::text                                              AS failure_domain,
                EXTRACT(EPOCH FROM (now() - updated_at)) / 60          AS stuck_minutes,
                $1                                                      AS threshold_minutes
            FROM c_ha_automation_deployments
            WHERE drift_status IN ('failed', 'pending')
              AND updated_at < $2
            ORDER BY updated_at ASC
            """,
            older_than_minutes, cutoff,
        )

    async def get_terminal_failures(
        self,
        site_id: Optional[str] = None,
    ) -> list[asyncpg.Record]:
        if site_id:
            return await self._pool.fetch(
                """
                SELECT
                    deployment_id   AS id,
                    site_id::text   AS site_id,
                    automation_key  AS alias,
                    automation_key  AS deployment_key,
                    drift_status    AS state,
                    NULL::text      AS failure_domain,
                    last_error,
                    last_status     AS ack_result,
                    NULL::text      AS ack_detail,
                    created_at,
                    updated_at
                FROM c_ha_automation_deployments
                WHERE drift_status IN ('failed')
                  AND site_id::text = $1
                ORDER BY updated_at DESC
                """,
                site_id,
            )
        return await self._pool.fetch(
            """
            SELECT
                deployment_id   AS id,
                site_id::text   AS site_id,
                automation_key  AS alias,
                automation_key  AS deployment_key,
                drift_status    AS state,
                NULL::text      AS failure_domain,
                last_error,
                last_status     AS ack_result,
                NULL::text      AS ack_detail,
                created_at,
                updated_at
            FROM c_ha_automation_deployments
            WHERE drift_status IN ('failed')
            ORDER BY updated_at DESC
            """
        )

    async def get_retry_queue(self) -> list[asyncpg.Record]:
        return await self._pool.fetch(
            """
            SELECT
                deployment_id   AS id,
                site_id::text   AS site_id,
                automation_key  AS alias,
                automation_key  AS deployment_key,
                retry_count,
                0               AS attempt_number,
                next_retry_at,
                last_error,
                NULL::text      AS failure_domain,
                CASE
                    WHEN next_retry_at <= now() THEN 'overdue'
                    ELSE 'scheduled'
                END AS retry_status,
                GREATEST(0, EXTRACT(EPOCH FROM (next_retry_at - now())) / 60)
                    AS minutes_until_retry
            FROM c_ha_automation_deployments
            WHERE drift_status = 'failed'
              AND next_retry_at IS NOT NULL
            ORDER BY next_retry_at ASC
            """
        )

    async def get_never_acknowledged(
        self,
        site_id: str,
    ) -> list[asyncpg.Record]:
        return await self._pool.fetch(
            """
            SELECT
                deployment_id       AS id,
                site_id::text       AS site_id,
                automation_key      AS alias,
                automation_key      AS deployment_key,
                drift_status        AS state,
                0                   AS attempt_number,
                retry_count,
                NULL::text          AS failure_domain,
                last_error,
                NULL::text          AS ack_result,
                NULL::text          AS ack_detail,
                created_at,
                last_pushed_at      AS pushed_at,
                NULL::timestamptz   AS acknowledged_at,
                updated_at
            FROM c_ha_automation_deployments
            WHERE site_id::text = $1
              AND drift_status NOT IN ('in_sync')
              AND last_status  NOT IN ('acknowledged')
            ORDER BY created_at DESC
            """,
            site_id,
        )

    async def get_recent_deployments(
        self,
        hours: int = 24,
        site_id: Optional[str] = None,
    ) -> list[asyncpg.Record]:
        cutoff = _now() - timedelta(hours=hours)
        if site_id:
            return await self._pool.fetch(
                """
                SELECT
                    deployment_id       AS id,
                    site_id::text       AS site_id,
                    automation_key      AS alias,
                    automation_key      AS deployment_key,
                    drift_status        AS state,
                    0                   AS attempt_number,
                    retry_count,
                    NULL::text          AS failure_domain,
                    last_error,
                    last_status         AS ack_result,
                    NULL::text          AS ack_detail,
                    created_at,
                    last_pushed_at      AS pushed_at,
                    NULL::timestamptz   AS acknowledged_at,
                    updated_at
                FROM c_ha_automation_deployments
                WHERE updated_at >= $1 AND site_id::text = $2
                ORDER BY updated_at DESC
                """,
                cutoff, site_id,
            )
        return await self._pool.fetch(
            """
            SELECT
                deployment_id       AS id,
                site_id::text       AS site_id,
                automation_key      AS alias,
                automation_key      AS deployment_key,
                drift_status        AS state,
                0                   AS attempt_number,
                retry_count,
                NULL::text          AS failure_domain,
                last_error,
                last_status         AS ack_result,
                NULL::text          AS ack_detail,
                created_at,
                last_pushed_at      AS pushed_at,
                NULL::timestamptz   AS acknowledged_at,
                updated_at
            FROM c_ha_automation_deployments
            WHERE updated_at >= $1
            ORDER BY updated_at DESC
            """,
            cutoff,
        )

    async def get_deployment_history(
        self,
        site_id: str,
        deployment_key: str,
    ) -> list[asyncpg.Record]:
        return await self._pool.fetch(
            """
            SELECT
                deployment_id       AS id,
                drift_status        AS state,
                0                   AS attempt_number,
                retry_count,
                NULL::text          AS failure_domain,
                last_error,
                last_status         AS ack_result,
                NULL::text          AS ack_detail,
                NULL::text          AS matched_entity_id,
                created_at,
                last_pushed_at      AS pushed_at,
                NULL::timestamptz   AS acknowledged_at,
                updated_at
            FROM c_ha_automation_deployments
            WHERE site_id::text = $1 AND automation_key = $2
            ORDER BY updated_at ASC
            """,
            site_id, deployment_key,
        )