# routers/ops.py

from typing import Optional, Literal
from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel, UUID4, computed_field
from datetime import datetime
import logging

import asyncpg
import httpx

from repositories.ops_repository import OpsRepository
from dependencies import get_pool


router = APIRouter(prefix="/ops", tags=["ops"])


def get_ops_repo(pool: asyncpg.Pool = Depends(get_pool)) -> OpsRepository:
    return OpsRepository(pool)


# ------------------------------------------------------------------
# Response models
# ------------------------------------------------------------------

class StateCount(BaseModel):
    state:          str
    count:          int
    auth_failures:  Optional[int] = None
    config_failures: Optional[int] = None


class GlobalHealthSummary(BaseModel):
    total:  int
    states: list[StateCount]


class SiteStateCount(BaseModel):
    site_id: str
    state:   str
    count:   int


class PerSiteHealthSummary(BaseModel):
    totals_by_site: dict[str, int]
    states:         list[SiteStateCount]


class HealthSummary(BaseModel):
    global_summary:   GlobalHealthSummary
    per_site_summary: PerSiteHealthSummary


class StuckDeployment(BaseModel):
    id:                UUID4
    site_id:           str
    alias:             str
    deployment_key:    str
    state:             str
    updated_at:        datetime
    retry_count:       int
    last_error:        Optional[str]
    failure_domain:    Optional[str]
    stuck_minutes:     float
    threshold_minutes: int


class TerminalFailure(BaseModel):
    id:             UUID4
    site_id:        str
    alias:          str
    deployment_key: str
    state:          str
    failure_domain: Optional[str]
    last_error:     Optional[str]
    ack_result:     Optional[str]
    ack_detail:     Optional[str]
    created_at:     datetime
    updated_at:     datetime

    @computed_field
    @property
    def severity(self) -> Literal["critical", "high"]:
        return "critical" if self.state == "mismatch" else "high"


class RetryQueueItem(BaseModel):
    id:                  UUID4
    site_id:             str
    alias:               str
    deployment_key:      str
    retry_count:         int
    attempt_number:      int
    next_retry_at:       Optional[datetime]
    last_error:          Optional[str]
    failure_domain:      Optional[str]
    retry_status:        Literal["scheduled", "overdue"]
    minutes_until_retry: float


class DeploymentRecord(BaseModel):
    id:             UUID4
    site_id:        str
    alias:          str
    deployment_key: str
    state:          str
    attempt_number: int
    retry_count:    int
    failure_domain: Optional[str]
    last_error:     Optional[str]
    ack_result:     Optional[str]
    ack_detail:     Optional[str]
    created_at:     datetime
    pushed_at:      Optional[datetime]
    acknowledged_at: Optional[datetime]
    updated_at:     datetime


class DeploymentAttempt(BaseModel):
    id:                UUID4
    state:             str
    attempt_number:    int
    retry_count:       int
    failure_domain:    Optional[str]
    last_error:        Optional[str]
    ack_result:        Optional[str]
    ack_detail:        Optional[str]
    matched_entity_id: Optional[str]
    created_at:        datetime
    pushed_at:         Optional[datetime]
    acknowledged_at:   Optional[datetime]
    updated_at:        datetime


# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------

@router.get("/health", response_model=HealthSummary)
async def health_summary(repo: OpsRepository = Depends(get_ops_repo)):
    """Global and per-site deployment state counts with totals."""
    global_data   = await repo.health_summary_global()
    per_site_data = await repo.health_summary_per_site()
    return HealthSummary(
        global_summary=GlobalHealthSummary(
            total=global_data["total"],
            states=[StateCount(**r) for r in global_data["states"]],
        ),
        per_site_summary=PerSiteHealthSummary(
            totals_by_site=per_site_data["totals_by_site"],
            states=[SiteStateCount(**r) for r in per_site_data["states"]],
        ),
    )


@router.get("/stuck", response_model=list[StuckDeployment])
async def stuck_deployments(
    state: Optional[str] = Query(None),
    older_than_minutes: int = Query(30, ge=1),
    repo: OpsRepository = Depends(get_ops_repo),
):
    """Deployments stuck in transient states longer than threshold."""
    if state:
        rows = await repo.get_stuck(state, older_than_minutes)
    else:
        rows = await repo.get_all_stuck(older_than_minutes)
    return [StuckDeployment(**dict(r)) for r in rows]


@router.get("/terminal-failures", response_model=list[TerminalFailure])
async def terminal_failures(
    site_id: Optional[str] = Query(None),
    repo: OpsRepository = Depends(get_ops_repo),
):
    """Mismatch and permanent_failure records — require human intervention."""
    rows = await repo.get_terminal_failures(site_id=site_id)
    return [TerminalFailure(**dict(r)) for r in rows]


@router.get("/retry-queue", response_model=list[RetryQueueItem])
async def retry_queue(repo: OpsRepository = Depends(get_ops_repo)):
    """Pending retries — scheduled and overdue."""
    rows = await repo.get_retry_queue()
    return [RetryQueueItem(**dict(r)) for r in rows]


@router.get(
    "/sites/{site_id}/never-acknowledged",
    response_model=list[DeploymentRecord],
)
async def never_acknowledged(
    site_id: str,
    repo: OpsRepository = Depends(get_ops_repo),
):
    """Deployments for a site that never reached acknowledged or in_sync."""
    rows = await repo.get_never_acknowledged(site_id)
    return [DeploymentRecord(**dict(r)) for r in rows]


@router.get("/history", response_model=list[DeploymentRecord])
async def recent_history(
    hours: int = Query(24, ge=1, le=168),
    site_id: Optional[str] = Query(None),
    repo: OpsRepository = Depends(get_ops_repo),
):
    """All deployment attempts in the last N hours (max 7 days)."""
    rows = await repo.get_recent_deployments(hours=hours, site_id=site_id)
    return [DeploymentRecord(**dict(r)) for r in rows]


@router.get(
    "/sites/{site_id}/deployments/{deployment_key}/history",
    response_model=list[DeploymentAttempt],
)
async def deployment_history(
    site_id: str,
    deployment_key: str,
    repo: OpsRepository = Depends(get_ops_repo),
):
    """Full attempt history for a specific deployment key at a site."""
    rows = await repo.get_deployment_history(site_id, deployment_key)
    if not rows:
        raise HTTPException(status_code=404, detail="No deployment history found")
    return [DeploymentAttempt(**dict(r)) for r in rows]


# ------------------------------------------------------------------
# Modbus device discovery
# ------------------------------------------------------------------

logger = logging.getLogger(__name__)


class ModbusDiscoveryRequest(BaseModel):
    site_id: str
    dry_run: bool = False


class ModbusDiscoveryResult(BaseModel):
    site_id: str
    devices_found: int
    devices_updated: int
    details: list[dict]


@router.post("/discover-modbus-devices", response_model=ModbusDiscoveryResult)
async def discover_modbus_devices(
    req: ModbusDiscoveryRequest,
    pool: asyncpg.Pool = Depends(get_pool),
):
    """
    Query HA for Modbus integration config entries, discover device IPs,
    and auto-update a_devices.ip_address for NQM (and future Modbus TCP) devices.
    """
    site_id = req.site_id

    # Get site's HA connection info
    row = await pool.fetchrow(
        "SELECT ha_url, ha_token FROM a_sites WHERE site_id = $1", site_id
    )
    if not row or not row["ha_url"] or not row["ha_token"]:
        raise HTTPException(status_code=400, detail="Site has no HA connection configured")

    ha_url = row["ha_url"].rstrip("/")
    ha_token = row["ha_token"]
    headers = {"Authorization": f"Bearer {ha_token}", "Content-Type": "application/json"}

    details = []
    devices_found = 0
    devices_updated = 0

    async with httpx.AsyncClient(timeout=10.0) as client:
        # Step 1: Query HA config entries for Modbus integrations
        try:
            resp = await client.get(f"{ha_url}/api/config/config_entries/entry", headers=headers)
            resp.raise_for_status()
            config_entries = resp.json()
        except Exception as e:
            logger.warning(f"[modbus-discovery] Failed to fetch HA config entries for site {site_id}: {e}")
            raise HTTPException(status_code=502, detail=f"HA config query failed: {e}")

        modbus_entries = [
            e for e in config_entries
            if e.get("domain") == "modbus"
        ]

        if not modbus_entries:
            return ModbusDiscoveryResult(
                site_id=site_id, devices_found=0, devices_updated=0,
                details=[{"message": "No Modbus integrations found in HA"}],
            )

        for entry in modbus_entries:
            entry_data = entry.get("data", {})
            discovered_ip = entry_data.get("host")
            discovered_port = entry_data.get("port", 502)
            entry_id = entry.get("entry_id", "unknown")

            if not discovered_ip:
                details.append({"entry_id": entry_id, "status": "skipped", "reason": "no host in config"})
                continue

            devices_found += 1
            logger.info(f"[modbus-discovery] site={site_id} found Modbus device at {discovered_ip}:{discovered_port}")

            # Step 2: Check entity states (optional — for diagnostics)
            entity_states = []
            for entity_id in [
                "sensor.water_heater_hot_temp",
                "sensor.water_heater_hot_flow_rate",
                "sensor.water_heater_hot_accumulated_volume",
            ]:
                try:
                    state_resp = await client.get(f"{ha_url}/api/states/{entity_id}", headers=headers)
                    if state_resp.status_code == 200:
                        state_data = state_resp.json()
                        entity_states.append({
                            "entity_id": entity_id,
                            "state": state_data.get("state"),
                            "available": state_data.get("state") != "unavailable",
                        })
                except Exception:
                    pass

            # Step 3: Auto-update a_devices.ip_address for NQM devices
            existing = await pool.fetchrow(
                """SELECT device_id, ip_address FROM a_devices
                   WHERE site_id = $1 AND device_type_code = 'NQM'
                   LIMIT 1""",
                site_id,
            )

            if existing:
                old_ip = existing["ip_address"]
                if old_ip != discovered_ip:
                    if not req.dry_run:
                        await pool.execute(
                            """UPDATE a_devices SET ip_address = $1, updated_at = now()
                               WHERE device_id = $2""",
                            discovered_ip, existing["device_id"],
                        )
                        await pool.execute(
                            """INSERT INTO b_records_log (site_id, event_type, source, message, created_by, event_date)
                               VALUES ($1, 'modbus_ip_update', 'modbus_discovery',
                                       $2, 'system', CURRENT_DATE)""",
                            site_id,
                            f"NQM IP auto-updated: {old_ip} -> {discovered_ip} (from HA Modbus config)",
                        )
                    devices_updated += 1
                    logger.info(
                        f"[modbus-discovery] site={site_id} NQM IP {'would update' if req.dry_run else 'updated'}: {old_ip} -> {discovered_ip}"
                    )

                    details.append({
                        "entry_id": entry_id,
                        "status": "would_update" if req.dry_run else "updated",
                        "dry_run": req.dry_run,
                        "old_ip": old_ip,
                        "new_ip": discovered_ip,
                        "port": discovered_port,
                        "entity_states": entity_states,
                    })
                else:
                    details.append({
                        "entry_id": entry_id,
                        "status": "unchanged",
                        "ip": discovered_ip,
                        "port": discovered_port,
                        "entity_states": entity_states,
                    })
            else:
                details.append({
                    "entry_id": entry_id,
                    "status": "no_nqm_device",
                    "discovered_ip": discovered_ip,
                    "port": discovered_port,
                    "message": "No NQM device in a_devices for this site",
                    "entity_states": entity_states,
                })

    return ModbusDiscoveryResult(
        site_id=site_id,
        devices_found=devices_found,
        devices_updated=devices_updated,
        details=details,
    )