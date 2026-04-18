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
    template_probes: Optional[list[dict]] = None
    discovery_mode: Optional[Literal["config_entries", "yaml_unknown"]] = None
    entities_live: Optional[bool] = None
    ip_confidence: Optional[Literal["high", "low", "none"]] = None
    entity_states: Optional[dict] = None


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

    template_probes: list[dict] = []

    async with httpx.AsyncClient(timeout=10.0) as client:
        # Step 0: Live template probes — tells us whether the sensor is alive,
        # unavailable, or zero, regardless of whether we can discover the IP.
        probe_templates = [
            ("hot_flow_rate_state", "{{ states('sensor.water_heater_hot_flow_rate') }}"),
            ("hot_flow_rate_friendly_name",
             "{{ state_attr('sensor.water_heater_hot_flow_rate', 'friendly_name') }}"),
        ]
        for probe_name, tmpl in probe_templates:
            probe_entry: dict = {"name": probe_name, "template": tmpl}
            try:
                probe_resp = await client.post(
                    f"{ha_url}/api/template",
                    headers=headers,
                    json={"template": tmpl},
                )
                probe_entry["status_code"] = probe_resp.status_code
                if probe_resp.status_code == 200:
                    probe_entry["result"] = probe_resp.text
                else:
                    probe_entry["error"] = probe_resp.text[:300]
            except Exception as e:
                probe_entry["error"] = f"request failed: {e}"
            template_probes.append(probe_entry)

        # Step 1: Entity liveness — primary signal for YAML Modbus sites.
        entity_ids = [
            "sensor.water_heater_hot_temp",
            "sensor.water_heater_hot_flow_rate",
            "sensor.water_heater_hot_accumulated_volume",
        ]
        entity_states: dict = {}
        for entity_id in entity_ids:
            try:
                state_resp = await client.get(f"{ha_url}/api/states/{entity_id}", headers=headers)
                if state_resp.status_code == 200:
                    state_data = state_resp.json()
                    state_val = state_data.get("state")
                    entity_states[entity_id] = {
                        "state": state_val,
                        "available": state_val not in (None, "unavailable", "unknown"),
                    }
                else:
                    entity_states[entity_id] = {
                        "state": None,
                        "available": False,
                        "error": f"HTTP {state_resp.status_code}",
                    }
            except Exception as e:
                entity_states[entity_id] = {"state": None, "available": False, "error": str(e)}

        entities_live = bool(entity_states) and all(
            es.get("available") for es in entity_states.values()
        )

        # Step 2: Query HA config entries for Modbus integrations
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
            # YAML-configured (or missing) Modbus — fall back to the DB IP and
            # report entity liveness as the primary health signal.
            db_device = await pool.fetchrow(
                """SELECT device_id, ip_address FROM a_devices
                   WHERE site_id = $1 AND device_type_code = 'NQM'
                   LIMIT 1""",
                site_id,
            )
            db_ip = db_device["ip_address"] if db_device else None
            ip_confidence_val: Literal["low", "none"] = "low" if db_ip else "none"

            if db_device and not req.dry_run:
                await pool.execute(
                    """UPDATE a_devices
                       SET ip_discovery_mode = 'yaml_unknown',
                           last_discovery_at = now(),
                           updated_at = now()
                       WHERE device_id = $1""",
                    db_device["device_id"],
                )

            details.append({
                "source": "db_fallback",
                "discovery_mode": "yaml_unknown",
                "db_ip": db_ip,
                "ip_confidence": ip_confidence_val,
                "entities_live": entities_live,
                "message": (
                    "No Modbus config_entries in HA — using DB IP fallback"
                    if db_device else
                    "No Modbus config_entries in HA and no NQM device in DB"
                ),
            })

            return ModbusDiscoveryResult(
                site_id=site_id,
                devices_found=0,
                devices_updated=0,
                details=details,
                template_probes=template_probes,
                discovery_mode="yaml_unknown",
                entities_live=entities_live,
                ip_confidence=ip_confidence_val,
                entity_states=entity_states,
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
                            """UPDATE a_devices
                               SET ip_address = $1,
                                   ip_confidence = 'high',
                                   ip_discovery_mode = 'config_entries',
                                   last_discovery_at = now(),
                                   updated_at = now()
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
                    })
                else:
                    details.append({
                        "entry_id": entry_id,
                        "status": "unchanged",
                        "ip": discovered_ip,
                        "port": discovered_port,
                    })
            else:
                details.append({
                    "entry_id": entry_id,
                    "status": "no_nqm_device",
                    "discovered_ip": discovered_ip,
                    "port": discovered_port,
                    "message": "No NQM device in a_devices for this site",
                })

    return ModbusDiscoveryResult(
        site_id=site_id,
        devices_found=devices_found,
        devices_updated=devices_updated,
        details=details,
        template_probes=template_probes,
        discovery_mode="config_entries",
        entities_live=entities_live,
        ip_confidence="high",
        entity_states=entity_states,
    )


# ------------------------------------------------------------------
# HA service-call proxy
# ------------------------------------------------------------------

class HaServiceRequest(BaseModel):
    site_id: str
    domain: str
    service: str
    service_data: Optional[dict] = None


class HaServiceResult(BaseModel):
    site_id: str
    domain: str
    service: str
    status_code: int
    ok: bool
    result: Optional[list | dict] = None
    error: Optional[str] = None


@router.post("/ha-service", response_model=HaServiceResult)
async def call_ha_service(
    req: HaServiceRequest,
    pool: asyncpg.Pool = Depends(get_pool),
):
    """
    Proxy an HA service call through the Render worker. Posts to
    {ha_url}/api/services/{domain}/{service} with the provided service_data.
    Examples:
      {"site_id": "...", "domain": "homeassistant", "service": "reload_all"}
      {"site_id": "...", "domain": "modbus", "service": "reload"}
    """
    row = await pool.fetchrow(
        "SELECT ha_url, ha_token FROM a_sites WHERE site_id = $1", req.site_id
    )
    if not row or not row["ha_url"] or not row["ha_token"]:
        raise HTTPException(status_code=400, detail="Site has no HA connection configured")

    ha_url = row["ha_url"].rstrip("/")
    ha_token = row["ha_token"]
    headers = {"Authorization": f"Bearer {ha_token}", "Content-Type": "application/json"}
    url = f"{ha_url}/api/services/{req.domain}/{req.service}"
    payload = req.service_data or {}

    logger.info(f"[ha-service] site={req.site_id} -> {req.domain}.{req.service}")

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.post(url, headers=headers, json=payload)
        except Exception as e:
            logger.warning(f"[ha-service] call failed for site {req.site_id}: {e}")
            raise HTTPException(status_code=502, detail=f"HA service call failed: {e}")

    result: Optional[list | dict] = None
    error: Optional[str] = None
    ok = 200 <= resp.status_code < 300

    if ok:
        try:
            result = resp.json()
        except Exception:
            result = None
    else:
        error = resp.text[:500]

    return HaServiceResult(
        site_id=req.site_id,
        domain=req.domain,
        service=req.service,
        status_code=resp.status_code,
        ok=ok,
        result=result,
        error=error,
    )


# ------------------------------------------------------------------
# HA entity state diagnostics
# ------------------------------------------------------------------

class HAEntityState(BaseModel):
    entity_id:      str
    found:          bool
    state:          Optional[str] = None
    last_changed:   Optional[str] = None
    last_updated:   Optional[str] = None
    attributes:     Optional[dict] = None
    error:          Optional[str] = None
    status_code:    Optional[int] = None


class HAEntityStatesRequest(BaseModel):
    site_id:    str
    entity_ids: list[str]


class HAEntityStatesResponse(BaseModel):
    site_id:  str
    ha_url:   str
    count:    int
    states:   list[HAEntityState]


async def _fetch_ha_credentials(pool: asyncpg.Pool, site_id: str) -> tuple[str, str]:
    row = await pool.fetchrow(
        "SELECT ha_url, ha_token FROM a_sites WHERE site_id = $1", site_id
    )
    if not row or not row["ha_url"] or not row["ha_token"]:
        raise HTTPException(status_code=400, detail="Site has no HA connection configured")
    return row["ha_url"].rstrip("/"), row["ha_token"]


async def _fetch_ha_entity_state(
    client: httpx.AsyncClient, ha_url: str, headers: dict, entity_id: str
) -> HAEntityState:
    try:
        resp = await client.get(f"{ha_url}/api/states/{entity_id}", headers=headers)
    except httpx.HTTPError as e:
        return HAEntityState(entity_id=entity_id, found=False, error=str(e))

    if resp.status_code == 404:
        return HAEntityState(entity_id=entity_id, found=False, status_code=404)
    if resp.status_code != 200:
        return HAEntityState(
            entity_id=entity_id,
            found=False,
            status_code=resp.status_code,
            error=resp.text[:500],
        )

    data = resp.json()
    return HAEntityState(
        entity_id=entity_id,
        found=True,
        state=data.get("state"),
        last_changed=data.get("last_changed"),
        last_updated=data.get("last_updated"),
        attributes=data.get("attributes"),
        status_code=200,
    )


@router.get("/ha-entity-state", response_model=HAEntityState)
async def ha_entity_state(
    site_id:   str = Query(..., description="Site UUID"),
    entity_id: str = Query(..., description="HA entity_id, e.g. sensor.foo_bar"),
    pool: asyncpg.Pool = Depends(get_pool),
):
    """Fetch the current HA state for a single entity at a site."""
    ha_url, ha_token = await _fetch_ha_credentials(pool, site_id)
    headers = {"Authorization": f"Bearer {ha_token}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=10.0) as client:
        return await _fetch_ha_entity_state(client, ha_url, headers, entity_id)


@router.post("/ha-entity-states", response_model=HAEntityStatesResponse)
async def ha_entity_states(
    req: HAEntityStatesRequest,
    pool: asyncpg.Pool = Depends(get_pool),
):
    """Fetch current HA states for a batch of entities at a site."""
    if not req.entity_ids:
        raise HTTPException(status_code=400, detail="entity_ids must not be empty")

    ha_url, ha_token = await _fetch_ha_credentials(pool, req.site_id)
    headers = {"Authorization": f"Bearer {ha_token}", "Content-Type": "application/json"}

    async with httpx.AsyncClient(timeout=15.0) as client:
        states = [
            await _fetch_ha_entity_state(client, ha_url, headers, eid)
            for eid in req.entity_ids
        ]

    return HAEntityStatesResponse(
        site_id=req.site_id,
        ha_url=ha_url,
        count=len(states),
        states=states,
    )