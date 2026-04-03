# routers/ops.py

from typing import Optional, Literal
from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel, UUID4, computed_field
from datetime import datetime

import asyncpg

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