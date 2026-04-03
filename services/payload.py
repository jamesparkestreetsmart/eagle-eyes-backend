# services/payload.py

import logging
import uuid
import yaml
from dataclasses import dataclass
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)


class RenderBlockedError(Exception):
    pass


class SnapshotNotFoundError(Exception):
    pass


class ChecksumMismatchError(Exception):
    pass


@dataclass
class RenderedSnapshot:
    snapshot_id:  uuid.UUID
    package_id:   uuid.UUID
    site_id:      uuid.UUID
    alias:        str
    checksum:     str
    payload:      dict


async def get_payload(
    package_id: uuid.UUID,
    site_id: uuid.UUID,
    pool: asyncpg.Pool,
    expected_checksum: Optional[str] = None,
) -> RenderedSnapshot:
    """
    Fetch the latest validated rendered snapshot for a (site, package) pair.

    Rules:
    - Must exist in c_ha_rendered_snapshots
    - render_validated must be True
    - render_blocked must be False
    - is_valid must be True
    - If expected_checksum provided, must match
    - Payload parsed from rendered_yaml TEXT → dict at fetch time
    """
    row = await pool.fetchrow(
        """
        SELECT
            snapshot_id,
            package_id,
            site_id,
            checksum,
            rendered_yaml,
            render_validated,
            render_blocked,
            unresolved_variables,
            missing_deps,
            is_valid
        FROM c_ha_rendered_snapshots
        WHERE site_id = $1 AND package_id = $2
        ORDER BY rendered_at DESC
        LIMIT 1
        """,
        site_id, package_id,
    )

    if not row:
        raise SnapshotNotFoundError(
            f"No snapshot found for site '{site_id}', package '{package_id}'"
        )

    if row["render_blocked"]:
        unresolved = row["unresolved_variables"] or []
        missing    = row["missing_deps"] or []
        reason     = []
        if unresolved:
            reason.append(f"unresolved variables: {unresolved}")
        if missing:
            reason.append(f"missing dependencies: {missing}")
        raise RenderBlockedError(
            f"Snapshot is render-blocked — "
            f"{'; '.join(reason) or 'reason unknown'}"
        )

    if not row["render_validated"]:
        raise RenderBlockedError(
            f"Snapshot has not been render-validated "
            f"(is_valid={row['is_valid']})"
        )

    if not row["is_valid"]:
        raise RenderBlockedError(
            "Snapshot marked is_valid=false — cannot deploy"
        )

    if expected_checksum and row["checksum"] != expected_checksum:
        raise ChecksumMismatchError(
            f"Checksum mismatch: expected {expected_checksum}, "
            f"got {row['checksum']}"
        )

    try:
        payload = yaml.safe_load(row["rendered_yaml"])
    except yaml.YAMLError as e:
        raise RenderBlockedError(f"rendered_yaml failed to parse: {e}")

    if not isinstance(payload, dict):
        raise RenderBlockedError(
            f"rendered_yaml parsed to {type(payload).__name__}, expected dict"
        )

    alias = payload.get("alias", "")
    if not alias:
        raise RenderBlockedError("Parsed payload has no 'alias' field")

    logger.info("Snapshot loaded", extra={
        "snapshot_id": str(row["snapshot_id"]),
        "package_id":  str(package_id),
        "site_id":     str(site_id),
        "alias":       alias,
        "checksum":    row["checksum"],
    })

    return RenderedSnapshot(
        snapshot_id=row["snapshot_id"],
        package_id=package_id,
        site_id=site_id,
        alias=alias,
        checksum=row["checksum"],
        payload=payload,
    )