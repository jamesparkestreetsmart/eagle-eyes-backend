# services/alerting.py

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class OpsAlert:
    site_id:        str
    alias:          str
    deployment_key: str
    record_id:      str
    reason:         str
    failure_domain: Optional[str]
    last_error:     Optional[str]


class AlertService:
    """
    Ops alert interface.
    Currently: structured log at ERROR level.
    Production: swap in PagerDuty, Slack, webhook, etc.
    """

    async def send(self, alert: OpsAlert) -> None:
        logger.error(
            "OPS ALERT",
            extra={
                "site_id":        alert.site_id,
                "alias":          alert.alias,
                "deployment_key": alert.deployment_key,
                "record_id":      alert.record_id,
                "reason":         alert.reason,
                "failure_domain": alert.failure_domain,
                "last_error":     alert.last_error,
            }
        )
        # TODO: wire to PagerDuty / Slack / webhook