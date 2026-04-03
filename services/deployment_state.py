# services/deployment_state.py

from enum import Enum


class DeploymentState(Enum):
    pending           = "pending"
    rendered          = "rendered"
    push_attempted    = "push_attempted"
    pushed            = "pushed"
    acknowledged      = "acknowledged"
    in_sync           = "in_sync"
    failed            = "failed"
    mismatch          = "mismatch"
    permanent_failure = "permanent_failure"


class FailureDomain(Enum):
    availability = "availability"
    config       = "config"
    auth         = "auth"
    unknown      = "unknown"


TRANSITIONS: dict[DeploymentState, set[DeploymentState]] = {
    DeploymentState.pending:           {DeploymentState.rendered, DeploymentState.failed},
    DeploymentState.rendered:          {DeploymentState.push_attempted, DeploymentState.failed},
    DeploymentState.push_attempted:    {DeploymentState.pushed, DeploymentState.failed},
    DeploymentState.pushed:            {DeploymentState.acknowledged, DeploymentState.failed,
                                        DeploymentState.mismatch},
    DeploymentState.acknowledged:      {DeploymentState.in_sync},
    DeploymentState.in_sync:           set(),
    DeploymentState.failed:            {DeploymentState.permanent_failure},
    DeploymentState.mismatch:          set(),
    DeploymentState.permanent_failure: set(),
}


class InvalidTransitionError(Exception):
    pass


def assert_transition(
    current: DeploymentState,
    next_state: DeploymentState,
) -> None:
    """
    Raise if transition is not permitted.
    Call this before every state write.
    """
    allowed = TRANSITIONS.get(current, set())
    if next_state not in allowed:
        raise InvalidTransitionError(
            f"Invalid transition: {current.value} → {next_state.value}. "
            f"Allowed: {[s.value for s in allowed]}"
        )