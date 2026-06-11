"""Phase coordinators — decide which phase a bot should be in.

Local mode walks a TEST_PLANS entry; the Redis variant is a stub that falls back
to local so the code path stays exercised until redis-py is wired in.
"""

import os

from botfleet.core.plans import TEST_PLANS
from botfleet.runtime.settings import TEST_ID


class PhaseCoordinator:
    """
    Decides which phase a bot should be in. Subclass to swap the source.

    Contract:
      - get_phase()  -> (phase_name, rate) or None when the test is over
      - should_switch(current_name) -> True if a phase change is pending
    """

    def get_phase(self) -> tuple[str, int] | None: ...
    def should_switch(self, current: str) -> bool: ...


class LocalPhaseCoordinator(PhaseCoordinator):
    """Walks through a TEST_PLANS entry. Used when no Redis is available."""

    def __init__(self, plan_name: str):
        self.plan = list(TEST_PLANS[plan_name])
        self.idx = 0

    def get_phase(self):
        if self.idx >= len(self.plan):
            return None
        return self.plan[self.idx]

    def should_switch(self, current: str):
        # Local mode just lets the sender's has_more() drive the boundary.
        return False

    def advance(self):
        self.idx += 1


class RedisPhaseCoordinator(PhaseCoordinator):
    """
    Stub. When redis-py is wired in:
      r = redis.Redis(...)
      phase = r.get(f"test:{TEST_ID}:phase")
      rate  = int(r.get(f"test:{TEST_ID}:rate"))
    For now: falls back to a local plan so the code path is exercised.
    """
    def __init__(self, plan_name: str):
        self._fallback = LocalPhaseCoordinator(plan_name)
        # self.r = redis.Redis(host=os.environ["REDIS_HOST"], ...)
        self._last_seen: str | None = None

    def get_phase(self):
        # current = self.r.get(f"test:{TEST_ID}:phase")
        # if current is None: return None
        # rate = int(self.r.get(f"test:{TEST_ID}:rate") or 100)
        # self._last_seen = current.decode()
        # return (self._last_seen, rate)
        return self._fallback.get_phase()

    def should_switch(self, current: str):
        # latest = self.r.get(f"test:{TEST_ID}:phase")
        # return latest is not None and latest.decode() != current
        return False

    def advance(self):
        self._fallback.advance()


def make_coordinator(plan_name: str) -> PhaseCoordinator:
    if os.environ.get("PHASE_SOURCE", "local").lower() == "redis":
        return RedisPhaseCoordinator(plan_name)
    return LocalPhaseCoordinator(plan_name)
