"""Phase coordinators — decide which phase a bot should be in.

Local mode walks a fixed TEST_PLANS entry; Redis mode follows a phase that a
central controller publishes, keyed by test_id, so a whole fleet of pods can be
driven in lockstep from the outside.
"""

import os
import time

from botfleet.core.plans import PHASE_CONFIGS, TEST_PLANS
from botfleet.runtime.settings import REDIS_URL, TEST_ID

# redis-py is only needed when PHASE_SOURCE=redis — import lazily so the default
# local path works without the dependency installed.
try:
    import redis
except ImportError:
    redis = None

# Phase-key values that mean "no active phase → stop this bot".
_STOP_VALUES = {"", "done", "stop", "finished", "none", "end"}


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
    Follows a phase published by a central controller, keyed by test_id:

        test:{test_id}:phase  ->  phase name (one of PHASE_CONFIGS), or a stop
                                  sentinel ("done"/"stop"/empty) to end the run
        test:{test_id}:rate   ->  orders/sec per bot for the current phase

    Bots don't advance a plan themselves — they run whatever Redis says and switch
    when it changes. The phase read is cached briefly so `should_switch` (polled
    in the sender's hot loop) doesn't hit Redis on every check.
    """

    POLL_S = 0.5        # min seconds between Redis reads of the phase key

    def __init__(self, plan_name: str, test_id: str = TEST_ID):
        if redis is None:
            raise RuntimeError(
                "PHASE_SOURCE=redis but redis-py is not installed (pip install redis)"
            )
        self.test_id = test_id
        self.r = redis.from_url(REDIS_URL, decode_responses=True)
        self._phase_key = f"test:{test_id}:phase"
        self._rate_key = f"test:{test_id}:rate"
        self._cached_phase: str | None = None
        self._cached_at = 0.0

    def _read_phase(self) -> str | None:
        """Current phase name, or None if no active phase. Cached for POLL_S."""
        now = time.monotonic()
        if self._cached_at and (now - self._cached_at) < self.POLL_S:
            return self._cached_phase
        val = self.r.get(self._phase_key)
        if val is not None:
            val = val.strip()
            # stop sentinel, or an unknown phase name → treat as "no active phase"
            if val.lower() in _STOP_VALUES or val not in PHASE_CONFIGS:
                val = None
        self._cached_phase = val
        self._cached_at = now
        return val

    def get_phase(self):
        phase = self._read_phase()
        if phase is None:
            return None
        try:
            rate = int(self.r.get(self._rate_key))
        except (TypeError, ValueError):
            rate = 100
        return (phase, rate)

    def should_switch(self, current: str):
        # True if the controller moved to a different phase, or ended the run.
        return self._read_phase() != current

    def advance(self):
        # No-op: the controller drives phase transitions via Redis, not the bot.
        pass


def make_coordinator(plan_name: str) -> PhaseCoordinator:
    if os.environ.get("PHASE_SOURCE", "local").lower() == "redis":
        return RedisPhaseCoordinator(plan_name)
    return LocalPhaseCoordinator(plan_name)
