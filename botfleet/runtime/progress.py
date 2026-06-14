"""Per-pod progress reporting to Redis.

A pod (one run of this program) is a fleet of `num_bots` async bots all trading
ONE symbol. This module reports that pod's *aggregate* progress to Redis so the
orchestrator's live status page can show, per pod:

    Pod 3 (symbol 4): heavy_mixed   62%   145000 / 234000 orders

Ownership contract (must not clash with the orchestrator):
  * the pod owns the *work-progress* fields it alone knows:
    bots:{team}:pod{N}:{phase, orders_sent, orders_target, started_at,
    completed_at, error} (plus the static symbol/plan/num_bots scaffold).
  * the pod does NOT own  bots:{team}:pod{N}:status  — liveness is the
    orchestrator's job, derived from Kubernetes Job status (a *crashed* pod can't
    write "failed" about itself, so K8s is the source of truth there).
  * the pod must NOT write the aggregate  bots:{team}:status  — that key is the
    orchestrator's "all pods finished" signal to the aggregator. If a single pod
    set it, the aggregator would stop draining when the first pod finished.

Everything here is best-effort: a Redis blip never interferes with order flow.
Writes are throttled (a periodic flush + phase boundaries), never per-order.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

from botfleet.runtime.settings import REDIS_URL, TEST_ID

try:
    import redis
except ImportError:  # redis-py optional; absence just disables reporting
    redis = None

log = logging.getLogger("botfleet.progress")

# How often the background task pushes the live counters to Redis.
FLUSH_S = float(os.environ.get("PROGRESS_FLUSH_S", "5.0"))


class SentCounter:
    """A shared, asyncio-safe (single-threaded loop) order-sent tally that every
    bot in the pod increments. PodProgress reads it on its flush tick."""

    __slots__ = ("n",)

    def __init__(self) -> None:
        self.n = 0


class NullProgress:
    """No-op stand-in used when Redis is unavailable or reporting is disabled, so
    callers never need to branch."""

    counter = SentCounter()

    def start(self) -> None: ...
    def set_phase(self, name: str, target: int) -> None: ...
    async def stop(self, ok: bool = True, error: str = "") -> None: ...


class PodProgress:
    def __init__(self, team: str, pod_id: int, symbol: int, plan: str,
                 num_bots: int, counter: SentCounter):
        self.r = redis.from_url(REDIS_URL, decode_responses=True)
        self.prefix = f"bots:{team}:pod{pod_id}"
        self.counter = counter
        self._num_bots = num_bots
        self._phase = ""
        self._target = 0
        self._task: asyncio.Task | None = None
        self._stop = False
        # Seed the static fields immediately so the pod shows up on the page the
        # moment it starts, even before the first phase.
        self._safe(lambda: self.r.mset({
            f"{self.prefix}:symbol": symbol,
            f"{self.prefix}:plan": plan,
            f"{self.prefix}:num_bots": num_bots,
            f"{self.prefix}:orders_sent": 0,
            f"{self.prefix}:orders_target": 0,
            f"{self.prefix}:phase": "",
            f"{self.prefix}:error": "",
            f"{self.prefix}:started_at": int(time.time()),
        }))

    # ── lifecycle ─────────────────────────────────────────────────────────────
    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._loop())

    def set_phase(self, name: str, target: int) -> None:
        """Called by every bot at a phase boundary (idempotent — same value)."""
        self._phase = name
        self._target = target
        self._safe(lambda: self.r.mset({
            f"{self.prefix}:phase": name,
            f"{self.prefix}:orders_target": target,
        }))

    async def stop(self, ok: bool = True, error: str = "") -> None:
        self._stop = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._safe(lambda: self.r.mset({
            f"{self.prefix}:orders_sent": self.counter.n,
            f"{self.prefix}:error": error or "",
            f"{self.prefix}:completed_at": int(time.time()),
        }))

    # ── internals ─────────────────────────────────────────────────────────────
    async def _loop(self) -> None:
        try:
            while not self._stop:
                await asyncio.sleep(FLUSH_S)
                self._safe(lambda: self.r.mset({
                    f"{self.prefix}:orders_sent": self.counter.n,
                    f"{self.prefix}:phase": self._phase,
                    f"{self.prefix}:orders_target": self._target,
                }))
        except asyncio.CancelledError:
            pass

    def _safe(self, op) -> None:
        try:
            op()
        except Exception as e:  # redis errors must never disturb order flow
            log.debug("progress write failed (ignored): %s", e)


def make_pod_progress(pod_id: int, symbol: int, plan: str, num_bots: int):
    """Build a reporter, or a no-op if reporting is off / Redis missing.

    The shared SentCounter is returned on the object as ``.counter`` so the bots
    can increment it on every send.
    """
    disabled = os.environ.get("PROGRESS_SINK", "redis").lower() in {"none", "off", "0"}
    if disabled or redis is None:
        return NullProgress()
    counter = SentCounter()
    try:
        prog = PodProgress(TEST_ID, pod_id, symbol, plan, num_bots, counter)
        prog.counter = counter
        return prog
    except Exception as e:
        log.warning("Redis progress disabled (%s)", e)
        return NullProgress()
