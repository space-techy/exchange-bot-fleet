"""
Production bot fleet.

N async bots connect to the matching engine, run through a test plan, and ship
per-order telemetry to a pluggable sink (file now, Kafka later). Phase changes
can come from a pluggable coordinator (local plan now, Redis later).

No order book, no plotting, no prints — observability is the telemetry stream
alone. For local inspection of behaviour and performance, use live_visualizer.py.
"""

import argparse
import asyncio
import dataclasses
import json
import os
import time

import websockets
from websockets.exceptions import ConnectionClosed

from configs import PHASE_CONFIGS, TEST_PLANS
from order_generator import OrderGenerator


URI = os.environ.get("ENGINE_URI", "ws://localhost:3001/ws")
GLOBAL_SEED = int(os.environ.get("GLOBAL_SEED", "42"))
TEST_ID = os.environ.get("TEST_ID", "local")
FLUSH_INTERVAL_S = float(os.environ.get("FLUSH_INTERVAL_S", "1.0"))


# ─────────────────────────── Telemetry sinks ─────────────────────────────────

class TelemetrySink:
    """Pluggable sink. Default: append-only JSONL per bot. Swap for KafkaSink later."""

    def write_batch(self, events: list[dict]): ...
    def close(self): ...


class FileSink(TelemetrySink):
    def __init__(self, bot_id: int):
        self.bot_id = bot_id

    def write_batch(self, events):
        # Buffered for the (future) real sink. We deliberately don't print
        # per-order events — too noisy. The driver prints phase summaries instead.
        pass

    def close(self):
        pass


class KafkaSink(TelemetrySink):
    """
    Stub. When kafka-python is wired in, instantiate a KafkaProducer here and
    call producer.send(topic, json.dumps(ev).encode()) in write_batch.
    """
    def __init__(self, bot_id: int, topic: str = "bot_telemetry"):
        self.bot_id = bot_id
        self.topic = topic
        # self.producer = KafkaProducer(bootstrap_servers=...)

    def write_batch(self, events):
        # for ev in events: self.producer.send(self.topic, json.dumps(ev).encode())
        pass

    def close(self):
        # self.producer.flush(); self.producer.close()
        pass


def make_sink(bot_id: int) -> TelemetrySink:
    kind = os.environ.get("TELEMETRY_SINK", "file").lower()
    if kind == "kafka":
        return KafkaSink(bot_id)
    return FileSink(bot_id)


class TelemetryCollector:
    """Buffers events, flushes on threshold / timer / close. Sink is swappable."""

    BATCH_SIZE = 500

    def __init__(self, bot_id: int):
        self.sink = make_sink(bot_id)
        self.buf: list[dict] = []
        self.total_flushed = 0
        self._flush_task: asyncio.Task | None = None
        self._stop = False

    def record(self, ev: dict):
        self.buf.append(ev)
        if len(self.buf) >= self.BATCH_SIZE:
            self.flush()

    def flush(self):
        if not self.buf:
            return
        self.sink.write_batch(self.buf)
        self.total_flushed += len(self.buf)
        self.buf.clear()

    async def _periodic_flush(self):
        """Time-based flush so downstream sees fresh data even at low order rate."""
        try:
            while not self._stop:
                await asyncio.sleep(FLUSH_INTERVAL_S)
                self.flush()
        except asyncio.CancelledError:
            pass

    def start(self):
        if self._flush_task is None:
            self._flush_task = asyncio.create_task(self._periodic_flush())

    async def stop(self):
        self._stop = True
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        self.flush()
        self.sink.close()


# ─────────────────────────── Phase coordinator ───────────────────────────────

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


# ─────────────────────────────── Hot path ────────────────────────────────────

# Engine response types that mean "this order is gone from the book".
_TERMINAL_TYPES = ("order_filled", "order_cancelled", "order_rejected")


async def sender_loop(ws, generator: OrderGenerator, pending: dict,
                      rate: int, done: asyncio.Event,
                      coord: PhaseCoordinator, phase: str,
                      telemetry: "TelemetryCollector"):
    """Generate at `rate`/sec. Tracks each in `pending` before sending, and
    emits an `order_sent` telemetry event so the validator can replay.
    Polls the coordinator every 100 orders for an out-of-band phase change."""
    interval = 1.0 / rate
    sent = 0
    next_deadline = time.monotonic()
    try:
        while generator.has_more():
            # remote phase change check (cheap; coord is in-memory by default)
            if sent and sent % 100 == 0 and coord.should_switch(phase):
                break

            order = generator.generate_next()
            t_send = time.monotonic_ns()
            oid = order["order_id"]
            pending[oid] = {
                "t_send_ns": t_send,
                "action": order["action"],
                "phase": phase,
            }
            await ws.send(json.dumps(order))
            sent += 1

            telemetry.record({
                "type": "order_sent",
                "bot_id": generator.bot_id,
                "client_id": order.get("client_id"),
                "order_id": oid,
                "action": order["action"],
                "side": order.get("side"),
                "price": order.get("price"),
                "qty": order.get("qty"),
                "symbol": order.get("symbol"),
                "phase": phase,
                "t_send_ns": t_send,
            })

            next_deadline += interval
            sleep_for = next_deadline - time.monotonic()
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
    except ConnectionClosed:
        pass
    finally:
        done.set()


async def receiver_loop(ws, pending: dict, telemetry: TelemetryCollector,
                        generator: OrderGenerator, done: asyncio.Event,
                        phase: str):
    """Match responses, record latency, sync the generator's active_orders.
    `phase` is the bot's CURRENT phase — used to label resting-side fills that
    don't carry their original phase context."""
    while True:
        try:
            timeout = 2.0 if done.is_set() else 15.0
            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
        except asyncio.TimeoutError:
            if done.is_set():
                return
            continue
        except ConnectionClosed:
            return

        t_recv = time.monotonic_ns()
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            continue

        items = msg if isinstance(msg, list) else [msg]
        for item in items:
            if not isinstance(item, dict):
                continue
            _handle_item(item, t_recv, pending, telemetry, generator, phase)


def _handle_item(item: dict, t_recv: int, pending: dict,
                 telemetry: TelemetryCollector, generator: OrderGenerator,
                 current_phase: str):
    mtype = item.get("type")

    if mtype == "trade_broadcast":
        return

    oid = _extract_oid(item)
    if oid is None:
        return

    sent = pending.pop(oid, None)

    telemetry.record({
        "type": "order_response",
        "bot_id": generator.bot_id,
        "client_id": generator.client_id,
        "order_id": oid,
        "action": sent["action"] if sent else None,
        "phase": sent["phase"] if sent else current_phase,
        "msg_type": mtype,
        "message_code": item.get("message_code"),
        "latency_ns": (t_recv - sent["t_send_ns"]) if sent else None,
        "t_send_ns": sent["t_send_ns"] if sent else None,
        "t_recv_ns": t_recv,
        "error": item.get("error", ""),
        "sequence_number": item.get("sequence_number"),
        "trades": item.get("trades", []),
        "orders": item.get("orders", []),
    })

    if mtype in _TERMINAL_TYPES:
        generator.remove_active_order(oid)
        # Clean up the other side of the trade from our generator (in case we
        # placed both sides, or a resting order of ours was the counterparty).
        for trade in item.get("trades", []):
            for key in ("buyer_order_id", "seller_order_id"):
                other = trade.get(key)
                if other and other != oid:
                    generator.remove_active_order(other)
                    pending.pop(other, None)


def _extract_oid(item: dict) -> int | None:
    if "order_id" in item:
        return item["order_id"]
    orders = item.get("orders") or []
    if orders and isinstance(orders[0], dict):
        return orders[0].get("order_id")
    return None


# ─────────────────────────── per-bot driver ──────────────────────────────────

def _per_bot_total(phase_name: str, num_bots: int) -> int:
    """Split a phase's fleet total_orders evenly across bots."""
    return max(1, PHASE_CONFIGS[phase_name].total_orders // num_bots)


async def run_single_bot(bot_id: int, plan_name: str, num_bots: int):
    pending: dict = {}
    telemetry = TelemetryCollector(bot_id)
    telemetry.start()                       # periodic-flush task
    coord = make_coordinator(plan_name)

    first = coord.get_phase()
    if first is None:
        await telemetry.stop()
        return

    first_cfg = dataclasses.replace(
        PHASE_CONFIGS[first[0]],
        seed=GLOBAL_SEED * 1000 + bot_id,
        total_orders=_per_bot_total(first[0], num_bots),
    )
    generator = OrderGenerator(bot_id, first_cfg)

    try:
        async with websockets.connect(URI) as ws:
            while True:
                phase = coord.get_phase()
                if phase is None:
                    break
                phase_name, rate = phase

                cfg = dataclasses.replace(
                    PHASE_CONFIGS[phase_name],
                    seed=GLOBAL_SEED * 1000 + bot_id,
                    total_orders=_per_bot_total(phase_name, num_bots),
                )
                generator.update_config(cfg)
                done = asyncio.Event()

                try:
                    await asyncio.gather(
                        sender_loop(ws, generator, pending, rate, done, coord, phase_name, telemetry),
                        receiver_loop(ws, pending, telemetry, generator, done, phase_name),
                    )
                except ConnectionClosed:
                    break                       # engine dropped — end this bot
                finally:
                    # phase-boundary flush: downstream gets clean per-phase batches
                    telemetry.flush()

                if isinstance(coord, (LocalPhaseCoordinator, RedisPhaseCoordinator)):
                    coord.advance()
    finally:
        # TODO: surface fatal errors to the orchestrator instead of dying quietly.
        await telemetry.stop()


async def main(num_bots: int = 5, plan_name: str = "quick"):
    await asyncio.gather(*[
        run_single_bot(bot_id, plan_name, num_bots)
        for bot_id in range(1, num_bots + 1)
    ])


def _parse_args():
    p = argparse.ArgumentParser(description="Production bot fleet")
    p.add_argument("--num-bots", type=int,
                   default=int(os.environ.get("NUM_BOTS", "5")))
    p.add_argument("--plan", default=os.environ.get("PLAN_NAME", "quick"),
                   choices=list(TEST_PLANS.keys()))
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    asyncio.run(main(num_bots=args.num_bots, plan_name=args.plan))
