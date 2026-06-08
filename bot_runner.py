"""
Async WebSocket bot fleet. Connects N virtual bots to a matching engine,
runs through a test plan, and writes per-bot telemetry JSONL.
"""

import asyncio
import dataclasses
import json
import time

import websockets
from websockets.exceptions import ConnectionClosed

from configs import PHASE_CONFIGS, TEST_PLANS
from order_generator import OrderGenerator


URI = "ws://localhost:3001/ws"
GLOBAL_SEED = 42


class TelemetryCollector:
    """Buffers per-order events, flushes JSONL to disk."""

    def __init__(self, bot_id: int):
        self.path = f"telemetry_bot_{bot_id}.jsonl"
        self.f = open(self.path, "w")
        self.buf: list[dict] = []

    def record(self, ev: dict):
        self.buf.append(ev)
        if len(self.buf) >= 100:
            self.flush()

    def flush(self):
        for ev in self.buf:
            self.f.write(json.dumps(ev) + "\n")
        self.buf.clear()

    def close(self):
        self.flush()
        self.f.close()


async def sender_loop(ws, generator: OrderGenerator, pending: dict,
                      rate: int, done: asyncio.Event,
                      bot_id: int, phase: str):
    """Generate orders at `rate`/sec. Track each in `pending` BEFORE sending."""
    interval = 1.0 / rate
    sent = 0
    phase_start = time.monotonic()
    next_deadline = time.monotonic()

    try:
        while generator.has_more():
            order = generator.generate_next()
            t_send = time.monotonic_ns()
            pending[order["order_id"]] = {
                "t_send_ns": t_send,
                "order": order,
                "phase": phase,
            }
            await ws.send(json.dumps(order))
            sent += 1

            if sent % 500 == 0:
                elapsed = time.monotonic() - phase_start
                actual_rate = sent / elapsed if elapsed > 0 else 0
                print(f"[bot {bot_id} {phase}] sent={sent} rate={actual_rate:.0f}/s "
                      f"pending={len(pending)} book={generator.get_active_order_count()}")

            # deadline-based pacing — doesn't drift when ws.send is slow
            next_deadline += interval
            sleep_for = next_deadline - time.monotonic()
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
    except ConnectionClosed:
        print(f"[bot {bot_id} {phase}] connection closed during send (sent={sent})")
    finally:
        elapsed = time.monotonic() - phase_start
        rate_actual = sent / elapsed if elapsed > 0 else 0
        print(f"[bot {bot_id} {phase}] phase done: {sent} orders in {elapsed:.1f}s "
              f"({rate_actual:.0f}/s)")
        done.set()


async def receiver_loop(ws, pending: dict, telemetry: TelemetryCollector,
                        done: asyncio.Event, bot_id: int, phase: str):
    """Match responses to pending orders, record latency."""
    while True:
        try:
            timeout = 2.0 if done.is_set() else 15.0
            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
        except asyncio.TimeoutError:
            if done.is_set():
                return                   # sender finished, drain timed out
            print(f"[bot {bot_id} {phase}] 15s with no response — engine slow?")
            continue                     # engine just slow, keep waiting
        except ConnectionClosed:
            print(f"[bot {bot_id} {phase}] connection closed during recv")
            return

        t_recv = time.monotonic_ns()
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            print(f"[bot {bot_id} {phase}] bad JSON: {raw[:200]}")
            continue

        items = msg if isinstance(msg, list) else [msg]
        for item in items:
            if not isinstance(item, dict):
                continue
            oid = item.get("order_id")
            if oid is None or oid not in pending:
                continue
            sent = pending.pop(oid)
            telemetry.record({
                "bot_id": bot_id,
                "order_id": oid,
                "action": sent["order"]["action"],
                "phase": sent["phase"],
                "latency_ns": t_recv - sent["t_send_ns"],
                "t_send_ns": sent["t_send_ns"],
                "t_recv_ns": t_recv,
                "response": item,
            })


async def run_single_bot(bot_id: int, plan_name: str):
    plan = TEST_PLANS[plan_name]
    pending: dict = {}
    telemetry = TelemetryCollector(bot_id)

    first_cfg = dataclasses.replace(
        PHASE_CONFIGS[plan[0][0]], seed=GLOBAL_SEED + bot_id * 100
    )
    generator = OrderGenerator(bot_id, first_cfg)

    try:
        async with websockets.connect(URI) as ws:
            for phase_name, rate in plan:
                cfg = dataclasses.replace(
                    PHASE_CONFIGS[phase_name], seed=GLOBAL_SEED + bot_id * 100
                )
                generator.update_config(cfg)
                done = asyncio.Event()
                print(f"[bot {bot_id}] starting phase '{phase_name}' at {rate}/s")
                try:
                    await asyncio.gather(
                        sender_loop(ws, generator, pending, rate, done, bot_id, phase_name),
                        receiver_loop(ws, pending, telemetry, done, bot_id, phase_name),
                    )
                except ConnectionClosed:
                    print(f"[bot {bot_id}] connection closed mid-phase; stopping plan")
                    break
    except Exception as e:
        print(f"[bot {bot_id}] error: {e!r}")
    finally:
        telemetry.close()
        print(f"[bot {bot_id}] telemetry written to {telemetry.path}")


async def main(num_bots: int = 5, plan_name: str = "quick"):
    print(f"launching {num_bots} bots on plan '{plan_name}' against {URI}")
    await asyncio.gather(*[
        run_single_bot(bot_id, plan_name)
        for bot_id in range(1, num_bots + 1)
    ])


# TODO: orjson for faster (de)serialization on the hot path
# TODO: Redis phase-coordination signal (for K8s deployment)
# TODO: Kafka producer in TelemetryCollector.flush


if __name__ == "__main__":
    asyncio.run(main())
