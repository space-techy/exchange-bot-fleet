"""The async hot loops: one sends orders at a paced rate, one drains responses."""

import asyncio
import json
import time

from websockets.exceptions import ConnectionClosed

from botfleet.core.generator import OrderGenerator
from botfleet.runtime.coordination import PhaseCoordinator
from botfleet.runtime.protocol import handle_item
from botfleet.runtime.telemetry import TelemetryCollector


async def sender_loop(ws, generator: OrderGenerator, pending: dict,
                      rate: int, done: asyncio.Event,
                      coord: PhaseCoordinator, phase: str,
                      telemetry: "TelemetryCollector",
                      stop_event: asyncio.Event | None = None):
    """Generate at `rate`/sec. Tracks each in `pending` before sending, and
    emits an `order_sent` telemetry event so the validator can replay.
    Polls the coordinator every 100 orders for an out-of-band phase change.
    Stops promptly (after the current order) when `stop_event` is set."""
    interval = 1.0 / rate
    sent = 0
    next_deadline = time.monotonic()
    try:
        while generator.has_more():
            # graceful shutdown: stop sending, let the receiver drain + flush
            if stop_event is not None and stop_event.is_set():
                break

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
                "client_id": order.get("client_id"),
                "order_id": oid,
                "action": order["action"],
                "side": order.get("side"),
                "price": order.get("price"),
                "qty": order.get("qty"),
                "symbol": order.get("symbol"),
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
                        generator: OrderGenerator, done: asyncio.Event):
    """Record latency for the engine's replies to our own requests, sync the
    generator's active_orders, and discard unsolicited resting-side fills."""
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
            handle_item(item, t_recv, pending, telemetry, generator)
