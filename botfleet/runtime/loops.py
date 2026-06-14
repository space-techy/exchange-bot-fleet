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
                      stop_event: asyncio.Event | None = None,
                      sent_counter=None):
    """Generate at `rate`/sec. Tracks each in `pending` before sending, and
    emits an `order_sent` telemetry event so the validator can replay.
    Polls the coordinator every 100 orders for an out-of-band phase change.
    Stops promptly (after the current order) when `stop_event` is set.
    Increments `sent_counter` (shared across the pod's bots) for live progress.

    Coordinated-omission correction: we stamp each order with the time it was
    SUPPOSED to be dispatched under the fixed offered rate —
    `t_intended = start + i/rate` — so the aggregator can measure latency from
    when load *should* have arrived, not from when a back-pressured sender finally
    managed to send it. Without this, a stalled engine hides its own slowness
    (the classic coordinated-omission blind spot)."""
    interval = 1.0 / rate
    interval_ns = 1_000_000_000.0 / rate
    sent = 0
    start_ns = time.monotonic_ns()
    next_deadline = start_ns / 1e9  # seconds, same monotonic clock as the pacer
    try:
        while generator.has_more():
            # graceful shutdown: stop sending, let the receiver drain + flush
            if stop_event is not None and stop_event.is_set():
                break

            # remote phase change check (cheap; coord is in-memory by default)
            if sent and sent % 100 == 0 and coord.should_switch(phase):
                break

            order = generator.generate_next()
            # When this order *should* have left under the fixed rate (the CO
            # reference), vs. when it actually does (t_send). i == `sent` here.
            t_intended = start_ns + int(sent * interval_ns)
            t_send = time.monotonic_ns()
            # Every request — new_order, cancel AND modify — carries its own
            # fresh client_order_id, which the engine echoes on the direct
            # response. Pairing is therefore an exact one-shot lookup; a plain
            # dict entry per request can never collide or be overwritten.
            client_order_id = order["client_order_id"]
            pending[client_order_id] = {
                "t_send_ns": t_send,
                "t_intended_ns": t_intended,
                "action": order["action"],
                "target_client_order_id": order.get("target_client_order_id"),
                "phase": phase,
            }
            await ws.send(json.dumps(order))
            sent += 1
            if sent_counter is not None:
                sent_counter.n += 1

            telemetry.record({
                "type": "order_sent",
                "client_id": order.get("client_id"),
                "client_order_id": client_order_id,
                "target_client_order_id": order.get("target_client_order_id"),
                "action": order["action"],
                "side": order.get("side"),
                "price": order.get("price"),
                "qty": order.get("qty"),
                "symbol": order.get("symbol"),
                "t_send_ns": t_send,
                "t_intended_ns": t_intended,
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
