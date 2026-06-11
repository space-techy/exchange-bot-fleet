"""
LIVE visualizer / debugger.

Runs the bot fleet against the real matching engine over WebSocket, reconstructs
the engine's order book from its RESPONSES, and after each phase produces a
TWO-PANEL matplotlib chart + text summary:

  TOP    — Submitted: what the fleet TRIED to put on the book (no matching).
  BOTTOM — Engine truth: what's ACTUALLY on the book after matching.

The gap between the two visualises the matching activity for that phase:
how much of the submitted depth got consumed, where it got consumed.

Engine must be at ws://localhost:3001/ws.
Outputs: results/live_book_shape_<phase>.png + stdout summary
"""

import asyncio
import dataclasses
import json
import time
from collections import Counter

import websockets
from websockets.exceptions import ConnectionClosed

from botfleet.core.generator import OrderGenerator
from botfleet.core.plans import PHASE_CONFIGS, TEST_PLANS
from botfleet.visualization.books import FleetBook, SubmittedBook
from botfleet.visualization.plots import plot_phase_compare
from botfleet.visualization.summary import print_summary


URI = "ws://localhost:3001/ws"
GLOBAL_SEED = 42
NUM_BOTS = 5
PLAN_NAME = "standard"

# This debug tool is effectively a single pod: every bot trades ONE symbol, so
# the reconstructed book plot reflects exactly that one book.
SYMBOL = 1
ORDER_DIVISOR = 1        # divide every phase's total_orders (volume dial)
POD_ID = 0               # offsets client_id/order_id; keep 0 for a standalone run


# ─────────────────────────── sender / receiver ───────────────────────────────

_TERMINAL_TYPES = ("order_filled", "order_cancelled", "order_rejected")


async def sender_loop(ws, generator: OrderGenerator, pending: dict,
                      rate: int, done: asyncio.Event,
                      bot_id: int, phase: str,
                      telemetry: list, submitted: SubmittedBook,
                      counts: Counter):
    interval = 1.0 / rate
    sent = 0
    next_deadline = time.monotonic()
    try:
        while generator.has_more():
            order = generator.generate_next()
            t_send = time.monotonic_ns()
            oid = order["order_id"]
            pending[oid] = {"t_send_ns": t_send, "action": order["action"], "phase": phase}

            # apply to the shared intent book (single-task at a time, no lock needed)
            submitted.apply_sent(order)
            counts[order["action"]] += 1

            await ws.send(json.dumps(order))
            sent += 1

            telemetry.append({
                "type": "order_sent",
                "bot_id": bot_id,
                "client_id": bot_id,
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
        print(f"[bot {bot_id} {phase}] connection closed during send")
    finally:
        print(f"[bot {bot_id} {phase}] sender done: {sent} orders")
        done.set()


async def receiver_loop(ws, pending: dict, telemetry: list,
                        done: asyncio.Event, bot_id: int, phase: str,
                        book: FleetBook, generator: OrderGenerator):
    while True:
        try:
            timeout = 2.0 if done.is_set() else 15.0
            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
        except asyncio.TimeoutError:
            if done.is_set():
                return
            print(f"[bot {bot_id} {phase}] 15s with no response")
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
            book.handle(item)
            _handle_item(item, t_recv, pending, telemetry, generator, bot_id, phase)


def _handle_item(item, t_recv, pending, telemetry, generator, bot_id, current_phase):
    mtype = item.get("type")

    if mtype == "trade_broadcast":
        return

    oid = _extract_oid(item)
    if oid is None:
        return

    sent = pending.pop(oid, None)

    telemetry.append({
        "type": "order_response",
        "bot_id": bot_id,
        "client_id": bot_id,
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
        for trade in item.get("trades", []):
            for key in ("buyer_order_id", "seller_order_id"):
                other = trade.get(key)
                if other and other != oid:
                    generator.remove_active_order(other)
                    pending.pop(other, None)


def _extract_oid(item):
    if "order_id" in item:
        return item["order_id"]
    orders = item.get("orders") or []
    if orders and isinstance(orders[0], dict):
        return orders[0].get("order_id")
    return None


# ────────────────────────── per-bot driver ───────────────────────────────────

async def run_bot(bot_id, plan, fleet_book, submitted_book,
                  barrier1, barrier2, generators_out, fleet_counts):
    pending: dict = {}
    telemetry: list = []

    first_cfg = dataclasses.replace(
        PHASE_CONFIGS[plan[0][0]],
        seed=GLOBAL_SEED * 1000 + bot_id,
        total_orders=max(1, (PHASE_CONFIGS[plan[0][0]].total_orders // max(1, ORDER_DIVISOR)) // NUM_BOTS),
        symbol=SYMBOL,
    )
    generator = OrderGenerator(bot_id, first_cfg, pod_id=POD_ID)
    generators_out[bot_id] = generator

    try:
        async with websockets.connect(URI) as ws:
            for phase_name, rate in plan:
                cfg = dataclasses.replace(
                    PHASE_CONFIGS[phase_name],
                    seed=GLOBAL_SEED * 1000 + bot_id,
                    total_orders=max(1, (PHASE_CONFIGS[phase_name].total_orders // max(1, ORDER_DIVISOR)) // NUM_BOTS),
                    symbol=SYMBOL,
                )
                generator.update_config(cfg)
                done = asyncio.Event()
                print(f"[bot {bot_id}] phase '{phase_name}' at {rate}/s")
                try:
                    await asyncio.gather(
                        sender_loop(ws, generator, pending, rate, done,
                                    bot_id, phase_name, telemetry, submitted_book,
                                    fleet_counts),
                        receiver_loop(ws, pending, telemetry, done,
                                      bot_id, phase_name, fleet_book, generator),
                    )
                except Exception as e:
                    print(f"[bot {bot_id}] phase '{phase_name}' error: {e!r}")
                    barrier1.abort(); barrier2.abort()
                    raise

                await barrier1.wait()
                if bot_id == 1:
                    try:
                        gens = [generators_out[i] for i in sorted(generators_out)]
                        # summary: lean on what we know — actually-sent counts
                        print_summary(phase_name, fleet_counts, fleet_book, gens)
                        plot_phase_compare(phase_name, fleet_book, submitted_book,
                                           cfg.fair_value, fleet_counts)
                    except Exception as e:
                        print(f"[plot] failed for {phase_name}: {e!r}")
                    finally:
                        fleet_book.reset_phase_trades()
                        fleet_counts.clear()
                await barrier2.wait()
    except Exception as e:
        print(f"[bot {bot_id}] fatal: {e!r}")
    finally:
        print(f"[bot {bot_id}] done. telemetry_events={len(telemetry)} "
              f"pending_left={len(pending)}")


async def main():
    plan = TEST_PLANS[PLAN_NAME]
    fleet_book = FleetBook()
    submitted_book = SubmittedBook()
    fleet_counts: Counter = Counter()
    barrier1 = asyncio.Barrier(NUM_BOTS)
    barrier2 = asyncio.Barrier(NUM_BOTS)
    generators_out: dict[int, OrderGenerator] = {}

    print(f"launching {NUM_BOTS} bots on plan '{PLAN_NAME}' against {URI}")
    try:
        await asyncio.gather(*[
            run_bot(bot_id, plan, fleet_book, submitted_book,
                    barrier1, barrier2, generators_out, fleet_counts)
            for bot_id in range(1, NUM_BOTS + 1)
        ])
    except Exception as e:
        print(f"run aborted: {e!r}")


if __name__ == "__main__":
    asyncio.run(main())
