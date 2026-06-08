"""
LIVE visualizer / debugger.

Runs the bot fleet against the real matching engine over WebSocket, reconstructs
the engine's order book from its RESPONSES (not from the generator's view), and
after each phase produces a matplotlib chart + text summary.

Use this to see what the engine actually does, vs. what visualizer.py shows
(which is what the generator THOUGHT the book looks like).

Run: python live_visualizer.py     (engine must be at ws://localhost:3001/ws)
Outputs: live_book_shape_<phase>.png + stdout summary
"""

import asyncio
import dataclasses
import json
import time
from collections import defaultdict, Counter

import websockets
from websockets.exceptions import ConnectionClosed

from configs import PHASE_CONFIGS, TEST_PLANS
from order_generator import OrderGenerator
from visualizer import print_summary, plot_phase


URI = "ws://localhost:3001/ws"
GLOBAL_SEED = 42
NUM_BOTS = 5
PLAN_NAME = "standard"
ORDERS_PER_BOT_PER_PHASE = 2000          # ~10k per phase across the fleet


class FleetBook:
    """
    Engine-truth book, built by feeding every response message back through
    `handle()`. Shape is duck-typed to match visualizer.LocalBook so we can
    reuse its print_summary / plot_phase helpers.
    """

    def __init__(self):
        self.bids: dict[int, int] = defaultdict(int)    # price -> total remaining qty
        self.asks: dict[int, int] = defaultdict(int)
        self.resting: dict[int, dict] = {}              # order_id -> {side, price, qty}
        self.trades: list[tuple[int, int]] = []         # (price, qty) for the CURRENT phase

    def reset_phase_trades(self):
        self.trades = []

    def best_bid(self) -> int | None:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> int | None:
        return min(self.asks) if self.asks else None

    def handle(self, msg: dict):
        """Dispatch one engine message into book mutations."""
        if not isinstance(msg, dict):
            return
        mtype = msg.get("type")

        if mtype == "fill_notification":
            # one of OUR resting orders got hit — engine tells us the new remaining_qty
            self._upsert(
                oid=msg["order_id"],
                side=None,                              # learn from existing record
                price=None,
                remaining_qty=msg.get("remaining_qty", 0),
            )
            return

        if mtype == "trade_broadcast":
            return                                       # would double-count; skip

        if mtype == "order_rejected":
            return

        # all remaining types are responses to our submitted order, with orders[] + trades[]
        if mtype in ("partial_fill", "order_filled"):
            for t in msg.get("trades", []):
                p, q = t.get("price"), t.get("qty")
                if p is not None and q is not None:
                    self.trades.append((p, q))

        for o in msg.get("orders", []):
            oid = o.get("order_id")
            if oid is None:
                continue
            if mtype == "order_cancelled":
                self._remove(oid)
                continue
            self._upsert(
                oid=oid,
                side=o.get("side"),
                price=o.get("price"),
                remaining_qty=o.get("remaining_qty", 0),
            )

    # ---- internal book mutations ----

    def _upsert(self, oid: int, side: str | None, price: int | None, remaining_qty: int):
        prev = self.resting.get(oid)

        if remaining_qty <= 0:
            self._remove(oid)
            return

        # learn side/price from prev when caller didn't pass them (fill_notification)
        if side is None and prev is not None:
            side = prev["side"]
        if price is None and prev is not None:
            price = prev["price"]
        if side is None or price is None:
            return                                       # not enough info to place; ignore

        book = self.bids if side == "buy" else self.asks

        if prev is None:
            # new resting
            book[price] += remaining_qty
            self.resting[oid] = {"side": side, "price": price, "qty": remaining_qty}
        else:
            # adjust by delta at the same price level
            delta = remaining_qty - prev["qty"]
            book[prev["price"]] += delta
            if book[prev["price"]] <= 0:
                del book[prev["price"]]
            prev["qty"] = remaining_qty

    def _remove(self, oid: int):
        prev = self.resting.pop(oid, None)
        if prev is None:
            return
        book = self.bids if prev["side"] == "buy" else self.asks
        book[prev["price"]] -= prev["qty"]
        if book[prev["price"]] <= 0:
            del book[prev["price"]]


# ─────────────────────────── sender / receiver ───────────────────────────────

async def sender_loop(ws, generator: OrderGenerator, pending: dict,
                      rate: int, done: asyncio.Event,
                      bot_id: int, phase: str):
    interval = 1.0 / rate
    sent = 0
    phase_start = time.monotonic()
    next_deadline = time.monotonic()
    try:
        while generator.has_more():
            order = generator.generate_next()
            t_send = time.monotonic_ns()
            pending[order["order_id"]] = {
                "t_send_ns": t_send, "order": order, "phase": phase,
            }
            await ws.send(json.dumps(order))
            sent += 1
            if sent % 500 == 0:
                elapsed = time.monotonic() - phase_start
                rate_actual = sent / elapsed if elapsed > 0 else 0
                print(f"[bot {bot_id} {phase}] sent={sent} rate={rate_actual:.0f}/s "
                      f"pending={len(pending)} gen_book={generator.get_active_order_count()}")
            next_deadline += interval
            sleep_for = next_deadline - time.monotonic()
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
    except ConnectionClosed:
        print(f"[bot {bot_id} {phase}] connection closed during send")
    finally:
        elapsed = time.monotonic() - phase_start
        rate_actual = sent / elapsed if elapsed > 0 else 0
        print(f"[bot {bot_id} {phase}] sender done: {sent} in {elapsed:.1f}s "
              f"({rate_actual:.0f}/s)")
        done.set()


async def receiver_loop(ws, pending: dict, telemetry: list,
                        done: asyncio.Event,
                        bot_id: int, phase: str,
                        book: FleetBook):
    while True:
        try:
            timeout = 2.0 if done.is_set() else 15.0
            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
        except asyncio.TimeoutError:
            if done.is_set():
                return
            print(f"[bot {bot_id} {phase}] 15s with no response — engine slow?")
            continue
        except ConnectionClosed:
            print(f"[bot {bot_id} {phase}] connection closed during recv")
            return

        t_recv = time.monotonic_ns()
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            print(f"[bot {bot_id} {phase}] bad JSON")
            continue

        items = msg if isinstance(msg, list) else [msg]
        for item in items:
            if not isinstance(item, dict):
                continue

            # feed every message into the shared engine-truth book
            book.handle(item)

            # also do latency tracking for orders we sent (best-effort match on orders[0])
            oid = None
            if "order_id" in item:
                oid = item["order_id"]
            else:
                orders = item.get("orders") or []
                if orders and isinstance(orders[0], dict):
                    oid = orders[0].get("order_id")
            if oid is not None and oid in pending:
                sent = pending.pop(oid)
                telemetry.append({
                    "bot_id": bot_id,
                    "order_id": oid,
                    "action": sent["order"]["action"],
                    "phase": sent["phase"],
                    "latency_ns": t_recv - sent["t_send_ns"],
                    "t_send_ns": sent["t_send_ns"],
                    "t_recv_ns": t_recv,
                    "msg_type": item.get("type"),
                })


# ────────────────────────── per-bot driver ───────────────────────────────────

async def run_bot(bot_id: int, plan: list, book: FleetBook,
                  barrier1: asyncio.Barrier, barrier2: asyncio.Barrier,
                  generators_out: dict[int, OrderGenerator]):
    pending: dict = {}
    telemetry: list = []

    first_cfg = dataclasses.replace(
        PHASE_CONFIGS[plan[0][0]],
        seed=GLOBAL_SEED + bot_id * 100,
        total_orders=ORDERS_PER_BOT_PER_PHASE,
    )
    generator = OrderGenerator(bot_id, first_cfg)
    generators_out[bot_id] = generator

    try:
        async with websockets.connect(URI) as ws:
            for phase_name, rate in plan:
                cfg = dataclasses.replace(
                    PHASE_CONFIGS[phase_name],
                    seed=GLOBAL_SEED + bot_id * 100,
                    total_orders=ORDERS_PER_BOT_PER_PHASE,
                )
                generator.update_config(cfg)
                done = asyncio.Event()
                print(f"[bot {bot_id}] phase '{phase_name}' at {rate}/s")
                try:
                    await asyncio.gather(
                        sender_loop(ws, generator, pending, rate, done, bot_id, phase_name),
                        receiver_loop(ws, pending, telemetry, done, bot_id, phase_name, book),
                    )
                except Exception as e:
                    print(f"[bot {bot_id}] phase '{phase_name}' error: {e!r}")
                    barrier1.abort(); barrier2.abort()
                    raise

                # sync barrier 1: all bots done with phase
                await barrier1.wait()
                # bot 1 plots + resets phase trades
                if bot_id == 1:
                    try:
                        gens = [generators_out[i] for i in sorted(generators_out)]
                        counts = _approx_counts_from_book(book)
                        print_summary(phase_name, counts, book, gens)
                        plot_phase(phase_name, book, cfg.fair_value, gens, counts)
                        # rename to live_book_shape_*.png
                        _rename_to_live(phase_name)
                    except Exception as e:
                        print(f"[plot] failed for {phase_name}: {e!r}")
                    finally:
                        book.reset_phase_trades()
                # sync barrier 2: plot done, next phase starts
                await barrier2.wait()
    except Exception as e:
        print(f"[bot {bot_id}] fatal: {e!r}")


def _approx_counts_from_book(book: FleetBook) -> Counter:
    """
    We don't track per-op counts in live mode (the engine's view is post-match).
    Surface what we know: resting count + trades count.
    """
    c: Counter = Counter()
    c["resting"] = len(book.resting)
    c["trades"] = len(book.trades)
    return c


def _rename_to_live(phase: str):
    import os
    src = f"book_shape_{phase}.png"
    dst = f"live_book_shape_{phase}.png"
    if os.path.exists(src):
        if os.path.exists(dst):
            os.remove(dst)
        os.rename(src, dst)


async def main():
    plan = TEST_PLANS[PLAN_NAME]
    book = FleetBook()
    barrier1 = asyncio.Barrier(NUM_BOTS)
    barrier2 = asyncio.Barrier(NUM_BOTS)
    generators_out: dict[int, OrderGenerator] = {}

    print(f"launching {NUM_BOTS} bots on plan '{PLAN_NAME}' against {URI}")
    try:
        await asyncio.gather(*[
            run_bot(bot_id, plan, book, barrier1, barrier2, generators_out)
            for bot_id in range(1, NUM_BOTS + 1)
        ])
    except Exception as e:
        print(f"run aborted: {e!r}")


if __name__ == "__main__":
    asyncio.run(main())
