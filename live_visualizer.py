"""
LIVE visualizer / debugger.

Runs the bot fleet against the real matching engine over WebSocket, reconstructs
the engine's order book from its RESPONSES, and after each phase produces a
TWO-PANEL matplotlib chart + text summary:

  TOP    — Submitted: what the fleet TRIED to put on the book (no matching).
  BOTTOM — Engine truth: what's ACTUALLY on the book after matching.

The gap between the two visualises the matching activity for that phase:
how much of the submitted depth got consumed, where it got consumed.

Run: python live_visualizer.py     (engine must be at ws://localhost:3001/ws)
Outputs: live_book_shape_<phase>.png + stdout summary
"""

import asyncio
import dataclasses
import json
import os
import time
from collections import defaultdict, Counter

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import websockets
from websockets.exceptions import ConnectionClosed

from configs import PHASE_CONFIGS, TEST_PLANS
from order_generator import OrderGenerator
from visualizer import print_summary


URI = "ws://localhost:3001/ws"
GLOBAL_SEED = 42
NUM_BOTS = 5
PLAN_NAME = "standard"


# ─────────────────────────── books ──────────────────────────────────────────

class FleetBook:
    """Engine truth, built from every response message via handle()."""

    def __init__(self):
        self.bids: dict[int, int] = defaultdict(int)
        self.asks: dict[int, int] = defaultdict(int)
        self.resting: dict[int, dict] = {}
        self.trades: list[tuple[int, int]] = []          # per-phase

    def reset_phase_trades(self):
        self.trades = []

    def best_bid(self) -> int | None:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> int | None:
        return min(self.asks) if self.asks else None

    def handle(self, msg: dict):
        """Engine truth update. Same envelope for everything — aggressor responses
        AND resting-side fill events. We just trust orders[].remaining_qty."""
        if not isinstance(msg, dict):
            return
        mtype = msg.get("type")

        if mtype in ("trade_broadcast", "order_rejected"):
            return

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
                oid=oid, side=o.get("side"), price=o.get("price"),
                remaining_qty=o.get("remaining_qty", 0),
            )

    def _upsert(self, oid, side, price, remaining_qty):
        prev = self.resting.get(oid)
        if remaining_qty <= 0:
            self._remove(oid)
            return
        if side is None and prev is not None:
            side = prev["side"]
        if price is None and prev is not None:
            price = prev["price"]
        if side is None or price is None:
            return
        book = self.bids if side == "buy" else self.asks
        if prev is None:
            book[price] += remaining_qty
            self.resting[oid] = {"side": side, "price": price, "qty": remaining_qty}
        else:
            delta = remaining_qty - prev["qty"]
            book[prev["price"]] += delta
            if book[prev["price"]] <= 0:
                del book[prev["price"]]
            prev["qty"] = remaining_qty

    def _remove(self, oid):
        prev = self.resting.pop(oid, None)
        if prev is None:
            return
        book = self.bids if prev["side"] == "buy" else self.asks
        book[prev["price"]] -= prev["qty"]
        if book[prev["price"]] <= 0:
            del book[prev["price"]]


class SubmittedBook:
    """
    Fleet intent. Built from the sender side. Every new_order adds to the book,
    every cancel removes, every modify adjusts. NO MATCHING is ever applied —
    so aggressive orders just sit at their crossing price.

    Comparing this to FleetBook reveals what the matching engine consumed.
    """

    def __init__(self):
        self.bids: dict[int, int] = defaultdict(int)
        self.asks: dict[int, int] = defaultdict(int)
        self.resting: dict[int, dict] = {}
        self.trades: list = []          # always empty, kept for duck-type compat

    def reset_phase_trades(self):
        pass

    def best_bid(self) -> int | None:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> int | None:
        return min(self.asks) if self.asks else None

    def apply_sent(self, order: dict):
        action = order["action"]
        oid = order["order_id"]
        if action == "new_order":
            side, price, qty = order["side"], order["price"], order["qty"]
            book = self.bids if side == "buy" else self.asks
            book[price] += qty
            self.resting[oid] = {"side": side, "price": price, "qty": qty}
        elif action == "cancel":
            prev = self.resting.pop(oid, None)
            if prev is None:
                return
            book = self.bids if prev["side"] == "buy" else self.asks
            book[prev["price"]] -= prev["qty"]
            if book[prev["price"]] <= 0:
                del book[prev["price"]]
        elif action == "modify":
            prev = self.resting.get(oid)
            if prev is None:
                return
            new_qty = order["qty"]
            delta = new_qty - prev["qty"]
            book = self.bids if prev["side"] == "buy" else self.asks
            book[prev["price"]] += delta
            prev["qty"] = new_qty
            if book[prev["price"]] <= 0:
                del book[prev["price"]]
                self.resting.pop(oid, None)


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


# ────────────────────────── plotting ─────────────────────────────────────────

def _plot_book_on(ax, book, fair_value, title, show_trades=False):
    if book.bids:
        bx = sorted(book.bids)
        by = [book.bids[p] for p in bx]
        ax.bar(bx, by, color="green", alpha=0.7, label=f"bids ({sum(by)} qty)", width=1.0)
    if book.asks:
        ax_x = sorted(book.asks)
        ay = [book.asks[p] for p in ax_x]
        ax.bar(ax_x, ay, color="red", alpha=0.7, label=f"asks ({sum(ay)} qty)", width=1.0)
    if show_trades and book.trades:
        agg: dict[int, int] = defaultdict(int)
        for p, q in book.trades:
            agg[p] += q
        tx = list(agg.keys())
        ty = [0] * len(tx)
        sizes = [max(20, agg[p]) for p in tx]
        ax.scatter(tx, ty, s=sizes, color="blue", alpha=0.4,
                   label=f"trades ({len(book.trades)})", zorder=5)
    ax.axvline(fair_value, color="black", linestyle="--", linewidth=1,
               label=f"fair_value={fair_value}")
    ax.set_title(title)
    ax.set_ylabel("qty at price")
    ax.legend(loc="upper right", fontsize=8)
    ax.grid(True, alpha=0.3)


def plot_phase_compare(phase, fleet_book: FleetBook, submitted_book: SubmittedBook,
                       fair_value: int, counts: Counter):
    fig, (ax_top, ax_bot) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

    total_sent = sum(counts.values())
    new = counts.get("new_order", 0)
    can = counts.get("cancel", 0)
    mod = counts.get("modify", 0)
    _plot_book_on(
        ax_top, submitted_book, fair_value,
        f"SUBMITTED (no matching applied)  |  sent={total_sent}  "
        f"new={new}  cancel={can}  modify={mod}  resting={len(submitted_book.resting)}",
    )
    _plot_book_on(
        ax_bot, fleet_book, fair_value,
        f"ENGINE TRUTH (after matching)  |  resting={len(fleet_book.resting)}  "
        f"trades_this_phase={len(fleet_book.trades)}",
        show_trades=True,
    )
    ax_bot.set_xlabel("price")
    fig.suptitle(f"Phase: {phase}", fontsize=14, fontweight="bold")

    os.makedirs("results", exist_ok=True)
    out = os.path.join("results", f"live_book_shape_{phase}.png")
    plt.tight_layout()
    plt.savefig(out, dpi=110)
    plt.close(fig)
    print(f"   -> wrote {out}")


# ────────────────────────── per-bot driver ───────────────────────────────────

async def run_bot(bot_id, plan, fleet_book, submitted_book,
                  barrier1, barrier2, generators_out, fleet_counts):
    pending: dict = {}
    telemetry: list = []

    first_cfg = dataclasses.replace(
        PHASE_CONFIGS[plan[0][0]],
        seed=GLOBAL_SEED * 1000 + bot_id,
        total_orders=max(1, PHASE_CONFIGS[plan[0][0]].total_orders // NUM_BOTS),
    )
    generator = OrderGenerator(bot_id, first_cfg)
    generators_out[bot_id] = generator

    try:
        async with websockets.connect(URI) as ws:
            for phase_name, rate in plan:
                cfg = dataclasses.replace(
                    PHASE_CONFIGS[phase_name],
                    seed=GLOBAL_SEED * 1000 + bot_id,
                    total_orders=max(1, PHASE_CONFIGS[phase_name].total_orders // NUM_BOTS),
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
