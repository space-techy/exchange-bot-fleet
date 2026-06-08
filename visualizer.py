"""
Offline visualizer / debugger for the bot fleet.

Runs the "standard" test plan with 5 virtual bots, maintains a local order book
(no WebSocket, no network), and produces a per-phase matplotlib chart +
text summary so we can sanity-check that the generator produces realistic book
shapes.

Run: python visualizer.py
Outputs: book_shape_<phase>.png  +  stdout summary
"""

from collections import defaultdict, Counter
from dataclasses import replace

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from configs import PHASE_CONFIGS, TEST_PLANS
from order_generator import OrderGenerator


NUM_BOTS = 5
# Target ~10k orders TOTAL across the whole simulation (6 phases × 5 bots × ~340 ≈ 10k).
ORDERS_PER_BOT_PER_PHASE = 12000


class LocalBook:
    """Simple in-process order book. Tracks per-price quantities + per-order state."""

    def __init__(self):
        self.bids: dict[int, int] = defaultdict(int)   # price -> total_qty
        self.asks: dict[int, int] = defaultdict(int)
        self.resting: dict[int, dict] = {}              # order_id -> {side, price, qty}
        self.trades: list[tuple[int, int]] = []         # (price, qty) for the current phase

    def reset_phase_trades(self):
        self.trades = []

    def best_bid(self) -> int | None:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> int | None:
        return min(self.asks) if self.asks else None

    def apply(self, order: dict) -> str:
        """Apply one generator order. Returns op category string."""
        action = order["action"]
        if action == "new_order":
            return self._apply_new(order)
        if action == "cancel":
            self._apply_cancel(order)
            return "cancel"
        if action == "modify":
            self._apply_modify(order)
            return "modify"
        return "unknown"

    def _apply_new(self, order: dict) -> str:
        side, price, qty, oid = order["side"], order["price"], order["qty"], order["order_id"]

        crosses = (
            side == "buy"  and self.best_ask() is not None and price >= self.best_ask()
        ) or (
            side == "sell" and self.best_bid() is not None and price <= self.best_bid()
        )

        if not crosses:
            self._rest(oid, side, price, qty)
            return "passive"

        # aggressive — walk the opposite side
        remaining = qty
        if side == "buy":
            # match against asks at price <= our price, ascending
            while remaining > 0 and self.asks:
                best = min(self.asks)
                if best > price:
                    break
                level_qty = self.asks[best]
                fill = min(level_qty, remaining)
                self.trades.append((best, fill))
                remaining -= fill
                if fill >= level_qty:
                    del self.asks[best]
                else:
                    self.asks[best] = level_qty - fill
        else:
            while remaining > 0 and self.bids:
                best = max(self.bids)
                if best < price:
                    break
                level_qty = self.bids[best]
                fill = min(level_qty, remaining)
                self.trades.append((best, fill))
                remaining -= fill
                if fill >= level_qty:
                    del self.bids[best]
                else:
                    self.bids[best] = level_qty - fill

        # any leftover rests at limit price
        if remaining > 0:
            self._rest(oid, side, price, remaining)
        return "aggressive"

    def _rest(self, oid: int, side: str, price: int, qty: int):
        self.resting[oid] = {"side": side, "price": price, "qty": qty}
        if side == "buy":
            self.bids[price] += qty
        else:
            self.asks[price] += qty

    def _apply_cancel(self, order: dict):
        oid = order["order_id"]
        rec = self.resting.pop(oid, None)
        if rec is None:
            return  # already fully filled / never rested
        book = self.bids if rec["side"] == "buy" else self.asks
        book[rec["price"]] -= rec["qty"]
        if book[rec["price"]] <= 0:
            del book[rec["price"]]

    def _apply_modify(self, order: dict):
        # generator only ever lowers qty
        oid, new_qty = order["order_id"], order["qty"]
        rec = self.resting.get(oid)
        if rec is None:
            return
        delta = new_qty - rec["qty"]   # negative
        rec["qty"] = new_qty
        book = self.bids if rec["side"] == "buy" else self.asks
        book[rec["price"]] += delta
        if book[rec["price"]] <= 0:
            del book[rec["price"]]
            self.resting.pop(oid, None)


def print_summary(phase: str, counts: Counter, book: LocalBook, generators: list[OrderGenerator]):
    total = sum(counts.values())
    print(f"\n== Phase: {phase}  ({total} orders generated) ==")
    for op in ("passive", "aggressive", "cancel", "modify", "unknown"):
        if counts[op]:
            print(f"   {op:11s} {counts[op]:6d}  ({counts[op]/total*100:5.1f}%)")

    print(f"   bid levels: {len(book.bids):5d}  ask levels: {len(book.asks):5d}")
    if book.bids:
        print(f"   bid range:  {min(book.bids)} .. {max(book.bids)}   total qty: {sum(book.bids.values())}")
    if book.asks:
        print(f"   ask range:  {min(book.asks)} .. {max(book.asks)}   total qty: {sum(book.asks.values())}")
    bb, ba = book.best_bid(), book.best_ask()
    if bb is not None and ba is not None:
        print(f"   best bid:   {bb}   best ask: {ba}   spread: {ba - bb}")

    if book.trades:
        tq = sum(q for _, q in book.trades)
        vwap = sum(p * q for p, q in book.trades) / tq if tq else 0
        print(f"   trades:     {len(book.trades)}   filled qty: {tq}   vwap: {vwap:.2f}")

    gen_active = sum(len(g.active_orders) for g in generators)
    print(f"   generator active_orders total: {gen_active}   book.resting: {len(book.resting)}   "
          f"mismatch: {gen_active - len(book.resting)}")


def plot_phase(phase: str, book: LocalBook, fair_value: int,
               generators: list[OrderGenerator], counts: Counter):
    fig, ax = plt.subplots(figsize=(14, 6))

    if book.bids:
        bx = sorted(book.bids)
        by = [book.bids[p] for p in bx]
        ax.bar(bx, by, color="green", alpha=0.7, label=f"bids ({sum(by)} qty)", width=1.0)

    if book.asks:
        ax_x = sorted(book.asks)
        ay = [book.asks[p] for p in ax_x]
        ax.bar(ax_x, ay, color="red", alpha=0.7, label=f"asks ({sum(ay)} qty)", width=1.0)

    # trade overlay — scatter at trade prices, sized by filled qty
    if book.trades:
        # aggregate by price
        agg: dict[int, int] = defaultdict(int)
        for p, q in book.trades:
            agg[p] += q
        tx = list(agg.keys())
        ty = [0] * len(tx)
        sizes = [max(20, agg[p]) for p in tx]
        ax.scatter(tx, ty, s=sizes, color="blue", alpha=0.4, label=f"trades ({len(book.trades)})",
                   zorder=5)

    ax.axvline(fair_value, color="black", linestyle="--", linewidth=1, label=f"fair_value={fair_value}")

    total = sum(counts.values())
    gen_active = sum(len(g.active_orders) for g in generators)
    ax.set_title(
        f"Phase: {phase}  |  Orders generated: {total}  |  "
        f"Active(gen): {gen_active}  |  Book resting: {len(book.resting)}"
    )
    ax.set_xlabel("price")
    ax.set_ylabel("total qty at price level")
    ax.legend(loc="upper right")
    ax.grid(True, alpha=0.3)

    out = f"book_shape_{phase}.png"
    plt.tight_layout()
    plt.savefig(out, dpi=110)
    plt.close(fig)
    print(f"   -> wrote {out}")


def main():
    plan = TEST_PLANS["standard"]

    # generators created ONCE per bot — active_orders carries across phases
    first_cfg = PHASE_CONFIGS[plan[0][0]]
    generators = [OrderGenerator(bot_id, first_cfg) for bot_id in range(1, NUM_BOTS + 1)]

    book = LocalBook()

    for phase_name, _rate in plan:
        cfg = replace(PHASE_CONFIGS[phase_name], total_orders=ORDERS_PER_BOT_PER_PHASE)
        for g in generators:
            g.update_config(cfg)

        book.reset_phase_trades()
        counts: Counter = Counter()

        # round-robin so all 5 bots progress through the phase together
        while any(g.has_more() for g in generators):
            for g in generators:
                if not g.has_more():
                    continue
                order = g.generate_next()
                counts[book.apply(order)] += 1

        print_summary(phase_name, counts, book, generators)
        plot_phase(phase_name, book, cfg.fair_value, generators, counts)


if __name__ == "__main__":
    main()
