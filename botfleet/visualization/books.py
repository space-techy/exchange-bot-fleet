"""Order-book reconstructions used by the visualizers.

  LocalBook      — full in-process matching sim (offline visualizer).
  FleetBook      — engine truth, rebuilt from the engine's responses (live).
  SubmittedBook  — fleet intent, built from the sender side with NO matching (live).
"""

from collections import defaultdict


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
