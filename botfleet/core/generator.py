"""OrderGenerator — the deterministic order black box (one symbol per bot)."""

import random

from botfleet.core.config import GeneratorConfig

# Per-pod namespace for bot indices. Each pod owns a block of this many bot ids,
# so client_id / order_id never collide across pods. Far larger than any real
# per-pod bot count.
MAX_BOTS_PER_POD = 100


class OrderGenerator:
    """
    The blackbox. Takes a Generator config, generates orders one at a time.
    Deterministic: same config + same seed = same orders always.
    """

    def __init__(self, bot_id : int, config: GeneratorConfig, pod_id: int = 0):

        self.bot_id = bot_id
        self.pod_id = pod_id
        self.config = config

        # Globally-unique bot index across pods. pod_id carves out a 100k-bot
        # namespace each, so (pod, bot) never collide on client_id or order_id.
        # pod_id=0 reproduces the old single-pod ids (client_id == bot_id).
        global_bot = pod_id * MAX_BOTS_PER_POD + bot_id

        # Deterministic per-(pod,bot) seed — folds global_bot so two pods don't
        # emit byte-identical streams.
        self.rng = random.Random(config.seed * 1000 + global_bot)

        # one mean-reverting price walk for this bot's single symbol
        self.ref_price = config.start_price

        # Order Tracking — client_id is the global index.
        self.client_id = global_bot

        # client_order_id: a fresh ticket for EVERY request (new_order, cancel
        # AND modify each get their own). The engine echoes it on the direct
        # response (and omits it on unsolicited notices), so response pairing
        # is an exact lookup — no inference. Each (pod, bot) owns a disjoint
        # 100M-wide range, so ids never collide or repeat across the fleet.
        # The engine names book orders with its own internal order_id; we
        # reference an order by the client_order_id of the new_order that
        # created it (target_client_order_id on cancel/modify).
        self.next_client_order_id = global_bot * 100_000_000

        # client_order_id of the creating new_order → {side, price, qty}
        self.active_orders : dict[ int, dict] = {}

        # Counters
        self.orders_generated = 0


    def has_more(self) -> bool:
        return self.orders_generated < self.config.total_orders

    def update_config(self, config: GeneratorConfig):
        self.orders_generated = 0
        self.config = config

        # self.rng = random.Random(config.seed * 1000 + self.bot_id)
        # should we change our seed or keep it same with previous one for each config change?

    def get_active_order_count(self) -> int:
        return len(self.active_orders)

    def remove_active_order(self, client_order_id: int) -> bool:
        """Drop an order the engine told us is gone (fully filled / cancelled / rejected),
        identified by the client_order_id of the new_order that created it.
        Keeps the generator's view in sync so future cancel/modify ops target live orders only."""
        return self.active_orders.pop(client_order_id, None) is not None


    def generate_next(self) -> dict:
        self._step_price()
        op_type = self._pick_operation()
        order = {}
        if(op_type == "passive_new"):
            order = self._make_passive_order()
        elif(op_type == "aggressive_new"):
            order = self._make_aggressive_order()
        elif(op_type == "cancel"):
            order = self._make_cancel()
        else:
            order = self._make_modify()

        self.orders_generated += 1
        return order

    def _step_price(self):
        """Mean-reverting random walk for this bot's single symbol."""
        config = self.config
        price_pull = config.mean_reversion * (config.fair_value - self.ref_price)
        noise = self.rng.gauss(0, config.volatility)
        self.ref_price += round(price_pull + noise)
        self.ref_price = max(1, self.ref_price)

    def _pick_operation(self) -> str:
        """Weighted random choice of operation type."""
        config = self.config
        roll = self.rng.random()

        # can't cancel if there are no active orders
        if self.get_active_order_count() <= 0:
            if roll < config.aggressive_new_orders:
                return "aggressive_new"
            else:
                return "passive_new"

        cumulative = 0.0
        cumulative += config.passive_new_orders
        if roll < cumulative:
            return "passive_new"

        cumulative += config.aggressive_new_orders
        if roll < cumulative:
            return "aggressive_new"

        cumulative += config.cancel_orders
        if roll < cumulative:
            return "cancel"

        return "modify"

    def _pick_side(self) -> str:
        roll = self.rng.random()

        if roll < self.config.buy_probability:
            return "buy"
        return "sell"

    def _sample_offset(self) -> int:
        """Exponential or Squared Random distribution — most offsets small, few large."""
        if(self.config.price_distribution == "squared"):
            raw = (self.rng.random() ** 2) * self.config.max_price_deviation
            return max(1, round(raw))
        else:
            raw = self.rng.expovariate(self.config.offset_lambda)
            return max(1, round(raw))

    def _sample_qty(self) -> int:
        if(self.config.qty_distribution == "heavy_tail"):
            base = self.config.qty_min
            scale = self.config.qty_scale
            raw = base + self.rng.randint(0, scale) * self.rng.randint(0, scale) * self.rng.randint(0, scale)
            return min(raw, self.config.qty_max)
        else:
            return self.rng.randint(self.config.qty_min, self.config.qty_max)

    def _next_client_order_id(self) -> int:
        client_order_id = self.next_client_order_id
        self.next_client_order_id += 1
        return client_order_id

    def _make_passive_order(self) -> dict:
        """Order placed AWAY from mid — will rest in book."""
        price  = self.ref_price
        offset = self._sample_offset()
        side = self._pick_side()

        if side == "buy":
            price = price - offset
        else:
            price = price + offset

        price = max(1, price)
        qty = self._sample_qty()
        client_order_id = self._next_client_order_id()

        self.active_orders[client_order_id] = {"side" : side, "qty" : qty, "price": price}

        return {
            "action": "new_order",
            "client_id": self.client_id,
            "symbol" : self.config.symbol,
            "client_order_id": client_order_id,
            "side" : side,
            "qty" : qty,
            "price": price
        }

    def _make_aggressive_order(self) -> dict:
        """Order placed THROUGH mid — will match against resting orders."""
        price = self.ref_price
        side = self._pick_side()

        overshoot = self.rng.randint(self.config.aggressive_overshoot_min, self.config.aggressive_overshoot_max)
        if side == "buy":
            price = price + overshoot
        else:
            price = price - overshoot

        price = max(1, price)
        qty = self._sample_qty()
        client_order_id = self._next_client_order_id()

        self.active_orders[client_order_id] = {"side" : side, "qty" : qty, "price": price}

        return {
            "action": "new_order",
            "client_id": self.client_id,
            "client_order_id": client_order_id,
            "symbol": self.config.symbol,
            "side": side,
            "price": price,
            "qty": qty
        }

    def _make_cancel(self) -> dict:
        """Cancel a random active order. The request gets its OWN fresh
        client_order_id; the order it targets is named separately."""
        target = self.rng.choice(list(self.active_orders.keys()))
        del self.active_orders[target]
        return {
            "action": "cancel",
            "client_id": self.client_id,
            "client_order_id": self._next_client_order_id(),
            "target_client_order_id": target,
            "symbol": self.config.symbol
        }

    def _make_modify(self) -> dict:
        """Modify a random active order — decrease quantity only. Fresh
        client_order_id for the request, target names the order."""
        target = self.rng.choice(list(self.active_orders.keys()))
        old_qty = self.active_orders[target]["qty"]
        new_qty = self.rng.randint(1, max(1, old_qty - 1))
        self.active_orders[target]["qty"] = new_qty

        return {
            "action": "modify",
            "client_id": self.client_id,
            "client_order_id": self._next_client_order_id(),
            "target_client_order_id": target,
            "symbol": self.config.symbol,
            "qty": new_qty
        }
