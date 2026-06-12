"""Engine-response handling for the production fleet.

One unified envelope shape covers every order event. `handle_item` records a
telemetry row for the engine's reply to OUR request (with a real latency), and
keeps the generator's view of live orders in sync with the engine's truth.

Unsolicited resting-side fills (someone else's aggressor hit one of our resting
orders) are deliberately DISCARDED, not recorded: the same trade is already
captured in the aggressor's response, which is what the validator replays. So
recording the resting notice would just duplicate that data — and would make a
`sequence_number` show up in more than one telemetry row. We only sync generator
state for those and move on.
"""

from botfleet.core.generator import OrderGenerator
from botfleet.runtime.telemetry import TelemetryCollector

# Engine response types that mean "this order is gone from the book".
TERMINAL_TYPES = ("order_filled", "order_cancelled", "order_rejected")

# A fill is only ever the answer to our own aggressive new_order. If one of these
# arrives while a cancel/modify is at the head of the oid's queue, it must be an
# unsolicited resting-side fill that happens to share the (reused) order_id.
FILL_TYPES = ("order_filled", "partial_fill")


def handle_item(item: dict, t_recv: int, pending: dict,
                telemetry: TelemetryCollector, generator: OrderGenerator):
    mtype = item.get("type")

    if mtype == "trade_broadcast":
        return

    oid = extract_oid(item)
    if oid is None:
        return

    # FIFO-pair against our own sends: pop the oldest un-answered send for this
    # oid (sender queues per-oid; responses arrive in send order on one socket).
    q = pending.get(oid)
    if mtype in FILL_TYPES and q and q[0]["action"] != "new_order":
        # Unsolicited resting-side fill of an order we're concurrently
        # cancelling/modifying — share the reused order_id but isn't our reply.
        # Leave the cancel/modify request queued for its real response.
        sent = None
    elif q:
        sent = q.popleft()
        if not q:
            pending.pop(oid, None)
    else:
        sent = None
    if sent is None:
        # Unsolicited resting-side fill — redundant with the aggressor's response.
        # Don't record; just keep our generator's live-order view in sync. Only
        # a full fill kills the order — a partial_fill notice means it's still
        # live in the book (just smaller), so it stays cancellable/modifiable.
        if mtype == "order_filled":
            generator.remove_active_order(oid)
        return

    # Direct response to a request we sent — always carries a real latency.
    telemetry.record({
        "type": "order_response",
        "client_id": generator.client_id,
        "order_id": oid,
        "action": sent["action"],
        "msg_type": mtype,
        "message_code": item.get("message_code"),
        "latency_ns": t_recv - sent["t_send_ns"],
        "t_send_ns": sent["t_send_ns"],
        "t_recv_ns": t_recv,
        "error": item.get("error", ""),
        "sequence_number": item.get("sequence_number"),
        "trades": item.get("trades", []),
        "orders": item.get("orders", []),
    })

    if mtype in TERMINAL_TYPES:
        generator.remove_active_order(oid)
    # Counterparty resting orders are NOT retired here. The trades list can't
    # say whether a counterparty was fully consumed or only partially eaten
    # (the last order in a sweep usually survives with qty left), so acting on
    # it would retire live orders — and popping their pending queues destroyed
    # the tracking for in-flight cancels/modifies, whose responses then went
    # unrecorded and silently desynced the validator's book. Each resting
    # order's own unsolicited order_filled notice (handled above) retires it
    # at exactly the right moment instead.


def extract_oid(item: dict) -> int | None:
    if "order_id" in item:
        return item["order_id"]
    orders = item.get("orders") or []
    if orders and isinstance(orders[0], dict):
        return orders[0].get("order_id")
    return None
