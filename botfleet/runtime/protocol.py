"""Engine-response handling for the production fleet.

Pairing rule (the whole protocol in one line): a message that carries a
`client_order_id` is the direct response to OUR request with that ticket;
a message without one is an unsolicited notice and is never recorded.

Every request (new_order, cancel and modify alike) is sent with its own fresh
client_order_id, so `pending.pop(client_order_id)` is an exact one-shot match —
no FIFO assumptions, no inference from message types.

Unsolicited resting-side fills (someone else's aggressor hit one of our resting
orders) are deliberately DISCARDED, not recorded: the same trade is already
captured in the aggressor's response, which is what the validator replays. So
recording the resting notice would just duplicate that data — and would make a
`sequence_number` show up in more than one telemetry row. We only sync generator
state for those (via `orig_client_order_id`) and move on.
"""

from botfleet.core.generator import OrderGenerator
from botfleet.runtime.telemetry import TelemetryCollector

# Engine response types that mean "the order this request was about is gone
# from the book".
TERMINAL_TYPES = ("order_filled", "order_cancelled", "order_rejected")


def handle_item(item: dict, t_recv: int, pending: dict,
                telemetry: TelemetryCollector, generator: OrderGenerator):
    mtype = item.get("type")

    if mtype == "trade_broadcast":
        return

    client_order_id = item.get("client_order_id")
    if client_order_id is None:
        # No request ticket → unsolicited resting-side notice. Don't record;
        # just keep the generator's live-order view in sync. Only a full fill
        # kills the order — a partial_fill notice means it's still live in the
        # book (just smaller), so it stays cancellable/modifiable.
        if mtype == "order_filled":
            orig = item.get("orig_client_order_id")
            if orig is not None:
                generator.remove_active_order(orig)
        return

    sent = pending.pop(client_order_id, None)
    if sent is None:
        # Echoed ticket we never tracked — engine answering a question nobody
        # asked. Nothing sane to record against it.
        return

    # Direct response to a request we sent — always carries a real latency.
    telemetry.record({
        "type": "order_response",
        "client_id": generator.client_id,
        "client_order_id": client_order_id,
        "target_client_order_id": sent.get("target_client_order_id"),
        "order_id": item.get("order_id"),       # engine's internal book id
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
        # The book order this request was about: a new_order's own ticket IS
        # the order's identity; a cancel/modify names it via target.
        generator.remove_active_order(
            sent.get("target_client_order_id") or client_order_id)
    # Counterparty resting orders are NOT retired here: the trades list can't
    # say whether a counterparty was fully consumed or only partially eaten.
    # Each resting order's own unsolicited order_filled notice (handled above)
    # retires it at exactly the right moment instead.
