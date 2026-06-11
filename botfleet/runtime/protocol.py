"""Engine-response handling for the production fleet.

One unified envelope shape covers every order event. `handle_item` records a
telemetry row, computes latency for our own requests, and keeps the generator's
view of live orders in sync with the engine's truth.
"""

from botfleet.core.generator import OrderGenerator
from botfleet.runtime.telemetry import TelemetryCollector

# Engine response types that mean "this order is gone from the book".
TERMINAL_TYPES = ("order_filled", "order_cancelled", "order_rejected")


def handle_item(item: dict, t_recv: int, pending: dict,
                telemetry: TelemetryCollector, generator: OrderGenerator,
                current_phase: str):
    mtype = item.get("type")

    if mtype == "trade_broadcast":
        return

    oid = extract_oid(item)
    if oid is None:
        return

    sent = pending.pop(oid, None)

    telemetry.record({
        "type": "order_response",
        "client_id": generator.client_id,
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

    if mtype in TERMINAL_TYPES:
        generator.remove_active_order(oid)
        # Clean up the other side of the trade from our generator (in case we
        # placed both sides, or a resting order of ours was the counterparty).
        for trade in item.get("trades", []):
            for key in ("buyer_order_id", "seller_order_id"):
                other = trade.get(key)
                if other and other != oid:
                    generator.remove_active_order(other)
                    pending.pop(other, None)


def extract_oid(item: dict) -> int | None:
    if "order_id" in item:
        return item["order_id"]
    orders = item.get("orders") or []
    if orders and isinstance(orders[0], dict):
        return orders[0].get("order_id")
    return None
