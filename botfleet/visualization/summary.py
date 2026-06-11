"""Per-phase text summary, shared by the offline and live visualizers."""

from collections import Counter

from botfleet.core.generator import OrderGenerator
from botfleet.visualization.books import LocalBook


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
