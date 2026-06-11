"""matplotlib chart helpers for both visualizers.

  plot_phase          single-panel book chart (offline visualizer).
  plot_phase_compare  two-panel submitted-vs-engine-truth chart (live visualizer).
"""

import os
from collections import Counter, defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from botfleet.core.generator import OrderGenerator
from botfleet.visualization.books import FleetBook, LocalBook, SubmittedBook


# ───────────────────────── offline (single panel) ────────────────────────────

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


# ───────────────────────── live (two panels) ─────────────────────────────────

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
