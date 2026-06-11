"""
Offline visualizer / debugger for the bot fleet.

Runs the "standard" test plan with 5 virtual bots, maintains a local order book
(no WebSocket, no network), and produces a per-phase matplotlib chart +
text summary so we can sanity-check that the generator produces realistic book
shapes.

Outputs: book_shape_<phase>.png  +  stdout summary
"""

from collections import Counter
from dataclasses import replace

from botfleet.core.generator import OrderGenerator
from botfleet.core.plans import PHASE_CONFIGS, TEST_PLANS
from botfleet.visualization.books import LocalBook
from botfleet.visualization.plots import plot_phase
from botfleet.visualization.summary import print_summary


NUM_BOTS = 5
# Target ~10k orders TOTAL across the whole simulation (6 phases × 5 bots × ~340 ≈ 10k).
ORDERS_PER_BOT_PER_PHASE = 12000


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
