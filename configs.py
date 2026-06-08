# configs.py

from order_generator import GeneratorConfig

PHASE_CONFIGS = {

    # ──────────────────────────────────────────────
    # PHASE: build_book
    # PURPOSE: fill the book to target depth before stress testing
    # WHY: matching and cancel performance depends on book depth.
    #      testing a cancel on an empty book tells you nothing.
    #      we need 10K-50K resting orders before the real test starts.
    # WHAT HAPPENS: 100% passive orders, no matching, no cancels.
    #      book grows from 0 to target_book_depth.
    #      after this phase, the book looks realistic — thick near mid, thin at edges.
    # ──────────────────────────────────────────────
    "build_book": GeneratorConfig(
        seed=42,
        passive_new_orders=1.0,
        aggressive_new_orders=0.0,
        cancel_orders=0.0,
        modify_orders=0.0,
        total_orders=60000,         # overshoot target to be safe
        offset_lambda=0.2,
        price_distribution="squared",
        max_price_deviation=50,
        qty_distribution="heavy_tail",
        qty_min=1,
        qty_max=100,
        buy_probability=0.5,        # balanced book
    ),

    # ──────────────────────────────────────────────
    # PHASE: light_mixed
    # PURPOSE: baseline latency measurement under gentle load
    # WHY: this gives you the "clean" numbers — what does the engine do
    #      when it's not under stress? you compare all other phases against this.
    #      if light_mixed p99 is 0.5ms and heavy_mixed p99 is 5ms,
    #      you know the engine degrades 10x under load.
    # WHAT HAPPENS: realistic mix of all operations at low rate.
    #      book depth stays stable (self-regulation maintains it).
    #      few matches, few cancels, mostly steady state.
    # ──────────────────────────────────────────────
    "light_mixed": GeneratorConfig(
        seed=42,
        passive_new_orders=0.50,
        aggressive_new_orders=0.10,
        cancel_orders=0.30,
        modify_orders=0.10,
        total_orders=10000,
        offset_lambda=0.2,
        price_distribution="squared",
        max_price_deviation=50,
        qty_distribution="heavy_tail",
        qty_min=1,
        qty_max=100,
    ),

    # ──────────────────────────────────────────────
    # PHASE: heavy_mixed
    # PURPOSE: same mix as light, but 10x more orders
    # WHY: reveals if the engine degrades under sustained volume.
    #      some engines handle 1K/sec fine but fall apart at 10K/sec
    #      because of memory allocation, cache misses, or lock contention.
    # WHAT HAPPENS: same operation mix as light_mixed.
    #      the RATE is what changes (controlled by the bot, not the config).
    #      book depth stays roughly stable.
    # ──────────────────────────────────────────────
    "heavy_mixed": GeneratorConfig(
        seed=42,
        passive_new_orders=0.50,
        aggressive_new_orders=0.10,
        cancel_orders=0.30,
        modify_orders=0.10,
        total_orders=100000,
        offset_lambda=0.2,
        price_distribution="squared",
        max_price_deviation=50,
        qty_distribution="heavy_tail",
        qty_min=1,
        qty_max=100,
    ),

    # ──────────────────────────────────────────────
    # PHASE: cancel_storm
    # PURPOSE: stress the cancel code path specifically
    # WHY: in real markets, 80-90% of orders are cancelled (HFT firms).
    #      cancel requires: hash map lookup → linked list removal → 
    #      price level cleanup. bad implementations do linear scan = slow.
    #      this phase reveals cancel path bottlenecks.
    # WHAT HAPPENS: 80% cancels rapidly drain the book.
    #      self-regulation kicks in when book gets thin, adds passive orders.
    #      creates an oscillation: drain → refill → drain → refill.
    #      engine must handle rapid state changes.
    # ──────────────────────────────────────────────
    "cancel_storm": GeneratorConfig(
        seed=42,
        passive_new_orders=0.10,
        aggressive_new_orders=0.05,
        cancel_orders=0.80,
        modify_orders=0.05,
        total_orders=30000,
        offset_lambda=0.2,          # but 80% cancel rate will win, book will thin
        price_distribution="squared",
        max_price_deviation=50,
        qty_distribution="heavy_tail",
        qty_min=1,
        qty_max=20,                 # small orders — more cancels per unit time
    ),

    # ──────────────────────────────────────────────
    # PHASE: matching_spike
    # PURPOSE: stress the matching loop specifically
    # WHY: matching is the most expensive operation — it walks through
    #      price levels, fills orders, updates book state, generates trades.
    #      whale orders (qty=1000) that sweep 20+ price levels are the worst case.
    #      this reveals if the matching loop is O(n) per level or O(1).
    # WHAT HAPPENS: 70% aggressive orders with large quantities.
    #      each aggressive order matches against many resting orders.
    #      book depth drops rapidly as orders get filled.
    #      self-regulation tries to refill but can't keep up — that's intentional.
    #      this is the "flash crash" scenario.
    # ──────────────────────────────────────────────
    "matching_spike": GeneratorConfig(
        seed=42,
        passive_new_orders=0.10,
        aggressive_new_orders=0.70,
        cancel_orders=0.10,
        modify_orders=0.10,
        total_orders=50000,
        aggressive_overshoot_min=5,
        aggressive_overshoot_max=30, # crosses deep into the book
        price_distribution="squared",
        max_price_deviation=50,
        qty_distribution="heavy_tail",
        qty_min=100,
        qty_max=1000,               # whale orders — sweep many levels
    ),

    # ──────────────────────────────────────────────
    # PHASE: wide_book
    # PURPOSE: test with orders spread across many price levels
    # WHY: a wide book means the sorted map (std::map) has many entries.
    #      iterating from best bid to a price 200 ticks away requires
    #      traversing many tree nodes. some implementations degrade here.
    #      also tests how the engine handles a large number of price levels.
    # WHAT HAPPENS: orders spread across 100+ price levels.
    #      book looks flat and wide instead of the normal hump shape.
    #      matching sweeps through many levels even for small aggressive orders.
    # ──────────────────────────────────────────────
    "wide_book": GeneratorConfig(
        seed=42,
        passive_new_orders=0.60,
        aggressive_new_orders=0.10,
        cancel_orders=0.20,
        modify_orders=0.10,
        total_orders=20000,
        offset_lambda=0.05,          # very low lambda = wide spread
        price_distribution="exponential",  # exponential works better for wide
        max_price_deviation=200,     # if using squared, spread up to 200 ticks
        qty_min=1,
        qty_max=50,
    ),

    # ──────────────────────────────────────────────
    # PHASE: tight_book
    # PURPOSE: test with orders concentrated at very few price levels
    # WHY: tight book means 3-4 price levels with very long queues.
    #      each price level has thousands of orders in the linked list.
    #      matching at the best level walks a long list.
    #      cancel within a long list tests if the engine uses O(1) lookup
    #      (your OrderLocation with iterator) or O(n) scan.
    # WHAT HAPPENS: most orders within 1-3 ticks of mid.
    #      best bid and best ask each have 5000+ orders queued.
    #      aggressive orders match against the long queue sequentially.
    # ──────────────────────────────────────────────
    "tight_book": GeneratorConfig(
        seed=42,
        passive_new_orders=0.60,
        aggressive_new_orders=0.10,
        cancel_orders=0.20,
        modify_orders=0.10,
        total_orders=20000,
        offset_lambda=0.5,           # high lambda = very tight
        price_distribution="squared",
        max_price_deviation=5,       # max 5 ticks from mid
        qty_min=1,
        qty_max=50,
    ),

    # ──────────────────────────────────────────────
    # PHASE: buy_pressure
    # PURPOSE: test asymmetric book — one side much larger than the other
    # WHY: real markets have imbalances. during a selloff, everyone buys the dip
    #      and the bid side grows huge while asks are thin. the engine must handle
    #      this without performance degradation on either side.
    #      also tests that the sorted map for bids (descending) and asks (ascending)
    #      perform equally well under different loads.
    # WHAT HAPPENS: 90% of orders are buys.
    #      bid side of book grows to 10x the ask side.
    #      aggressive sells (10% of orders) match against the deep bid side.
    # ──────────────────────────────────────────────
    "buy_pressure": GeneratorConfig(
        seed=42,
        passive_new_orders=0.50,
        aggressive_new_orders=0.10,
        cancel_orders=0.30,
        modify_orders=0.10,
        total_orders=20000,
        buy_probability=0.9,         # 90% buys
        offset_lambda=0.2,
        price_distribution="squared",
        max_price_deviation=50,
    ),

    # ──────────────────────────────────────────────
    # PHASE: recovery
    # PURPOSE: measure if the engine returns to baseline after stress
    # WHY: some engines allocate memory under stress but never free it.
    #      some have internal queues that back up and take time to drain.
    #      a good engine should return to light_mixed latency levels
    #      within seconds of the stress ending.
    #      if it doesn't recover, that's a serious quality issue.
    # WHAT HAPPENS: same as light_mixed. gentle load.
    #      compare these latencies against the original light_mixed.
    #      if p99 is 2x higher, the engine didn't recover fully.
    # ──────────────────────────────────────────────
    "recovery": GeneratorConfig(
        seed=42,
        passive_new_orders=0.50,
        aggressive_new_orders=0.10,
        cancel_orders=0.30,
        modify_orders=0.10,
        total_orders=10000,
        offset_lambda=0.2,
        price_distribution="squared",
        max_price_deviation=50,
        qty_min=1,
        qty_max=100,
    ),
}


# ──────────────────────────────────────────────
# TEST PLANS
# A test plan is a sequence of (phase_name, rate_per_bot_per_second)
# The rate is how fast EACH BOT sends orders.
# 50 bots × 100 rate = 5000 orders/sec total
# ──────────────────────────────────────────────

TEST_PLANS = {
    # Standard test: ~10 minutes, covers all important scenarios
    "standard": [
        ("build_book", 200),        # 30 sec to build 50K orders (50 bots × 200 = 10K/sec)
        ("light_mixed", 50),        # 60 sec baseline (50 bots × 50 = 2.5K/sec)
        ("heavy_mixed", 200),       # 120 sec sustained load (50 bots × 200 = 10K/sec)
        ("cancel_storm", 300),      # 30 sec cancel stress (50 bots × 300 = 15K/sec)
        ("matching_spike", 200),    # 60 sec matching stress (50 bots × 200 = 10K/sec)
        ("recovery", 50),           # 30 sec recovery check
    ],

    # Quick smoke test: ~2 minutes, basic validation
    "quick": [
        ("build_book", 200),
        ("light_mixed", 100),
    ],

    # Extreme test: ~15 minutes, everything at maximum
    "extreme": [
        ("build_book", 200),
        ("heavy_mixed", 500),       # 50 bots × 500 = 25K/sec
        ("matching_spike", 500),
        ("cancel_storm", 500),
        ("wide_book", 200),
        ("tight_book", 200),
        ("buy_pressure", 200),
        ("recovery", 50),
    ],
}