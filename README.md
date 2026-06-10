# Exchange Bot Fleet

A modular load generator for order-matching engines. It spins up a fleet of
async bots that stream **bulk, realistic order traffic** at a matching engine
over WebSocket, and records what comes back so you can measure how the engine
behaves under different kinds of pressure.

## Why this exists

I needed a way to fire a large volume of order requests at a matching engine —
but not random noise. The traffic had to *look* like a real market (orders
clustered near the mid price, a few whales, a realistic mix of new/cancel/modify),
and it had to be **highly customizable** so I could dial up one kind of stress at
a time (a cancel storm, a matching spike, a wide book, etc.) without rewriting
anything.

It's written in Python on purpose: under time constraints, Python's `asyncio`
made the concurrent "many bots, one socket each" model quick to build and easy to
reshape. The hot path is lean enough for the load levels this targets; the design
leaves clean seams (pluggable telemetry sink, pluggable phase source) to swap in
heavier infrastructure later.

## The two programs

| File | What it's for |
|------|---------------|
| **`bot_runner.py`** | The production fleet. Silent, lean. Sends orders, collects telemetry, ships it to a sink. No prints, no plotting. |
| **`live_visualizer.py`** | The debugging lens. Runs the same traffic but rebuilds the order book from the engine's replies and draws a chart per phase so you can *see* what the engine did. Use this locally to inspect behaviour and results. |

Both drive the **same** order generator and the same test plans — so what you see
in the visualizer is what the production fleet sends.

## The math, in plain terms

You don't need any formulas to use this, but it helps to know *why* the generated
orders look the way they do. Three ideas do all the work:

**1. Prices wander, but on a leash (mean-reverting random walk).**
A pure random walk drifts away forever — prices would float off to zero or
infinity. Real prices don't do that: they wobble around some "fair value" and get
pulled back when they stray. So every step we nudge the price by a small random
amount, then tug it back toward fair value — and the further it has drifted, the
stronger the tug. Picture a ball on a rubber band: it jiggles around, but the band
always pulls it home. This gives order streams that cluster naturally around a
center instead of running away.

**2. Most orders sit near the mid price, a few sit far (skewed offset).**
When a bot places a resting ("passive") order, how far from the mid should it go?
In a real book, depth is thick near the mid and thins out toward the edges. To
reproduce that shape we take a plain 0–1 random number and **square it** — squaring
pushes most results small (0.5 becomes 0.25), so most orders land close to the mid
and only a few land far. That's what gives the book its familiar "hump near the
middle, thin at the edges" shape. (An exponential variant is available for
deliberately *wide* books with a longer tail.)

**3. Most orders are small, a few are giant (heavy tail).**
Real markets are full of small orders with the occasional whale. Instead of one
flat random size, we **multiply a few random numbers together** — that naturally
produces lots of small values and rare large ones. This matters because a whale
order that sweeps through twenty price levels is the *worst case* for a matching
loop, and we want to generate those on purpose.

When a bot wants to *cause a match* (an "aggressive" order), it deliberately prices
the order past the mid by a random **overshoot** so it crosses into the resting
orders on the other side — a bigger overshoot sweeps deeper into the book.

## How the configs fit together

There are three layers, from the smallest knob to the whole run.

### `GeneratorConfig` (in `order_generator.py`)

A dataclass holding **every knob** that shapes order generation. The important ones:

- **Operation mix** — `passive_new_orders`, `aggressive_new_orders`, `cancel_orders`,
  `modify_orders`. These are probabilities that should sum to 1.0. They decide what
  *kind* of order each step produces. A phase with 80% cancels stresses the cancel
  path; a phase with 70% aggressive orders stresses the matching loop.
- **Price model** — `start_price`, `fair_value`, `volatility` (how wild the wobble
  is), `mean_reversion` (how hard the leash pulls back).
- **Price spread** — `price_distribution` (`"squared"` or `"exponential"`),
  `offset_lambda` and `max_price_deviation` (how tight or wide the book is),
  `aggressive_overshoot_min/max` (how deep aggressive orders cross).
- **Quantity** — `qty_distribution` (`"heavy_tail"` or `"uniform"`),
  `qty_min`, `qty_max`, `qty_scale`.
- **Balance & scale** — `buy_probability` (0.5 = balanced, 0.9 = heavy buy
  pressure) and `total_orders` (how many orders the phase generates — see the note
  on splitting below).
- **Symbols** — `min_symbol`/`max_symbol`: each order draws a random symbol in that
  range, so load spreads across many instruments (the default is `1`–`100`). Set
  them equal to pin everything to a single symbol (it falls back to the `symbol`
  field). A cancel or modify always reuses the symbol its original order was
  created with.

### `OrderGenerator` (in `order_generator.py`)

The black box that turns a `GeneratorConfig` into orders, one at a time, via
`generate_next()`. It is **deterministic**: the same config + same seed always
produces the same order stream, which makes runs reproducible and comparable. It
also tracks which of its orders are still live (`active_orders`) so it only ever
cancels or modifies orders that actually exist — and the receiver keeps that view
in sync with the engine's truth (orders the engine says are filled/cancelled get
dropped).

### `PHASE_CONFIGS` and `TEST_PLANS` (in `configs.py`)

- **`PHASE_CONFIGS`** is a catalogue of named, ready-made `GeneratorConfig`s — each
  one a deliberate stress scenario. See the table below.
- **`TEST_PLANS`** is where you compose phases into a run. A plan is just a list of
  `(phase_name, rate)` pairs, where `rate` is **orders per second per bot**. So
  `("heavy_mixed", 200)` with 5 bots = 1,000 orders/sec of `heavy_mixed` traffic.
  Three plans ship by default: `quick` (smoke test), `standard` (the full sweep),
  and `extreme` (everything at maximum).

### The phase catalogue

| Phase | What it stresses |
|-------|------------------|
| `build_book` | Fills an empty book with 100% passive orders so later phases have real depth to work against. |
| `light_mixed` | Gentle, realistic mix — your **baseline** latency. Everything else is compared to this. |
| `heavy_mixed` | Same mix, far more volume — does the engine degrade under sustained load? |
| `cancel_storm` | 80% cancels — hammers the cancel/lookup path. |
| `matching_spike` | 70% aggressive whale orders — the "flash crash"; hammers the matching loop. |
| `wide_book` | Orders spread across 100+ price levels — stresses traversal of a large book. |
| `tight_book` | Orders crammed into a few levels with long queues — stresses in-queue lookup. |
| `buy_pressure` | 90% buys — an asymmetric, lopsided book. |
| `recovery` | Back to gentle load — did the engine return to baseline after the stress? |

> **Note on `total_orders`:** the number in a phase config is the **fleet total**.
> The runner divides it by the number of bots, so each bot does its share. Run with
> more bots and each one does proportionally less — the total stays the same.

## Running it

**Dependencies.** Just two: `websockets` (the connection) and `matplotlib` (the
visualizer's charts).

```bash
pip install websockets matplotlib
```

Have your matching engine listening for WebSocket connections (default
`ws://localhost:3001/ws`).

**Run the production fleet:**

```bash
python bot_runner.py --num-bots 5 --plan standard
```

It runs silently and streams telemetry to the configured sink. Behaviour is tuned
with environment variables:

| Variable | Default | Meaning |
|----------|---------|---------|
| `ENGINE_URI` | `ws://localhost:3001/ws` | Where the engine is. |
| `NUM_BOTS` | `5` | Fleet size (or `--num-bots`). |
| `PLAN_NAME` | `quick` | Which test plan (or `--plan`). |
| `GLOBAL_SEED` | `42` | Base seed. Each bot's seed is `GLOBAL_SEED * 1000 + bot_id`. |
| `TELEMETRY_SINK` | `file` | `file` (no-op placeholder today) or `kafka` (stub). |
| `PHASE_SOURCE` | `local` | `local` (walk the plan) or `redis` (stub → falls back to local). |
| `FLUSH_INTERVAL_S` | `1.0` | How often telemetry is flushed even at low rate. |
| `TEST_ID` | `local` | Identifier reserved for the future Redis/Kafka keys. |

**Run the visualizer:**

Edit the constants at the top of `live_visualizer.py` (`URI`, `NUM_BOTS`,
`PLAN_NAME`, `GLOBAL_SEED`), then:

```bash
python live_visualizer.py
```

It writes one chart per phase into `results/`.

## Telemetry

Every bot emits two kinds of events into its telemetry stream:

- **`order_sent`** — emitted the moment an order goes out (id, action, side, price,
  qty, phase, send timestamp). This is the *input* record — enough to replay the
  exact sequence later.
- **`order_response`** — emitted when the engine replies (message type, message
  code, round-trip `latency_ns`, the raw `trades`/`orders`/`sequence_number`). An
  unsolicited fill on one of our resting orders shows up here too, with a `null`
  latency (we never sent a request for it, so there's no round trip to measure).

Today the file sink is a deliberate no-op — the events are produced and buffered,
ready for a real sink to be dropped in (see Improvements). To inspect behaviour
locally, use the visualizer.

## Results (the charts)

The PNGs in [`results/`](results/) are produced by the visualizer — one per phase,
each with **two stacked panels** for the same moment in time:

- **Top — SUBMITTED.** Everything the fleet *tried* to put on the book, with **no
  matching applied**. Green bars are bids, red bars are asks, plotted by price.
  This is the fleet's raw intent.
- **Bottom — ENGINE TRUTH.** The book the engine *actually* holds after matching,
  rebuilt from its responses, with executed **trades** scattered in blue.

The gap between the two panels is the whole point: it shows you exactly **what the
matching engine consumed**. Where the submitted depth is tall but the engine-truth
depth is short, orders crossed and matched away.

Reading them by phase:

- **`build_book`** — both panels look similar: 100% passive orders, almost nothing
  matches, so submitted ≈ engine truth. A healthy "hump" near the mid.
- **`light_mixed`** — a steady book with a sprinkle of trades; the baseline shape.
- **`heavy_mixed`** — same shape, much denser; this is the sustained-load picture.
- **`cancel_storm`** — the engine-truth book is visibly thinner than submitted: the
  flood of cancels has drained depth.
- **`matching_spike`** — the dramatic one. The submitted panel is full, but the
  engine-truth panel is carved out around the mid and the blue trade scatter is
  heavy — aggressive whales swept through the book.
- **`recovery`** — should look like `light_mixed` again. If it doesn't, the engine
  didn't fully recover from the spike.

## Limitations (current)

- **Sinks/coordination are stubs.** The file sink is a no-op, the Kafka sink and the
  Redis phase coordinator are scaffolding that falls back to local behaviour. There
  is no validator/aggregator consuming the telemetry yet.
- **Single symbol per run.**
- **Backpressure is transport-level only.** `await ws.send()` naturally pauses a bot
  when the engine's socket buffer fills; there's no application-level throttle for an
  engine that reads fast but processes slowly.
- **No zombie sweep.** If the engine never replies to an order, its entry lingers in
  the in-flight map for the bot's lifetime (reported as `pending_left` is gone now
  that prints are off, but the memory effect remains).
- **Cancel/modify targets depend on engine acks.** Generation is deterministic, but
  which live orders are available to cancel/modify depends on what the engine has
  confirmed — so wall-clock timing can influence those choices.
- **Some config comments are aspirational.** A few phase comments mention
  "self-regulation" of book depth; that regulator isn't in the generator — the
  operation mix and `total_orders` are what actually shape each phase.

## Improvements (where this goes next)

- Wire a **real Kafka producer** into `KafkaSink` and a **real Redis client** into
  `RedisPhaseCoordinator` so phase changes can be driven out-of-band across a
  distributed fleet.
- Build the downstream **validator** (replays `order_sent` against a reference
  engine to check correctness) and **aggregator** (rolls `order_response` latencies
  into live p50/p99 per phase).
- **Multi-symbol** generation.
