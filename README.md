# Exchange Bot Fleet

> **Part of the [Quant-Force](https://github.com/space-techy/quant-force-infra)
> platform** (the stress fleet in `test_execution`) — one pod per symbol, driving
> the phased order flow. The order protocol it speaks is `docs/ENGINE_SPEC.md` in
> the platform repo.

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

## Project layout

Everything lives in the `botfleet` package, grouped by what it does, with thin
entrypoint scripts at the repo root:

```
run_fleet.py                  → botfleet.runtime.fleet      (production pod)
run_live_visualizer.py        → botfleet.visualization.live (live debug view)
run_offline_visualizer.py     → botfleet.visualization.offline (local sim)

botfleet/
  core/           the domain — "what to send"
    config.py       GeneratorConfig (every generation knob)
    generator.py    OrderGenerator (deterministic order stream)
    plans.py        PHASE_CONFIGS + TEST_PLANS (scenarios & sequencing)
  runtime/        the production fleet — "send it for real"
    settings.py     env-driven config (engine URI, seed, flush, ...)
    telemetry.py    pluggable sinks + buffering collector
    coordination.py pluggable phase coordinators (local / Redis stub)
    protocol.py     engine-response handling (latency, generator sync)
    loops.py        async sender / receiver hot loops
    fleet.py        per-bot driver + CLI
  visualization/  the debug tools — "see what happened"
    books.py        LocalBook / FleetBook / SubmittedBook
    summary.py      shared per-phase text summary
    plots.py        matplotlib chart helpers
    offline.py      offline visualizer (local sim, no network)
    live.py         live visualizer (drives the fleet, rebuilds the book)
```

## The two programs

| Command | What it's for |
|------|---------------|
| **`python run_fleet.py`** | The production fleet. Silent, lean. Sends orders, collects telemetry, ships it to a sink. No prints, no plotting. |
| **`python run_live_visualizer.py`** | The debugging lens. Runs the same traffic but rebuilds the order book from the engine's replies and draws a chart per phase so you can *see* what the engine did. Use this locally to inspect behaviour and results. |

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

### `GeneratorConfig` (in `botfleet/core/config.py`)

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
- **Symbol** — `symbol`: the single instrument this generator trades. It's set
  per-pod by the runner (see Pods below), not in the phase config, so every order a
  bot sends — including its cancels and modifies — carries the same symbol.

### `OrderGenerator` (in `botfleet/core/generator.py`)

The black box that turns a `GeneratorConfig` into orders, one at a time, via
`generate_next()`. It is **deterministic**: the same config + same seed always
produces the same order stream, which makes runs reproducible and comparable. It
also tracks which of its orders are still live (`active_orders`) so it only ever
cancels or modifies orders that actually exist — and the receiver keeps that view
in sync with the engine's truth (orders the engine says are filled/cancelled get
dropped).

### `PHASE_CONFIGS` and `TEST_PLANS` (in `botfleet/core/plans.py`)

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

### Pods: one symbol, two dials, an identity

A **pod** is one run of `run_fleet.py` — a fleet of bots all trading **one symbol**.
It's the unit of load you point at a single book, and it has four knobs:

- **`--symbol`** — the one book every bot in the pod hammers.
- **`--num-bots`** — the **pressure** dial: concurrency against that book.
- **`--order-divisor`** — the **volume** dial: every phase's `total_orders` is divided
  by it, so the pod runs the standard plan at 1/N scale. `build_book` is 60k, so
  divisor `2` → 30k, `6` → 10k, `60` → 1k. Per bot per phase you get
  `max(1, (total // divisor) // num_bots)`.
- **`--pod-id`** — a unique id per pod. It offsets `client_id` and `order_id`
  (`global_bot = pod_id * 100_000 + bot_id`) so two pods never collide and every
  event is attributable. `--pod-id 0` reproduces the old single-pod ids.

You build a whole market by launching **many pods**: e.g. symbols 2,3,4 heavy
(divisor `2`), the rest light (divisor `60`). Multiple pods can even point at the
**same** symbol (different `--pod-id`s) to stack heavier load on one book.

## Running it

**Dependencies.** Just two: `websockets` (the connection) and `matplotlib` (the
visualizer's charts).

```bash
pip install websockets matplotlib
```

Have your matching engine listening for WebSocket connections (default
`ws://localhost:3001/ws`).

**Run the production fleet (one pod):**

```bash
# pod targeting symbol 2, 5 bots, full volume
python run_fleet.py --plan standard --symbol 2 --num-bots 5

# a lighter pod on symbol 7: 1/6 the orders
python run_fleet.py --plan standard --symbol 7 --num-bots 5 --order-divisor 6 --pod-id 1
```

It runs silently and streams telemetry to the configured sink. Every flag has an
env-var equivalent:

| Variable / flag | Default | Meaning |
|----------|---------|---------|
| `ENGINE_URI` | `ws://localhost:3001/ws` | Where the engine is. |
| `NUM_BOTS` / `--num-bots` | `5` | Bots in this pod — the pressure dial. |
| `SYMBOL` / `--symbol` | `1` | The single symbol this pod trades. |
| `ORDER_DIVISOR` / `--order-divisor` | `1` | Volume dial — divides every phase's `total_orders`. |
| `POD_ID` / `--pod-id` | `0` | Unique pod id — offsets `client_id`/`order_id` so pods don't collide. |
| `PLAN_NAME` / `--plan` | `quick` | Which test plan. |
| `GLOBAL_SEED` | `42` | Base seed. Each bot's seed folds `GLOBAL_SEED`, `pod_id`, and `bot_id`. |
| `TELEMETRY_SINK` | `file` | `file` (no-op placeholder today) or `kafka` (stub). |
| `PHASE_SOURCE` | `local` | `local` (walk the plan) or `redis` (stub → falls back to local). |
| `FLUSH_INTERVAL_S` | `1.0` | How often telemetry is flushed even at low rate. |
| `TEST_ID` | `local` | Identifier reserved for the future Redis/Kafka keys. |

**Run the live visualizer:**

Edit the constants at the top of `botfleet/visualization/live.py` (`URI`,
`NUM_BOTS`, `PLAN_NAME`, `GLOBAL_SEED`, and `SYMBOL`/`ORDER_DIVISOR`/`POD_ID`), then:

```bash
python run_live_visualizer.py
```

It writes one chart per phase into `results/`. Because the whole run trades one
`SYMBOL`, the reconstructed book is exactly that one book.

**Run the offline visualizer** (local sim, no engine needed):

```bash
python run_offline_visualizer.py
```

## Scaling & how much load is "real" stress

The pod model gives you two independent dials, and which one you turn depends on
what you want to break:

- **Pressure on a single book** (lock contention, deep matching, long cancel
  queues) comes from **bots-per-symbol × rate**. To push one book hard, point
  *many* bots — or *many pods* (same `--symbol`, different `--pod-id`) — at it.
  One book at 50k orders/sec is a very different test than fifty books at 1k each.
- **Breadth** (memory, many books, cache footprint) comes from **how many distinct
  symbols** are live. Spread pods across many `--symbol`s, each gentle.

**Depth to make matching/cancel phases bite:** an aggressive order on symbol X only
matches symbol X's resting orders, so a thin book just rests instead of trading. In
`build_book` each book ends up with roughly `total_orders // divisor` resting orders
(split across the pod's bots). So if you want ~5k depth per book before the spike,
size the divisor accordingly — at the default 60k, divisor `1` gives ~60k per book,
divisor `12` gives ~5k.

**What's "enough" for real stress:** drive aggregate throughput toward the engine's
ceiling. A single pod of 5 bots at 1k/s is 5k orders/sec — gentle. Ten pods of 5
bots at 1k/s is 50k/sec; stack several pods on the same symbol and you concentrate
that into one book's hot path. Start by raising throughput until `order_response`
latency percentiles start climbing — that inflection is where the engine begins to
hurt, and it's the number worth reporting.

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
- **One symbol per pod (by design).** A single run trades one symbol; many symbols
  means many pods. That's the model, not a bug — but there's no single-process
  "spread across N symbols" mode, so multi-symbol load is an orchestration concern.
- **Backpressure is transport-level only.** `await ws.send()` naturally pauses a bot
  when the engine's socket buffer fills; there's no application-level throttle for an
  engine that reads fast but processes slowly.
- **No zombie sweep.** If the engine never replies to an order, its entry lingers in
  the in-flight map for the bot's lifetime (memory only).
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
