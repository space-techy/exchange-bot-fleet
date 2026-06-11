"""The per-bot driver and entrypoint wiring for the production fleet.

A pod is one run of this module: a fleet of bots all trading ONE symbol, with
`--num-bots` (pressure), `--order-divisor` (volume), and `--pod-id` (identity).
"""

import argparse
import asyncio
import dataclasses
import os
import signal

import websockets
from websockets.exceptions import ConnectionClosed

from botfleet.core.generator import OrderGenerator
from botfleet.core.plans import PHASE_CONFIGS, TEST_PLANS
from botfleet.runtime.coordination import (
    LocalPhaseCoordinator,
    RedisPhaseCoordinator,
    make_coordinator,
)
from botfleet.runtime.loops import receiver_loop, sender_loop
from botfleet.runtime.settings import GLOBAL_SEED, URI
from botfleet.runtime.telemetry import TelemetryCollector


def _per_bot_total(phase_name: str, num_bots: int, divisor: int) -> int:
    """This bot's order count for a phase: the fleet total scaled down by the
    pod's volume divisor, then split evenly across the pod's bots."""
    pod_total = PHASE_CONFIGS[phase_name].total_orders // max(1, divisor)
    return max(1, pod_total // num_bots)


async def run_single_bot(bot_id: int, plan_name: str, num_bots: int,
                         symbol: int, divisor: int, pod_id: int,
                         stop_event: asyncio.Event | None = None):
    pending: dict = {}
    telemetry = TelemetryCollector(bot_id)
    telemetry.start()                       # periodic-flush task
    coord = make_coordinator(plan_name)

    first = coord.get_phase()
    if first is None:
        await telemetry.stop()
        return

    first_cfg = dataclasses.replace(
        PHASE_CONFIGS[first[0]],
        seed=GLOBAL_SEED * 1000 + bot_id,
        total_orders=_per_bot_total(first[0], num_bots, divisor),
        symbol=symbol,
    )
    generator = OrderGenerator(bot_id, first_cfg, pod_id=pod_id)

    try:
        async with websockets.connect(URI) as ws:
            while True:
                # graceful shutdown requested between phases — stop cleanly
                if stop_event is not None and stop_event.is_set():
                    break

                phase = coord.get_phase()
                if phase is None:
                    break
                phase_name, rate = phase

                cfg = dataclasses.replace(
                    PHASE_CONFIGS[phase_name],
                    seed=GLOBAL_SEED * 1000 + bot_id,
                    total_orders=_per_bot_total(phase_name, num_bots, divisor),
                    symbol=symbol,
                )
                generator.update_config(cfg)
                done = asyncio.Event()

                try:
                    await asyncio.gather(
                        sender_loop(ws, generator, pending, rate, done, coord,
                                    phase_name, telemetry, stop_event),
                        receiver_loop(ws, pending, telemetry, generator, done, phase_name),
                    )
                except ConnectionClosed:
                    break                       # engine dropped — end this bot
                finally:
                    # phase-boundary flush: downstream gets clean per-phase batches
                    telemetry.flush()

                if isinstance(coord, (LocalPhaseCoordinator, RedisPhaseCoordinator)):
                    coord.advance()
    finally:
        # TODO: surface fatal errors to the orchestrator instead of dying quietly.
        await telemetry.stop()


def _install_signal_handlers(stop_event: asyncio.Event):
    """Turn SIGINT/SIGTERM into a cooperative stop. Critically, this makes
    SIGTERM (how a pod is told to shut down) flush telemetry and close the
    socket instead of killing the process mid-flight."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, AttributeError):
            # Windows: the asyncio signal API is limited. SIGINT still arrives
            # as KeyboardInterrupt via asyncio.run; fall back to signal.signal
            # for what is deliverable in the main thread.
            try:
                signal.signal(sig, lambda *_: stop_event.set())
            except (ValueError, OSError):
                pass


async def main(num_bots: int = 5, plan_name: str = "quick",
               symbol: int = 1, divisor: int = 1, pod_id: int = 0):
    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)
    await asyncio.gather(*[
        run_single_bot(bot_id, plan_name, num_bots, symbol, divisor, pod_id, stop_event)
        for bot_id in range(1, num_bots + 1)
    ])


def parse_args():
    p = argparse.ArgumentParser(description="Production bot fleet (one pod = one symbol)")
    p.add_argument("--num-bots", type=int,
                   default=int(os.environ.get("NUM_BOTS", "5")),
                   help="bots in this pod — the pressure dial on the symbol")
    p.add_argument("--plan", default=os.environ.get("PLAN_NAME", "quick"),
                   choices=list(TEST_PLANS.keys()))
    p.add_argument("--symbol", type=int,
                   default=int(os.environ.get("SYMBOL", "1")),
                   help="the single symbol every bot in this pod trades")
    p.add_argument("--order-divisor", type=int,
                   default=int(os.environ.get("ORDER_DIVISOR", "1")),
                   help="volume dial — each phase's total_orders is divided by this")
    p.add_argument("--pod-id", type=int,
                   default=int(os.environ.get("POD_ID", "0")),
                   help="unique pod id — offsets client_id/order_id so pods never collide")
    return p.parse_args()


def run():
    args = parse_args()
    asyncio.run(main(num_bots=args.num_bots, plan_name=args.plan,
                     symbol=args.symbol, divisor=args.order_divisor,
                     pod_id=args.pod_id))


if __name__ == "__main__":
    run()
