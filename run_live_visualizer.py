#!/usr/bin/env python
"""Entrypoint: run the LIVE visualizer against a running engine.

Edit the constants at the top of botfleet/visualization/live.py (URI, NUM_BOTS,
PLAN_NAME, SYMBOL, ...) to configure the run.
"""

import asyncio

from botfleet.visualization.live import main

if __name__ == "__main__":
    asyncio.run(main())
