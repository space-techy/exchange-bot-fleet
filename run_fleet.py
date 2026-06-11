#!/usr/bin/env python
"""Entrypoint: run a production bot-fleet pod.

    python run_fleet.py --plan standard --symbol 2 --num-bots 5
"""

from botfleet.runtime.fleet import run

if __name__ == "__main__":
    run()
