"""
Core domain: order generation and the test-plan catalogue.

  config.py     GeneratorConfig — every knob that shapes order generation.
  generator.py  OrderGenerator — turns a config into a deterministic order stream.
  plans.py      PHASE_CONFIGS + TEST_PLANS — named scenarios and how to sequence them.
"""

from botfleet.core.config import GeneratorConfig
from botfleet.core.generator import OrderGenerator, MAX_BOTS_PER_POD
from botfleet.core.plans import PHASE_CONFIGS, TEST_PLANS

__all__ = [
    "GeneratorConfig",
    "OrderGenerator",
    "MAX_BOTS_PER_POD",
    "PHASE_CONFIGS",
    "TEST_PLANS",
]
