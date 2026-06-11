"""Environment-driven runtime settings for the production fleet."""

import os

URI = os.environ.get("ENGINE_URI", "ws://localhost:3001/ws")
GLOBAL_SEED = int(os.environ.get("GLOBAL_SEED", "42"))
TEST_ID = os.environ.get("TEST_ID", "local")
FLUSH_INTERVAL_S = float(os.environ.get("FLUSH_INTERVAL_S", "1.0"))
