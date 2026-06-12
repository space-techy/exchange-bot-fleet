"""Environment-driven runtime settings for the production fleet."""

import os

URI = os.environ.get("ENGINE_URI", "ws://localhost:3001/ws")
GLOBAL_SEED = int(os.environ.get("GLOBAL_SEED", "42"))

# Identifies a contestant submission (team name + id). Used as the suffix on the
# Kafka topics and the prefix on the Redis keys, so every pod of a submission
# reads/writes the same place.
TEST_ID = os.environ.get("TEST_ID", "team1")

FLUSH_INTERVAL_S = float(os.environ.get("FLUSH_INTERVAL_S", "1.0"))

# Kafka (used when TELEMETRY_SINK=kafka). Comma-separated host:port list.
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Redis (used when PHASE_SOURCE=redis). A central controller drives the test by
# writing the current phase/rate under keys prefixed with the test_id.
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
