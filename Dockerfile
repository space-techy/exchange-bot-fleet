# ── Bot-fleet pod image ───────────────────────────────────────────────────────
# One container = one pod = a fleet of bots all trading ONE symbol.
# An orchestrator spawns many of these, each with its own POD_ID + SYMBOL.
#
# Build:
#   docker build -t botfleet .
#
# Run (all knobs are env vars; CLI flags also work and override env):
#   docker run --rm \
#     -e POD_ID=3 -e SYMBOL=2 -e NUM_BOTS=10 -e PLAN_NAME=standard \
#     -e ENGINE_URI=ws://engine:3001/ws \
#     -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
#     -e TEST_ID=team1 \
#     botfleet

FROM python:3.12-slim

# Never buffer stdout/stderr — the orchestrator needs to see logs live.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Runtime deps only. matplotlib (visualizers) is deliberately left out — it
# drags in ~100MB of GUI libs and the pod never plots anything.
# confluent-kafka ships manylinux wheels with librdkafka bundled, so no
# apt build tools are needed.
RUN pip install --no-cache-dir \
        websockets \
        confluent-kafka \
        redis

# Code last, so dependency layers stay cached across code changes.
COPY botfleet/ ./botfleet/
COPY run_fleet.py .

# Exec form (no shell wrapper): SIGTERM from `docker stop` / the orchestrator
# reaches Python directly, triggering the fleet's graceful shutdown — bots stop
# sending, the receiver drains in-flight responses, telemetry flushes to Kafka.
ENTRYPOINT ["python", "run_fleet.py"]
