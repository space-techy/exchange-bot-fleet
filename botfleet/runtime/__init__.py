"""
Production bot fleet runtime.

  settings.py      environment-driven configuration (engine URI, seed, flush, ...).
  telemetry.py     pluggable telemetry sinks + the buffering collector.
  coordination.py  pluggable phase coordinators (local plan now, Redis later).
  protocol.py      engine-response handling (latency, generator sync, telemetry).
  loops.py         the async sender / receiver hot loops.
  fleet.py         the per-bot driver + entrypoint wiring.
"""
