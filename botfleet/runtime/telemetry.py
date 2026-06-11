"""Telemetry sinks + the buffering collector.

The sink is swappable (file no-op now, Kafka later); the collector buffers events
and flushes them on a size threshold, a timer, or close.
"""

import asyncio
import os

from botfleet.runtime.settings import FLUSH_INTERVAL_S


# ─────────────────────────── Telemetry sinks ─────────────────────────────────

class TelemetrySink:
    """Pluggable sink. Default: append-only JSONL per bot. Swap for KafkaSink later."""

    def write_batch(self, events: list[dict]): ...
    def close(self): ...


class FileSink(TelemetrySink):
    def __init__(self, bot_id: int):
        self.bot_id = bot_id

    def write_batch(self, events):
        # Buffered for the (future) real sink. We deliberately don't print
        # per-order events — too noisy. The driver prints phase summaries instead.
        for e in events:
            print(e)

    def close(self):
        pass


class KafkaSink(TelemetrySink):
    """
    Stub. When kafka-python is wired in, instantiate a KafkaProducer here and
    call producer.send(topic, json.dumps(ev).encode()) in write_batch.
    """
    def __init__(self, bot_id: int, topic: str = "bot_telemetry"):
        self.bot_id = bot_id
        self.topic = topic
        # self.producer = KafkaProducer(bootstrap_servers=...)

    def write_batch(self, events):
        # for ev in events: self.producer.send(self.topic, json.dumps(ev).encode())
        pass

    def close(self):
        # self.producer.flush(); self.producer.close()
        pass


def make_sink(bot_id: int) -> TelemetrySink:
    kind = os.environ.get("TELEMETRY_SINK", "file").lower()
    if kind == "kafka":
        return KafkaSink(bot_id)
    return FileSink(bot_id)


class TelemetryCollector:
    """Buffers events, flushes on threshold / timer / close. Sink is swappable."""

    BATCH_SIZE = 500

    def __init__(self, bot_id: int):
        self.sink = make_sink(bot_id)
        self.buf: list[dict] = []
        self.total_flushed = 0
        self._flush_task: asyncio.Task | None = None
        self._stop = False

    def record(self, ev: dict):
        self.buf.append(ev)
        if len(self.buf) >= self.BATCH_SIZE:
            self.flush()

    def flush(self):
        if not self.buf:
            return
        self.sink.write_batch(self.buf)
        self.total_flushed += len(self.buf)
        self.buf.clear()

    async def _periodic_flush(self):
        """Time-based flush so downstream sees fresh data even at low order rate."""
        try:
            while not self._stop:
                await asyncio.sleep(FLUSH_INTERVAL_S)
                self.flush()
        except asyncio.CancelledError:
            pass

    def start(self):
        if self._flush_task is None:
            self._flush_task = asyncio.create_task(self._periodic_flush())

    async def stop(self):
        self._stop = True
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        self.flush()
        self.sink.close()
