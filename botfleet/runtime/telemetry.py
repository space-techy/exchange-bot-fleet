"""Telemetry sinks + the buffering collector.

The sink is swappable (file no-op, or Kafka); the collector buffers events and
flushes them on a size threshold, a timer, or close.
"""

import asyncio
import json
import os
import re

from botfleet.runtime.settings import (
    FLUSH_INTERVAL_S,
    KAFKA_BOOTSTRAP_SERVERS,
    TEST_ID,
)

# confluent-kafka (librdkafka) is only needed when TELEMETRY_SINK=kafka — import
# lazily so the default file/local path works without the dependency installed.
try:
    from confluent_kafka import Producer as KafkaProducer
except ImportError:
    KafkaProducer = None


# Kafka topic names allow only [a-zA-Z0-9._-]; team-name test_ids may not, so
# slugify before using one as a topic suffix.
_BAD_TOPIC_CHARS = re.compile(r"[^a-zA-Z0-9._-]")


def _topic_safe(s: str) -> str:
    return _BAD_TOPIC_CHARS.sub("-", s)


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
        # for e in events:
        #     print(e)
        pass

    def close(self):
        pass


class KafkaSink(TelemetrySink):
    """
    Routes events to two topics, split by event type:

        type == "order_sent"      ->  order-sent-{test_id}
        type == "order_response"  ->  order-response-{test_id}

    Every pod of a submission shares the same test_id, so all bots feed the same
    two topics. Events are keyed by order_id, so all events for one order land on
    the same partition and stay ordered (important for the validator).
    """

    def __init__(self, bot_id: int, test_id: str = TEST_ID):
        if KafkaProducer is None:
            raise RuntimeError(
                "TELEMETRY_SINK=kafka but confluent-kafka is not installed "
                "(pip install confluent-kafka)"
            )
        self.bot_id = bot_id
        tid = _topic_safe(test_id)
        self.sent_topic = f"order-sent-{tid}"
        self.response_topic = f"order-response-{tid}"
        # librdkafka does its network I/O on a background C thread, so produce()
        # is a fast non-blocking enqueue; we just poll() to serve callbacks.
        self.producer = KafkaProducer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "acks": "1",                            # leader ack — throughput/durability balance
            "linger.ms": 50,                        # small batching window
            "compression.type": "lz4",
            "queue.buffering.max.messages": 1_000_000,
        })

    def write_batch(self, events):
        for ev in events:
            topic = self.sent_topic if ev.get("type") == "order_sent" else self.response_topic
            # Key by the request's ticket so a request's order_sent and its
            # order_response land on the same partition, in order.
            key = ev.get("client_order_id")
            self._produce(topic, key, json.dumps(ev))
        # serve delivery callbacks and free the queue (non-blocking)
        self.producer.poll(0)

    def _produce(self, topic: str, key, value: str):
        key_b = str(key).encode("utf-8") if key is not None else None
        try:
            self.producer.produce(topic, key=key_b, value=value.encode("utf-8"))
        except BufferError:
            # local queue full — block briefly to let it drain, then retry once
            self.producer.poll(0.5)
            self.producer.produce(topic, key=key_b, value=value.encode("utf-8"))

    def close(self):
        # block until every buffered message is delivered (or fails)
        self.producer.flush(10)


def make_sink(bot_id: int) -> TelemetrySink:
    kind = os.environ.get("TELEMETRY_SINK", "kafka").lower()
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
