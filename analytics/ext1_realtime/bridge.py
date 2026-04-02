"""
Extension 1 — NATS → Kafka bridge.

Subscribes to:
  sim.snapshots    → Kafka topic: sim-snapshots
  sim.obs.>        → Kafka topic: sim-observations

Each Kafka message is keyed by vehicle_id (or "snapshot") and the value is
the raw JSON string from NATS.

Usage:
    python analytics/ext1_realtime/bridge.py
    python analytics/ext1_realtime/bridge.py --nats nats://localhost:4222 --kafka localhost:9092
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path

import nats
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from analytics.shared.config import (
    NATS_URL, KAFKA_BOOTSTRAP,
    KAFKA_TOPIC_SNAPSHOTS, KAFKA_TOPIC_OBSERVATIONS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bridge")


class NatsKafkaBridge:
    def __init__(self, nats_url: str, kafka_bootstrap: str):
        self.nats_url = nats_url
        self.kafka_bootstrap = kafka_bootstrap
        self._nc = None
        self._producer: KafkaProducer | None = None
        self._stop = asyncio.Event()
        self._snap_count = 0
        self._obs_count  = 0

    def _make_producer(self) -> KafkaProducer:
        return KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap,
            value_serializer=lambda v: v if isinstance(v, bytes) else v.encode(),
            key_serializer=lambda k: k.encode() if k else None,
            acks="all",
            retries=3,
            max_block_ms=5000,
        )

    async def _on_snapshot(self, msg) -> None:
        raw = msg.data
        self._producer.send(
            KAFKA_TOPIC_SNAPSHOTS,
            key="snapshot",
            value=raw,
        )
        self._snap_count += 1
        if self._snap_count % 200 == 0:
            log.info("snapshots forwarded: %d", self._snap_count)

    async def _on_observation(self, msg) -> None:
        # Subject: sim.obs.{vehicle_id}
        parts = msg.subject.split(".")
        vehicle_id = parts[-1] if len(parts) >= 3 else "unknown"
        self._producer.send(
            KAFKA_TOPIC_OBSERVATIONS,
            key=vehicle_id,
            value=msg.data,
        )
        self._obs_count += 1

    async def run(self) -> None:
        log.info("Connecting to Kafka: %s", self.kafka_bootstrap)
        try:
            self._producer = self._make_producer()
        except NoBrokersAvailable:
            log.error("Kafka not reachable at %s — is the analytics stack running?", self.kafka_bootstrap)
            log.error("Start it with:  docker compose -f analytics/docker-compose.analytics.yml up -d")
            sys.exit(1)

        log.info("Connecting to NATS: %s", self.nats_url)
        self._nc = await nats.connect(self.nats_url)
        log.info("Bridge active — forwarding sim.snapshots and sim.obs.>")

        await self._nc.subscribe("sim.snapshots", cb=self._on_snapshot)
        await self._nc.subscribe("sim.obs.>",     cb=self._on_observation)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._stop.set)

        await self._stop.wait()
        log.info("Shutting down — snapshots=%d obs=%d", self._snap_count, self._obs_count)
        self._producer.flush()
        self._producer.close()
        await self._nc.drain()


def main() -> None:
    ap = argparse.ArgumentParser(description="ATS NATS→Kafka bridge")
    ap.add_argument("--nats",  default=NATS_URL,        help="NATS server URL")
    ap.add_argument("--kafka", default=KAFKA_BOOTSTRAP,  help="Kafka bootstrap server")
    args = ap.parse_args()

    bridge = NatsKafkaBridge(nats_url=args.nats, kafka_bootstrap=args.kafka)
    asyncio.run(bridge.run())


if __name__ == "__main__":
    main()
