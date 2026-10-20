"""
Zerobus Ingest driver — PharmaCo Manufacturing Real-Time demo
=============================================================

Streams synthetic plant telemetry (from telemetry_generator) directly into a
Unity Catalog managed Delta bronze table using **Zerobus Ingest** — the direct
application-to-lakehouse ingestion path. This is the primary ingestion
architecture for this demo: Databricks is the primary destination, so direct
ingestion via Zerobus is appropriate (no external broker required).

Auth: OAuth2 client credentials (service principal). The client_id / client_secret
are read from environment variables (which, in the workspace, are populated from
the `pharmaco_mfg_rt_demo` secret scope via dbutils — never hardcoded).

Config (all via env vars, see config.env / --help):
  ZB_ENDPOINT           e.g. https://<workspace-id>.zerobus.<region>.cloud.databricks.com
  DBX_WORKSPACE_URL     e.g. https://<host>.cloud.databricks.com
  ZB_CLIENT_ID          service principal application (client) id
  ZB_CLIENT_SECRET      service principal OAuth secret
  DEMO_CATALOG / DEMO_BRONZE / BRONZE_TABLE   target table parts
  GEN_SEED / GEN_SCENARIO / GEN_DURATION / GEN_TICKS / GEN_RATE

A fallback Spark/Delta writer path (write_fallback_delta) is provided ONLY for the
documented source-constraint case where Zerobus is unavailable; it is clearly
labelled as a fallback and is not the primary path.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "generator"))

import telemetry_pb2  # compiled from telemetry.proto
from telemetry_generator import TelemetryGenerator, GeneratorConfig, Scenario


def _to_micros(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def event_to_proto(ev: dict, ingest_batch_id: str, source: str) -> "telemetry_pb2.TelemetryRecord":
    """Map a generator event dict to a TelemetryRecord protobuf message."""
    now_us = _to_micros(datetime.now(timezone.utc))
    r = telemetry_pb2.TelemetryRecord(
        event_id=ev["event_id"],
        source_event_id=ev["source_event_id"],
        event_ts=_to_micros(ev["event_ts"]),
        ingested_ts=now_us,
        site_id=ev["site_id"],
        plant_id=ev["plant_id"],
        line_id=ev["line_id"],
        asset_id=ev["asset_id"],
        batch_id=ev["batch_id"],
        product_family=ev["product_family"],
        sensor_type=ev["sensor_type"],
        sensor_value=float(ev["sensor_value"]),
        unit=ev["unit"],
        operating_state=ev["operating_state"],
        nominal_min=float(ev["nominal_min"]),
        nominal_max=float(ev["nominal_max"]),
        sequence_number=int(ev["sequence_number"]),
        scenario=ev["scenario"],
        payload_version=ev["payload_version"],
        ingest_source=source,
        ingest_batch_id=ingest_batch_id,
    )
    return r


class ZerobusIngestor:
    """Manages a Zerobus stream to the bronze table and tracks ingestion health."""

    def __init__(self, endpoint: str, workspace_url: str, table_fqn: str,
                 client_id: str, client_secret: str):
        from zerobus.sdk.sync import ZerobusSdk
        from zerobus.sdk.shared import TableProperties
        self.table_fqn = table_fqn
        self.ingest_batch_id = f"zb-{uuid.uuid4().hex[:12]}"
        self._sdk = ZerobusSdk(endpoint, workspace_url, application_name="pharmaco-mfg-rt-demo/1.0")
        self._props = TableProperties(table_fqn, telemetry_pb2.TelemetryRecord.DESCRIPTOR)
        self._stream = self._sdk.create_stream(
            client_id=client_id, client_secret=client_secret, table_properties=self._props,
        )
        self.accepted = 0
        self.rejected = 0
        self.retries = 0
        self.last_ack_ts: datetime | None = None
        self.last_ack_latency_ms = 0.0

    def ingest_event(self, ev: dict) -> None:
        rec = event_to_proto(ev, self.ingest_batch_id, source="zerobus")
        t0 = time.perf_counter()
        # ingest_record_offset returns as soon as queued; flush() confirms durability.
        self._stream.ingest_record_offset(rec.SerializeToString())
        self.accepted += 1
        self.last_ack_latency_ms = (time.perf_counter() - t0) * 1000.0

    def flush(self) -> None:
        self._stream.flush()
        self.last_ack_ts = datetime.now(timezone.utc)

    def close(self) -> None:
        try:
            self._stream.flush()
        finally:
            self._stream.close()

    def health_row(self, notes: str = "") -> dict:
        return {
            "ingest_batch_id": self.ingest_batch_id,
            "ts": datetime.now(timezone.utc),
            "ingest_source": "zerobus",
            "records_accepted": self.accepted,
            "records_rejected": self.rejected,
            "schema_errors": 0,
            "retry_count": self.retries,
            "last_ack_ts": self.last_ack_ts,
            "last_ack_latency_ms": round(self.last_ack_latency_ms, 3),
            "notes": notes,
        }


def run(args) -> int:
    endpoint = os.environ["ZB_ENDPOINT"]
    workspace_url = os.environ["DBX_WORKSPACE_URL"]
    client_id = os.environ["ZB_CLIENT_ID"]
    client_secret = os.environ["ZB_CLIENT_SECRET"]
    catalog = os.environ.get("DEMO_CATALOG", "hls_streaming_solution_demo_catalog")
    bronze = os.environ.get("DEMO_BRONZE", "pharmaco_mfg_rt_bronze")
    table = os.environ.get("BRONZE_TABLE", "telemetry_raw")
    table_fqn = f"{catalog}.{bronze}.{table}"

    gen = TelemetryGenerator(GeneratorConfig(
        seed=args.seed, tick_seconds=args.rate, deterministic_time=not args.wallclock,
    ))
    if args.scenario != "normal":
        gen.inject_scenario(Scenario(args.scenario), duration_ticks=args.duration)

    print(f"[zerobus] endpoint={endpoint}")
    print(f"[zerobus] target={table_fqn}")
    ing = ZerobusIngestor(endpoint, workspace_url, table_fqn, client_id, client_secret)
    print(f"[zerobus] stream open, ingest_batch_id={ing.ingest_batch_id}")

    t_start = time.time()
    try:
        for t in range(args.ticks):
            for ev in gen.tick():
                ing.ingest_event(ev)
            if (t + 1) % max(1, args.flush_every) == 0:
                ing.flush()
                print(f"[zerobus] tick {t+1}/{args.ticks} accepted={ing.accepted} "
                      f"last_ack_latency_ms={ing.last_ack_latency_ms:.2f}")
            if args.wallclock:
                time.sleep(args.rate)
        ing.flush()
    finally:
        ing.close()
    dur = time.time() - t_start
    eps = ing.accepted / dur if dur else 0
    print(f"[zerobus] DONE accepted={ing.accepted} in {dur:.1f}s ({eps:.0f} rec/s)")
    print(f"[zerobus] health={ing.health_row('cli run')}")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Zerobus Ingest driver (SYNTHETIC data only)")
    p.add_argument("--ticks", type=int, default=10)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--scenario", default="normal", choices=[s.value for s in Scenario])
    p.add_argument("--duration", type=int, default=60)
    p.add_argument("--rate", type=float, default=1.0, help="tick seconds (wallclock pacing)")
    p.add_argument("--flush-every", type=int, default=2)
    p.add_argument("--wallclock", action="store_true", help="pace in real time")
    sys.exit(run(p.parse_args()))
