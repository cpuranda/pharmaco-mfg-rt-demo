"""In-app synthetic telemetry ingest controller.

Runs a background thread that generates synthetic plant telemetry and ingests it
into the bronze Delta table via **Zerobus Ingest**. Controlled at runtime by the
operator scenario controls. Fully SYNTHETIC data.

Graceful degradation: if the Zerobus SDK or credentials are unavailable in the
runtime, the controller still tracks state and reports availability=False so the
UI can show a clear 'ingest unavailable' state instead of failing.
"""
import os
import threading
import time
import traceback
from datetime import datetime, timezone

from .config import (ZB_ENDPOINT, ZB_CLIENT_ID, ZB_CLIENT_SECRET, BRONZE_TABLE,
                     CATALOG, BRONZE, get_workspace_host)

# generator + compiled proto are bundled alongside the server package
try:
    from . import telemetry_generator as tg
    _GEN_OK = True
except Exception:  # pragma: no cover
    tg = None
    _GEN_OK = False

VALID_SCENARIOS = ["normal", "temp_excursion", "vibration_degradation",
                   "pressure_instability", "line_slowdown", "alarm_burst", "recovery"]


class IngestController:
    def __init__(self):
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self.running = False
        self.scenario = "normal"
        self.rate = 1.0            # ticks per second
        self.seed = 42
        self._seed_pinned = False  # True once an operator explicitly sets a seed
        self.anomaly_secs = 60
        self.available = False
        self.reason = "not started"
        self.total_events = 0
        self.last_ingest_ts: datetime | None = None
        self.last_error: str | None = None
        self._stream = None
        self._gen = None
        self._pb2 = None

    # ---- lifecycle ----
    def status(self) -> dict:
        return {
            "running": self.running,
            "scenario": self.scenario,
            "rate": self.rate,
            "seed": self.seed,
            "anomaly_secs": self.anomaly_secs,
            "ingest_available": self.available,
            "reason": self.reason,
            "total_events": self.total_events,
            "last_ingest_ts": self.last_ingest_ts.isoformat() if self.last_ingest_ts else None,
            "last_error": self.last_error,
            "generator_ok": _GEN_OK,
            "target_table": f"{CATALOG}.{BRONZE}.{BRONZE_TABLE}",
            "valid_scenarios": VALID_SCENARIOS,
        }

    def configure(self, scenario=None, rate=None, seed=None, anomaly_secs=None):
        with self._lock:
            if scenario is not None:
                if scenario not in VALID_SCENARIOS:
                    raise ValueError(f"invalid scenario: {scenario}")
                self.scenario = scenario
                if self._gen is not None:
                    self._arm_scenario()
            if rate is not None:
                self.rate = max(0.1, min(20.0, float(rate)))
            if seed is not None:
                self.seed = int(seed)
                self._seed_pinned = True  # operator pinned a seed (for reproducible replay)
            if anomaly_secs is not None:
                self.anomaly_secs = max(5, int(anomaly_secs))

    def start(self):
        with self._lock:
            if self.running:
                return
            # Unless the operator pinned a specific seed (for reproducible replay),
            # use a unique seed per start so restarts don't re-emit source_event_ids
            # that already exist (which dedup would drop, freezing silver/gold).
            if not self._seed_pinned:
                self.seed = int(time.time()) % 2_000_000_000
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, daemon=True, name="ingest")
            self._thread.start()
            self.running = True

    def stop(self):
        with self._lock:
            self._stop.set()
            self.running = False

    # ---- internals ----
    def _init_stream(self) -> bool:
        if not _GEN_OK:
            self.reason = "generator module not available"
            return False
        if not (ZB_ENDPOINT and ZB_CLIENT_ID and ZB_CLIENT_SECRET):
            self.reason = "Zerobus credentials/endpoint not configured"
            return False
        try:
            from . import telemetry_pb2  # compiled proto bundled with server
            from zerobus.sdk.sync import ZerobusSdk
            from zerobus.sdk.shared import TableProperties
            self._pb2 = telemetry_pb2
            table_fqn = f"{CATALOG}.{BRONZE}.{BRONZE_TABLE}"
            sdk = ZerobusSdk(ZB_ENDPOINT, get_workspace_host(),
                             application_name="pharmaco-mfg-rt-app/1.0")
            props = TableProperties(table_fqn, telemetry_pb2.TelemetryRecord.DESCRIPTOR)
            self._stream = sdk.create_stream(client_id=ZB_CLIENT_ID,
                                             client_secret=ZB_CLIENT_SECRET,
                                             table_properties=props)
            self.available = True
            self.reason = "ok"
            return True
        except Exception as e:
            self.available = False
            self.reason = f"zerobus init failed: {type(e).__name__}: {e}"
            self.last_error = traceback.format_exc()[-800:]
            return False

    def _new_gen(self):
        self._gen = tg.TelemetryGenerator(tg.GeneratorConfig(seed=self.seed, tick_seconds=1.0))
        self._arm_scenario()

    def _arm_scenario(self):
        if self.scenario == "normal":
            self._gen.clear_scenario()
        else:
            # Persist the selected scenario (ramps to peak in ~45s, then holds) until
            # the operator changes it. Large duration so it does not auto-expire.
            self._gen.inject_scenario(self.scenario, duration_ticks=10_000_000)

    def _to_micros(self, dt) -> int:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1_000_000)

    def _emit(self, ev, batch_id):
        pb = self._pb2
        now_us = self._to_micros(datetime.now(timezone.utc))
        rec = pb.TelemetryRecord(
            event_id=ev["event_id"], source_event_id=ev["source_event_id"],
            event_ts=self._to_micros(ev["event_ts"]), ingested_ts=now_us,
            site_id=ev["site_id"], plant_id=ev["plant_id"], line_id=ev["line_id"],
            asset_id=ev["asset_id"], batch_id=ev["batch_id"],
            product_family=ev["product_family"], sensor_type=ev["sensor_type"],
            sensor_value=float(ev["sensor_value"]), unit=ev["unit"],
            operating_state=ev["operating_state"], nominal_min=float(ev["nominal_min"]),
            nominal_max=float(ev["nominal_max"]), sequence_number=int(ev["sequence_number"]),
            scenario=ev["scenario"], payload_version=ev["payload_version"],
            ingest_source="zerobus-app", ingest_batch_id=batch_id)
        self._stream.ingest_record_nowait(rec.SerializeToString())

    def _run(self):
        import uuid
        if not self._init_stream():
            self.running = False
            return
        self._new_gen()
        batch_id = f"app-{uuid.uuid4().hex[:10]}"
        flush_counter = 0
        while not self._stop.is_set():
            try:
                events = self._gen.tick()
                for ev in events:
                    self._emit(ev, batch_id)
                self.total_events += len(events)
                self.last_ingest_ts = datetime.now(timezone.utc)
                flush_counter += 1
                if flush_counter % 3 == 0 and self._stream:
                    self._stream.flush()
            except Exception as e:
                self.last_error = f"{type(e).__name__}: {e}"
                self.available = False
                self.reason = self.last_error
                break
            time.sleep(1.0 / max(0.1, self.rate))
        try:
            if self._stream:
                self._stream.flush()
        except Exception:
            pass


controller = IngestController()
