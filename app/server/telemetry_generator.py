"""
PharmaCo Manufacturing Real-Time Demo — Synthetic Plant Telemetry Generator
============================================================================

Generates synthetic shop-floor telemetry for a single (fictional) PharmaCo-style
pharmaceutical manufacturing plant. The output is designed to be ingested through
**Zerobus Ingest** into a Unity Catalog managed Delta bronze table, then processed
by Spark Structured Streaming Real-Time Mode.

IMPORTANT: All data produced here is 100% SYNTHETIC. It contains no real patient,
employee, product, batch, or production information of any kind.

Design goals
------------
* Pure Python standard library only (runs in notebooks, jobs, the app backend, or
  a laptop) — no third-party dependencies.
* Deterministic replay: a fixed ``seed`` + simulated clock reproduces an identical
  event stream, so demos are repeatable.
* Controllable scenario injection: temperature excursion, vibration degradation,
  pressure instability, line slowdown, correlated alarm burst, and recovery.
* Emits one event PER SENSOR READING, matching the bronze table schema exactly.

Each ``tick()`` advances the simulation by one interval and returns a list of
telemetry events (dicts). The caller decides what to do with them (print, write to
Delta, hand to the Zerobus SDK, etc.).
"""

from __future__ import annotations

import dataclasses
import hashlib
import math
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Iterable

PAYLOAD_VERSION = "1.0.0"
SITE_ID = "SITE-EU01-SYN"        # synthetic site
PLANT_ID = "PLANT-EU01-OSD-SYN"    # synthetic plant


# --------------------------------------------------------------------------- #
# Domain enums
# --------------------------------------------------------------------------- #
class OperatingState(str, Enum):
    RUNNING = "RUNNING"
    IDLE = "IDLE"
    STARVED = "STARVED"
    BLOCKED = "BLOCKED"
    MAINTENANCE = "MAINTENANCE"
    FAULT = "FAULT"


class Scenario(str, Enum):
    NORMAL = "normal"
    TEMP_EXCURSION = "temp_excursion"
    VIBRATION_DEGRADATION = "vibration_degradation"
    PRESSURE_INSTABILITY = "pressure_instability"
    LINE_SLOWDOWN = "line_slowdown"
    ALARM_BURST = "alarm_burst"
    RECOVERY = "recovery"


# Sensor catalog: nominal operating band + engineering unit.
# These bands are ALSO published on every event (nominal_min / nominal_max) so the
# downstream RTM pipeline can do threshold detection without a side lookup, and so
# the demo stays self-describing.
@dataclass(frozen=True)
class SensorSpec:
    sensor_type: str
    unit: str
    nominal_min: float
    nominal_max: float
    base: float          # typical steady-state value
    noise: float         # 1-sigma gaussian noise in steady state


SENSOR_SPECS: dict[str, SensorSpec] = {
    "temperature":   SensorSpec("temperature", "degC", 55.0, 75.0, 65.0, 1.2),
    "vibration":     SensorSpec("vibration", "mm_s", 0.5, 4.5, 1.8, 0.25),
    "pressure":      SensorSpec("pressure", "bar", 3.0, 6.0, 4.5, 0.15),
    "motor_current": SensorSpec("motor_current", "amp", 18.0, 42.0, 30.0, 1.5),
    "speed":         SensorSpec("speed", "rpm", 900.0, 1500.0, 1200.0, 20.0),
    "throughput":    SensorSpec("throughput", "units_min", 80.0, 140.0, 120.0, 4.0),
    "energy":        SensorSpec("energy", "kwh", 20.0, 60.0, 40.0, 2.0),
}


# --------------------------------------------------------------------------- #
# Plant topology
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Asset:
    asset_id: str
    asset_type: str
    line_id: str


@dataclass(frozen=True)
class Line:
    line_id: str
    product_family: str
    assets: tuple[Asset, ...]


def _default_plant() -> tuple[Line, ...]:
    """1 plant, 3 lines, 4-6 assets/line. Names are synthetic pharma-style."""
    topology = {
        "LINE-A": ("OSD_TABLET", [
            ("A-DISPENSE", "dispenser"),
            ("A-GRANULATE", "granulator"),
            ("A-BLEND", "blender"),
            ("A-COMPRESS", "tablet_press"),
            ("A-COAT", "coater"),
            ("A-INSPECT", "vision_inspector"),
        ]),
        "LINE-B": ("STERILE_VIAL", [
            ("B-WASH", "vial_washer"),
            ("B-DEPYRO", "depyrogenation_tunnel"),
            ("B-FILL", "aseptic_filler"),
            ("B-STOPPER", "stoppering_unit"),
            ("B-LYO", "lyophilizer"),
        ]),
        "LINE-C": ("PACKAGING", [
            ("C-LABEL", "labeler"),
            ("C-CARTON", "cartoner"),
            ("C-SERIALIZE", "serialization_unit"),
            ("C-CASEPACK", "case_packer"),
        ]),
    }
    lines = []
    for line_id, (family, assets) in topology.items():
        line_assets = tuple(Asset(aid, atype, line_id) for aid, atype in assets)
        lines.append(Line(line_id, family, line_assets))
    return tuple(lines)


# --------------------------------------------------------------------------- #
# Generator configuration & runtime state
# --------------------------------------------------------------------------- #
@dataclass
class GeneratorConfig:
    seed: int = 42
    # events are grouped by "tick"; each RUNNING asset emits one reading per sensor
    # per tick. tick_seconds controls the simulated cadence.
    tick_seconds: float = 1.0
    anomaly_probability: float = 0.01     # baseline chance of a transient blip
    burst_multiplier: float = 1.0         # >1 emits extra readings per tick (load test)
    deterministic_time: bool = True       # simulated clock (True) vs wall clock (False)
    start_time_iso: str | None = None     # simulated clock start; defaults to now (UTC)


@dataclass
class _ScenarioState:
    """Active scenario injection with a countdown of remaining ticks."""
    scenario: Scenario = Scenario.NORMAL
    ticks_remaining: int = 0
    target_line: str | None = None
    target_asset: str | None = None
    intensity: float = 1.0
    elapsed: int = 0  # ticks since this scenario began (drives ramps/trends)


@dataclass
class _AssetRuntime:
    operating_state: OperatingState = OperatingState.RUNNING
    # per-asset multiplicative offsets that scenarios nudge over time
    temp_offset: float = 0.0
    vibration_offset: float = 0.0
    pressure_wobble: float = 0.0
    speed_factor: float = 1.0
    throughput_factor: float = 1.0


class TelemetryGenerator:
    """Stateful, controllable synthetic telemetry generator.

    Usage::

        gen = TelemetryGenerator(GeneratorConfig(seed=7))
        gen.inject_scenario(Scenario.TEMP_EXCURSION, duration_ticks=60, target_asset="A-COMPRESS")
        for _ in range(120):
            events = gen.tick()
            ... hand events to Zerobus Ingest ...
    """

    def __init__(self, config: GeneratorConfig | None = None,
                 lines: tuple[Line, ...] | None = None):
        self.config = config or GeneratorConfig()
        self.lines = lines or _default_plant()
        self._rng = random.Random(self.config.seed)
        self._seq = 0
        self._tick_index = 0
        self._scenario = _ScenarioState()
        self._assets: dict[str, _AssetRuntime] = {}
        self._asset_index: dict[str, Asset] = {}
        self._line_of: dict[str, str] = {}
        for line in self.lines:
            for asset in line.assets:
                self._assets[asset.asset_id] = _AssetRuntime()
                self._asset_index[asset.asset_id] = asset
                self._line_of[asset.asset_id] = line.line_id
        # batch ids per line (synthetic)
        self._batch: dict[str, str] = {
            line.line_id: self._new_batch(line) for line in self.lines
        }
        # simulated clock
        if self.config.start_time_iso:
            self._sim_time = datetime.fromisoformat(self.config.start_time_iso)
            if self._sim_time.tzinfo is None:
                self._sim_time = self._sim_time.replace(tzinfo=timezone.utc)
        else:
            self._sim_time = datetime.now(timezone.utc)

    # ---------------- public API ---------------- #
    def all_assets(self) -> list[Asset]:
        return list(self._asset_index.values())

    def all_lines(self) -> list[Line]:
        return list(self.lines)

    def inject_scenario(self, scenario: Scenario | str, duration_ticks: int = 60,
                        target_line: str | None = None,
                        target_asset: str | None = None,
                        intensity: float = 1.0) -> _ScenarioState:
        """Arm a scenario. If no target is given a sensible default is chosen
        deterministically so replays are reproducible."""
        scenario = Scenario(scenario)
        # pick deterministic defaults where the caller didn't specify a target
        if scenario in (Scenario.TEMP_EXCURSION, Scenario.VIBRATION_DEGRADATION,
                        Scenario.PRESSURE_INSTABILITY) and not target_asset:
            target_asset = {
                Scenario.TEMP_EXCURSION: "A-COMPRESS",
                Scenario.VIBRATION_DEGRADATION: "B-FILL",
                Scenario.PRESSURE_INSTABILITY: "B-LYO",
            }[scenario]
        if scenario in (Scenario.LINE_SLOWDOWN, Scenario.ALARM_BURST) and not target_line:
            target_line = "LINE-A" if scenario == Scenario.LINE_SLOWDOWN else "LINE-B"
        self._scenario = _ScenarioState(
            scenario=scenario, ticks_remaining=max(1, duration_ticks),
            target_line=target_line, target_asset=target_asset,
            intensity=max(0.1, intensity), elapsed=0,
        )
        return self._scenario

    def clear_scenario(self) -> None:
        self._scenario = _ScenarioState()

    @property
    def scenario_state(self) -> _ScenarioState:
        return self._scenario

    def tick(self) -> list[dict]:
        """Advance one interval; return the list of telemetry events emitted."""
        if not self.config.deterministic_time:
            self._sim_time = datetime.now(timezone.utc)  # stamp events at emission time
        self._advance_scenario()
        events: list[dict] = []
        reps = max(1, int(round(self.config.burst_multiplier)))
        for line in self.lines:
            for asset in line.assets:
                for _ in range(reps):
                    events.extend(self._emit_asset(line, asset))
        self._tick_index += 1
        if self.config.deterministic_time:
            self._sim_time += timedelta(seconds=self.config.tick_seconds)
        else:
            self._sim_time = datetime.now(timezone.utc)
        return events

    def stream(self, ticks: int | None = None,
               real_time: bool = False) -> Iterable[dict]:
        """Generator that yields events one at a time. If ``real_time`` sleeps
        ``tick_seconds`` between ticks (wall clock pacing)."""
        n = 0
        while ticks is None or n < ticks:
            for ev in self.tick():
                yield ev
            n += 1
            if real_time:
                time.sleep(self.config.tick_seconds)

    # ---------------- internals ---------------- #
    def _new_batch(self, line: Line) -> str:
        # synthetic, human-readable, deterministic-ish batch id
        token = self._rng.randint(1000, 9999)
        return f"BATCH-{line.line_id[-1]}-{token}"

    def _advance_scenario(self) -> None:
        s = self._scenario
        if s.scenario == Scenario.NORMAL or s.ticks_remaining <= 0:
            # relax all assets back toward nominal
            for rt in self._assets.values():
                rt.temp_offset *= 0.85
                rt.vibration_offset *= 0.85
                rt.pressure_wobble *= 0.85
                rt.speed_factor += (1.0 - rt.speed_factor) * 0.2
                rt.throughput_factor += (1.0 - rt.throughput_factor) * 0.2
                if rt.operating_state == OperatingState.FAULT and self._rng.random() < 0.1:
                    rt.operating_state = OperatingState.RUNNING
            if s.ticks_remaining <= 0 and s.scenario != Scenario.NORMAL:
                self._scenario = _ScenarioState()  # scenario expired -> normal
            return

        s.elapsed += 1
        s.ticks_remaining -= 1
        # ramp to full intensity over ~45 ticks, then HOLD at peak until the scenario
        # is cleared/changed (so a selected demo scenario persists, not a one-shot blip).
        progress = min(1.0, s.elapsed / 45.0)

        if s.scenario == Scenario.TEMP_EXCURSION:
            rt = self._assets[s.target_asset]
            rt.temp_offset = 18.0 * s.intensity * progress  # climbs above nominal max
        elif s.scenario == Scenario.VIBRATION_DEGRADATION:
            rt = self._assets[s.target_asset]
            rt.vibration_offset = 6.0 * s.intensity * progress  # bearing trend (breaches ~4.5 band)
        elif s.scenario == Scenario.PRESSURE_INSTABILITY:
            rt = self._assets[s.target_asset]
            rt.pressure_wobble = 1.5 * s.intensity
        elif s.scenario == Scenario.LINE_SLOWDOWN:
            for asset in self._line_assets(s.target_line):
                rt = self._assets[asset.asset_id]
                rt.speed_factor = 1.0 - 0.45 * s.intensity * progress
                rt.throughput_factor = 1.0 - 0.55 * s.intensity * progress
        elif s.scenario == Scenario.ALARM_BURST:
            # correlated multi-asset excursions across the target line
            for i, asset in enumerate(self._line_assets(s.target_line)):
                rt = self._assets[asset.asset_id]
                rt.temp_offset = 12.0 * s.intensity if i % 2 == 0 else rt.temp_offset
                rt.vibration_offset = 3.0 * s.intensity if i % 2 == 1 else rt.vibration_offset
                if i == 0 and s.elapsed == 1:
                    rt.operating_state = OperatingState.FAULT
        elif s.scenario == Scenario.RECOVERY:
            # maintenance intervention: force MAINTENANCE then relax to RUNNING
            targets = ([self._asset_index[s.target_asset]] if s.target_asset
                       else self._line_assets(s.target_line or "LINE-A"))
            for asset in targets:
                rt = self._assets[asset.asset_id]
                rt.operating_state = (OperatingState.MAINTENANCE if progress < 0.5
                                      else OperatingState.RUNNING)
                rt.temp_offset *= 0.6
                rt.vibration_offset *= 0.6
                rt.speed_factor += (1.0 - rt.speed_factor) * 0.3
                rt.throughput_factor += (1.0 - rt.throughput_factor) * 0.3

    def _line_assets(self, line_id: str) -> tuple[Asset, ...]:
        for line in self.lines:
            if line.line_id == line_id:
                return line.assets
        return tuple()

    def _emit_asset(self, line: Line, asset: Asset) -> list[dict]:
        rt = self._assets[asset.asset_id]
        # organic state transitions (rare) when not driven by a scenario
        if rt.operating_state not in (OperatingState.MAINTENANCE, OperatingState.FAULT):
            roll = self._rng.random()
            if roll < 0.002:
                rt.operating_state = OperatingState.STARVED
            elif roll < 0.004:
                rt.operating_state = OperatingState.BLOCKED
            elif roll < 0.02:
                rt.operating_state = OperatingState.IDLE
            elif rt.operating_state in (OperatingState.STARVED, OperatingState.BLOCKED,
                                        OperatingState.IDLE) and roll > 0.3:
                rt.operating_state = OperatingState.RUNNING

        ts = self._sim_time
        events = []
        for sensor_type, spec in SENSOR_SPECS.items():
            value = self._sensor_value(rt, spec, sensor_type)
            self._seq += 1
            # include seed so distinct runs produce distinct logical ids (avoids
            # cross-run dedup collisions); same seed still replays identical ids.
            source_event_id = self._deterministic_id(self.config.seed, asset.asset_id,
                                                     sensor_type, self._seq)
            events.append({
                # event_id: unique per physical emission (includes seq); the RTM
                # layer dedups on source_event_id which is stable for a logical reading
                "event_id": self._deterministic_id(asset.asset_id, sensor_type,
                                                    self._seq, self._tick_index),
                "source_event_id": source_event_id,
                "event_ts": ts,
                "ingested_ts": None,  # stamped by the ingestion layer / Zerobus writer
                "site_id": SITE_ID,
                "plant_id": PLANT_ID,
                "line_id": line.line_id,
                "asset_id": asset.asset_id,
                "batch_id": self._batch[line.line_id],
                "product_family": line.product_family,
                "sensor_type": sensor_type,
                "sensor_value": round(value, 4),
                "unit": spec.unit,
                "operating_state": rt.operating_state.value,
                "nominal_min": spec.nominal_min,
                "nominal_max": spec.nominal_max,
                "sequence_number": self._seq,
                "scenario": self._scenario.scenario.value,
                "payload_version": PAYLOAD_VERSION,
            })
        return events

    def _sensor_value(self, rt: _AssetRuntime, spec: SensorSpec, sensor_type: str) -> float:
        state = rt.operating_state
        # non-running states suppress dynamic sensors
        if state in (OperatingState.IDLE, OperatingState.MAINTENANCE):
            if sensor_type in ("speed", "throughput", "motor_current"):
                return max(0.0, self._rng.gauss(spec.base * 0.03, spec.noise * 0.2))
        if state == OperatingState.STARVED and sensor_type == "throughput":
            return max(0.0, self._rng.gauss(spec.base * 0.4, spec.noise))
        if state == OperatingState.BLOCKED and sensor_type in ("throughput", "speed"):
            return max(0.0, self._rng.gauss(spec.base * 0.2, spec.noise))
        if state == OperatingState.FAULT:
            if sensor_type in ("speed", "throughput"):
                return max(0.0, self._rng.gauss(spec.base * 0.05, spec.noise * 0.3))
            if sensor_type == "vibration":
                return spec.base + rt.vibration_offset + abs(self._rng.gauss(3.0, 0.6))

        value = self._rng.gauss(spec.base, spec.noise)
        # transient anomaly blip
        if self._rng.random() < self.config.anomaly_probability:
            value += self._rng.choice([-1, 1]) * spec.noise * 6

        if sensor_type == "temperature":
            value += rt.temp_offset
        elif sensor_type == "vibration":
            value += rt.vibration_offset
        elif sensor_type == "pressure":
            # oscillation for instability scenario
            value += rt.pressure_wobble * math.sin(self._tick_index / 3.0)
        elif sensor_type == "speed":
            value *= rt.speed_factor
        elif sensor_type == "throughput":
            value *= rt.throughput_factor
        elif sensor_type == "motor_current":
            # motor current tracks vibration/temperature stress mildly
            value += 0.4 * rt.vibration_offset + 0.15 * rt.temp_offset
        elif sensor_type == "energy":
            value += 0.2 * rt.temp_offset
        return value

    @staticmethod
    def _deterministic_id(*parts) -> str:
        raw = "|".join(str(p) for p in parts)
        return hashlib.sha1(raw.encode()).hexdigest()[:24]


def config_summary(gen: TelemetryGenerator) -> dict:
    """Small dict describing the plant topology, for READMEs / the app /about panel."""
    return {
        "site_id": SITE_ID,
        "plant_id": PLANT_ID,
        "payload_version": PAYLOAD_VERSION,
        "seed": gen.config.seed,
        "lines": [
            {
                "line_id": line.line_id,
                "product_family": line.product_family,
                "assets": [{"asset_id": a.asset_id, "asset_type": a.asset_type}
                           for a in line.assets],
            }
            for line in gen.lines
        ],
        "sensors": [dataclasses.asdict(s) for s in SENSOR_SPECS.values()],
        "operating_states": [s.value for s in OperatingState],
        "scenarios": [s.value for s in Scenario],
    }


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Synthetic plant telemetry generator (SYNTHETIC DATA ONLY)")
    parser.add_argument("--ticks", type=int, default=5, help="number of ticks to emit")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--scenario", type=str, default="normal", choices=[s.value for s in Scenario])
    parser.add_argument("--duration", type=int, default=60, help="scenario duration in ticks")
    parser.add_argument("--summary", action="store_true", help="print plant topology summary and exit")
    args = parser.parse_args()

    gen = TelemetryGenerator(GeneratorConfig(seed=args.seed))
    if args.summary:
        print(json.dumps(config_summary(gen), indent=2, default=str))
        raise SystemExit(0)

    if args.scenario != "normal":
        gen.inject_scenario(args.scenario, duration_ticks=args.duration)

    total = 0
    for t in range(args.ticks):
        evs = gen.tick()
        total += len(evs)
        print(f"# tick {t}: {len(evs)} events (scenario={gen.scenario_state.scenario.value})")
        # print a couple sample events per tick
        for ev in evs[:2]:
            print(json.dumps(ev, default=str))
    print(f"# total events: {total}")
