"""
Synthetic SOP / work-instruction corpus generator — PharmaCo MFG RT demo.
=========================================================================

Produces a small library of *fictional* pharmaceutical-manufacturing Standard
Operating Procedures (SOPs) and troubleshooting work instructions, written to
markdown files under ``knowledge/sops/``. These feed:

  1. the app's code-path Fix Assistant (Vector Search index over the chunks), and
  2. an Agent Bricks **Knowledge Assistant** created over the same UC Volume.

IMPORTANT: 100% SYNTHETIC. These are demo documents only. They are NOT real
PharmaCo procedures and must never be used for actual GxP manufacturing.

Each SOP carries metadata (asset types / sensor types / scenarios it applies to)
so retrieval can be scoped to an alert's context, and is split into sections that
become individually-retrievable chunks.
"""
from __future__ import annotations

import json
import os
import textwrap

DISCLAIMER = ("> **SYNTHETIC DEMO DOCUMENT — NOT A REAL PROCEDURE.** Fictional content "
              "generated for a Databricks demonstration. Do not use for actual manufacturing.")

# --------------------------------------------------------------------------- #
# SOP definitions. `sections` is a list of (heading, body) tuples; each becomes
# one retrievable chunk. Bodies are dedented/stripped on write.
# --------------------------------------------------------------------------- #
SOPS: list[dict] = [
    {
        "sop_id": "SOP-TEMP-014",
        "title": "Tablet Press Over-Temperature / Thermal Excursion Response",
        "revision": "3.2",
        "asset_types": ["tablet_press", "granulator", "coater"],
        "sensor_types": ["temperature", "motor_current", "energy"],
        "scenarios": ["temp_excursion"],
        "sections": [
            ("Purpose & Scope",
             """This procedure defines the operator response when a compression or
             thermal-processing asset (e.g. tablet press, granulator, coater) reports a
             sustained temperature above its validated nominal band (55–75 °C for
             compression tooling). It applies to OSD Line-A assets, in particular the
             tablet press (A-COMPRESS)."""),
            ("Trigger Conditions",
             """Initiate this SOP when the process historian / monitoring app raises a
             temperature WARNING or CRITICAL alert, i.e. the sustained running average
             temperature exceeds 75 °C (WARNING) or exceeds 82.5 °C / +10% (CRITICAL),
             OR the anomaly score is >= 0.4 with a rising (TREND) signature."""),
            ("Immediate Actions",
             """1. Do NOT stop the press abruptly under load — reduce turret speed to
             50% setpoint to lower compaction heat generation.
             2. Verify the cooling/HVAC supply to the compression zone; confirm chilled-
             water valve is open and supply temperature is <= 12 °C.
             3. Check tooling lubrication and punch/die clearance; inspect for binding.
             4. Confirm the temperature sensor against a handheld reference to rule out
             a faulty RTD before treating as a real excursion.
             5. If temperature does not trend down within 10 minutes, initiate a
             controlled stop and segregate any product produced above the limit."""),
            ("Root-Cause Checklist",
             """- Chiller / cooling loop failure or fouled heat exchanger.
             - Excessive turret speed or compression force vs. product recipe.
             - Worn tooling increasing friction; inadequate lubrication.
             - Ambient HVAC excursion in the suite.
             - Sensor drift / RTD fault (rule out first)."""),
            ("Escalation & Deviation",
             """Product manufactured while temperature exceeded the validated band is a
             potential quality event. Raise a deviation (per SOP-GEN-001), notify the
             shift supervisor and QA, and quarantine affected batches pending
             disposition. Record the alert id, observed peak temperature, and duration."""),
        ],
    },
    {
        "sop_id": "SOP-VIB-021",
        "title": "Rotating Equipment Vibration & Bearing Degradation",
        "revision": "2.5",
        "asset_types": ["aseptic_filler", "blender", "granulator", "case_packer", "cartoner"],
        "sensor_types": ["vibration", "motor_current"],
        "scenarios": ["vibration_degradation"],
        "sections": [
            ("Purpose & Scope",
             """Guidance for responding to rising vibration on rotating equipment
             (fillers, blenders, packers). Nominal vibration band is 0.5–4.5 mm/s RMS.
             A slow upward trend typically indicates bearing wear or imbalance."""),
            ("Trigger Conditions",
             """Sustained running-average vibration above 4.5 mm/s (WARNING) or above
             ~5.0 mm/s / +10% (CRITICAL), or a TREND alert where the anomaly score is
             climbing over successive windows. On the aseptic filler (B-FILL) treat any
             sustained breach as high priority due to sterility risk."""),
            ("Immediate Actions",
             """1. Correlate vibration with motor current — a simultaneous rise suggests
             mechanical load/bearing friction rather than a sensor artefact.
             2. Reduce speed to the minimum validated rate and observe whether vibration
             falls; if it persists, plan an orderly stop.
             3. Inspect bearing temperature and listen for characteristic bearing noise.
             4. Check mounting bolts, coupling alignment, and rotor balance.
             5. For the aseptic filler, verify no impact to fill integrity; if sterility
             assurance is in doubt, stop and escalate immediately."""),
            ("Root-Cause Checklist",
             """- Bearing wear / inadequate lubrication (most common trend cause).
             - Shaft misalignment or coupling wear.
             - Rotor/impeller imbalance or fouling.
             - Loose mounting / resonance.
             - Accelerometer mounting fault (rule out for step-changes)."""),
            ("Escalation & Maintenance",
             """Create a maintenance work order for bearing inspection/replacement. If
             CRITICAL, transition the asset to MAINTENANCE and follow SOP-REC-011 for
             return-to-service. Sterile-line stoppages require QA notification."""),
        ],
    },
    {
        "sop_id": "SOP-PRES-009",
        "title": "Lyophilizer & Pressure-Loop Instability",
        "revision": "1.8",
        "asset_types": ["lyophilizer", "depyrogenation_tunnel", "vial_washer"],
        "sensor_types": ["pressure"],
        "scenarios": ["pressure_instability"],
        "sections": [
            ("Purpose & Scope",
             """Response to unstable or oscillating pressure on sterile Line-B assets,
             especially the lyophilizer (B-LYO). Nominal pressure band is 3.0–6.0 bar.
             Oscillation (rather than a steady offset) usually indicates a control-loop
             or valve problem."""),
            ("Trigger Conditions",
             """Pressure readings oscillating across or beyond the 3.0–6.0 bar band, a
             WARNING/CRITICAL pressure alert, or a visibly cyclic signal in the telemetry
             history chart for the asset."""),
            ("Immediate Actions",
             """1. Switch the affected pressure loop to manual and hold at a safe midpoint
             (~4.5 bar) to arrest oscillation.
             2. Inspect control valves for hunting/stiction; verify actuator air supply.
             3. Check the pressure transmitter calibration and impulse lines for blockage.
             4. Review recent PID tuning changes; revert to the last validated parameter
             set if a change preceded the instability.
             5. Protect any in-process lyo cycle — do not break vacuum integrity."""),
            ("Root-Cause Checklist",
             """- Control valve stiction / hunting or actuator air issues.
             - Aggressive PID tuning (gain too high) causing oscillation.
             - Transmitter fault or blocked impulse line.
             - Upstream supply pressure fluctuation.
             - Seal/leak allowing pressure decay-recover cycling."""),
            ("Escalation & Deviation",
             """A lyo cycle excursion may impact product quality/sterility. Notify QA,
             raise a deviation (SOP-GEN-001), and preserve the batch record and cycle
             chart for investigation."""),
        ],
    },
    {
        "sop_id": "SOP-THRU-033",
        "title": "Line Slowdown & Throughput-Loss Investigation",
        "revision": "2.1",
        "asset_types": ["tablet_press", "cartoner", "case_packer", "labeler", "blender"],
        "sensor_types": ["throughput", "speed"],
        "scenarios": ["line_slowdown"],
        "sections": [
            ("Purpose & Scope",
             """Systematic investigation when a production line's throughput falls below
             its baseline (health status DEGRADED with 'is_slowing_down'). Applies at the
             line level (e.g. LINE-A) when throughput is < 80% of baseline."""),
            ("Trigger Conditions",
             """Line throughput_pct < 80% of baseline, a SLOWDOWN indication in the line
             health summary, or multiple assets on a line showing reduced speed/throughput
             concurrently."""),
            ("Immediate Actions",
             """1. Identify the bottleneck asset — the app's line health 'slowdown_hint'
             names the worst asset and sensor; start there.
             2. Check for upstream STARVED (feed) or downstream BLOCKED (accumulation)
             states — throughput loss is often a material-flow problem, not the asset.
             3. Verify speed setpoints against the product recipe; confirm no operator
             speed override is in place.
             4. Inspect for jams, reject diverts, and change-part wear.
             5. Restore feed/clear blockage, then ramp speed back to setpoint gradually."""),
            ("Root-Cause Checklist",
             """- Upstream starvation (dispensing/feed) or downstream blockage.
             - Speed setpoint reduced (recipe, override, or drive fault).
             - Mechanical drag / worn change parts.
             - Frequent rejects reducing good-unit throughput.
             - Correlated equipment degradation (see per-asset SOPs)."""),
            ("Escalation",
             """If throughput cannot be restored within the shift target, notify
             production scheduling and log an OEE loss event with the identified
             bottleneck asset and cause code."""),
        ],
    },
    {
        "sop_id": "SOP-ALM-002",
        "title": "Correlated Alarm Burst / Multi-Asset Event Triage",
        "revision": "4.0",
        "asset_types": ["aseptic_filler", "stoppering_unit", "vial_washer", "lyophilizer",
                        "depyrogenation_tunnel"],
        "sensor_types": ["temperature", "vibration", "pressure", "motor_current"],
        "scenarios": ["alarm_burst"],
        "sections": [
            ("Purpose & Scope",
             """Triage guidance when many alerts fire across a line within a short window
             (an 'alarm burst'), e.g. on sterile Line-B. The goal is to find the common
             cause rather than chase each alert individually."""),
            ("Trigger Conditions",
             """Multiple assets on one line raising WARNING/CRITICAL alerts within the
             same monitoring window, often with at least one asset in FAULT state."""),
            ("Immediate Actions",
             """1. Assume a common cause first: utilities (power, HVAC, compressed air,
             chilled water), a shared upstream fault, or a control-network/PLC issue.
             2. Identify the earliest-firing / FAULTED asset — it is frequently the
             origin of the cascade.
             3. Verify utilities and shared services before touching individual assets.
             4. Stabilize the line safely; place clearly-affected assets in a controlled
             stop rather than restarting blindly.
             5. Acknowledge alerts in the app as you triage so the team has a shared view."""),
            ("Root-Cause Checklist",
             """- Utility excursion (power dip, HVAC, air, chilled water).
             - Shared upstream equipment fault cascading downstream.
             - Control system / network / PLC fault raising spurious alarms.
             - Environmental event in the suite.
             - Single origin FAULT propagating (check the first-faulted asset)."""),
            ("Escalation & Deviation",
             """Alarm bursts on sterile lines are high priority. Notify the supervisor and
             QA, raise a deviation (SOP-GEN-001) capturing the full alert set and
             timeline, and do not release affected product without QA disposition."""),
        ],
    },
    {
        "sop_id": "SOP-REC-011",
        "title": "Post-Maintenance Recovery & Return-to-Service",
        "revision": "2.3",
        "asset_types": ["tablet_press", "aseptic_filler", "lyophilizer", "blender",
                        "granulator", "coater", "cartoner", "case_packer"],
        "sensor_types": ["temperature", "vibration", "pressure", "speed", "throughput"],
        "scenarios": ["recovery"],
        "sections": [
            ("Purpose & Scope",
             """Controlled return-to-service after a maintenance intervention, ensuring an
             asset is stable and within nominal bands before resuming normal production
             and before alerts are considered resolved."""),
            ("Return-to-Service Steps",
             """1. Confirm the maintenance work order is closed and the asset is set to
             MAINTENANCE (not FAULT) in the monitoring app.
             2. Run the asset at reduced speed and confirm all sensors return within
             nominal bands for a sustained period (>= 3 monitoring windows).
             3. Verify the anomaly score has returned toward 0 and condition is HEALTHY.
             4. Resolve the originating alert in the Alert Console with a note referencing
             the work order; select 'Mark asset for maintenance' only if further work is
             pending.
             5. Ramp to full speed and monitor for recurrence over the next hour."""),
            ("Acceptance Criteria",
             """- All sensors within nominal bands for >= 3 consecutive windows.
             - Anomaly score < 0.2 and condition HEALTHY.
             - No re-fired alerts on the asset.
             - Throughput restored to >= 95% of baseline where applicable."""),
            ("Documentation",
             """Record return-to-service time, acceptance readings, and the closing
             operator. Link the resolved alert id to the maintenance work order for
             traceability."""),
        ],
    },
    {
        "sop_id": "SOP-MTR-007",
        "title": "Motor Current / Drive Overload",
        "revision": "1.4",
        "asset_types": ["blender", "granulator", "tablet_press", "case_packer",
                        "aseptic_filler"],
        "sensor_types": ["motor_current", "vibration", "speed"],
        "scenarios": ["vibration_degradation", "line_slowdown"],
        "sections": [
            ("Purpose & Scope",
             """Response to elevated motor current (nominal 18–42 A), which indicates
             increased mechanical load and often precedes or accompanies vibration or
             throughput problems."""),
            ("Immediate Actions",
             """1. Correlate with vibration and speed — rising current + rising vibration
             points to bearing/mechanical drag; rising current + falling speed points to
             a jam or overload.
             2. Reduce load/speed and check for material binding or foreign object.
             3. Verify the VFD/drive fault log and thermal overload settings.
             4. Inspect the mechanical train for drag, misalignment, or lubrication loss.
             5. If the drive trips repeatedly, stop and raise a maintenance work order."""),
            ("Root-Cause Checklist",
             """- Mechanical binding / jam / foreign object.
             - Bearing degradation (cross-reference SOP-VIB-021).
             - Drive/VFD fault or mis-set overload.
             - Product overfill / recipe load beyond design.
             - Current sensor fault (rule out for implausible readings)."""),
        ],
    },
    {
        "sop_id": "SOP-ENR-015",
        "title": "Energy-Consumption Anomaly",
        "revision": "1.1",
        "asset_types": ["lyophilizer", "depyrogenation_tunnel", "coater", "tablet_press"],
        "sensor_types": ["energy", "temperature"],
        "scenarios": ["temp_excursion"],
        "sections": [
            ("Purpose & Scope",
             """Investigation of abnormal energy draw (nominal 20–60 kWh) which can signal
             heating/cooling inefficiency, mechanical drag, or a developing fault, and is
             also an OEE/sustainability metric."""),
            ("Immediate Actions",
             """1. Correlate energy with temperature and motor current to localize the
             driver (thermal load vs mechanical load).
             2. Check heaters/chillers for cycling or fouling.
             3. Verify no equipment is running idle/unloaded unnecessarily.
             4. Compare against the product recipe's expected energy profile."""),
            ("Root-Cause Checklist",
             """- Heating/cooling inefficiency (fouling, failing element).
             - Mechanical drag increasing motor load.
             - Idle running / setpoint misconfiguration.
             - Metering fault."""),
        ],
    },
    {
        "sop_id": "SOP-GEN-001",
        "title": "Deviation Handling, Escalation & CAPA (GMP)",
        "revision": "5.1",
        "asset_types": [],  # applies to all
        "sensor_types": [],
        "scenarios": ["temp_excursion", "vibration_degradation", "pressure_instability",
                      "alarm_burst", "line_slowdown", "recovery"],
        "sections": [
            ("Purpose & Scope",
             """Defines how manufacturing deviations detected via real-time monitoring are
             documented, escalated, and driven to CAPA. Applies whenever a process
             parameter exceeds its validated band or a quality-relevant event occurs."""),
            ("Deviation Workflow",
             """1. Capture the event: alert id, asset, sensor, observed vs. threshold
             value, start/end time, and duration outside band.
             2. Assess product impact with QA — quarantine potentially-affected batches.
             3. Log the deviation in the QMS within the required timeframe (typically
             1 business day; immediately for sterility-relevant events).
             4. Perform root-cause analysis using the relevant equipment SOP checklist.
             5. Define corrective and preventive actions (CAPA) and assign owners."""),
            ("Escalation Matrix",
             """- WARNING, self-recovering: log; no immediate escalation.
             - CRITICAL or sustained breach: notify shift supervisor immediately.
             - Sterility / patient-safety relevant (Line-B): notify QA and site quality
             lead immediately; do not release product.
             - Repeated / systemic: escalate to engineering and quality management."""),
            ("Records & Traceability",
             """Every deviation must be traceable from the monitoring alert to the QMS
             record and CAPA. Retain telemetry history, the alert acknowledgement trail,
             and operator notes."""),
        ],
    },
    {
        "sop_id": "SOP-CLR-004",
        "title": "Line Clearance & Changeover Verification",
        "revision": "3.0",
        "asset_types": ["labeler", "cartoner", "serialization_unit", "case_packer",
                        "tablet_press"],
        "sensor_types": ["throughput", "speed"],
        "scenarios": ["line_slowdown", "recovery"],
        "sections": [
            ("Purpose & Scope",
             """Line clearance and verification before/after a product changeover to
             prevent mix-ups and confirm the line is ready to run at rate."""),
            ("Procedure",
             """1. Confirm all prior-product materials, labels, and printed components are
             removed and reconciled.
             2. Verify the correct recipe/format parts are installed.
             3. Run a start-up sample and confirm throughput and speed reach setpoint.
             4. Confirm serialization/aggregation is reading correctly before full run.
             5. Document the clearance with a second-person check."""),
            ("Common Issues",
             """- Residual reduced throughput after changeover → verify format parts and
             feed; cross-reference SOP-THRU-033.
             - Serialization read failures → check camera/scanner alignment."""),
        ],
    },
]


def _slug(sop_id: str) -> str:
    return sop_id.lower().replace(" ", "-")


def render_markdown(sop: dict) -> str:
    meta = {
        "sop_id": sop["sop_id"],
        "title": sop["title"],
        "revision": sop["revision"],
        "asset_types": sop["asset_types"],
        "sensor_types": sop["sensor_types"],
        "scenarios": sop["scenarios"],
    }
    lines = [f"# {sop['sop_id']} — {sop['title']}", "", DISCLAIMER, "",
             f"*Revision {sop['revision']} · Applies to: "
             f"{', '.join(sop['asset_types']) or 'all assets'} · "
             f"Sensors: {', '.join(sop['sensor_types']) or 'all'}*", "",
             "<!-- METADATA: " + json.dumps(meta) + " -->", ""]
    for heading, body in sop["sections"]:
        lines.append(f"## {heading}")
        lines.append("")
        lines.append(textwrap.dedent(body).strip())
        lines.append("")
    return "\n".join(lines)


def chunk_records(sop: dict) -> list[dict]:
    """One record per section (retrievable chunk)."""
    recs = []
    for i, (heading, body) in enumerate(sop["sections"]):
        text = textwrap.dedent(body).strip()
        recs.append({
            "chunk_id": f"{sop['sop_id']}#{i:02d}",
            "sop_id": sop["sop_id"],
            "title": sop["title"],
            "revision": sop["revision"],
            "section": heading,
            "asset_types": ",".join(sop["asset_types"]),
            "sensor_types": ",".join(sop["sensor_types"]),
            "scenarios": ",".join(sop["scenarios"]),
            "source_file": f"{_slug(sop['sop_id'])}.md",
            # embed a self-describing header into the content so retrieval matches on
            # asset/sensor/scenario terms as well as the prose.
            "content": (f"[{sop['sop_id']} — {sop['title']} · {heading}] "
                        f"(applies to: {', '.join(sop['asset_types']) or 'all assets'}; "
                        f"sensors: {', '.join(sop['sensor_types']) or 'all'}; "
                        f"scenarios: {', '.join(sop['scenarios'])})\n\n{text}"),
        })
    return recs


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    out_dir = os.path.join(here, "sops")
    os.makedirs(out_dir, exist_ok=True)
    all_chunks = []
    for sop in SOPS:
        md = render_markdown(sop)
        with open(os.path.join(out_dir, f"{_slug(sop['sop_id'])}.md"), "w") as f:
            f.write(md)
        all_chunks.extend(chunk_records(sop))
    with open(os.path.join(here, "sop_chunks.json"), "w") as f:
        json.dump(all_chunks, f, indent=2)
    print(f"Wrote {len(SOPS)} SOP markdown files to {out_dir}")
    print(f"Wrote {len(all_chunks)} chunks to {os.path.join(here, 'sop_chunks.json')}")


if __name__ == "__main__":
    main()
