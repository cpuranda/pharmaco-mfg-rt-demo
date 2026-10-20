# SOP-VIB-021 — Rotating Equipment Vibration & Bearing Degradation

> **SYNTHETIC DEMO DOCUMENT — NOT A REAL PROCEDURE.** Fictional content generated for a Databricks demonstration. Do not use for actual manufacturing.

*Revision 2.5 · Applies to: aseptic_filler, blender, granulator, case_packer, cartoner · Sensors: vibration, motor_current*

<!-- METADATA: {"sop_id": "SOP-VIB-021", "title": "Rotating Equipment Vibration & Bearing Degradation", "revision": "2.5", "asset_types": ["aseptic_filler", "blender", "granulator", "case_packer", "cartoner"], "sensor_types": ["vibration", "motor_current"], "scenarios": ["vibration_degradation"]} -->

## Purpose & Scope

Guidance for responding to rising vibration on rotating equipment
             (fillers, blenders, packers). Nominal vibration band is 0.5–4.5 mm/s RMS.
             A slow upward trend typically indicates bearing wear or imbalance.

## Trigger Conditions

Sustained running-average vibration above 4.5 mm/s (WARNING) or above
             ~5.0 mm/s / +10% (CRITICAL), or a TREND alert where the anomaly score is
             climbing over successive windows. On the aseptic filler (B-FILL) treat any
             sustained breach as high priority due to sterility risk.

## Immediate Actions

1. Correlate vibration with motor current — a simultaneous rise suggests
             mechanical load/bearing friction rather than a sensor artefact.
             2. Reduce speed to the minimum validated rate and observe whether vibration
             falls; if it persists, plan an orderly stop.
             3. Inspect bearing temperature and listen for characteristic bearing noise.
             4. Check mounting bolts, coupling alignment, and rotor balance.
             5. For the aseptic filler, verify no impact to fill integrity; if sterility
             assurance is in doubt, stop and escalate immediately.

## Root-Cause Checklist

- Bearing wear / inadequate lubrication (most common trend cause).
             - Shaft misalignment or coupling wear.
             - Rotor/impeller imbalance or fouling.
             - Loose mounting / resonance.
             - Accelerometer mounting fault (rule out for step-changes).

## Escalation & Maintenance

Create a maintenance work order for bearing inspection/replacement. If
             CRITICAL, transition the asset to MAINTENANCE and follow SOP-REC-011 for
             return-to-service. Sterile-line stoppages require QA notification.
