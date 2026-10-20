# SOP-MTR-007 — Motor Current / Drive Overload

> **SYNTHETIC DEMO DOCUMENT — NOT A REAL PROCEDURE.** Fictional content generated for a Databricks demonstration. Do not use for actual manufacturing.

*Revision 1.4 · Applies to: blender, granulator, tablet_press, case_packer, aseptic_filler · Sensors: motor_current, vibration, speed*

<!-- METADATA: {"sop_id": "SOP-MTR-007", "title": "Motor Current / Drive Overload", "revision": "1.4", "asset_types": ["blender", "granulator", "tablet_press", "case_packer", "aseptic_filler"], "sensor_types": ["motor_current", "vibration", "speed"], "scenarios": ["vibration_degradation", "line_slowdown"]} -->

## Purpose & Scope

Response to elevated motor current (nominal 18–42 A), which indicates
             increased mechanical load and often precedes or accompanies vibration or
             throughput problems.

## Immediate Actions

1. Correlate with vibration and speed — rising current + rising vibration
             points to bearing/mechanical drag; rising current + falling speed points to
             a jam or overload.
             2. Reduce load/speed and check for material binding or foreign object.
             3. Verify the VFD/drive fault log and thermal overload settings.
             4. Inspect the mechanical train for drag, misalignment, or lubrication loss.
             5. If the drive trips repeatedly, stop and raise a maintenance work order.

## Root-Cause Checklist

- Mechanical binding / jam / foreign object.
             - Bearing degradation (cross-reference SOP-VIB-021).
             - Drive/VFD fault or mis-set overload.
             - Product overfill / recipe load beyond design.
             - Current sensor fault (rule out for implausible readings).
