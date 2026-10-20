# SOP-GEN-001 — Deviation Handling, Escalation & CAPA (GMP)

> **SYNTHETIC DEMO DOCUMENT — NOT A REAL PROCEDURE.** Fictional content generated for a Databricks demonstration. Do not use for actual manufacturing.

*Revision 5.1 · Applies to: all assets · Sensors: all*

<!-- METADATA: {"sop_id": "SOP-GEN-001", "title": "Deviation Handling, Escalation & CAPA (GMP)", "revision": "5.1", "asset_types": [], "sensor_types": [], "scenarios": ["temp_excursion", "vibration_degradation", "pressure_instability", "alarm_burst", "line_slowdown", "recovery"]} -->

## Purpose & Scope

Defines how manufacturing deviations detected via real-time monitoring are
             documented, escalated, and driven to CAPA. Applies whenever a process
             parameter exceeds its validated band or a quality-relevant event occurs.

## Deviation Workflow

1. Capture the event: alert id, asset, sensor, observed vs. threshold
             value, start/end time, and duration outside band.
             2. Assess product impact with QA — quarantine potentially-affected batches.
             3. Log the deviation in the QMS within the required timeframe (typically
             1 business day; immediately for sterility-relevant events).
             4. Perform root-cause analysis using the relevant equipment SOP checklist.
             5. Define corrective and preventive actions (CAPA) and assign owners.

## Escalation Matrix

- WARNING, self-recovering: log; no immediate escalation.
             - CRITICAL or sustained breach: notify shift supervisor immediately.
             - Sterility / patient-safety relevant (Line-B): notify QA and site quality
             lead immediately; do not release product.
             - Repeated / systemic: escalate to engineering and quality management.

## Records & Traceability

Every deviation must be traceable from the monitoring alert to the QMS
             record and CAPA. Retain telemetry history, the alert acknowledgement trail,
             and operator notes.
