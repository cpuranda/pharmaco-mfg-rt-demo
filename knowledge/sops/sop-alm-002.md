# SOP-ALM-002 — Correlated Alarm Burst / Multi-Asset Event Triage

> **SYNTHETIC DEMO DOCUMENT — NOT A REAL PROCEDURE.** Fictional content generated for a Databricks demonstration. Do not use for actual manufacturing.

*Revision 4.0 · Applies to: aseptic_filler, stoppering_unit, vial_washer, lyophilizer, depyrogenation_tunnel · Sensors: temperature, vibration, pressure, motor_current*

<!-- METADATA: {"sop_id": "SOP-ALM-002", "title": "Correlated Alarm Burst / Multi-Asset Event Triage", "revision": "4.0", "asset_types": ["aseptic_filler", "stoppering_unit", "vial_washer", "lyophilizer", "depyrogenation_tunnel"], "sensor_types": ["temperature", "vibration", "pressure", "motor_current"], "scenarios": ["alarm_burst"]} -->

## Purpose & Scope

Triage guidance when many alerts fire across a line within a short window
             (an 'alarm burst'), e.g. on sterile Line-B. The goal is to find the common
             cause rather than chase each alert individually.

## Trigger Conditions

Multiple assets on one line raising WARNING/CRITICAL alerts within the
             same monitoring window, often with at least one asset in FAULT state.

## Immediate Actions

1. Assume a common cause first: utilities (power, HVAC, compressed air,
             chilled water), a shared upstream fault, or a control-network/PLC issue.
             2. Identify the earliest-firing / FAULTED asset — it is frequently the
             origin of the cascade.
             3. Verify utilities and shared services before touching individual assets.
             4. Stabilize the line safely; place clearly-affected assets in a controlled
             stop rather than restarting blindly.
             5. Acknowledge alerts in the app as you triage so the team has a shared view.

## Root-Cause Checklist

- Utility excursion (power dip, HVAC, air, chilled water).
             - Shared upstream equipment fault cascading downstream.
             - Control system / network / PLC fault raising spurious alarms.
             - Environmental event in the suite.
             - Single origin FAULT propagating (check the first-faulted asset).

## Escalation & Deviation

Alarm bursts on sterile lines are high priority. Notify the supervisor and
             QA, raise a deviation (SOP-GEN-001) capturing the full alert set and
             timeline, and do not release affected product without QA disposition.
