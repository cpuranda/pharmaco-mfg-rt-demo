# SOP-TEMP-014 — Tablet Press Over-Temperature / Thermal Excursion Response

> **SYNTHETIC DEMO DOCUMENT — NOT A REAL PROCEDURE.** Fictional content generated for a Databricks demonstration. Do not use for actual manufacturing.

*Revision 3.2 · Applies to: tablet_press, granulator, coater · Sensors: temperature, motor_current, energy*

<!-- METADATA: {"sop_id": "SOP-TEMP-014", "title": "Tablet Press Over-Temperature / Thermal Excursion Response", "revision": "3.2", "asset_types": ["tablet_press", "granulator", "coater"], "sensor_types": ["temperature", "motor_current", "energy"], "scenarios": ["temp_excursion"]} -->

## Purpose & Scope

This procedure defines the operator response when a compression or
             thermal-processing asset (e.g. tablet press, granulator, coater) reports a
             sustained temperature above its validated nominal band (55–75 °C for
             compression tooling). It applies to OSD Line-A assets, in particular the
             tablet press (A-COMPRESS).

## Trigger Conditions

Initiate this SOP when the process historian / monitoring app raises a
             temperature WARNING or CRITICAL alert, i.e. the sustained running average
             temperature exceeds 75 °C (WARNING) or exceeds 82.5 °C / +10% (CRITICAL),
             OR the anomaly score is >= 0.4 with a rising (TREND) signature.

## Immediate Actions

1. Do NOT stop the press abruptly under load — reduce turret speed to
             50% setpoint to lower compaction heat generation.
             2. Verify the cooling/HVAC supply to the compression zone; confirm chilled-
             water valve is open and supply temperature is <= 12 °C.
             3. Check tooling lubrication and punch/die clearance; inspect for binding.
             4. Confirm the temperature sensor against a handheld reference to rule out
             a faulty RTD before treating as a real excursion.
             5. If temperature does not trend down within 10 minutes, initiate a
             controlled stop and segregate any product produced above the limit.

## Root-Cause Checklist

- Chiller / cooling loop failure or fouled heat exchanger.
             - Excessive turret speed or compression force vs. product recipe.
             - Worn tooling increasing friction; inadequate lubrication.
             - Ambient HVAC excursion in the suite.
             - Sensor drift / RTD fault (rule out first).

## Escalation & Deviation

Product manufactured while temperature exceeded the validated band is a
             potential quality event. Raise a deviation (per SOP-GEN-001), notify the
             shift supervisor and QA, and quarantine affected batches pending
             disposition. Record the alert id, observed peak temperature, and duration.
