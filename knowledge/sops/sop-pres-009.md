# SOP-PRES-009 — Lyophilizer & Pressure-Loop Instability

> **SYNTHETIC DEMO DOCUMENT — NOT A REAL PROCEDURE.** Fictional content generated for a Databricks demonstration. Do not use for actual manufacturing.

*Revision 1.8 · Applies to: lyophilizer, depyrogenation_tunnel, vial_washer · Sensors: pressure*

<!-- METADATA: {"sop_id": "SOP-PRES-009", "title": "Lyophilizer & Pressure-Loop Instability", "revision": "1.8", "asset_types": ["lyophilizer", "depyrogenation_tunnel", "vial_washer"], "sensor_types": ["pressure"], "scenarios": ["pressure_instability"]} -->

## Purpose & Scope

Response to unstable or oscillating pressure on sterile Line-B assets,
             especially the lyophilizer (B-LYO). Nominal pressure band is 3.0–6.0 bar.
             Oscillation (rather than a steady offset) usually indicates a control-loop
             or valve problem.

## Trigger Conditions

Pressure readings oscillating across or beyond the 3.0–6.0 bar band, a
             WARNING/CRITICAL pressure alert, or a visibly cyclic signal in the telemetry
             history chart for the asset.

## Immediate Actions

1. Switch the affected pressure loop to manual and hold at a safe midpoint
             (~4.5 bar) to arrest oscillation.
             2. Inspect control valves for hunting/stiction; verify actuator air supply.
             3. Check the pressure transmitter calibration and impulse lines for blockage.
             4. Review recent PID tuning changes; revert to the last validated parameter
             set if a change preceded the instability.
             5. Protect any in-process lyo cycle — do not break vacuum integrity.

## Root-Cause Checklist

- Control valve stiction / hunting or actuator air issues.
             - Aggressive PID tuning (gain too high) causing oscillation.
             - Transmitter fault or blocked impulse line.
             - Upstream supply pressure fluctuation.
             - Seal/leak allowing pressure decay-recover cycling.

## Escalation & Deviation

A lyo cycle excursion may impact product quality/sterility. Notify QA,
             raise a deviation (SOP-GEN-001), and preserve the batch record and cycle
             chart for investigation.
