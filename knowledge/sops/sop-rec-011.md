# SOP-REC-011 — Post-Maintenance Recovery & Return-to-Service

> **SYNTHETIC DEMO DOCUMENT — NOT A REAL PROCEDURE.** Fictional content generated for a Databricks demonstration. Do not use for actual manufacturing.

*Revision 2.3 · Applies to: tablet_press, aseptic_filler, lyophilizer, blender, granulator, coater, cartoner, case_packer · Sensors: temperature, vibration, pressure, speed, throughput*

<!-- METADATA: {"sop_id": "SOP-REC-011", "title": "Post-Maintenance Recovery & Return-to-Service", "revision": "2.3", "asset_types": ["tablet_press", "aseptic_filler", "lyophilizer", "blender", "granulator", "coater", "cartoner", "case_packer"], "sensor_types": ["temperature", "vibration", "pressure", "speed", "throughput"], "scenarios": ["recovery"]} -->

## Purpose & Scope

Controlled return-to-service after a maintenance intervention, ensuring an
             asset is stable and within nominal bands before resuming normal production
             and before alerts are considered resolved.

## Return-to-Service Steps

1. Confirm the maintenance work order is closed and the asset is set to
             MAINTENANCE (not FAULT) in the monitoring app.
             2. Run the asset at reduced speed and confirm all sensors return within
             nominal bands for a sustained period (>= 3 monitoring windows).
             3. Verify the anomaly score has returned toward 0 and condition is HEALTHY.
             4. Resolve the originating alert in the Alert Console with a note referencing
             the work order; select 'Mark asset for maintenance' only if further work is
             pending.
             5. Ramp to full speed and monitor for recurrence over the next hour.

## Acceptance Criteria

- All sensors within nominal bands for >= 3 consecutive windows.
             - Anomaly score < 0.2 and condition HEALTHY.
             - No re-fired alerts on the asset.
             - Throughput restored to >= 95% of baseline where applicable.

## Documentation

Record return-to-service time, acceptance readings, and the closing
             operator. Link the resolved alert id to the maintenance work order for
             traceability.
