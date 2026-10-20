# SOP-ENR-015 — Energy-Consumption Anomaly

> **SYNTHETIC DEMO DOCUMENT — NOT A REAL PROCEDURE.** Fictional content generated for a Databricks demonstration. Do not use for actual manufacturing.

*Revision 1.1 · Applies to: lyophilizer, depyrogenation_tunnel, coater, tablet_press · Sensors: energy, temperature*

<!-- METADATA: {"sop_id": "SOP-ENR-015", "title": "Energy-Consumption Anomaly", "revision": "1.1", "asset_types": ["lyophilizer", "depyrogenation_tunnel", "coater", "tablet_press"], "sensor_types": ["energy", "temperature"], "scenarios": ["temp_excursion"]} -->

## Purpose & Scope

Investigation of abnormal energy draw (nominal 20–60 kWh) which can signal
             heating/cooling inefficiency, mechanical drag, or a developing fault, and is
             also an OEE/sustainability metric.

## Immediate Actions

1. Correlate energy with temperature and motor current to localize the
             driver (thermal load vs mechanical load).
             2. Check heaters/chillers for cycling or fouling.
             3. Verify no equipment is running idle/unloaded unnecessarily.
             4. Compare against the product recipe's expected energy profile.

## Root-Cause Checklist

- Heating/cooling inefficiency (fouling, failing element).
             - Mechanical drag increasing motor load.
             - Idle running / setpoint misconfiguration.
             - Metering fault.
