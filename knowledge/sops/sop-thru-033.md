# SOP-THRU-033 — Line Slowdown & Throughput-Loss Investigation

> **SYNTHETIC DEMO DOCUMENT — NOT A REAL PROCEDURE.** Fictional content generated for a Databricks demonstration. Do not use for actual manufacturing.

*Revision 2.1 · Applies to: tablet_press, cartoner, case_packer, labeler, blender · Sensors: throughput, speed*

<!-- METADATA: {"sop_id": "SOP-THRU-033", "title": "Line Slowdown & Throughput-Loss Investigation", "revision": "2.1", "asset_types": ["tablet_press", "cartoner", "case_packer", "labeler", "blender"], "sensor_types": ["throughput", "speed"], "scenarios": ["line_slowdown"]} -->

## Purpose & Scope

Systematic investigation when a production line's throughput falls below
             its baseline (health status DEGRADED with 'is_slowing_down'). Applies at the
             line level (e.g. LINE-A) when throughput is < 80% of baseline.

## Trigger Conditions

Line throughput_pct < 80% of baseline, a SLOWDOWN indication in the line
             health summary, or multiple assets on a line showing reduced speed/throughput
             concurrently.

## Immediate Actions

1. Identify the bottleneck asset — the app's line health 'slowdown_hint'
             names the worst asset and sensor; start there.
             2. Check for upstream STARVED (feed) or downstream BLOCKED (accumulation)
             states — throughput loss is often a material-flow problem, not the asset.
             3. Verify speed setpoints against the product recipe; confirm no operator
             speed override is in place.
             4. Inspect for jams, reject diverts, and change-part wear.
             5. Restore feed/clear blockage, then ramp speed back to setpoint gradually.

## Root-Cause Checklist

- Upstream starvation (dispensing/feed) or downstream blockage.
             - Speed setpoint reduced (recipe, override, or drive fault).
             - Mechanical drag / worn change parts.
             - Frequent rejects reducing good-unit throughput.
             - Correlated equipment degradation (see per-asset SOPs).

## Escalation

If throughput cannot be restored within the shift target, notify
             production scheduling and log an OEE loss event with the identified
             bottleneck asset and cause code.
