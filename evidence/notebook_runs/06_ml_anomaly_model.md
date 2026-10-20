# Executed notebook: `06_ml_anomaly_model`

> Exported from Databricks job run **24808449534624** / task run **834583285418978** — result **SUCCESS**, executed 2026-10-20T13:35:45+00:00 on serverless compute in workspace `https://fevm-hls-streaming-solution-demo.cloud.databricks.com`. Outputs below are the real cell outputs of that run.

# 06 · ML — per-asset anomaly detector (MLflow + Unity Catalog)

The rule-based gold layer flags a sensor once its average **leaves the validated band**.
Process engineers want earlier warning — and want to catch instabilities that never cross
the band at all (an oscillating lyophilizer pressure loop stays "in spec" on average).

* **Features:** `gold_asset_features_20s` from the Lakeflow Declarative Pipeline
  (per asset, per 20 s window: mean / std / max of 7 sensors, running fraction)
* **Model:** learns a **per-asset operating baseline** for every sensor from *healthy RUNNING*
  history only (semi-supervised — labelled failures are rare in real plants) and scores the
  multivariate deviation; outputs a 0..1 score **and the sensor that drove it** (explainable)
* **Evaluation:** against the synthetic ground-truth label, vs the **rule baseline**
  (any sensor's window average outside its validated band while RUNNING)
* **Governance:** logged with MLflow, registered to **Unity Catalog**
  (`…pharmaco_mfg_rt_ml.asset_anomaly_detector`), alias `champion`
* **Serving:** batch-scored into Delta gold **and Lakebase**, so the operator app, Genie and
  the agents all see the ML score

```python
%pip install -q "psycopg[binary]==3.2.3" "mlflow[databricks]==2.22.1" scikit-learn==1.5.2
%restart_python
```

**Output:**

```text
ERROR: pip's dependency resolver does not currently take into account all the packages that are installed. This behaviour is the source of the following dependency conflicts.
jupyter-server 1.23.4 requires anyio<4,>=3.1.0, but you have anyio 4.15.1 which is incompatible.
Note: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.
```

```python
%run ./_config
```

```python

```

**Output:**

```text
repo root : /Workspace/Users/chandrashekhar.purandare@databricks.com/pharmaco_mfg_rt_demo
catalog   : hls_streaming_solution_demo_catalog  schemas=['pharmaco_mfg_rt_bronze', 'pharmaco_mfg_rt_silver', 'pharmaco_mfg_rt_gold', 'pharmaco_mfg_rt_ldp', 'pharmaco_mfg_rt_ml', 'pharmaco_mfg_rt_knowledge']
run as    : chandrashekhar.purandare@databricks.com   at 2026-10-20T13:36:56+00:00
```

```python
import mlflow, numpy as np, pandas as pd
from mlflow.models import infer_signature
from sklearn.metrics import (roc_auc_score, average_precision_score, precision_score,
                             recall_score, f1_score, confusion_matrix)
from ml_scoring import FEATURES          # same feature list the scoring job uses
from anomaly_model import AssetAnomalyDetector, SENSORS
from telemetry_generator import SENSOR_SPECS   # validated bands (for the rule baseline)
ANOMALY_MODEL_SRC = os.path.join(REPO_ROOT, "pipeline", "anomaly_model.py")
mlflow.set_registry_uri("databricks-uc")

pdf = (spark.table(f"{CATALOG}.{LDP}.gold_asset_features_20s")
       .filter("n_readings >= 14").toPandas().sort_values("window_start").reset_index(drop=True))
pdf[FEATURES] = pdf[FEATURES].astype(float).fillna(0.0)
print(f"feature rows: {len(pdf)}  assets: {pdf.asset_id.nunique()}  "
      f"anomalous windows (ground truth): {int(pdf.label_is_anomalous.sum())}")
display(spark.createDataFrame(
    pdf.assign(scenario=pdf.active_scenario.fillna("normal"))
       .groupby(["scenario", "label_is_anomalous"]).size().reset_index(name="windows")))
```

**Output:**

```text
feature rows: 1515  assets: 15  anomalous windows (ground truth): 114
```

| scenario | label_is_anomalous | windows |
|---|---|---|
| line_slowdown | 0 | 117 |
| line_slowdown | 1 | 78 |
| normal | 0 | 780 |
| pressure_instability | 0 | 182 |
| pressure_instability | 1 | 13 |
| temp_excursion | 0 | 140 |
| temp_excursion | 1 | 10 |
| vibration_degradation | 0 | 182 |
| vibration_degradation | 1 | 13 |

```python
# train ONLY on healthy history (no scenario active anywhere in the plant)
train = pdf[pdf.active_scenario.isna()]
print(f"training windows (healthy only): {len(train)}   evaluation windows (all): {len(pdf)}")

def rule_baseline(d):
    """Validated-band rule: any sensor's window average outside [nominal_min, nominal_max] while RUNNING."""
    hit = np.zeros(len(d), dtype=bool)
    for s_ in SENSORS:
        spec = SENSOR_SPECS[s_]
        hit |= (d[f"{s_}_avg"] < spec.nominal_min) | (d[f"{s_}_avg"] > spec.nominal_max)
    return (hit & (d.running_frac >= 0.8)).astype(int).values
```

**Output:**

```text
training windows (healthy only): 780   evaluation windows (all): 1515
```

```python
banner("Train, evaluate vs the rule baseline, log to MLflow")
mlflow.set_experiment(f"/Users/{ME}/pharmaco_mfg_rt_anomaly_detector")
with mlflow.start_run(run_name="per_asset_baseline_z6") as run:
    model = AssetAnomalyDetector(z_threshold=6.0).fit(train)
    y = pdf.label_is_anomalous.astype(int).values
    out = model.score(pdf)
    s, yhat = out.ml_anomaly_score.values, out.ml_is_anomaly.astype(int).values
    base = rule_baseline(pdf)
    metrics = {
        "roc_auc": roc_auc_score(y, s), "pr_auc": average_precision_score(y, s),
        "precision": precision_score(y, yhat, zero_division=0),
        "recall": recall_score(y, yhat, zero_division=0), "f1": f1_score(y, yhat, zero_division=0),
        "rule_precision": precision_score(y, base, zero_division=0),
        "rule_recall": recall_score(y, base, zero_division=0),
        "rule_f1": f1_score(y, base, zero_division=0),
        "train_rows": len(train), "eval_rows": len(pdf),
    }
    mlflow.log_params({"algorithm": "per-asset baseline + multivariate z-score", "z_threshold": 6.0,
                       "min_running_frac": 0.8, "sensors": len(SENSORS),
                       "feature_source": f"{CATALOG}.{LDP}.gold_asset_features_20s"})
    mlflow.log_metrics(metrics)
    sample = pdf[["asset_id"] + FEATURES].head(20)
    sig = infer_signature(sample, model.predict(None, sample))
    mlflow.pyfunc.log_model(artifact_path="model", python_model=model, signature=sig,
                            input_example=sample.head(3), pip_requirements=["pandas", "numpy"],
                            code_paths=[ANOMALY_MODEL_SRC], registered_model_name=ML_MODEL_NAME)
print(json.dumps({k: round(v, 4) if isinstance(v, float) else v for k, v in metrics.items()}, indent=1))
tn, fp, fn, tp = confusion_matrix(y, yhat).ravel()
btn, bfp, bfn, btp = confusion_matrix(y, base).ravel()
display(spark.createDataFrame(pd.DataFrame([
    ("ML per-asset baseline", tp, fp, fn, round(metrics["precision"], 3), round(metrics["recall"], 3), round(metrics["f1"], 3)),
    ("Rule: validated band", btp, bfp, bfn, round(metrics["rule_precision"], 3), round(metrics["rule_recall"], 3), round(metrics["rule_f1"], 3))],
    columns=["detector", "TP", "FP", "FN", "precision", "recall", "f1"])))
print(f"MLflow run: {run.info.run_id}")
```

**Output:**

```text

==============================================================================
Train, evaluate vs the rule baseline, log to MLflow
==============================================================================
```

```text
2026/10/20 13:37:11 INFO mlflow.pyfunc: Validating input example against model signature
Registered model 'hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ml.asset_anomaly_detector' already exists. Creating a new version of this model...
Created version '4' of model 'hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ml.asset_anomaly_detector'.
```

```text
{
 "roc_auc": 0.9207,
 "pr_auc": 0.8717,
 "precision": 0.9775,
 "recall": 0.7632,
 "f1": 0.8571,
 "rule_precision": 1.0,
 "rule_recall": 0.6579,
 "rule_f1": 0.7937,
 "train_rows": 780,
 "eval_rows": 1515
}
```

| detector | TP | FP | FN | precision | recall | f1 |
|---|---|---|---|---|---|---|
| ML per-asset baseline | 87 | 2 | 27 | 0.978 | 0.763 | 0.857 |
| Rule: validated band | 75 | 0 | 39 | 1.0 | 0.658 | 0.794 |

```text
MLflow run: 689b6f2b7b564d2a8795c255ff388fd0
```

```python
banner("Per scenario: does ML detect it, and how many seconds before the band rule?")
pdf["ml_flag"], pdf["rule_flag"], pdf["top_sensor"] = yhat, base, out.top_sensor.values
rows = []
for scen, grp in pdf[pdf.label_is_anomalous == 1].groupby("active_scenario"):
    fm = grp[grp.ml_flag == 1].window_start.min(); fr = grp[grp.rule_flag == 1].window_start.min()
    rows.append((scen, int(grp.ml_flag.sum()), int(grp.rule_flag.sum()), len(grp),
                 grp[grp.ml_flag == 1].top_sensor.mode().iat[0] if grp.ml_flag.any() else None,
                 str(fm) if pd.notna(fm) else "never", str(fr) if pd.notna(fr) else "never",
                 (fr - fm).total_seconds() if pd.notna(fm) and pd.notna(fr) else None))
display(spark.createDataFrame(pd.DataFrame(rows, columns=[
    "scenario", "ml_windows_flagged", "rule_windows_flagged", "anomalous_windows",
    "ml_top_sensor", "first_ml_alert", "first_rule_alert", "ml_lead_seconds"])))
```

**Output:**

```text

==============================================================================
Per scenario: does ML detect it, and how many seconds before the band rule?
==============================================================================
```

| scenario | ml_windows_flagged | rule_windows_flagged | anomalous_windows | ml_top_sensor | first_ml_alert | first_rule_alert | ml_lead_seconds |
|---|---|---|---|---|---|---|---|
| line_slowdown | 59 | 58 | 78 | throughput | 2026-10-20 13:16:00 | 2026-10-20 13:16:00 | 0.0 |
| pressure_instability | 9 | 0 | 13 | pressure | 2026-10-20 13:08:40 | never |  |
| temp_excursion | 9 | 8 | 10 | temperature | 2026-10-20 13:27:40 | 2026-10-20 13:28:00 | 20.0 |
| vibration_degradation | 10 | 9 | 13 | vibration | 2026-10-20 13:01:40 | 2026-10-20 13:02:00 | 20.0 |

```python
banner("Register in Unity Catalog + set alias 'champion'")
from mlflow import MlflowClient
client = MlflowClient()
ver = max(int(v.version) for v in client.search_model_versions(f"name='{ML_MODEL_NAME}'"))
client.set_registered_model_alias(ML_MODEL_NAME, "champion", ver)
client.update_registered_model(ML_MODEL_NAME, description=(
    "Per-asset learned-baseline anomaly detector for PharmaCo line telemetry (synthetic). "
    "Input: gold_asset_features_20s rows. Output: ml_anomaly_score 0..1, ml_is_anomaly, top_sensor."))
mv = client.get_model_version_by_alias(ML_MODEL_NAME, "champion")
print(f"{ML_MODEL_NAME} v{mv.version} @champion  status={mv.status}  run={mv.run_id}")
```

**Output:**

```text

==============================================================================
Register in Unity Catalog + set alias 'champion'
==============================================================================
hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ml.asset_anomaly_detector v4 @champion  status=READY  run=689b6f2b7b564d2a8795c255ff388fd0
```

```python
banner("Batch-score the latest windows with the UC champion model → Delta gold + Lakebase")
import importlib, ml_scoring
importlib.reload(ml_scoring)
top = ml_scoring.score_latest(
    spark, w, features_table=f"{CATALOG}.{LDP}.gold_asset_features_20s",
    scores_table=f"{CATALOG}.{GOLD}.asset_ml_anomaly_scores", model_name=ML_MODEL_NAME,
    lakebase_instance=LAKEBASE_INSTANCE, lakebase_db=LAKEBASE_DB, pg_schema=PG_SCHEMA)
display(spark.createDataFrame(top))
print(f"highest ML anomaly now: {top.iloc[0].asset_id} score={top.iloc[0].ml_anomaly_score}")
```

**Output:**

```text

==============================================================================
Batch-score the latest windows with the UC champion model → Delta gold + Lakebase
==============================================================================
```

| asset_id | line_id | window_start | window_end | ml_anomaly_score | ml_is_anomaly | top_sensor | model_name | model_version | scored_ts |
|---|---|---|---|---|---|---|---|---|---|
| A-COMPRESS | LINE-A | 2026-10-20T13:30:20.000Z | 2026-10-20T13:30:40.000Z | 1.0 | True | temperature | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ml.asset_anomaly_detector | 4 | 2026-10-20T13:37:24.387Z |
| A-GRANULATE | LINE-A | 2026-10-20T13:30:20.000Z | 2026-10-20T13:30:40.000Z | 0.1535 | False | vibration | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ml.asset_anomaly_detector | 4 | 2026-10-20T13:37:24.387Z |
| B-FILL | LINE-B | 2026-10-20T13:30:20.000Z | 2026-10-20T13:30:40.000Z | 0.1335 | False | temperature | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ml.asset_anomaly_detector | 4 | 2026-10-20T13:37:24.387Z |
| C-CASEPACK | LINE-C | 2026-10-20T13:30:20.000Z | 2026-10-20T13:30:40.000Z | 0.1231 | False | vibration | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ml.asset_anomaly_detector | 4 | 2026-10-20T13:37:24.387Z |
| A-INSPECT | LINE-A | 2026-10-20T13:30:20.000Z | 2026-10-20T13:30:40.000Z | 0.1124 | False | vibration | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ml.asset_anomaly_detector | 4 | 2026-10-20T13:37:24.387Z |
| C-CARTON | LINE-C | 2026-10-20T13:30:20.000Z | 2026-10-20T13:30:40.000Z | 0.1107 | False | energy | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ml.asset_anomaly_detector | 4 | 2026-10-20T13:37:24.387Z |
| B-STOPPER | LINE-B | 2026-10-20T13:30:20.000Z | 2026-10-20T13:30:40.000Z | 0.1042 | False | energy | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ml.asset_anomaly_detector | 4 | 2026-10-20T13:37:24.387Z |
| C-LABEL | LINE-C | 2026-10-20T13:30:20.000Z | 2026-10-20T13:30:40.000Z | 0.0957 | False | throughput | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ml.asset_anomaly_detector | 4 | 2026-10-20T13:37:24.387Z |
| C-SERIALIZE | LINE-C | 2026-10-20T13:30:20.000Z | 2026-10-20T13:30:40.000Z | 0.0927 | False | temperature | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ml.asset_anomaly_detector | 4 | 2026-10-20T13:37:24.387Z |
| B-DEPYRO | LINE-B | 2026-10-20T13:30:20.000Z | 2026-10-20T13:30:40.000Z | 0.0893 | False | energy | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ml.asset_anomaly_detector | 4 | 2026-10-20T13:37:24.387Z |
| A-COAT | LINE-A | 2026-10-20T13:30:20.000Z | 2026-10-20T13:30:40.000Z | 0.0836 | False | energy | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ml.asset_anomaly_detector | 4 | 2026-10-20T13:37:24.387Z |
| B-LYO | LINE-B | 2026-10-20T13:30:20.000Z | 2026-10-20T13:30:40.000Z | 0.0754 | False | energy | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ml.asset_anomaly_detector | 4 | 2026-10-20T13:37:24.387Z |
| A-BLEND | LINE-A | 2026-10-20T13:30:20.000Z | 2026-10-20T13:30:40.000Z | 0.0692 | False | pressure | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ml.asset_anomaly_detector | 4 | 2026-10-20T13:37:24.387Z |
| A-DISPENSE | LINE-A | 2026-10-20T13:30:20.000Z | 2026-10-20T13:30:40.000Z | 0.0666 | False | motor_current | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ml.asset_anomaly_detector | 4 | 2026-10-20T13:37:24.387Z |
| B-WASH | LINE-B | 2026-10-20T13:30:20.000Z | 2026-10-20T13:30:40.000Z | 0.0624 | False | motor_current | hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ml.asset_anomaly_detector | 4 | 2026-10-20T13:37:24.387Z |

```text
highest ML anomaly now: A-COMPRESS score=1.0
```
