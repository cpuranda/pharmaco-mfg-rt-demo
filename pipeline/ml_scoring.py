"""
Batch scoring for the UC-registered asset anomaly detector (PharmaCo MFG RT demo).

Scores the most recent 20-second feature windows from the Lakeflow Declarative
Pipeline (gold_asset_features_20s) with the `@champion` model version, appends the
scores to Delta gold, and upserts the latest score per asset into Lakebase so the
operator app can show it next to the rule-based condition. Used by notebook 06 and
by the scheduled operational job. All data is SYNTHETIC.
"""
from __future__ import annotations

import uuid

import pandas as pd

FEATURES = [f"{s}_{a}" for s in ["temperature", "vibration", "pressure", "motor_current",
                                 "speed", "throughput", "energy"] for a in ["avg", "std", "max"]] \
           + ["oob_frac", "running_frac"]
_LB_COLS = ["asset_id", "line_id", "window_start", "window_end", "ml_anomaly_score",
            "ml_is_anomaly", "top_sensor", "model_name", "model_version", "scored_ts"]


def score_latest(spark, w, *, features_table: str, scores_table: str, model_name: str,
                 lakebase_instance: str, lakebase_db: str, pg_schema: str,
                 windows_per_asset: int = 3) -> pd.DataFrame:
    import mlflow
    import psycopg
    from mlflow import MlflowClient

    mlflow.set_registry_uri("databricks-uc")
    mv = MlflowClient().get_model_version_by_alias(model_name, "champion")
    model = mlflow.pyfunc.load_model(f"models:/{model_name}@champion")

    latest = (spark.table(features_table).filter("n_readings >= 14").toPandas()
              .sort_values("window_start").groupby("asset_id").tail(windows_per_asset))
    latest[FEATURES] = latest[FEATURES].astype(float).fillna(0.0)
    pred = model.predict(latest[["asset_id"] + FEATURES])

    scored = latest[["asset_id", "line_id", "window_start", "window_end"]].reset_index(drop=True)
    scored["ml_anomaly_score"] = pred["ml_anomaly_score"].astype(float).round(4).values
    scored["ml_is_anomaly"] = pred["ml_is_anomaly"].astype(bool).values
    scored["top_sensor"] = pred["top_sensor"].astype(str).values
    scored["model_name"], scored["model_version"] = model_name, str(mv.version)
    scored["scored_ts"] = pd.Timestamp.now(tz="UTC")
    spark.createDataFrame(scored).write.mode("append").option("mergeSchema", "true") \
        .saveAsTable(scores_table)

    # latest score per asset -> Lakebase (operational serving)
    top = scored.sort_values("window_start").groupby("asset_id").tail(1)
    inst = w.api_client.do("GET", f"/api/2.0/database/instances/{lakebase_instance}")
    cred = w.api_client.do("POST", "/api/2.0/database/credentials",
                           body={"request_id": str(uuid.uuid4()),
                                 "instance_names": [lakebase_instance]})
    rows = []
    for _, r in top.iterrows():
        rows.append([r.asset_id, r.line_id, r.window_start.to_pydatetime(),
                     r.window_end.to_pydatetime(), float(r.ml_anomaly_score),
                     bool(r.ml_is_anomaly), r.top_sensor, r.model_name, r.model_version,
                     r.scored_ts.to_pydatetime()])
    upd = ",".join(f"{c}=EXCLUDED.{c}" for c in _LB_COLS[1:]) + ", synced_ts=now()"
    with psycopg.connect(host=inst["read_write_dns"], dbname=lakebase_db,
                         user=w.current_user.me().user_name, password=cred["token"],
                         sslmode="require", autocommit=True) as conn, conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO {pg_schema}.asset_ml_scores ({','.join(_LB_COLS)}) "
            f"VALUES ({','.join(['%s'] * len(_LB_COLS))}) "
            f"ON CONFLICT (asset_id) DO UPDATE SET {upd}", rows)
    return top.sort_values("ml_anomaly_score", ascending=False)
