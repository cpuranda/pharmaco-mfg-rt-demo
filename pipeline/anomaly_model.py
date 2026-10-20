"""
Asset anomaly detector model (MLflow pyfunc) — PharmaCo MFG RT demo, SYNTHETIC data.

Learns a per-asset, per-sensor operating baseline (mean / std of 20-second window
averages) from *healthy RUNNING* history only — every tablet press, filler or
lyophilizer gets its own fingerprint instead of one fleet-wide band. Scoring computes
the deviation of each sensor from that asset's baseline (z-score) and combines them:

  ml_anomaly_score  0..1   (0.5 == the alert threshold of `z_threshold` sigmas)
  ml_is_anomaly     bool   (any sensor >= z_threshold sigmas off baseline while RUNNING)
  top_sensor        str    (sensor contributing most — explains the alert to the operator)

Unlike the fixed validated band, this fires on drift that is still *inside* the band
(e.g. an oscillating pressure loop) and earlier on ramps. Logged with `code_paths` so the
UC-registered model loads in any job/notebook.
"""
import mlflow
import numpy as np
import pandas as pd

SENSORS = ["temperature", "vibration", "pressure", "motor_current", "speed",
           "throughput", "energy"]


class AssetAnomalyDetector(mlflow.pyfunc.PythonModel):
    """Per-asset learned baseline + multivariate deviation score."""

    def __init__(self, z_threshold: float = 6.0, min_running_frac: float = 0.8):
        self.z_threshold, self.min_running_frac = z_threshold, min_running_frac
        self.cols = [f"{s}_avg" for s in SENSORS]

    def fit(self, df: pd.DataFrame):
        healthy = df[df["running_frac"] >= self.min_running_frac]
        self.baseline_ = {
            a: (g[self.cols].mean().values, g[self.cols].std().replace(0, 1).fillna(1).values)
            for a, g in healthy.groupby("asset_id")}
        self.fleet_ = (healthy[self.cols].mean().values,
                       healthy[self.cols].std().replace(0, 1).values)
        return self

    def zscores(self, df: pd.DataFrame) -> np.ndarray:
        x = df[self.cols].astype(float).fillna(0.0).values
        z = np.zeros_like(x)
        for i, a in enumerate(df["asset_id"].values):
            mu, sd = self.baseline_.get(a, self.fleet_)
            z[i] = np.abs(x[i] - mu) / sd
        return z

    def score(self, df: pd.DataFrame) -> pd.DataFrame:
        z = self.zscores(df)
        running = df["running_frac"].astype(float).values >= self.min_running_frac
        maxz = np.where(running, z.max(axis=1), 0.0)
        return pd.DataFrame({
            "ml_anomaly_score": np.clip(maxz / (2 * self.z_threshold), 0, 1).round(4),
            "ml_is_anomaly": maxz >= self.z_threshold,
            "top_sensor": np.where(running, np.array(SENSORS)[z.argmax(axis=1)], "not_running"),
            "max_z": maxz.round(2),
        })

    def predict(self, context, model_input: pd.DataFrame) -> pd.DataFrame:
        return self.score(model_input)
