# Databricks notebook source
# MAGIC %md
# MAGIC # ops · ML scoring task (scheduled)
# MAGIC Scores the freshest LDP feature windows with the UC `@champion` anomaly model and pushes
# MAGIC the latest score per asset to Delta gold + Lakebase. Task 3 of `pharmaco_mfg_rt_operational`.

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]==3.2.3" "mlflow[databricks]==2.22.1" scikit-learn==1.5.2
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

import ml_scoring
top = ml_scoring.score_latest(
    spark, w, features_table=f"{CATALOG}.{LDP}.gold_asset_features_20s",
    scores_table=f"{CATALOG}.{GOLD}.asset_ml_anomaly_scores", model_name=ML_MODEL_NAME,
    lakebase_instance=LAKEBASE_INSTANCE, lakebase_db=LAKEBASE_DB, pg_schema=PG_SCHEMA)
display(spark.createDataFrame(top))
print(f"scored {len(top)} assets; top: {top.iloc[0].asset_id} = {top.iloc[0].ml_anomaly_score}")
