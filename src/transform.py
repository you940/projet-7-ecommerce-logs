"""Étape 2 - Validation, nettoyage et rapport qualité."""
import os

from pyspark.sql import functions as F

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sql")
VALID_ACTIONS = ["page_view", "click", "add_to_cart", "remove_from_cart", "purchase"]


def read_sql(name):
    with open(os.path.join(SQL_DIR, f"{name}.sql"), encoding="utf-8") as f:
        return f.read()


def clean(spark, raw_df):
    """Applique les règles de sql/clean.sql puis supprime les doublons exacts."""
    raw_df.createOrReplaceTempView("raw_events")
    cleaned = spark.sql(read_sql("clean"))
    return cleaned.dropDuplicates()


def quality_report(raw_df, clean_df):
    """Compte les lignes avant / après nettoyage et détaille les rejets."""
    missing = (F.col("user_id").isNull() | F.col("product_id").isNull()
               | F.col("session_id").isNull())
    bad_ts = F.to_timestamp(F.col("timestamp")).isNull()
    bad_type = F.col("action_type").isNull() | ~F.col("action_type").isin(VALID_ACTIONS)
    rejected = missing | bad_ts | bad_type

    row = raw_df.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(missing, 1).otherwise(0)).alias("missing"),
        F.sum(F.when(bad_ts, 1).otherwise(0)).alias("bad_ts"),
        F.sum(F.when(bad_type, 1).otherwise(0)).alias("bad_type"),
        F.sum(F.when(rejected, 1).otherwise(0)).alias("rejected"),
    ).first()

    n_raw = int(row["total"])
    n_rejected = int(row["rejected"])
    n_clean = clean_df.count()
    return {
        "lignes_brutes": n_raw,
        "valeurs_manquantes": int(row["missing"]),
        "timestamps_invalides": int(row["bad_ts"]),
        "types_action_invalides": int(row["bad_type"]),
        "lignes_rejetees": n_rejected,
        "doublons_supprimes": n_raw - n_rejected - n_clean,
        "lignes_propres": n_clean,
    }