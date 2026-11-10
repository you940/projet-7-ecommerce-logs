"""ETL e-commerce : JSON -> nettoyage -> Parquet partitionné -> relecture."""
import argparse
import json
import os
import time

from pyspark.sql import SparkSession

from export_parquet import read_parquet, write_parquet
from ingest import load_raw
from transform import clean, quality_report, read_sql

QUERIES = []   # les requêtes Spark SQL seront ajoutées aux étapes suivantes


def main(input_path, parquet_path, reports_path):
    spark = (SparkSession.builder.appName("etl_ecommerce")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # 1. Chargement
    raw = load_raw(spark, input_path)

    # 2. Nettoyage + déduplication
    cleaned = clean(spark, raw).cache()
    report = quality_report(raw, cleaned)
    print("\n=== QUALITE DES DONNEES ===")
    print(json.dumps(report, indent=2))

    # 3. Parquet (écriture puis relecture)
    t0 = time.time()
    write_parquet(cleaned, parquet_path)
    events = read_parquet(spark, parquet_path)
    n_parquet = events.count()
    report["lignes_relues_parquet"] = n_parquet
    report["temps_ecriture_relecture_s"] = round(time.time() - t0, 2)
    assert n_parquet == report["lignes_propres"], "Parquet relu != données nettoyées"
    print(f"Parquet OK : {n_parquet} lignes relues depuis {parquet_path}")

    # 4. Requêtes Spark SQL sur le Parquet
    events.createOrReplaceTempView("events")
    os.makedirs(reports_path, exist_ok=True)

    # 4a. Window Functions : enrichissement des événements
    enriched = spark.sql(read_sql("events_enriched"))
    enriched.createOrReplaceTempView("events_enriched")
    print("\n=== EXEMPLE : une session avec rang et durée inter-actions ===")
    sample_session = enriched.select("session_id").first()["session_id"]
    (enriched.filter(f"session_id = '{sample_session}'")
             .orderBy("event_rank").show(20, truncate=False))

    # 4b. Sessions reconstituées
    sessions = spark.sql(read_sql("session_metrics")).cache()
    sessions.createOrReplaceTempView("sessions")
    sessions.write.mode("overwrite").option("compression", "snappy").parquet(
        os.path.join(os.path.dirname(parquet_path), "sessions"))
    print("\n=== SESSIONS RECONSTITUEES (extrait) ===")
    sessions.orderBy("session_id").show(10, truncate=False)
    print("Nombre de sessions :", sessions.count())

    # 4c. Autres requêtes (ajoutées aux étapes suivantes)
    for q in QUERIES:
        res = spark.sql(read_sql(q))
        print(f"\n=== {q.upper()} ===")
        res.show(20, truncate=False)
        res.coalesce(1).write.mode("overwrite").option("header", True).csv(
            os.path.join(reports_path, q))

    with open(os.path.join(reports_path, "quality_report.json"), "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nRésultats écrits dans {reports_path}")
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="ETL logs e-commerce (Spark SQL + Parquet)")
    p.add_argument("--input", default="/opt/data/raw/logs.json")
    p.add_argument("--parquet", default="/opt/outputs/parquet/events")
    p.add_argument("--reports", default="/opt/outputs/reports")
    a = p.parse_args()
    main(a.input, a.parquet, a.reports)