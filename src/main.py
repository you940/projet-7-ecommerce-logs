"""Point d'entree : CSV -> nettoyage -> Parquet -> relecture -> requetes Spark SQL -> resultats."""
import argparse
import json
import os
import time

from pyspark.sql import SparkSession

from export_parquet import read_parquet, write_parquet
from ingest import load_raw
from transform import clean, quality_report, read_sql

QUERIES = ["activity", "popularity", "engagement", "purchase_funnel"]


def main(input_path, parquet_path, reports_path, memory):
    spark = (SparkSession.builder.appName("gr8-ecommerce-pipeline")
             .master("local[*]")
             .config("spark.driver.memory", memory)
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # 1. Chargement
    raw = load_raw(spark, input_path)

    # 2. Nettoyage
    cleaned = clean(spark, raw).cache()
    report = quality_report(raw, cleaned)
    print("\n=== QUALITE DES DONNEES ===")
    print(json.dumps(report, indent=2))

    # 3. Parquet (ecriture puis relecture)
    t0 = time.time()
    write_parquet(cleaned, parquet_path)
    events = read_parquet(spark, parquet_path)
    n_parquet = events.count()
    report["lignes_relues_parquet"] = n_parquet
    report["temps_ecriture_relecture_s"] = round(time.time() - t0, 2)
    assert n_parquet == report["lignes_propres"], "Parquet relu != donnees nettoyees"
    print(f"Parquet OK : {n_parquet} lignes relues depuis {parquet_path}")

    # 4. Requetes Spark SQL sur les donnees Parquet
    events.createOrReplaceTempView("events")
    os.makedirs(reports_path, exist_ok=True)
    for q in QUERIES:
        res = spark.sql(read_sql(q))
        print(f"\n=== {q.upper()} ===")
        res.show(20, truncate=False)
        # CSV unique par requete (resultats petits : agregats)
        out_dir = os.path.join(reports_path, q)
        res.coalesce(1).write.mode("overwrite").option("header", True).csv(out_dir)

    with open(os.path.join(reports_path, "quality_report.json"), "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nResultats ecrits dans {reports_path}")
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Pipeline logs e-commerce (Spark SQL + Parquet)")
    p.add_argument("--input", required=True, help="Fichier CSV d'entree")
    p.add_argument("--parquet", default="outputs/parquet/events")
    p.add_argument("--reports", default="outputs/reports")
    p.add_argument("--memory", default="4g", help="Memoire du driver Spark (ex: 4g, 8g)")
    a = p.parse_args()
    main(a.input, a.parquet, a.reports, a.memory)
