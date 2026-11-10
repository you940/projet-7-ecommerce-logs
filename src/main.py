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


def size_mb(path):
    """Taille d'un fichier ou d'un dossier en Mo (ignore les fichiers .crc de Spark)."""
    if os.path.isfile(path):
        return round(os.path.getsize(path) / 1e6, 1)
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            if not f.endswith(".crc"):
                total += os.path.getsize(os.path.join(root, f))
    return round(total / 1e6, 1)


def main(input_path, parquet_path, reports_path, memory, partitions):
    spark = (SparkSession.builder.appName("gr8-ecommerce-pipeline")
             .master("local[*]")
             .config("spark.driver.memory", memory)
             .config("spark.sql.shuffle.partitions", str(partitions))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    timings = {}

    # 1-2. Chargement + nettoyage + rapport qualite
    t0 = time.time()
    raw = load_raw(spark, input_path)
    cleaned = clean(spark, raw).cache()
    report = quality_report(raw, cleaned)
    timings["chargement_nettoyage_s"] = round(time.time() - t0, 1)
    print("\n=== QUALITE DES DONNEES ===")
    print(json.dumps(report, indent=2, ensure_ascii=False))

    # 3. Parquet (ecriture puis relecture)
    t0 = time.time()
    write_parquet(cleaned, parquet_path)
    events = read_parquet(spark, parquet_path)
    n_parquet = events.count()
    timings["ecriture_relecture_parquet_s"] = round(time.time() - t0, 1)
    assert n_parquet == report["lignes_propres"], "Parquet relu != donnees nettoyees"
    report["lignes_relues_parquet"] = n_parquet
    csv_mb, pq_mb = size_mb(input_path), size_mb(parquet_path)
    report["taille_csv_mo"] = csv_mb
    report["taille_parquet_mo"] = pq_mb
    report["rapport_taille_csv_sur_parquet"] = round(csv_mb / pq_mb, 2) if pq_mb else None
    print(f"Parquet OK : {n_parquet} lignes relues | CSV {csv_mb} Mo -> Parquet {pq_mb} Mo")

    # 4. Requetes Spark SQL sur les donnees Parquet
    events.createOrReplaceTempView("events")
    os.makedirs(reports_path, exist_ok=True)
    t0 = time.time()
    for q in QUERIES:
        res = spark.sql(read_sql(q))
        print(f"\n=== {q.upper()} ===")
        res.show(40, truncate=False)
        res.coalesce(1).write.mode("overwrite").option("header", True).csv(os.path.join(reports_path, q))
    timings["requetes_sql_s"] = round(time.time() - t0, 1)

    report["temps"] = timings
    with open(os.path.join(reports_path, "quality_report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print("\n=== TEMPS D'EXECUTION ===")
    print(json.dumps(timings, indent=2))
    print(f"\nResultats ecrits dans {reports_path}")
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Pipeline logs e-commerce (Spark SQL + Parquet)")
    p.add_argument("--input", required=True, help="Fichier CSV d'entree")
    p.add_argument("--parquet", default="outputs/parquet/events")
    p.add_argument("--reports", default="outputs/reports")
    p.add_argument("--memory", default="4g", help="Memoire du driver Spark (ex: 4g, 6g)")
    p.add_argument("--partitions", type=int, default=16,
                   help="Partitions de shuffle (8 pour petits fichiers, 32 pour le fichier complet)")
    a = p.parse_args()
    main(a.input, a.parquet, a.reports, a.memory, a.partitions)