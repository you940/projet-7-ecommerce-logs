"""Démonstration du partition pruning sur le Parquet partitionné par dt."""
import os
import time

from pyspark.sql import SparkSession

PATH = "/opt/outputs/parquet/events"
DAY = "2026-11-05"

spark = SparkSession.builder.appName("partition_pruning_demo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

partitions = sorted(d for d in os.listdir(PATH) if d.startswith("dt="))
print(f"\nNombre de partitions physiques : {len(partitions)}")
print(f"Première : {partitions[0]}  |  Dernière : {partitions[-1]}")

df = spark.read.parquet(PATH)

print(f"\n=== PLAN AVEC FILTRE SUR dt = '{DAY}' (partition pruning) ===")
df.filter(f"dt = '{DAY}'").explain()

print("\n=== PLAN AVEC FILTRE SUR action_type (aucun pruning) ===")
df.filter("action_type = 'purchase'").explain()


def timed(label, d):
    d.count()                      # 1er passage : chauffe
    t0 = time.time()
    n = d.count()
    print(f"{label} : {n} lignes en {time.time() - t0:.2f} s")


print("\n=== CHRONOMETRAGE ===")
timed(f"Avec pruning    (dt = '{DAY}')", df.filter(f"dt = '{DAY}'"))
timed(f"Sans pruning    (to_date(event_time) = '{DAY}')",
      df.filter(f"to_date(event_time) = '{DAY}'"))

spark.stop()