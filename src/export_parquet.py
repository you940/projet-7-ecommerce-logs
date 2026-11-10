"""Étape 3 - Écriture et relecture du Parquet (Snappy, partitionné par dt)."""


def write_parquet(df, path, partition_col="dt"):
    """Parquet compressé Snappy, partitionné physiquement par date (dt=YYYY-MM-DD)."""
    (df.write.mode("overwrite")
       .option("compression", "snappy")
       .partitionBy(partition_col)
       .parquet(path))


def read_parquet(spark, path):
    """Relit le Parquet : preuve que l'écriture est valide."""
    return spark.read.parquet(path)