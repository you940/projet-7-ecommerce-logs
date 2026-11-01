"""Etape 3 - Ecriture et relecture du format Parquet."""


def write_parquet(df, path, partition_col="event_date"):
    """Ecrit en Parquet, partitionne par date (accelere les filtres par jour)."""
    df.write.mode("overwrite").partitionBy(partition_col).parquet(path)


def read_parquet(spark, path):
    """Relit le Parquet : sert de preuve que l'ecriture est valide."""
    return spark.read.parquet(path)
