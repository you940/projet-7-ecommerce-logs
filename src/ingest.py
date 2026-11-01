"""Etape 1 - Chargement des logs bruts (CSV) avec un schema explicite."""
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField, StructType)

SCHEMA = StructType([
    StructField("event_time", StringType()),    # converti en timestamp dans la transformation
    StructField("event_type", StringType()),
    StructField("product_id", LongType()),
    StructField("category_id", LongType()),
    StructField("category_code", StringType()),
    StructField("brand", StringType()),
    StructField("price", DoubleType()),
    StructField("user_id", LongType()),
    StructField("user_session", StringType()),
])


def load_raw(spark, path):
    """Lit le CSV avec le schema impose (pas d'inferSchema : plus rapide et plus sur)."""
    return (spark.read.option("header", True)
            .option("nullValue", "")
            .schema(SCHEMA)
            .csv(path))
