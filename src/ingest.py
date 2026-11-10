"""Étape 1 - Chargement des logs bruts (JSON) avec un schéma explicite."""
from pyspark.sql.types import StringType, StructField, StructType

COLUMNS = ["user_id", "product_id", "timestamp", "action_type", "session_id"]
SCHEMA = StructType([StructField(c, StringType()) for c in COLUMNS])


def load_raw(spark, path):
    """Lit le JSON (1 objet par ligne) avec le schéma imposé."""
    return spark.read.schema(SCHEMA).json(path)