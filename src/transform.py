"""Etape 2 - Validation, nettoyage et transformations (SQL + DataFrame)."""
import os

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sql")


def read_sql(name):
    with open(os.path.join(SQL_DIR, f"{name}.sql"), encoding="utf-8") as f:
        return f.read()


def clean(spark, raw_df):
    """Applique les regles de sql/clean.sql puis supprime les doublons exacts."""
    raw_df.createOrReplaceTempView("raw_events")
    cleaned = spark.sql(read_sql("clean"))
    return cleaned.dropDuplicates()


def quality_report(raw_df, clean_df):
    """Compte les lignes avant / apres nettoyage (a documenter dans le rapport)."""
    n_raw, n_clean = raw_df.count(), clean_df.count()
    return {"lignes_brutes": n_raw, "lignes_propres": n_clean, "lignes_supprimees": n_raw - n_clean}
