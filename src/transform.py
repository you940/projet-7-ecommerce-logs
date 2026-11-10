"""Etape 2 - Validation, nettoyage et transformations (SQL + DataFrame)."""
import os

from pyspark.sql import functions as F

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sql")
VALID_EVENTS = ["view", "cart", "remove_from_cart", "purchase"]


def read_sql(name):
    with open(os.path.join(SQL_DIR, f"{name}.sql"), encoding="utf-8") as f:
        return f.read()


def clean(spark, raw_df):
    """Applique les regles de sql/clean.sql puis supprime les doublons exacts."""
    raw_df.createOrReplaceTempView("raw_events")
    cleaned = spark.sql(read_sql("clean"))
    return cleaned.dropDuplicates()


def quality_report(raw_df, clean_df):
    """Rapport qualite : lignes avant/apres ET detail des suppressions par regle.

    Les compteurs par regle peuvent se chevaucher (une ligne peut enfreindre
    plusieurs regles) ; 'lignes_rejetees_par_regles' compte chaque ligne une seule fois.
    """
    missing = (F.col("event_time").isNull() | F.col("product_id").isNull()
               | F.col("user_id").isNull() | F.col("user_session").isNull())
    bad_type = F.col("event_type").isNull() | (~F.col("event_type").isin(VALID_EVENTS))
    neg_price = F.col("price") < 0

    def count_if(cond):
        return F.sum(F.when(cond, 1).otherwise(0))

    row = raw_df.agg(
        F.count(F.lit(1)).alias("total"),
        count_if(missing).alias("missing"),
        count_if(bad_type).alias("bad_type"),
        count_if(neg_price).alias("neg_price"),
        count_if(missing | bad_type | neg_price).alias("rejected"),
    ).collect()[0]

    total = int(row["total"] or 0)
    rejected = int(row["rejected"] or 0)
    n_clean = clean_df.count()
    return {
        "lignes_brutes": total,
        "lignes_propres": n_clean,
        "lignes_supprimees": total - n_clean,
        "detail_suppression": {
            "champ_obligatoire_manquant": int(row["missing"] or 0),
            "type_evenement_invalide": int(row["bad_type"] or 0),
            "prix_negatif": int(row["neg_price"] or 0),
            "lignes_rejetees_par_regles": rejected,
            "doublons_exacts": total - rejected - n_clean,
        },
    }