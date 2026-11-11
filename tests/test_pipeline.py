"""Tests du pipeline : nettoyage, qualité, Parquet, Window Functions et KPI."""
import os

from ingest import SCHEMA
from transform import clean, quality_report, read_sql
from export_parquet import write_parquet, read_parquet

# user_id, product_id, timestamp, action_type, session_id
ROWS = [
    ("U-1", "P-1", "2026-10-01 10:00:00", "page_view",   "S-1"),
    ("U-1", "P-1", "2026-10-01 10:00:00", "page_view",   "S-1"),   # doublon exact
    ("U-1", "P-1", "2026-10-01 10:05:00", "add_to_cart", "S-1"),
    ("U-1", "P-1", "2026-10-01 10:10:00", "purchase",    "S-1"),
    ("U-2", "P-2", "2026-10-01 11:00:00", "page_view",   "S-2"),
    ("U-2", "P-2", "2026-10-01 11:02:00", "add_to_cart", "S-2"),   # panier abandonné
    ("U-3", "P-2", "2026-10-02 09:00:00", "click",       "S-3"),
    ("U-3", "P-2", "2026-10-02 09:01:00", "hack",        "S-3"),   # type invalide
    (None,  "P-2", "2026-10-02 09:02:00", "click",       "S-3"),   # user manquant
    ("U-3", "P-2", "pas-une-date",        "click",       "S-3"),   # timestamp invalide
]


def raw(spark):
    return spark.createDataFrame(ROWS, SCHEMA)


def build(spark):
    """Nettoyage + vues events, events_enriched, sessions (comme dans l'ETL)."""
    cleaned = clean(spark, raw(spark))
    cleaned.createOrReplaceTempView("events")
    spark.sql(read_sql("events_enriched")).createOrReplaceTempView("events_enriched")
    spark.sql(read_sql("session_metrics")).createOrReplaceTempView("sessions")
    return cleaned


def test_clean_row_count(spark):
    # 10 lignes - 1 doublon - 1 type invalide - 1 user manquant - 1 date invalide = 6
    assert clean(spark, raw(spark)).count() == 6


def test_clean_derives_dt_and_event_time(spark):
    df = clean(spark, raw(spark))
    assert {r["dt"] for r in df.select("dt").distinct().collect()} == {"2026-10-01", "2026-10-02"}
    assert df.filter("event_time IS NULL").count() == 0


def test_no_invalid_action_types_remain(spark):
    types = {r[0] for r in clean(spark, raw(spark)).select("action_type").distinct().collect()}
    assert types <= {"page_view", "click", "add_to_cart", "remove_from_cart", "purchase"}


def test_quality_report(spark):
    r = quality_report(raw(spark), clean(spark, raw(spark)))
    assert r["lignes_brutes"] == 10
    assert r["valeurs_manquantes"] == 1
    assert r["timestamps_invalides"] == 1
    assert r["types_action_invalides"] == 1
    assert r["lignes_rejetees"] == 3
    assert r["doublons_supprimes"] == 1
    assert r["lignes_propres"] == 6


def test_parquet_roundtrip_partitioned_snappy(spark, tmp_path):
    df = clean(spark, raw(spark))
    path = str(tmp_path / "events")
    write_parquet(df, path)
    assert {"dt=2026-10-01", "dt=2026-10-02"} <= set(os.listdir(path))
    files = [f for _, _, fs in os.walk(path) for f in fs if f.endswith(".parquet")]
    assert files and all(f.endswith(".snappy.parquet") for f in files)
    back = read_parquet(spark, path)
    assert back.count() == df.count()
    assert set(df.columns) == set(back.columns)


def test_window_rank_and_inter_action_time(spark):
    build(spark)
    rows = (spark.table("events_enriched").filter("session_id = 'S-1'")
            .orderBy("event_rank").collect())
    assert [r["event_rank"] for r in rows] == [1, 2, 3]
    assert [r["secs_since_prev"] for r in rows] == [None, 300, 300]


def test_session_metrics(spark):
    build(spark)
    s = {r["session_id"]: r for r in spark.table("sessions").collect()}
    assert s["S-1"]["duration_s"] == 600 and s["S-1"]["nb_events"] == 3
    assert (s["S-1"]["has_cart"], s["S-1"]["has_purchase"]) == (1, 1)
    assert s["S-2"]["duration_s"] == 120
    assert (s["S-2"]["has_cart"], s["S-2"]["has_purchase"]) == (1, 0)


def test_cart_abandonment_and_conversion(spark):
    build(spark)
    r = spark.sql(read_sql("cart_abandonment")).collect()[0]
    assert r["total_sessions"] == 3
    assert r["sessions_avec_panier"] == 2
    assert r["paniers_abandonnes"] == 1
    assert float(r["taux_abandon_pct"]) == 50.0
    assert float(r["taux_conversion_pct"]) == 33.33


def test_clicks_per_product(spark):
    build(spark)
    rows = {r["product_id"]: r for r in spark.sql(read_sql("clicks_per_product")).collect()}
    assert rows["P-2"]["nb_clics"] == 1 and rows["P-2"]["nb_ajouts_panier"] == 1
    assert rows["P-1"]["nb_clics"] == 0 and rows["P-1"]["nb_achats"] == 1