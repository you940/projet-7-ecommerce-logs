"""Tests du pipeline : nettoyage, Parquet (aller-retour) et KPI sur un jeu minimal connu."""
from ingest import SCHEMA
from transform import clean, read_sql
from export_parquet import write_parquet, read_parquet

COLS = [f.name for f in SCHEMA.fields]

ROWS = [
    # event_time, type, product, category_id, category_code, brand, price, user, session
    ("2019-10-01 10:00:00 UTC", "view",     1, 10, "electronics.phone", "apple", 100.0, 1, "s1"),
    ("2019-10-01 10:00:00 UTC", "view",     1, 10, "electronics.phone", "apple", 100.0, 1, "s1"),  # doublon exact
    ("2019-10-01 10:05:00 UTC", "cart",     1, 10, "electronics.phone", "apple", 100.0, 1, "s1"),
    ("2019-10-01 10:10:00 UTC", "purchase", 1, 10, "electronics.phone", "apple", 100.0, 1, "s1"),
    ("2019-10-01 11:00:00 UTC", "view",     2, 11, None, None, 50.0, 2, "s2"),                   # brand/cat manquants
    ("2019-10-02 09:00:00 UTC", "view",     2, 11, None, None, 50.0, 3, "s3"),
    ("2019-10-02 09:01:00 UTC", "hack",     2, 11, None, None, 50.0, 3, "s3"),                   # type invalide
    ("2019-10-02 09:02:00 UTC", "view",     2, 11, None, None, -5.0, 3, "s3"),                   # prix negatif
    ("2019-10-02 09:03:00 UTC", "view",     2, 11, None, None, 50.0, None, "s3"),                # user manquant
]


def make_clean(spark):
    return clean(spark, spark.createDataFrame(ROWS, COLS))


def test_clean_row_count(spark):
    # 9 lignes - 1 doublon - 1 type invalide - 1 prix negatif - 1 user manquant = 5
    assert make_clean(spark).count() == 5


def test_clean_fills_unknown_and_derives_columns(spark):
    df = make_clean(spark)
    row = df.filter("product_id = 2 AND event_date = '2019-10-01'").collect()[0]
    assert row["brand"] == "unknown"
    assert row["category_code"] == "unknown"
    assert row["event_hour"] == 11
    top = df.filter("product_id = 1").collect()[0]
    assert top["category_main"] == "electronics"


def test_no_invalid_event_types_remain(spark):
    types = {r[0] for r in make_clean(spark).select("event_type").distinct().collect()}
    assert types <= {"view", "cart", "remove_from_cart", "purchase"}


def test_parquet_roundtrip(spark, tmp_path):
    df = make_clean(spark)
    path = str(tmp_path / "events")
    write_parquet(df, path)
    back = read_parquet(spark, path)
    assert back.count() == df.count()
    assert set(df.columns) == set(back.columns)


def test_kpi_queries_on_known_data(spark):
    make_clean(spark).createOrReplaceTempView("events")

    # activite : 4 evenements le 1er (view, cart, purchase, view p2), 1 le 2
    act = {str(r["event_date"]): r["nb_events"] for r in spark.sql(read_sql("activity")).collect()}
    assert act == {"2019-10-01": 4, "2019-10-02": 1}

    # engagement : 2 utilisateurs et 2 sessions le 1er
    eng = {str(r["event_date"]): (r["active_users"], r["sessions"])
           for r in spark.sql(read_sql("engagement")).collect()}
    assert eng["2019-10-01"] == (2, 2)

    # entonnoir : 3 sessions avec vue, 1 avec panier, 1 avec achat
    f = spark.sql(read_sql("purchase_funnel")).collect()[0]
    assert (f["sessions_view"], f["sessions_cart"], f["sessions_purchase"]) == (3, 1, 1)
    assert float(f["pct_view_to_purchase"]) == 33.33

    # popularite : le produit 1 a 1 vue (doublon retire), le produit 2 a 2 vues
    pop = spark.sql(read_sql("popularity")).collect()
    assert pop[0]["product_id"] == 2 and pop[0]["nb_views"] == 2
