# Pipeline d'analyse des logs de comportement e-commerce
**Spark SQL + Parquet + Docker** - Groupe Gr8

## Objectif
Charger des logs e-commerce (CSV), les valider et nettoyer, les stocker en **Parquet**,
puis calculer des KPI avec **Spark SQL**. Tout s'execute dans **Docker**.

## Architecture
```
CSV brut --> ingest.py (schema) --> sql/clean.sql (nettoyage) --> Parquet (partitionne par date)
        --> relecture Parquet --> sql/*.sql (KPI) --> outputs/reports/
```
Langage : PySpark (Python 3.11) pour piloter Spark ; les analyses sont ecrites en Spark SQL pur.

## Versions
Python 3.11, PySpark / Spark 3.5.1, Java 17 (dans l'image Docker), Docker Compose v2.

## Prerequis
Docker Desktop installe et demarre.

## Lancement rapide (echantillon fourni, synthetique)
```bash
docker compose up --build
```
Resultats : `outputs/parquet/events/` (donnees nettoyees) et `outputs/reports/` (KPI + `quality_report.json`).

> `data/sample/sample.csv` est un echantillon **synthetique** (genere par `scripts/make_sample.py`)
> uniquement pour tester. Les resultats du rapport doivent venir du vrai dataset.

## Utiliser le vrai dataset
1. Telecharger sur Kaggle *eCommerce behavior data from multi-category store* (REES46) :
   https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store
2. Placer un mois (ex. `2019-Oct.csv`) dans `data/raw/`.
3. Lancer :
```bash
docker compose run --rm spark python src/main.py --input data/raw/2019-Oct.csv --memory 6g
```
Pour un echantillon reel : `head -n 500001 data/raw/2019-Oct.csv > data/sample/real_sample.csv`

## Tests
```bash
docker compose run --rm spark pytest -v
```

## Structure
| Chemin | Role |
|---|---|
| `src/ingest.py` | lecture CSV avec schema explicite |
| `sql/clean.sql` | regles de nettoyage et colonnes derivees (Spark SQL) |
| `src/transform.py` | applique le nettoyage + rapport qualite (lignes avant/apres) |
| `src/export_parquet.py` | ecriture / relecture Parquet |
| `src/main.py` | orchestration du pipeline |
| `sql/*.sql` | KPI : activite, popularite, engagement, entonnoir d'achat |
| `tests/` | tests de nettoyage, Parquet et KPI |
| `outputs/` | Parquet et resultats (non versionnes) |

## Definitions des KPI
- **Utilisateur actif** : `user_id` distinct sur la periode.
- **Session** : `user_session` distincte.
- **Entonnoir** : sessions contenant au moins un evenement view / cart / purchase.
  Un evenement `purchase` n'est pas forcement une commande.
- **Taux view -> purchase** : sessions_purchase / sessions_view.

## Limites
Un seul mois de donnees ; pas de table commandes ; pas de fuseau horaire explicite (UTC suppose).
