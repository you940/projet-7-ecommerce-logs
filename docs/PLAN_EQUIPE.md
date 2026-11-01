# Plan d'equipe (3 personnes)

## Roles
| | Role | Fichiers principaux |
|---|---|---|
| A | Git, Docker, integration, README | Dockerfile, compose.yaml, main.py, README.md |
| B | Dataset, chargement, nettoyage, Parquet | ingest.py, clean.sql, transform.py, export_parquet.py |
| C | Spark SQL, KPI, tests, coordination rapport | sql/*.sql, tests/ |
Chacun redige le chapitre du rapport de sa partie et doit savoir expliquer tout le pipeline.

## Semaine 1 - Mise en place
- [ ] Reunion : confirmer avec le prof (duree, rapport, soutenance, langage PySpark OK ?)
- [ ] A : creer le depot GitHub, inviter B et C ; `docker compose up --build` doit marcher chez les 3
- [ ] B : telecharger 1 mois Kaggle, creer un echantillon reel, documenter colonnes/types/valeurs manquantes (docs/dataset.md)
- [ ] C : valider les definitions des 4 KPI ; lire sql/*.sql
## Semaine 2 - Pipeline
- [ ] B : adapter les regles de nettoyage au vrai dataset (noter lignes avant/apres)
- [ ] B : Parquet ecrit et relu sur echantillon reel
- [ ] C : verifier chaque KPI sur le vrai echantillon ; ajouter des requetes si besoin
- [ ] A : integrer, branches Git -> merge, 2 points d'avancement
## Semaine 3 - Execution complete et tests
- [ ] Lancer sur le fichier complet (1 mois), noter temps et memoire
- [ ] C : tests (pytest) tous verts ; captures des resultats
- [ ] A : README verifie par quelqu'un d'autre ; refaire l'execution depuis zero
- [ ] Option (si tout est fini) : comparer CSV vs Parquet avec temps REELLEMENT mesures
## Semaine 4 - Livraison
- [ ] Gel du code 3 jours avant le rendu
- [ ] Rapport final (plan ci-dessous) relu par les 3
- [ ] Diaporama + repetition de la demo (avec captures de secours)

## Plan du rapport
Intro / Ch.1 Contexte et technologies (Spark SQL, Parquet, Docker) / Ch.2 Besoins et conception
(dataset, schema, KPI, architecture) / Ch.3 Realisation (nettoyage, Parquet, requetes) /
Ch.4 Tests et resultats (tableaux, graphiques, limites) / Conclusion et perspectives.

## Plan de la soutenance (10-15 min)
Contexte 1' - Dataset 1-2' - Architecture 2' - Pipeline 3' - Demo et resultats 3-4' - Conclusion 1'.

## Workflow Git
`main` = version stable. Branches : `feature-docker`, `feature-transform`, `feature-sql`.
Pull request relue par un autre membre avant merge.
